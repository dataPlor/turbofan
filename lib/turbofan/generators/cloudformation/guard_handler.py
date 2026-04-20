# GuardLambda — bridges EventBridge triggers to a Turbofan state machine.
#
# Two responsibilities:
#
#   1. Concurrent-execution guard: if the pipeline's state machine already
#      has a RUNNING execution, this invocation is a no-op.
#   2. T1 input transform: `event.detail` becomes the pipeline input, with
#      __event_source / __event_detail_type / __event_time / __event_id /
#      __event_account / __event_region injected at the top level.
#
# For `trigger :schedule` rules the generator sets a static Rule Input that
# mimics the EventBridge envelope with `__event_schedule_expression` inside
# `detail` — T1 then lifts it to the pipeline input unchanged. For `trigger
# :event` rules EventBridge delivers the natural envelope and T1 applies
# identically.
#
# items_s3_uri passthrough: if `detail` contains an `items_s3_uri` key it
# lands in the pipeline input as a top-level string; pipelines whose first
# step is a fan_out can use it directly.

import json
import os
import boto3
from datetime import datetime, timezone

sfn = boto3.client("stepfunctions")

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]


def build_pipeline_input(event):
    """T1 input transform: envelope -> pipeline input with __event_* metadata."""
    detail = event.get("detail") or {}
    pipeline_input = dict(detail)
    pipeline_input["__event_source"] = event.get("source", "")
    pipeline_input["__event_detail_type"] = event.get("detail-type", "")
    pipeline_input["__event_time"] = event.get("time") or datetime.now(timezone.utc).isoformat()
    pipeline_input["__event_id"] = event.get("id", "")
    pipeline_input["__event_account"] = event.get("account", "")
    pipeline_input["__event_region"] = event.get("region", "")
    return pipeline_input


def handler(event, context):
    running = sfn.list_executions(
        stateMachineArn=STATE_MACHINE_ARN,
        statusFilter="RUNNING",
        maxResults=1,
    )
    if running["executions"]:
        return {"guarded": True, "started": False, "reason": "concurrent_execution_exists"}

    pipeline_input = build_pipeline_input(event)
    sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps(pipeline_input),
    )
    return {"guarded": True, "started": True}
