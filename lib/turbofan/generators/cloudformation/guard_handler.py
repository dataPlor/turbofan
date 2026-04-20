# GuardLambda — bridges EventBridge triggers to a Turbofan state machine.
#
# Two responsibilities:
#
#   1. Concurrent-execution guard: if the pipeline's state machine already
#      has a RUNNING execution, this invocation is a no-op (and the event
#      is dropped — see README "Triggers / Gotchas").
#   2. T1 input transform: `event.detail` becomes the pipeline input, with
#      provenance metadata namespaced under a single `_turbofan.event` key:
#
#        {
#          ...user detail fields...,
#          "_turbofan": {
#            "event": {
#              "source": "aws.s3",
#              "detail_type": "Object Created",
#              "time": "...",
#              "id": "...",
#              "account": "...",
#              "region": "...",
#              # only for :schedule triggers:
#              "schedule_expression": "cron(...)"
#            }
#          }
#        }
#
#      The single-namespace shape replaced flat `__event_*` keys after the
#      0.7 pre-cut review — dunder keys are Pythonic and flat keys collide
#      with user detail fields whose publishers happen to use the same
#      name.
#
# For `trigger :schedule` rules the generator sets a static Rule Input that
# mimics the EventBridge envelope (source="aws.scheduler") with the cron
# expression in detail under `_turbofan.event.schedule_expression`. T1 then
# applies identically to :event-rule and :schedule-rule invocations — one
# code path.
#
# items_s3_uri passthrough: if `detail` contains an `items_s3_uri` key it
# lands in the pipeline input as a top-level string; pipelines whose first
# step is a fan_out can use it directly.

import json
import logging
import os
import boto3
from datetime import datetime, timezone

sfn = boto3.client("stepfunctions")

STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def build_pipeline_input(event):
    """T1 input transform: envelope -> pipeline input with _turbofan.event metadata."""
    detail = event.get("detail") or {}
    incoming = detail.get("_turbofan", {}) if isinstance(detail, dict) else {}
    incoming_event = incoming.get("event", {}) if isinstance(incoming, dict) else {}

    # Preserve any upstream _turbofan fields the publisher set (e.g. forwarded
    # provenance), but the event-envelope sub-hash is ours — overwrite it.
    turbofan_ns = dict(incoming) if isinstance(incoming, dict) else {}
    turbofan_ns["event"] = {
        "source": event.get("source", ""),
        "detail_type": event.get("detail-type", ""),
        "time": event.get("time") or datetime.now(timezone.utc).isoformat(),
        "id": event.get("id", ""),
        "account": event.get("account", ""),
        "region": event.get("region", ""),
    }
    # schedule_expression comes through in detail._turbofan.event when the
    # Rule's Input override sets it (see cloudformation.rb#trigger_rule).
    if isinstance(incoming_event, dict) and incoming_event.get("schedule_expression"):
        turbofan_ns["event"]["schedule_expression"] = incoming_event["schedule_expression"]

    pipeline_input = {k: v for k, v in detail.items() if k != "_turbofan"}
    pipeline_input["_turbofan"] = turbofan_ns
    return pipeline_input


def handler(event, context):
    running = sfn.list_executions(
        stateMachineArn=STATE_MACHINE_ARN,
        statusFilter="RUNNING",
        maxResults=1,
    )
    if running["executions"]:
        detail = event.get("detail") or {}
        logger.warning(
            "turbofan.guard.drop: trigger event dropped because a RUNNING execution "
            "already exists. source=%s detail_type=%s event_id=%s items_s3_uri=%s",
            event.get("source", ""),
            event.get("detail-type", ""),
            event.get("id", ""),
            detail.get("items_s3_uri", "") if isinstance(detail, dict) else "",
        )
        return {"guarded": True, "started": False, "reason": "concurrent_execution_exists"}

    pipeline_input = build_pipeline_input(event)
    sfn.start_execution(
        stateMachineArn=STATE_MACHINE_ARN,
        input=json.dumps(pipeline_input),
    )
    return {"guarded": True, "started": True}
