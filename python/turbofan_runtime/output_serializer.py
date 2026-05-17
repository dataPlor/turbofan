"""Serialize step output to S3 + return the value Wrapper writes to stdout.

Mirrors lib/turbofan/runtime/output_serializer.rb branching:

- Non-fan_out (no `array_index`): delegate to `Payload.serialize`
  (writes to `{exec}/{step}/output.json`, returns envelope JSON for
  stdout — see Payload.serialize docstring for the envelope-on-stdout
  rationale).
- Fan_out: per-shard write to `{exec}/{step}/output/[size/][parent{N}/]{idx}.json`.
  Returns raw JSON (fan_out outputs are not consumed via stdout — the
  downstream `FanOut.collect_outputs` reads them via `get_object`).

Both legs wrap the S3 put in `Retryable.call(..., max_retry_seconds=None)`
because step output is a terminal write — losing the put to a budget
abort would silently fail the downstream step rather than fail this
one loudly.
"""

import os

from . import fan_out
from .payload import Payload, _serialize_json
from .retryable import Retryable


class OutputSerializer:
    @staticmethod
    def call(result, context):
        bucket = os.environ.get("TURBOFAN_BUCKET", "turbofan-data")

        if context.array_index is not None:
            step_name = os.environ["TURBOFAN_STEP_NAME"]
            parent_index = os.environ.get("TURBOFAN_PARENT_INDEX") or None

            if context.size and parent_index:
                segment = f"{context.size}/parent{parent_index}/"
            elif context.size:
                segment = f"{context.size}/"
            elif parent_index:
                segment = f"parent{parent_index}/"
            else:
                segment = ""

            key = fan_out.s3_key(
                context.execution_id,
                step_name,
                "output",
                f"{segment}{context.array_index}.json",
            )
            body = _serialize_json(result)
            # NOTE: `metrics=context.metrics` is intentionally omitted
            # here until the Wrapper passes a context with metrics already
            # built (Metrics module imports lazily on first access; in
            # tests that don't exercise the full Wrapper we'd trigger
            # an import for nothing). Logger is cheap to construct.
            Retryable.call(
                lambda: context.s3.put_object(Bucket=bucket, Key=key, Body=body),
                max_retry_seconds=None,
                logger=context.logger,
            )
            return body

        return Payload.serialize(
            result,
            s3_client=context.s3,
            bucket=bucket,
            execution_id=context.execution_id,
            step_name=context.step_name,
        )
