"""S3 key construction + fan_out input reader.

Mirrors lib/turbofan/runtime/fan_out.rb (subset — v1 only needs
`s3_key` and `read_input`; output writing happens in
`OutputSerializer`, and reading multi-shard outputs from a previous
step is deferred to v2 along with `TURBOFAN_PREV_STEP[S]`).

S3 key shape for fan_out input (mirrors Ruby `FanOut.read_input`):
    {prefix?/}{exec_id}/{step_name}/input/[{chunk}/][parent{N}/]items.json
"""

import json
import os

from .retryable import Retryable


def s3_key(*parts):
    """Build an S3 key from path parts, prepending TURBOFAN_BUCKET_PREFIX."""
    prefix = os.environ.get("TURBOFAN_BUCKET_PREFIX") or ""
    key = "/".join(str(p) for p in parts)
    return f"{prefix}/{key}" if prefix else key


def read_input(
    *,
    array_index,
    s3_client,
    bucket,
    execution_id,
    step_name,
    chunk=None,
    parent_index=None,
):
    """Read a fan_out input items.json from S3 and return items[array_index].

    The whole items.json (a JSON array) is loaded into memory, then the
    `array_index`-th element returned. Mirrors Ruby's
    `FanOut.read_input` (fan_out.rb lines 30-43).
    """
    # Truthy-with-explicit-None rather than just truthy: Ruby treats
    # the empty string as truthy in this context, but Python treats it
    # as falsy. Use `is not None` to match Ruby semantics for env vars
    # that might be set to "" rather than unset.
    chunk_set = chunk is not None and chunk != ""
    parent_set = parent_index is not None and parent_index != ""

    if chunk_set and parent_set:
        key = s3_key(execution_id, step_name, "input", str(chunk),
                     f"parent{parent_index}", "items.json")
    elif chunk_set:
        key = s3_key(execution_id, step_name, "input", str(chunk), "items.json")
    elif parent_set:
        key = s3_key(execution_id, step_name, "input",
                     f"parent{parent_index}", "items.json")
    else:
        key = s3_key(execution_id, step_name, "input", "items.json")

    response = Retryable.call(
        lambda: s3_client.get_object(Bucket=bucket, Key=key)
    )
    items = json.loads(response["Body"].read())
    return items[array_index]
