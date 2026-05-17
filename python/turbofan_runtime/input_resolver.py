"""Step input resolution: env var or S3 fan_out, then envelope normalize.

Mirrors lib/turbofan/runtime/input_resolver.rb. Returns a dict with at
least an "inputs" key (array). The Wrapper extracts "inputs", stashes
other envelope keys on `context.envelope`, then handles sentinel
`[None]` chunk + schema validation.

Resolution order (matches Ruby):
1. `AWS_BATCH_JOB_ARRAY_INDEX` set → fan_out path (S3 items.json)
2. `TURBOFAN_PREV_STEPS` or `TURBOFAN_PREV_STEP` → NotImplementedError
   (v2 work; v1 Python steps either drive the input themselves via
   the State Machine trigger or are fan_out children)
3. `TURBOFAN_INPUT` env → JSON-parse, hydrate any `__turbofan_s3_ref`,
   resolve `items_s3_uri` if present
"""

import json
import os

from . import fan_out
from .payload import Payload
from .retryable import Retryable


class InputResolver:
    @classmethod
    def call(cls, context):
        raw = cls._resolve(context)
        return normalize_envelope(raw)

    @classmethod
    def _resolve(cls, context):
        if context.array_index is not None:
            bucket = os.environ.get("TURBOFAN_BUCKET", "turbofan-data")
            step_name = os.environ["TURBOFAN_STEP_NAME"]
            parent_index = os.environ.get("TURBOFAN_PARENT_INDEX")
            return fan_out.read_input(
                array_index=context.array_index,
                s3_client=context.s3,
                bucket=bucket,
                execution_id=context.execution_id,
                step_name=step_name,
                chunk=context.size,
                parent_index=parent_index,
            )

        if "TURBOFAN_PREV_STEPS" in os.environ or "TURBOFAN_PREV_STEP" in os.environ:
            raise NotImplementedError(
                "Python runtime v1 does not support TURBOFAN_PREV_STEP / "
                "TURBOFAN_PREV_STEPS input resolution. Use the Ruby runtime "
                "for steps that depend on parallel or routed prev-step "
                "outputs, or wait for the v2 Python InputResolver expansion."
            )

        raw_json = os.environ.get("TURBOFAN_INPUT", "{}")
        parsed = json.loads(raw_json)
        parsed = Payload.deserialize(parsed, s3_client=context.s3)
        if isinstance(parsed, dict) and "items_s3_uri" in parsed:
            return cls._resolve_items_s3_uri(parsed["items_s3_uri"], context)
        return parsed

    @staticmethod
    def _resolve_items_s3_uri(uri, context):
        """Fetch and JSON-parse an s3:// URI from a trigger payload.

        Mirrors `input_resolver.rb#resolve_items_s3_uri`. Bucket is
        extracted from the URI itself (NOT TURBOFAN_BUCKET) so cross-
        bucket trigger payloads work correctly. (Ruby has the same
        behavior via `uri.sub("s3://#{bucket}/", "")` which only works
        when bucket matches; the urlparse approach here is strictly
        more correct.)
        """
        from urllib.parse import urlparse

        parsed = urlparse(uri)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        response = Retryable.call(
            lambda: context.s3.get_object(Bucket=bucket, Key=key),
            logger=context.logger,
        )
        return json.loads(response["Body"].read())


def normalize_envelope(raw):
    """Coerce arbitrary input into `{"inputs": [...]}` shape.

    Mirrors input_resolver.rb#normalize_envelope:
    - Array → {"inputs": [...]}
    - Dict with "inputs" key (Array) → returned as-is
    - Dict with "items" key (Array) → rename to "inputs", preserve siblings
    - Anything else → {"inputs": [raw]}
    """
    if isinstance(raw, list):
        return {"inputs": raw}
    if isinstance(raw, dict):
        if isinstance(raw.get("inputs"), list):
            return raw
        if isinstance(raw.get("items"), list):
            result = {k: v for k, v in raw.items() if k != "items"}
            result["inputs"] = raw["items"]
            return result
    return {"inputs": [raw]}
