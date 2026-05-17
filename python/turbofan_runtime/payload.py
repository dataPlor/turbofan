"""Payload serialize/deserialize — handles the always-S3 envelope contract.

Mirrors lib/turbofan/runtime/payload.rb. The wire format is:

    {"__turbofan_s3_ref": "s3://<bucket>/<key>"}

Anything downstream that needs the actual payload calls
`Payload.deserialize` (typically via `InputResolver`), which hydrates
from S3 transparently. Both legs use `Retryable` for transient AWS
errors.

`Payload.serialize` is implemented in ibk.5 (OutputSerializer task).
Only `deserialize` is needed here for the input path.
"""

import json
from urllib.parse import urlparse

import botocore.exceptions

from .errors import HydrationError
from .retryable import Retryable


class Payload:
    @staticmethod
    def deserialize(input_, *, s3_client):
        """Hydrate `__turbofan_s3_ref` envelopes; passthrough otherwise."""
        if not isinstance(input_, dict):
            return input_
        if "__turbofan_s3_ref" not in input_:
            return input_

        ref = input_["__turbofan_s3_ref"]
        parsed = urlparse(ref)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        try:
            response = Retryable.call(
                lambda: s3_client.get_object(Bucket=bucket, Key=key)
            )
        except botocore.exceptions.ClientError as exc:
            # NoSuchKey + AccessDenied both surface here. The downstream
            # contract (sentinel-skip) requires NoSuchKey to be visible
            # to the caller; but for envelope hydration there is no
            # sentinel-skip case — a missing key is a real failure.
            code = (exc.response.get("Error") or {}).get("Code")
            if code == "NoSuchKey":
                raise HydrationError(
                    f"Failed to hydrate from {ref}: object not found"
                ) from exc
            raise
        return json.loads(response["Body"].read())

    @staticmethod
    def serialize(result, *, s3_client, bucket, execution_id, step_name):
        """Write payload to S3, return envelope JSON for stdout.

        Always-S3 + envelope-on-stdout contract: the full payload goes
        to `s3://{bucket}/{exec}/{step}/output.json`; what's returned
        (and printed by Wrapper) is the small envelope referring to it.

        Diverges from current Ruby `Payload.serialize` which returns
        the raw payload JSON for stdout. Both forms hydrate identically
        downstream via `Payload.deserialize`, so the change is wire-
        compatible. A Ruby alignment follow-up will switch Ruby to the
        same contract.

        Wraps the S3 put in Retryable with `max_retry_seconds=None` —
        terminal write; losing this to a retry-budget abort would
        silently fail the downstream step.
        """
        from . import fan_out  # local import to avoid circular at import time

        body = _serialize_json(result)
        key = fan_out.s3_key(execution_id, step_name, "output.json")
        Retryable.call(
            lambda: s3_client.put_object(Bucket=bucket, Key=key, Body=body),
            max_retry_seconds=None,
        )
        envelope = {"__turbofan_s3_ref": f"s3://{bucket}/{key}"}
        return _serialize_json(envelope)


def _serialize_json(obj):
    """JSON-encode matching Ruby JSON.generate defaults.

    - separators=(',', ':') → no whitespace (Ruby default)
    - ensure_ascii=False → emit UTF-8 directly instead of \\uXXXX
      escapes (Ruby default; equivalence test depends on it)
    - sort_keys=False → preserve insertion order (Ruby Hash since 1.9,
      Python dict since 3.7)
    """
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False, sort_keys=False)
