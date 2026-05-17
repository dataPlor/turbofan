"""CloudWatch metrics emission — batched, thread-safe, retried.

Mirrors lib/turbofan/runtime/metrics.rb. Namespace `Turbofan/{pipeline}`,
dimensions `Pipeline`/`Stage`/`Step` (+ optional `Size`). Per-emit
appends to a mutex-protected pending list; flush() drains in batches
of 100 (the CloudWatch PutMetricData payload limit is 1000 / 1 MB but
100 keeps individual payloads small and limits per-call blast radius
on partial failures).

flush() is wrapped in `Retryable.call(..., max_retry_seconds=None)`
because it's typically called from the wrapper's `finally` block
during a SIGTERM unwind — if the global retry budget aborted the
flush, we'd lose the very telemetry that records the failure. Same
rationale as `OutputSerializer` and `Payload.serialize`.
"""

import sys
import threading

import boto3
from botocore.config import Config

from .retryable import Retryable


class Metrics:
    BATCH_SIZE = 100  # mirrors Ruby Metrics::BATCH_SIZE

    def __init__(
        self,
        *,
        pipeline_name,
        stage,
        step_name,
        size=None,
        cloudwatch_client=None,
    ):
        self._pipeline_name = pipeline_name
        self._stage = stage
        self._step_name = step_name
        self._size = size
        self._cw = cloudwatch_client
        self._pending = []
        self._lock = threading.Lock()

    def emit(self, name, value, unit=None):
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise TypeError(
                f"metric value must be numeric, got {type(value).__name__}"
            )
        entry = {"name": name, "value": value}
        if unit is not None:
            entry["unit"] = unit
        with self._lock:
            self._pending.append(entry)

    def flush(self):
        """Drain pending in batches; warn on failure, leave remainder for retry.

        Each batch is removed from `_pending` only after its
        put_metric_data succeeds. On exception, log a warning and
        return — a subsequent flush() (if the process lives long
        enough) gets another chance.
        """
        while True:
            with self._lock:
                if not self._pending:
                    return
                batch = self._pending[: self.BATCH_SIZE]

            try:
                Retryable.call(
                    lambda b=batch: self._client().put_metric_data(
                        Namespace=f"Turbofan/{self._pipeline_name}",
                        MetricData=[self._datum(e) for e in b],
                    ),
                    max_retry_seconds=None,
                )
            except Exception as exc:
                with self._lock:
                    remaining = len(self._pending)
                sys.stderr.write(
                    f"[turbofan_runtime] WARNING: failed to flush "
                    f"{remaining} metrics: {type(exc).__name__}: {exc}\n"
                )
                return

            with self._lock:
                self._pending = self._pending[len(batch):]

    def _client(self):
        if self._cw is not None:
            return self._cw
        with self._lock:
            if self._cw is None:
                # Disable SDK retries — Retryable owns the policy.
                self._cw = boto3.client(
                    "cloudwatch",
                    config=Config(
                        retries={"mode": "standard", "total_max_attempts": 1},
                        connect_timeout=10,
                        read_timeout=30,
                    ),
                )
            return self._cw

    def _datum(self, entry):
        datum = {
            "MetricName": entry["name"],
            "Value": entry["value"],
            "Dimensions": self._dimensions(),
        }
        if "unit" in entry:
            datum["Unit"] = entry["unit"]
        return datum

    def _dimensions(self):
        dims = [
            {"Name": "Pipeline", "Value": self._pipeline_name},
            {"Name": "Stage", "Value": self._stage},
            {"Name": "Step", "Value": self._step_name},
        ]
        if self._size:
            dims.append({"Name": "Size", "Value": str(self._size)})
        return dims
