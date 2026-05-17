"""Emit OpenLineage 2.0.2 events to stderr as JSON lines.

Mirrors lib/turbofan/runtime/lineage.rb. The Ruby version uses
`Kernel#warn` (stderr); CloudWatch Logs picks it up via the standard
container stderr scrape. We do the same here — NO S3 writes for
lineage, that was a misread in an early plan draft.

Each emitted line is shaped like a regular logger entry so CloudWatch
queries can grep `"OpenLineage event"` to filter:

    {"level":"info","message":"OpenLineage event","event":{...},"timestamp":"..."}

Known v1 limitation: `inputs` / `outputs` arrays are always empty
because the Python container has no access to the worker.rb
`uses:` / `writes_to:` declarations. Filling them requires a
deploy-side env-var contract that's not in v1 scope.
"""

import json
import sys

from .logger import _iso8601_utc_now


class Lineage:
    PRODUCER = "https://github.com/dataplor/turbofan"
    SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"

    @classmethod
    def start_event(cls, *, context, call_fn=None):
        return cls._build("START", context=context, call_fn=call_fn)

    @classmethod
    def complete_event(cls, *, context, call_fn=None):
        return cls._build("COMPLETE", context=context, call_fn=call_fn)

    @classmethod
    def fail_event(cls, *, context, error, call_fn=None):
        event = cls._build("FAIL", context=context, call_fn=call_fn)
        if error is not None:
            event["run"]["facets"] = {
                "errorMessage": f"{type(error).__name__}: {error}",
            }
        return event

    @classmethod
    def emit(cls, event, *, context, output=None):
        stream = output if output is not None else sys.stderr
        entry = {
            "level": "info",
            "message": "OpenLineage event",
            "event": event,
            "timestamp": _iso8601_utc_now(),
        }
        stream.write(json.dumps(entry) + "\n")
        stream.flush()

    @classmethod
    def _build(cls, event_type, *, context, call_fn):
        job = {"namespace": context.pipeline_name, "name": context.step_name}
        if call_fn is not None:
            job["facets"] = {
                "sourceCodeLocation": {
                    "type": "python",
                    "name": _qualified_name(call_fn),
                }
            }
        return {
            "eventType": event_type,
            "eventTime": _iso8601_utc_now(),
            "producer": cls.PRODUCER,
            "schemaURL": cls.SCHEMA_URL,
            "run": {"runId": str(context.execution_id)},
            "job": job,
            "inputs": [],
            "outputs": [],
        }


def _qualified_name(fn):
    module = getattr(fn, "__module__", "") or ""
    name = getattr(fn, "__qualname__", None) or getattr(fn, "__name__", "<callable>")
    return f"{module}.{name}" if module else name
