"""Structured JSON logger.

Mirrors lib/turbofan/runtime/logger.rb. Writes one JSON line per log
call to stdout (or a user-supplied stream). Field set matches Ruby
exactly so log search queries work uniformly across languages.

Timestamp format MUST match Ruby's `Time.now.utc.iso8601`:
`"2026-05-17T12:00:00Z"` (seconds precision, `Z` suffix). Python's
default `datetime.isoformat()` produces `"2026-05-17T12:00:00.123456+00:00"`
which is not interchangeable for log-grep purposes.
"""

import json
import sys
from datetime import datetime, timezone


def _iso8601_utc_now() -> str:
    """Match Ruby's Time.now.utc.iso8601 — seconds precision + Z suffix."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Logger:
    LEVELS = ("info", "warn", "error", "debug")

    def __init__(
        self,
        *,
        execution_id,
        step_name,
        stage,
        pipeline_name,
        array_index=None,
        output=None,
    ):
        self._output = output if output is not None else sys.stdout
        self._metadata = {
            "execution_id": execution_id,
            "step": step_name,
            "stage": stage,
            "pipeline": pipeline_name,
        }
        if array_index is not None:
            self._metadata["array_index"] = array_index

    def info(self, message, **extra):
        self._write("info", message, extra)

    def warn(self, message, **extra):
        self._write("warn", message, extra)

    def error(self, message, **extra):
        self._write("error", message, extra)

    def debug(self, message, **extra):
        self._write("debug", message, extra)

    def _write(self, level, message, extra):
        # Field order: level, message, *metadata, timestamp, *extra —
        # matches Ruby's logger.rb#write_entry exactly.
        entry = {
            "level": level,
            "message": message,
            **self._metadata,
            "timestamp": _iso8601_utc_now(),
            **extra,
        }
        self._output.write(json.dumps(entry) + "\n")
        self._output.flush()
