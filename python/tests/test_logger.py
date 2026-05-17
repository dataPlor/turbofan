import io
import json
import re

from turbofan_runtime.logger import Logger


def test_writes_json_with_required_fields():
    stream = io.StringIO()
    log = Logger(
        execution_id="exec-1",
        step_name="ingest",
        stage="staging",
        pipeline_name="my_pipe",
        array_index=3,
        output=stream,
    )

    log.info("hello world", row_count=42)

    entry = json.loads(stream.getvalue().strip())
    assert entry["level"] == "info"
    assert entry["message"] == "hello world"
    assert entry["execution_id"] == "exec-1"
    assert entry["step"] == "ingest"
    assert entry["stage"] == "staging"
    assert entry["pipeline"] == "my_pipe"
    assert entry["array_index"] == 3
    assert entry["row_count"] == 42


def test_timestamp_matches_ruby_iso8601_format():
    # Ruby Time.now.utc.iso8601 → "2026-05-17T12:00:00Z" (no microseconds,
    # Z suffix, no offset). Python's default isoformat() does NOT match;
    # the implementation must use a custom format string.
    stream = io.StringIO()
    log = Logger(
        execution_id="e",
        step_name="s",
        stage="d",
        pipeline_name="p",
        output=stream,
    )
    log.info("x")
    entry = json.loads(stream.getvalue().strip())
    assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", entry["timestamp"]), (
        f"timestamp {entry['timestamp']!r} does not match Ruby iso8601 format"
    )


def test_omits_array_index_when_none():
    stream = io.StringIO()
    log = Logger(
        execution_id="e",
        step_name="s",
        stage="d",
        pipeline_name="p",
        array_index=None,
        output=stream,
    )
    log.error("boom", error_class="RuntimeError")
    entry = json.loads(stream.getvalue().strip())
    assert "array_index" not in entry
    assert entry["error_class"] == "RuntimeError"


def test_all_levels_route_correctly():
    stream = io.StringIO()
    log = Logger(
        execution_id="e",
        step_name="s",
        stage="d",
        pipeline_name="p",
        output=stream,
    )
    for level in Logger.LEVELS:
        getattr(log, level)(f"{level}-msg")

    lines = [json.loads(line) for line in stream.getvalue().strip().splitlines()]
    assert [e["level"] for e in lines] == list(Logger.LEVELS)


def test_each_line_flushes_immediately():
    class FlushTracker(io.StringIO):
        def __init__(self):
            super().__init__()
            self.flush_count = 0

        def flush(self):
            super().flush()
            self.flush_count += 1

    stream = FlushTracker()
    log = Logger(
        execution_id="e",
        step_name="s",
        stage="d",
        pipeline_name="p",
        output=stream,
    )
    log.info("a")
    log.info("b")
    assert stream.flush_count == 2
