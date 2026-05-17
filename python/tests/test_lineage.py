import io
import json
import re

from turbofan_runtime.context import Context
from turbofan_runtime.lineage import Lineage


def _ctx(**overrides):
    defaults = dict(
        execution_id="exec-1",
        attempt_number=1,
        step_name="ingest",
        stage="dev",
        pipeline_name="my_pipe",
        array_index=None,
        storage_path=None,
    )
    defaults.update(overrides)
    return Context(**defaults)


class TestEventShape:
    def test_start_event_has_all_required_fields(self):
        event = Lineage.start_event(context=_ctx())
        assert event["eventType"] == "START"
        assert event["producer"] == "https://github.com/dataplor/turbofan"
        assert event["schemaURL"] == "https://openlineage.io/spec/2-0-2/OpenLineage.json"
        assert event["run"] == {"runId": "exec-1"}
        assert event["job"]["namespace"] == "my_pipe"
        assert event["job"]["name"] == "ingest"
        assert event["inputs"] == []
        assert event["outputs"] == []
        assert "eventTime" in event

    def test_complete_event(self):
        event = Lineage.complete_event(context=_ctx())
        assert event["eventType"] == "COMPLETE"

    def test_fail_event_includes_error_facet(self):
        event = Lineage.fail_event(context=_ctx(), error=RuntimeError("boom"))
        assert event["eventType"] == "FAIL"
        assert event["run"]["facets"]["errorMessage"] == "RuntimeError: boom"

    def test_fail_event_without_error_has_no_facets(self):
        event = Lineage.fail_event(context=_ctx(), error=None)
        assert "facets" not in event["run"]

    def test_event_time_matches_iso8601_z_format(self):
        event = Lineage.start_event(context=_ctx())
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", event["eventTime"])

    def test_run_id_stringified(self):
        # Even if execution_id is an int, runId is a string per OpenLineage spec
        event = Lineage.start_event(context=_ctx(execution_id=12345))
        assert event["run"]["runId"] == "12345"


class TestSourceCodeFacet:
    def test_facet_added_when_call_fn_provided(self):
        def my_step(inputs, ctx):
            return {}

        event = Lineage.start_event(context=_ctx(), call_fn=my_step)
        facet = event["job"]["facets"]["sourceCodeLocation"]
        assert facet["type"] == "python"
        assert facet["name"].endswith("my_step")

    def test_no_facet_when_call_fn_absent(self):
        event = Lineage.start_event(context=_ctx())
        assert "facets" not in event["job"]


class TestEmit:
    def test_writes_envelope_to_stream(self):
        stream = io.StringIO()
        event = Lineage.start_event(context=_ctx())
        Lineage.emit(event, context=_ctx(), output=stream)

        line = stream.getvalue().strip()
        entry = json.loads(line)
        assert entry["level"] == "info"
        assert entry["message"] == "OpenLineage event"
        assert entry["event"] == event
        assert re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", entry["timestamp"])

    def test_emit_appends_newline(self):
        stream = io.StringIO()
        Lineage.emit(Lineage.start_event(context=_ctx()),
                     context=_ctx(), output=stream)
        Lineage.emit(Lineage.complete_event(context=_ctx()),
                     context=_ctx(), output=stream)
        assert stream.getvalue().count("\n") == 2
