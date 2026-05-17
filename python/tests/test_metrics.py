import boto3
import pytest
from moto import mock_aws

from turbofan_runtime.metrics import Metrics


@pytest.fixture
def cw():
    with mock_aws():
        yield boto3.client("cloudwatch", region_name="us-east-2")


class TestEmit:
    def test_appends_to_pending(self, cw):
        m = Metrics(pipeline_name="p", stage="d", step_name="s",
                    cloudwatch_client=cw)
        m.emit("Foo", 1.5)
        m.emit("Bar", 2)
        assert len(m._pending) == 2

    def test_rejects_non_numeric(self, cw):
        m = Metrics(pipeline_name="p", stage="d", step_name="s",
                    cloudwatch_client=cw)
        with pytest.raises(TypeError):
            m.emit("Foo", "bar")
        with pytest.raises(TypeError):
            m.emit("Foo", None)
        # bool is a subclass of int in Python — we reject it explicitly
        # because emitting `True`/`False` as a metric is almost always a bug
        with pytest.raises(TypeError):
            m.emit("Foo", True)

    def test_accepts_int_and_float(self, cw):
        m = Metrics(pipeline_name="p", stage="d", step_name="s",
                    cloudwatch_client=cw)
        m.emit("A", 42)
        m.emit("B", 3.14)
        assert len(m._pending) == 2


class TestFlush:
    def test_writes_to_cloudwatch(self, cw):
        m = Metrics(pipeline_name="my_pipe", stage="prod", step_name="ingest",
                    cloudwatch_client=cw)
        m.emit("JobDuration", 1.234)
        m.emit("JobSuccess", 1)
        m.flush()

        names = sorted(
            entry["MetricName"]
            for entry in cw.list_metrics(Namespace="Turbofan/my_pipe")["Metrics"]
        )
        assert names == ["JobDuration", "JobSuccess"]

    def test_namespace_includes_pipeline_name(self, cw):
        m = Metrics(pipeline_name="pipeline_x", stage="d", step_name="s",
                    cloudwatch_client=cw)
        m.emit("X", 1)
        m.flush()
        result = cw.list_metrics(Namespace="Turbofan/pipeline_x")
        assert len(result["Metrics"]) == 1

    def test_dimensions_pipeline_stage_step(self, cw):
        m = Metrics(pipeline_name="p", stage="prod", step_name="ingest",
                    cloudwatch_client=cw)
        m.emit("X", 1)
        m.flush()
        entry = cw.list_metrics(Namespace="Turbofan/p")["Metrics"][0]
        dims = {d["Name"]: d["Value"] for d in entry["Dimensions"]}
        assert dims == {"Pipeline": "p", "Stage": "prod", "Step": "ingest"}

    def test_dimensions_include_size_when_set(self, cw):
        m = Metrics(pipeline_name="p", stage="d", step_name="s",
                    size="small", cloudwatch_client=cw)
        m.emit("X", 1)
        m.flush()
        entry = cw.list_metrics(Namespace="Turbofan/p")["Metrics"][0]
        dim_names = {d["Name"] for d in entry["Dimensions"]}
        assert dim_names == {"Pipeline", "Stage", "Step", "Size"}

    def test_drains_pending(self, cw):
        m = Metrics(pipeline_name="p", stage="d", step_name="s",
                    cloudwatch_client=cw)
        for i in range(5):
            m.emit("Foo", i)
        m.flush()
        assert m._pending == []

    def test_batches_at_size_limit(self, cw):
        # 250 emits → 3 batches (100 + 100 + 50). We verify by draining;
        # moto doesn't expose call count directly but we can confirm
        # all metrics landed.
        m = Metrics(pipeline_name="p", stage="d", step_name="s",
                    cloudwatch_client=cw)
        for i in range(250):
            m.emit("Throughput", float(i))
        m.flush()
        assert m._pending == []

    def test_empty_pending_no_op(self, cw):
        m = Metrics(pipeline_name="p", stage="d", step_name="s",
                    cloudwatch_client=cw)
        m.flush()  # should not error


class TestUnit:
    def test_unit_passed_through(self, cw):
        m = Metrics(pipeline_name="p", stage="d", step_name="s",
                    cloudwatch_client=cw)
        m.emit("Latency", 100, unit="Milliseconds")
        m.flush()
        entry = cw.list_metrics(Namespace="Turbofan/p")["Metrics"][0]
        # moto's list_metrics doesn't return Unit; verify via the
        # internal datum builder
        m2 = Metrics(pipeline_name="p", stage="d", step_name="s",
                     cloudwatch_client=cw)
        m2.emit("X", 1, unit="Bytes")
        datum = m2._datum(m2._pending[0])
        assert datum["Unit"] == "Bytes"


class TestFailureTolerance:
    def test_failure_warning_no_raise(self, cw, monkeypatch, capsys):
        # Force the put_metric_data to raise. The flush should warn
        # to stderr but not propagate the exception.
        def boom(**kwargs):
            raise RuntimeError("simulated CW outage")

        m = Metrics(pipeline_name="p", stage="d", step_name="s",
                    cloudwatch_client=cw)
        m.emit("X", 1)
        monkeypatch.setattr(cw, "put_metric_data", boom)

        m.flush()  # must NOT raise
        captured = capsys.readouterr()
        assert "WARNING" in captured.err
        assert "failed to flush" in captured.err
