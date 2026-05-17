import boto3
import pytest
from moto import mock_aws

from turbofan_runtime.context import Context
from turbofan_runtime.metrics import Metrics
from turbofan_runtime.step_metrics import StepMetrics


@pytest.fixture
def ctx_with_cw():
    """Build a Context with a Metrics already wired to moto CloudWatch."""
    with mock_aws():
        cw = boto3.client("cloudwatch", region_name="us-east-2")
        ctx = Context(
            execution_id="e", attempt_number=1, step_name="s",
            stage="d", pipeline_name="p", array_index=None,
            storage_path=None,
        )
        ctx._metrics = Metrics(
            pipeline_name=ctx.pipeline_name, stage=ctx.stage,
            step_name=ctx.step_name, size=ctx.size,
            cloudwatch_client=cw,
        )
        yield ctx, cw


class TestEmitSuccess:
    def test_emits_core_metrics(self, ctx_with_cw):
        ctx, cw = ctx_with_cw
        StepMetrics.emit_success(ctx, duration=2.5)
        ctx.metrics.flush()
        names = sorted(
            m["MetricName"]
            for m in cw.list_metrics(Namespace="Turbofan/p")["Metrics"]
        )
        assert "JobDuration" in names
        assert "JobSuccess" in names
        assert "PeakMemoryMB" in names
        assert "CpuUtilization" in names

    def test_skips_memory_utilization_when_env_unset(self, ctx_with_cw):
        # conftest strips TURBOFAN_ALLOCATED_RAM_MB; the metric should
        # NOT be emitted.
        ctx, cw = ctx_with_cw
        StepMetrics.emit_success(ctx, duration=1.0)
        ctx.metrics.flush()
        names = {
            m["MetricName"]
            for m in cw.list_metrics(Namespace="Turbofan/p")["Metrics"]
        }
        assert "MemoryUtilization" not in names

    def test_emits_memory_utilization_when_env_set(
        self, monkeypatch, ctx_with_cw,
    ):
        monkeypatch.setenv("TURBOFAN_ALLOCATED_RAM_MB", "2048")
        ctx, cw = ctx_with_cw
        StepMetrics.emit_success(ctx, duration=1.0)
        ctx.metrics.flush()
        names = {
            m["MetricName"]
            for m in cw.list_metrics(Namespace="Turbofan/p")["Metrics"]
        }
        assert "MemoryUtilization" in names

    def test_skips_memory_utilization_on_unparseable_env(
        self, monkeypatch, ctx_with_cw,
    ):
        monkeypatch.setenv("TURBOFAN_ALLOCATED_RAM_MB", "not-a-number")
        ctx, cw = ctx_with_cw
        StepMetrics.emit_success(ctx, duration=1.0)
        ctx.metrics.flush()
        names = {
            m["MetricName"]
            for m in cw.list_metrics(Namespace="Turbofan/p")["Metrics"]
        }
        assert "MemoryUtilization" not in names

    def test_skips_memory_utilization_on_zero_alloc(
        self, monkeypatch, ctx_with_cw,
    ):
        monkeypatch.setenv("TURBOFAN_ALLOCATED_RAM_MB", "0")
        ctx, cw = ctx_with_cw
        StepMetrics.emit_success(ctx, duration=1.0)
        ctx.metrics.flush()
        names = {
            m["MetricName"]
            for m in cw.list_metrics(Namespace="Turbofan/p")["Metrics"]
        }
        assert "MemoryUtilization" not in names


class TestEmitFailure:
    def test_emits_job_failure(self, ctx_with_cw):
        ctx, cw = ctx_with_cw
        StepMetrics.emit_failure(ctx)
        ctx.metrics.flush()
        names = {
            m["MetricName"]
            for m in cw.list_metrics(Namespace="Turbofan/p")["Metrics"]
        }
        assert names == {"JobFailure"}


class TestPeakMemory:
    def test_returns_non_negative(self):
        # The implementation has multiple fallback paths; verify SOMETHING
        # plausible comes back on this platform.
        from turbofan_runtime.step_metrics import _peak_memory_mb
        result = _peak_memory_mb()
        assert isinstance(result, float)
        assert result >= 0.0
