import pytest

from turbofan_runtime.context import Context


def _make(**overrides):
    defaults = dict(
        execution_id="e",
        attempt_number=1,
        step_name="s",
        stage="dev",
        pipeline_name="p",
        array_index=None,
        storage_path=None,
    )
    defaults.update(overrides)
    return Context(**defaults)


class TestBuild:
    def test_reads_env_vars(self, monkeypatch):
        monkeypatch.setenv("TURBOFAN_EXECUTION_ID", "exec-abc")
        monkeypatch.setenv("TURBOFAN_STEP_NAME", "ingest")
        monkeypatch.setenv("TURBOFAN_STAGE", "production")
        monkeypatch.setenv("TURBOFAN_PIPELINE", "my_pipe")
        monkeypatch.setenv("AWS_BATCH_JOB_ARRAY_INDEX", "7")
        monkeypatch.setenv("AWS_BATCH_JOB_ATTEMPT", "2")
        monkeypatch.setenv("TURBOFAN_SIZE", "small")

        ctx = Context.build(storage_path="/tmp/x")
        assert ctx.execution_id == "exec-abc"
        assert ctx.step_name == "ingest"
        assert ctx.stage == "production"
        assert ctx.pipeline_name == "my_pipe"
        assert ctx.array_index == 7
        assert ctx.attempt_number == 2
        assert ctx.size == "small"
        assert ctx.storage_path == "/tmp/x"

    def test_defaults_when_env_absent(self):
        # autouse isolate fixture already stripped all TURBOFAN_*
        ctx = Context.build(storage_path=None)
        assert ctx.execution_id.startswith("local-")
        assert ctx.step_name == "anonymous"
        assert ctx.stage == "development"
        assert ctx.pipeline_name == "unknown"
        assert ctx.array_index is None
        assert ctx.attempt_number == 1
        assert ctx.size is None

    def test_empty_string_array_index_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv("AWS_BATCH_JOB_ARRAY_INDEX", "")
        ctx = Context.build(storage_path=None)
        assert ctx.array_index is None

    def test_empty_string_attempt_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("AWS_BATCH_JOB_ATTEMPT", "")
        ctx = Context.build(storage_path=None)
        assert ctx.attempt_number == 1

    def test_empty_string_size_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv("TURBOFAN_SIZE", "")
        ctx = Context.build(storage_path=None)
        assert ctx.size is None


class TestInterrupt:
    def test_starts_false(self):
        assert _make().interrupted is False

    def test_interrupt_sets_flag(self):
        ctx = _make()
        ctx.interrupt()
        assert ctx.interrupted is True


class TestLazyLogger:
    def test_returns_same_instance_on_repeated_access(self):
        ctx = _make()
        assert ctx.logger is ctx.logger

    def test_logger_carries_context_identity(self):
        import io
        import json

        ctx = _make(execution_id="EX", step_name="STEP", stage="ST",
                    pipeline_name="PP", array_index=2)
        # Replace the lazily-constructed logger's output with a buffer
        # we can inspect.
        buf = io.StringIO()
        ctx._logger = None  # force re-init through __init__ guard
        from turbofan_runtime.logger import Logger

        ctx._logger = Logger(
            execution_id=ctx.execution_id,
            step_name=ctx.step_name,
            stage=ctx.stage,
            pipeline_name=ctx.pipeline_name,
            array_index=ctx.array_index,
            output=buf,
        )
        ctx.logger.info("hello")
        entry = json.loads(buf.getvalue().strip())
        assert entry["execution_id"] == "EX"
        assert entry["step"] == "STEP"
        assert entry["array_index"] == 2


class TestLazyBoto3Clients:
    def test_s3_returns_cached_client(self):
        ctx = _make()
        a = ctx.s3
        b = ctx.s3
        assert a is b

    def test_secrets_returns_cached_client(self):
        ctx = _make()
        a = ctx.secrets_client
        b = ctx.secrets_client
        assert a is b

    def test_s3_has_no_sdk_retries(self):
        # standard mode + total_max_attempts=1 means "1 attempt total,
        # no retries" — Turbofan's Retryable owns all retry decisions.
        ctx = _make()
        assert ctx.s3.meta.config.retries["mode"] == "standard"
        assert ctx.s3.meta.config.retries["total_max_attempts"] == 1

    def test_s3_has_short_read_timeout(self):
        # SIGTERM-during-syscall mitigation: bounded read_timeout means
        # a blocked socket unblocks within ~30s, giving the wrapper a
        # checkpoint to observe the interrupt flag.
        ctx = _make()
        assert ctx.s3.meta.config.read_timeout == 30
        assert ctx.s3.meta.config.connect_timeout == 10
