import io
import json

import botocore.exceptions
import pytest

from turbofan_runtime.errors import RetryBudgetExhausted
from turbofan_runtime.logger import Logger
from turbofan_runtime.retryable import (
    MAX_ATTEMPTS_LIMIT,
    Retryable,
    TRANSIENT_CODES,
)


def _client_error(code, status=500):
    return botocore.exceptions.ClientError(
        {
            "Error": {"Code": code, "Message": f"{code} happened"},
            "ResponseMetadata": {"HTTPStatusCode": status},
        },
        "TestOp",
    )


class FakeMetrics:
    def __init__(self):
        self.emitted = []

    def emit(self, name, value, unit=None):
        self.emitted.append((name, value))


class TestTransientClassification:
    @pytest.mark.parametrize("code", sorted(TRANSIENT_CODES))
    def test_transient_codes(self, code):
        assert Retryable.transient(_client_error(code, status=400)) is True

    @pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504, 599])
    def test_transient_http_status(self, status):
        assert Retryable.transient(_client_error("Other", status=status)) is True

    @pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
    def test_non_transient_http_status(self, status):
        assert Retryable.transient(_client_error("Other", status=status)) is False

    def test_endpoint_connection_error_is_transient(self):
        err = botocore.exceptions.EndpointConnectionError(endpoint_url="x")
        assert Retryable.transient(err) is True

    def test_read_timeout_is_transient(self):
        err = botocore.exceptions.ReadTimeoutError(endpoint_url="x")
        assert Retryable.transient(err) is True

    def test_non_client_error_is_not_transient(self):
        assert Retryable.transient(ValueError("nope")) is False
        assert Retryable.transient(KeyError("missing")) is False


class TestBasicCall:
    def test_returns_value_on_success(self):
        assert Retryable.call(lambda: 42) == 42

    def test_non_transient_re_raises_immediately(self):
        calls = []

        def fn():
            calls.append(1)
            raise ValueError("bad input")

        with pytest.raises(ValueError, match="bad input"):
            Retryable.call(fn, sleeper=lambda _: None)
        assert len(calls) == 1


class TestRetryBehavior:
    def test_transient_then_success(self):
        attempts = []

        def fn():
            attempts.append(1)
            if len(attempts) < 3:
                raise _client_error("Throttling", status=429)
            return "ok"

        sleeps = []
        result = Retryable.call(
            fn,
            sleeper=lambda d: sleeps.append(d),
            jitter_rand=lambda: 1.0,  # full backoff (no jitter randomness)
        )
        assert result == "ok"
        assert len(attempts) == 3
        # backoff: 0.5, 1.0 for the two retries
        assert sleeps == [0.5, 1.0]

    def test_exhausts_attempts_and_raises(self):
        attempts = []

        def fn():
            attempts.append(1)
            raise _client_error("Throttling", status=429)

        with pytest.raises(botocore.exceptions.ClientError):
            Retryable.call(fn, max=3, sleeper=lambda _: None, jitter_rand=lambda: 0.0)
        assert len(attempts) == 3

    def test_metrics_emit_on_each_retry(self):
        m = FakeMetrics()
        attempts = []

        def fn():
            attempts.append(1)
            if len(attempts) < 3:
                raise _client_error("ThrottlingException", status=429)
            return "done"

        Retryable.call(
            fn, metrics=m, sleeper=lambda _: None, jitter_rand=lambda: 0.0
        )
        retry_attempts = [n for n, _ in m.emitted if n == "RetryAttempt"]
        assert len(retry_attempts) == 2  # 2 retries before success

    def test_metrics_emit_retries_exhausted(self):
        m = FakeMetrics()
        with pytest.raises(botocore.exceptions.ClientError):
            Retryable.call(
                lambda: (_ for _ in ()).throw(_client_error("SlowDown", status=503)),
                max=2,
                metrics=m,
                sleeper=lambda _: None,
                jitter_rand=lambda: 0.0,
            )
        names = [n for n, _ in m.emitted]
        assert "RetriesExhausted" in names

    def test_logger_emits_per_retry(self):
        buf = io.StringIO()
        log = Logger(
            execution_id="e",
            step_name="s",
            stage="d",
            pipeline_name="p",
            output=buf,
        )
        attempts = []

        def fn():
            attempts.append(1)
            if len(attempts) < 2:
                raise _client_error("RequestTimeout", status=408)
            return "ok"

        Retryable.call(
            fn, logger=log, sleeper=lambda _: None, jitter_rand=lambda: 0.5
        )
        lines = [json.loads(line) for line in buf.getvalue().strip().splitlines()]
        assert len(lines) == 1
        assert lines[0]["message"] == "Retryable: transient error, retrying"
        assert lines[0]["attempt"] == 1
        assert lines[0]["code"] == "RequestTimeout"
        assert lines[0]["delay_ms"] == 250  # 0.5 * 0.5s


class TestBudget:
    def test_budget_enforced(self):
        attempts = []

        def fn():
            attempts.append(1)
            raise _client_error("Throttling", status=429)

        with pytest.raises(RetryBudgetExhausted) as excinfo:
            Retryable.call(
                fn,
                max_retry_seconds=0.4,
                sleeper=lambda _: None,
                jitter_rand=lambda: 1.0,
            )
        # first backoff = 0.5s, exceeds 0.4s budget on attempt 1
        assert excinfo.value.budget_seconds == 0.4
        assert isinstance(excinfo.value.last_error, botocore.exceptions.ClientError)
        assert len(attempts) == 1

    def test_explicit_none_bypasses_budget(self, monkeypatch):
        # Even with TURBOFAN_MAX_RETRY_SECONDS set, max_retry_seconds=None
        # explicitly bypasses it (terminal-write semantics).
        monkeypatch.setenv("TURBOFAN_MAX_RETRY_SECONDS", "0.001")
        attempts = []

        def fn():
            attempts.append(1)
            if len(attempts) < 3:
                raise _client_error("SlowDown", status=503)
            return "ok"

        result = Retryable.call(
            fn,
            max_retry_seconds=None,
            sleeper=lambda _: None,
            jitter_rand=lambda: 0.0,
        )
        assert result == "ok"

    def test_env_default_applied_when_not_passed(self, monkeypatch):
        monkeypatch.setenv("TURBOFAN_MAX_RETRY_SECONDS", "0.001")

        def fn():
            raise _client_error("Throttling", status=429)

        with pytest.raises(RetryBudgetExhausted):
            Retryable.call(fn, sleeper=lambda _: None, jitter_rand=lambda: 1.0)

    def test_budget_metric_emitted(self):
        m = FakeMetrics()
        with pytest.raises(RetryBudgetExhausted):
            Retryable.call(
                lambda: (_ for _ in ()).throw(_client_error("Throttling", status=429)),
                max_retry_seconds=0.1,
                metrics=m,
                sleeper=lambda _: None,
                jitter_rand=lambda: 1.0,
            )
        names = [n for n, _ in m.emitted]
        assert "RetryBudgetExhausted" in names


class TestValidation:
    def test_rejects_non_callable(self):
        with pytest.raises(TypeError):
            Retryable.call("not callable")

    @pytest.mark.parametrize("bad", [0, -1, MAX_ATTEMPTS_LIMIT + 1, 1.5, "5"])
    def test_rejects_invalid_max(self, bad):
        with pytest.raises(ValueError):
            Retryable.call(lambda: None, max=bad)

    def test_rejects_non_positive_base_or_cap(self):
        with pytest.raises(ValueError):
            Retryable.call(lambda: None, base=0)
        with pytest.raises(ValueError):
            Retryable.call(lambda: None, cap=-1)


class TestPublicExport:
    def test_retryable_importable_from_package(self):
        from turbofan_runtime import Retryable as Exported

        assert Exported is Retryable
