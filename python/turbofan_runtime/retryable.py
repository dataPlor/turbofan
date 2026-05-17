"""Retry transient AWS errors with full-jitter exponential backoff.

Mirrors lib/turbofan/retryable.rb. Design decisions documented in the
Ruby file apply here verbatim:

- Error matching by code + HTTP status, NOT pre-defined classes
  (botocore's `ClientError` is the umbrella; `EndpointConnectionError`
  and `ReadTimeoutError`/`ConnectTimeoutError` cover socket failures).
- Callers MUST configure boto3 clients with `total_max_attempts=1`
  and `mode='standard'` so the SDK's built-in retry doesn't stack on
  top of ours. `Context._build_boto3_client` does this.
- Full-jitter backoff (AWS recommendation):
  `delay = uniform(0, min(cap, base * 2^(attempt-1)))`. Prevents
  thundering herd on retry cohorts.
- `NoSuchKey` (and similar 404 semantics) must pass through unretried
  for sentinel-skip semantics. Callers rescue it OUTSIDE Retryable.

Usage:

    Retryable.call(lambda: s3.get_object(Bucket=b, Key=k))

    # Caller preserves sentinel-skip semantics:
    try:
        response = Retryable.call(lambda: s3.get_object(Bucket=b, Key=k))
    except s3.exceptions.NoSuchKey:
        # Sentinel chunk — no output written, skip
        ...
"""

import os
import random
import time

import botocore.exceptions

from .errors import RetryBudgetExhausted

# Transient AWS error codes across S3, CloudWatch, SecretsManager, etc.
# Matched case-sensitively against the boto3 ClientError code. Mirrors
# the Ruby TRANSIENT_CODES list.
TRANSIENT_CODES = frozenset({
    "Throttling",
    "ThrottlingException",
    "SlowDown",
    "ServiceUnavailable",
    "InternalError",
    "InternalFailure",
    "RequestTimeout",
    "RequestTimeoutException",
    "RequestLimitExceeded",
    "RequestThrottled",
    "RequestThrottledException",
    "LimitExceededException",
})

MAX_ATTEMPTS_LIMIT = 20

# Sentinel distinguishing "caller didn't pass max_retry_seconds" from
# "caller passed None to explicitly bypass". Used by terminal-write
# callers (Metrics.flush, OutputSerializer, Payload.serialize) that
# must not self-abort during SIGTERM.
_UNSET = object()


def _max_retry_seconds_default():
    """Read TURBOFAN_MAX_RETRY_SECONDS env once; None if unset."""
    raw = os.environ.get("TURBOFAN_MAX_RETRY_SECONDS")
    if raw is None or raw == "":
        return None
    return float(raw)


class Retryable:
    @classmethod
    def call(
        cls,
        fn,
        *,
        max=5,
        base=0.5,
        cap=30,
        max_retry_seconds=_UNSET,
        sleeper=time.sleep,
        jitter_rand=random.random,
        logger=None,
        metrics=None,
    ):
        """Execute `fn` with retry on transient errors.

        Args:
            fn: zero-arg callable to execute.
            max: total attempts allowed (1..MAX_ATTEMPTS_LIMIT). Default 5.
            base: backoff base seconds (default 0.5).
            cap: per-attempt backoff cap seconds (default 30).
            max_retry_seconds: per-call cumulative-sleep budget.
                - omitted (default) → reads TURBOFAN_MAX_RETRY_SECONDS env
                - None (explicit) → bypass budget (terminal writes)
                - float → enforce; raise RetryBudgetExhausted if exceeded
            sleeper: injectable sleep function (for tests).
            jitter_rand: injectable random [0,1) source (for tests).
            logger: object with .info(msg, **kwargs); emits per-retry log.
            metrics: object with .emit(name, value); emits RetryAttempt,
                RetriesExhausted, RetryBudgetExhausted datapoints.
        """
        if not callable(fn):
            raise TypeError("fn must be callable")
        if not (isinstance(max, int) and 1 <= max <= MAX_ATTEMPTS_LIMIT):
            raise ValueError(
                f"max must be int in 1..{MAX_ATTEMPTS_LIMIT}, got {max!r}"
            )
        if not (isinstance(base, (int, float)) and base > 0):
            raise ValueError(f"base must be > 0, got {base!r}")
        if not (isinstance(cap, (int, float)) and cap > 0):
            raise ValueError(f"cap must be > 0, got {cap!r}")

        budget = (
            _max_retry_seconds_default()
            if max_retry_seconds is _UNSET
            else max_retry_seconds
        )

        attempt = 0
        elapsed_sleep = 0.0
        while True:
            try:
                return fn()
            except Exception as exc:
                attempt += 1
                if not cls.transient(exc):
                    raise
                if attempt >= max:
                    if metrics is not None:
                        metrics.emit("RetriesExhausted", 1)
                    raise
                backoff = min(cap, base * (2 ** (attempt - 1)))
                delay = jitter_rand() * backoff
                if budget is not None and elapsed_sleep + delay > budget:
                    if metrics is not None:
                        metrics.emit("RetryBudgetExhausted", 1)
                    raise RetryBudgetExhausted(
                        elapsed_seconds=elapsed_sleep,
                        budget_seconds=budget,
                        last_error=exc,
                    )
                if logger is not None:
                    logger.info(
                        "Retryable: transient error, retrying",
                        attempt=attempt,
                        max=max,
                        error_class=type(exc).__name__,
                        code=_error_code(exc),
                        delay_ms=int(delay * 1000),
                    )
                if metrics is not None:
                    metrics.emit("RetryAttempt", 1)
                sleeper(delay)
                elapsed_sleep += delay

    @staticmethod
    def transient(exc):
        """Classify an error as worth retrying."""
        # Connection-layer errors: always transient.
        if isinstance(
            exc,
            (
                botocore.exceptions.EndpointConnectionError,
                botocore.exceptions.ConnectionClosedError,
                botocore.exceptions.ReadTimeoutError,
                botocore.exceptions.ConnectTimeoutError,
                botocore.exceptions.IncompleteReadError,
            ),
        ):
            return True
        # Service errors: check code + HTTP status.
        if isinstance(exc, botocore.exceptions.ClientError):
            code = _error_code(exc)
            if code and code in TRANSIENT_CODES:
                return True
            status = _http_status(exc)
            if status in (408, 429):
                return True
            if status is not None and 500 <= status < 600:
                return True
        return False


def _error_code(exc):
    """Pull the AWS error code from a botocore ClientError."""
    if not isinstance(exc, botocore.exceptions.ClientError):
        return None
    response = getattr(exc, "response", None) or {}
    return (response.get("Error") or {}).get("Code")


def _http_status(exc):
    """Pull HTTP status code from a botocore ClientError."""
    if not isinstance(exc, botocore.exceptions.ClientError):
        return None
    response = getattr(exc, "response", None) or {}
    metadata = response.get("ResponseMetadata") or {}
    status = metadata.get("HTTPStatusCode")
    return status if isinstance(status, int) else None
