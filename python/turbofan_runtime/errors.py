"""Exception hierarchy for turbofan_runtime.

Mirrors lib/turbofan/runtime/errors.rb + lib/turbofan/errors.rb on the
Ruby side. All framework-raised exceptions inherit from `TurbofanError`
so step authors can catch broadly when desired.
"""


class TurbofanError(Exception):
    """Base class for all turbofan_runtime exceptions."""


class Interrupted(TurbofanError):
    """Raised on SIGTERM-driven cooperative shutdown.

    Mirrors `Turbofan::Interrupted` in Ruby. The Wrapper re-raises this
    after logging at info level (NOT as a failure) and skips failure
    metrics + fail lineage emission. Step `main.py` is responsible for
    catching this and calling `sys.exit(143)`.
    """


class SchemaValidationError(TurbofanError):
    """Raised when input or output fails JSON Schema validation.

    Message format mirrors Ruby `Turbofan::SchemaValidationError`:
    `"Input validation failed for <step_name>: <err>, <err>..."` or
    `"Output validation failed for <step_name>: <err>, <err>..."`.
    """


class HydrationError(TurbofanError):
    """Raised when a `__turbofan_s3_ref` envelope cannot be hydrated."""


class RetryBudgetExhausted(TurbofanError):
    """Raised when Retryable's cumulative-sleep budget is exceeded.

    Distinct from "ran out of attempts" — this means we gave up on the
    wall-clock budget (typically because a Spot reclamation horizon is
    closer than the next retry's backoff window).
    """

    def __init__(self, *, elapsed_seconds, budget_seconds, last_error):
        self.elapsed_seconds = elapsed_seconds
        self.budget_seconds = budget_seconds
        self.last_error = last_error
        super().__init__(
            f"Retry budget exhausted after {elapsed_seconds:.2f}s "
            f"(budget {budget_seconds:.2f}s). Last error: "
            f"{type(last_error).__name__}: {last_error}"
        )
