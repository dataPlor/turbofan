"""turbofan_runtime — Python runtime contract for Turbofan polyglot steps.

Public API:
    from turbofan_runtime import Wrapper, Context, Interrupted, \\
        SchemaValidationError, HydrationError, RetryBudgetExhausted, \\
        Retryable

See ../PLAN-python-runtime-wrapper.md for the full design.
"""

from .errors import (
    HydrationError,
    Interrupted,
    RetryBudgetExhausted,
    SchemaValidationError,
    TurbofanError,
)

__all__ = [
    "HydrationError",
    "Interrupted",
    "RetryBudgetExhausted",
    "SchemaValidationError",
    "TurbofanError",
]

# Modules below are added in subsequent tasks (ibk-2 through ibk-8).
# They're guarded with try/except so partially-installed checkouts
# don't break `from turbofan_runtime import <errors>`.
try:
    from .context import Context  # noqa: F401  (ibk-2)
    __all__.append("Context")
except ImportError:
    pass

try:
    from .retryable import Retryable  # noqa: F401  (ibk-11)
    __all__.append("Retryable")
except ImportError:
    pass

try:
    from .wrapper import Wrapper  # noqa: F401  (ibk-8)
    __all__.append("Wrapper")
except ImportError:
    pass

__version__ = "0.8.0"
