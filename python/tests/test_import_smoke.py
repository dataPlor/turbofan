"""Smoke test: public API importable and exception hierarchy intact."""


def test_error_classes_importable_and_subclass_correctly():
    from turbofan_runtime import (
        HydrationError,
        Interrupted,
        RetryBudgetExhausted,
        SchemaValidationError,
        TurbofanError,
    )

    assert issubclass(Interrupted, TurbofanError)
    assert issubclass(SchemaValidationError, TurbofanError)
    assert issubclass(HydrationError, TurbofanError)
    assert issubclass(RetryBudgetExhausted, TurbofanError)


def test_version_present():
    import turbofan_runtime

    assert turbofan_runtime.__version__ == "0.8.0"


def test_retry_budget_exhausted_includes_context():
    from turbofan_runtime import RetryBudgetExhausted

    err = RetryBudgetExhausted(
        elapsed_seconds=12.5,
        budget_seconds=10.0,
        last_error=RuntimeError("upstream down"),
    )
    msg = str(err)
    assert "12.50s" in msg
    assert "10.00s" in msg
    assert "RuntimeError" in msg
    assert "upstream down" in msg
    assert err.elapsed_seconds == 12.5
    assert err.budget_seconds == 10.0
