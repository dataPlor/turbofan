import pathlib

import pytest

from turbofan_runtime.errors import SchemaValidationError
from turbofan_runtime.schema_validator import SchemaValidator

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "schemas"


@pytest.fixture(autouse=True)
def set_schemas_path(monkeypatch):
    monkeypatch.setenv("TURBOFAN_SCHEMAS_PATH", str(FIXTURES))


class TestValidateInputPass:
    def test_valid_items_no_error(self):
        SchemaValidator.validate_input(
            step_name="SampleStep",
            schema_file="sample_input.json",
            inputs=[{"name": "alice", "count": 1}, {"name": "bob"}],
        )

    def test_empty_inputs_list_no_error(self):
        # Per-item loop with no items = no validation = no error
        SchemaValidator.validate_input(
            step_name="X", schema_file="sample_input.json", inputs=[],
        )

    def test_double_underscore_keys_stripped(self):
        # __ prefix = framework-injected envelope metadata; stripped
        # before validation so they don't trip additionalProperties.
        SchemaValidator.validate_input(
            step_name="SampleStep",
            schema_file="sample_input.json",
            inputs=[{"name": "alice", "__execution_id": "exec-1",
                     "__trace_id": "abc"}],
        )

    def test_non_dict_input_passes_through(self):
        # Scalar inputs aren't stripped (only dicts are).
        # sample_input.json requires object type, so a scalar fails
        # validation cleanly — but the stripping logic shouldn't crash.
        with pytest.raises(SchemaValidationError):
            SchemaValidator.validate_input(
                step_name="X", schema_file="sample_input.json", inputs=["str"],
            )


class TestValidateInputFail:
    def test_missing_required(self):
        with pytest.raises(SchemaValidationError) as excinfo:
            SchemaValidator.validate_input(
                step_name="SampleStep",
                schema_file="sample_input.json",
                inputs=[{"count": 1}],
            )
        assert "Input validation failed for SampleStep" in str(excinfo.value)
        assert "name" in str(excinfo.value).lower()

    def test_additional_property(self):
        with pytest.raises(SchemaValidationError) as excinfo:
            SchemaValidator.validate_input(
                step_name="SampleStep",
                schema_file="sample_input.json",
                inputs=[{"name": "x", "extra": True}],
            )
        # additionalProperties violation; pointer + message format
        assert "Input validation failed for SampleStep" in str(excinfo.value)

    def test_wrong_type(self):
        with pytest.raises(SchemaValidationError):
            SchemaValidator.validate_input(
                step_name="X",
                schema_file="sample_input.json",
                inputs=[{"name": "alice", "count": "five"}],
            )

    def test_first_failing_item_short_circuits(self):
        # Loop raises on the FIRST failure (matches Ruby behavior —
        # `inputs.each { ... raise if ... }`).
        with pytest.raises(SchemaValidationError) as excinfo:
            SchemaValidator.validate_input(
                step_name="X",
                schema_file="sample_input.json",
                inputs=[{}, {"name": "alice"}],  # first fails
            )
        # Item 0 is missing 'name' — message should reference that
        assert "name" in str(excinfo.value).lower()


class TestValidateOutput:
    def test_valid_passes(self):
        SchemaValidator.validate_output(
            step_name="X",
            schema_file="sample_output.json",
            output={"status": "ok"},
        )

    def test_invalid_enum_fails(self):
        with pytest.raises(SchemaValidationError) as excinfo:
            SchemaValidator.validate_output(
                step_name="SampleStep",
                schema_file="sample_output.json",
                output={"status": "weird"},
            )
        assert "Output validation failed for SampleStep" in str(excinfo.value)

    def test_missing_required(self):
        with pytest.raises(SchemaValidationError):
            SchemaValidator.validate_output(
                step_name="X", schema_file="sample_output.json", output={},
            )


class TestPathResolution:
    def test_missing_env_var(self, monkeypatch):
        monkeypatch.delenv("TURBOFAN_SCHEMAS_PATH")
        with pytest.raises(SchemaValidationError, match="TURBOFAN_SCHEMAS_PATH"):
            SchemaValidator.validate_input(
                step_name="X", schema_file="sample_input.json",
                inputs=[{"name": "a"}],
            )

    def test_unknown_schema_file(self):
        with pytest.raises(SchemaValidationError, match="not found"):
            SchemaValidator.validate_input(
                step_name="X", schema_file="nope.json",
                inputs=[{"name": "a"}],
            )


class TestCaching:
    def test_schema_loaded_once(self, monkeypatch):
        # Trigger first validation
        SchemaValidator.validate_input(
            step_name="X", schema_file="sample_input.json",
            inputs=[{"name": "alice"}],
        )
        # Second call should hit cache. We assert by checking that
        # changing the file ON DISK does not affect the validator
        # (cache wins). Use a tmp schema for this since we don't want
        # to mutate fixtures.
        import tempfile
        from pathlib import Path

        with tempfile.TemporaryDirectory() as tmpd:
            schema_path = Path(tmpd) / "cached.json"
            schema_path.write_text('{"type":"object","required":["a"]}')
            monkeypatch.setenv("TURBOFAN_SCHEMAS_PATH", tmpd)

            # First call locks in the schema
            SchemaValidator.validate_input(
                step_name="X", schema_file="cached.json",
                inputs=[{"a": 1}],
            )
            # Mutate the file: now requires 'b' instead of 'a'
            schema_path.write_text('{"type":"object","required":["b"]}')
            # Second call should still use cached schema (require 'a')
            # so {'a': 1} still passes
            SchemaValidator.validate_input(
                step_name="X", schema_file="cached.json",
                inputs=[{"a": 1}],
            )
