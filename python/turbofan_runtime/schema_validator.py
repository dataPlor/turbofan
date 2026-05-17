"""JSON Schema validation against schemas resolved from $TURBOFAN_SCHEMAS_PATH.

Mirrors lib/turbofan/runtime/schema_validator.rb. Error message shape
matches Ruby's `json_schemer` output (best-effort — the underlying
libraries produce different message text for the same violation, but
the framing (`"Input validation failed for <step>: ..."` and `, `-
joined per-violation messages) is identical so log grep + alert
patterns work uniformly).

Schemas are cached (per file path) along with their compiled validator
so successive validations within the same process pay zero recompile
cost. The cache is process-scoped, so changes to schema files require
a process restart — fine for step containers (single-run).
"""

import json
import os
from functools import lru_cache
from pathlib import Path

import jsonschema

from .errors import SchemaValidationError


class SchemaValidator:
    @classmethod
    def validate_input(cls, *, step_name, schema_file, inputs):
        """Validate each item in `inputs` against the named schema.

        Strips `__`-prefixed top-level keys before validation (matches
        Ruby line 17 of schema_validator.rb — these are framework-
        injected envelope metadata, not user data).
        """
        _, validator = _load(schema_file)
        for item in inputs:
            clean = (
                {k: v for k, v in item.items() if not k.startswith("__")}
                if isinstance(item, dict)
                else item
            )
            errors = list(validator.iter_errors(clean))
            if not errors:
                continue
            msg = ", ".join(_format_error(e) for e in errors)
            raise SchemaValidationError(
                f"Input validation failed for {step_name}: {msg}"
            )

    @classmethod
    def validate_output(cls, *, step_name, schema_file, output):
        """Validate the whole result against the named schema (no per-item loop)."""
        _, validator = _load(schema_file)
        errors = list(validator.iter_errors(output))
        if not errors:
            return
        msg = ", ".join(_format_error(e) for e in errors)
        raise SchemaValidationError(
            f"Output validation failed for {step_name}: {msg}"
        )


def _load(filename):
    """Resolve `$TURBOFAN_SCHEMAS_PATH/<filename>` and return (schema, validator).

    Path resolution + load + validator construction are all cached so
    that subsequent validations on the same schema are free.
    """
    schemas_path = os.environ.get("TURBOFAN_SCHEMAS_PATH")
    if not schemas_path:
        raise SchemaValidationError(
            "TURBOFAN_SCHEMAS_PATH env var not set; cannot resolve schemas. "
            "The Ruby Dockerfile sets this to /app/schemas — ensure your "
            "Python step's Dockerfile does the same."
        )
    path = Path(schemas_path) / filename
    if not path.is_file():
        raise SchemaValidationError(
            f"Schema file not found: {path} "
            f"(looked under TURBOFAN_SCHEMAS_PATH={schemas_path})"
        )
    return _read_and_compile(str(path))


@lru_cache(maxsize=64)
def _read_and_compile(abs_path):
    with open(abs_path) as f:
        schema = json.load(f)
    cls = jsonschema.validators.validator_for(
        schema, default=jsonschema.Draft202012Validator
    )
    return schema, cls(schema)


def _format_error(err):
    """Format one jsonschema error to a single-line message.

    Includes the JSON pointer when non-empty so messages are
    unambiguous when many violations share the same `err.message`
    (e.g. "additional properties not allowed"). Ruby json_schemer
    similarly includes pointer + message.
    """
    if err.absolute_path:
        pointer = "/" + "/".join(str(p) for p in err.absolute_path)
        return f"{pointer}: {err.message}"
    return err.message
