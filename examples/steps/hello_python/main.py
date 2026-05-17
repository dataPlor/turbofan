# Polyglot Python step using turbofan_runtime.Wrapper.
#
# Container language is independent of the Step class declaration in
# worker.rb — Turbofan's ObjectSpace discovery, schema validation, and
# CFN/ASL generation work off worker.rb regardless of what the container
# runs. main.py is the actual runtime.
#
# Compare main_raw.py (preserved alongside) which re-implements
# envelope I/O against S3 by hand — what every polyglot step author
# had to write before turbofan_runtime existed.

import sys

from turbofan_runtime import Interrupted, Wrapper


def call(inputs, context):
    """Append a greeting to each input item's `output` array.

    Mirrors the Ruby HelloPolyglot step's behavior so the same input
    feeds Ruby and Python implementations and produces equivalent
    output S3 objects. Used by the cross-language equivalence test in
    python/tests/integration/.
    """
    for item in inputs:
        item.setdefault("output", []).append("Hello from Python")
    # hello_polyglot's output schema is permissive; passthrough is fine
    return inputs[0] if inputs else {}


if __name__ == "__main__":
    try:
        Wrapper.run(
            call,
            input_schema="hello_polyglot.json",
            output_schema="hello_polyglot.json",
        )
    except Interrupted:
        # SIGTERM cooperative shutdown — exit 143 (128 + SIGTERM=15)
        # so AWS Batch + downstream tooling correctly classify this
        # as a signal-driven shutdown rather than an error.
        sys.exit(143)
