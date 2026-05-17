"""Cross-language equivalence: a trivial step processed by Python and
Ruby implementations should produce JSON-equal S3 outputs.

For v1 we test ONLY the JSON encoding produced by `Payload._serialize_json`
(Python's `json.dumps(..., separators=(',',':'), ensure_ascii=False)`)
vs Ruby's `JSON.generate(...)`. Both are called against the same dict
literal via subprocess. We assert `json.loads(python) == json.loads(ruby)`
on the result — strict byte-equality is reserved for ASCII-only canary
cases since Ruby and Python may differ on whitespace conventions for
non-standard JSON constructs (we use neither, so equivalence holds).

Skipped if `ruby` is not on PATH.
"""

import json
import shutil
import subprocess

import pytest

from turbofan_runtime.payload import _serialize_json


def _ruby_available():
    return shutil.which("ruby") is not None


def _ruby_serialize(value):
    """Return the JSON bytes Ruby's JSON.generate would produce."""
    script = "require 'json'; print JSON.generate(JSON.parse(STDIN.read))"
    result = subprocess.run(
        ["ruby", "-e", script],
        input=json.dumps(value),
        capture_output=True,
        check=True,
        text=True,
    )
    return result.stdout


@pytest.mark.skipif(not _ruby_available(), reason="ruby not on PATH")
class TestJsonEncodingEquivalence:
    @pytest.mark.parametrize(
        "value",
        [
            {"status": "ok"},
            {"a": 1, "b": [1, 2, 3]},
            {"nested": {"x": "y", "n": 42}},
            ["str", 1, 2.5, True, False, None],
            {"unicode": "café"},
            {"empty_list": [], "empty_dict": {}},
        ],
    )
    def test_python_and_ruby_json_equivalent(self, value):
        py = _serialize_json(value)
        rb = _ruby_serialize(value)
        # Structural equivalence — both should round-trip to the same value
        assert json.loads(py) == json.loads(rb)


@pytest.mark.skipif(not _ruby_available(), reason="ruby not on PATH")
class TestByteIdentityForAscii:
    """When the payload is ASCII-only with no float quirks, the Python
    output and Ruby output should be byte-identical given our pinned
    serialization options (separators=(',',':') matches Ruby's default
    no-whitespace mode)."""

    @pytest.mark.parametrize(
        "value",
        [
            {"status": "ok"},
            {"a": 1, "b": [1, 2, 3]},
            {"nested": {"x": "y", "n": 42}},
        ],
    )
    def test_byte_identical(self, value):
        py = _serialize_json(value)
        rb = _ruby_serialize(value)
        assert py == rb
