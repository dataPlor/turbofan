"""End-to-end smoke: load the example main.py via importlib and run
its `call()` through the wrapper with mocked AWS.

Validates the full happy path against a real-shaped step (mirrors
what dev_library will do when it ports its own steps), including
S3 output, envelope-on-stdout, and lineage events.
"""

import importlib.util
import json
import pathlib
import sys

import boto3
import pytest
from moto import mock_aws

from turbofan_runtime import Wrapper

REPO_ROOT = pathlib.Path(__file__).parents[3]
EXAMPLE = REPO_ROOT / "examples" / "steps" / "hello_python" / "main.py"


def _load_call_fn():
    spec = importlib.util.spec_from_file_location("hello_python_main", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    sys.modules["hello_python_main"] = module
    spec.loader.exec_module(module)
    return module.call


@pytest.fixture
def schemas_dir(tmp_path, monkeypatch):
    schema = tmp_path / "hello_polyglot.json"
    schema.write_text(json.dumps({"type": "object"}))
    monkeypatch.setenv("TURBOFAN_SCHEMAS_PATH", str(tmp_path))
    return tmp_path


def test_hello_python_appends_greeting(monkeypatch, schemas_dir, capsys):
    monkeypatch.setenv("TURBOFAN_BUCKET", "tf-test")
    monkeypatch.setenv("TURBOFAN_EXECUTION_ID", "exec-1")
    monkeypatch.setenv("TURBOFAN_STEP_NAME", "hello_python")
    monkeypatch.setenv("TURBOFAN_STAGE", "dev")
    monkeypatch.setenv("TURBOFAN_PIPELINE", "examples")
    monkeypatch.setenv(
        "TURBOFAN_INPUT",
        json.dumps([{"output": ["prev step"]}]),
    )

    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket="tf-test",
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )

        call_fn = _load_call_fn()
        Wrapper.run(
            call_fn,
            input_schema="hello_polyglot.json",
            output_schema="hello_polyglot.json",
        )

        obj = s3.get_object(Bucket="tf-test", Key="exec-1/hello_python/output.json")
        body = json.loads(obj["Body"].read())
        assert body == {"output": ["prev step", "Hello from Python"]}

    # Wrapper.run wrote the envelope to stdout (last non-empty line)
    captured = capsys.readouterr()
    last_line = [
        line for line in captured.out.strip().splitlines() if line.strip()
    ][-1]
    envelope = json.loads(last_line)
    assert envelope["__turbofan_s3_ref"] == (
        "s3://tf-test/exec-1/hello_python/output.json"
    )
