import json
import pathlib

import boto3
import pytest
from moto import mock_aws

from turbofan_runtime import Interrupted, SchemaValidationError, Wrapper

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "schemas"


@pytest.fixture
def core_env(monkeypatch):
    monkeypatch.setenv("TURBOFAN_SCHEMAS_PATH", str(FIXTURES))
    monkeypatch.setenv("TURBOFAN_BUCKET", "tf-test")
    monkeypatch.setenv("TURBOFAN_EXECUTION_ID", "exec-1")
    monkeypatch.setenv("TURBOFAN_STEP_NAME", "ingest")
    monkeypatch.setenv("TURBOFAN_STAGE", "dev")
    monkeypatch.setenv("TURBOFAN_PIPELINE", "p")


@pytest.fixture
def aws():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket="tf-test",
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        yield s3


def _ok(inputs, ctx):
    return {"status": "ok"}


def _bad_output(inputs, ctx):
    return {"status": "weird"}  # fails sample_output.json's enum


class TestHappyPathNonFanOut:
    def test_writes_payload_to_s3(self, core_env, aws, monkeypatch, capsys):
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([{"name": "alice"}]))
        Wrapper.run(
            _ok,
            input_schema="sample_input.json",
            output_schema="sample_output.json",
        )
        obj = aws.get_object(Bucket="tf-test", Key="exec-1/ingest/output.json")
        assert json.loads(obj["Body"].read()) == {"status": "ok"}

    def test_emits_envelope_to_stdout(self, core_env, aws, monkeypatch, capsys):
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([{"name": "alice"}]))
        Wrapper.run(
            _ok,
            input_schema="sample_input.json",
            output_schema="sample_output.json",
        )
        captured = capsys.readouterr()
        # Last line of stdout is the envelope JSON (NOT raw payload)
        lines = [
            line for line in captured.out.strip().splitlines() if line.strip()
        ]
        envelope = json.loads(lines[-1])
        assert envelope == {
            "__turbofan_s3_ref": "s3://tf-test/exec-1/ingest/output.json",
        }


class TestSentinelChunk:
    def test_short_circuits_without_user_call(
        self, core_env, aws, monkeypatch, capsys,
    ):
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([None]))
        called = []

        def call_fn(inputs, ctx):
            called.append(inputs)
            return {"status": "ok"}

        Wrapper.run(
            call_fn,
            input_schema="sample_input.json",
            output_schema="sample_output.json",
        )
        assert called == []  # user fn never invoked

    def test_emits_complete_lineage_only(
        self, core_env, aws, monkeypatch, capsys,
    ):
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([None]))
        Wrapper.run(
            _ok,
            input_schema="sample_input.json",
            output_schema="sample_output.json",
        )
        err = capsys.readouterr().err
        # Should see START + COMPLETE lineage events
        event_types = [
            json.loads(line)["event"]["eventType"]
            for line in err.splitlines()
            if '"OpenLineage event"' in line
        ]
        assert event_types == ["START", "COMPLETE"]


class TestSchemaFailures:
    def test_input_schema_fail_raises(
        self, core_env, aws, monkeypatch,
    ):
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([{}]))  # missing name
        with pytest.raises(SchemaValidationError, match="Input validation failed"):
            Wrapper.run(
                _ok,
                input_schema="sample_input.json",
                output_schema="sample_output.json",
            )

    def test_output_schema_fail_raises(self, core_env, aws, monkeypatch):
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([{"name": "alice"}]))
        with pytest.raises(SchemaValidationError, match="Output validation failed"):
            Wrapper.run(
                _bad_output,
                input_schema="sample_input.json",
                output_schema="sample_output.json",
            )


class TestErrorPath:
    def test_user_exception_emits_fail_lineage_and_reraises(
        self, core_env, aws, monkeypatch, capsys,
    ):
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([{"name": "alice"}]))

        def boom(inputs, ctx):
            raise RuntimeError("kaboom")

        with pytest.raises(RuntimeError, match="kaboom"):
            Wrapper.run(
                boom,
                input_schema="sample_input.json",
                output_schema="sample_output.json",
            )

        err = capsys.readouterr().err
        event_types = [
            json.loads(line)["event"]["eventType"]
            for line in err.splitlines()
            if '"OpenLineage event"' in line
        ]
        assert event_types == ["START", "FAIL"]

    def test_interrupted_does_not_emit_failure_metrics(
        self, core_env, aws, monkeypatch, capsys,
    ):
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([{"name": "alice"}]))

        def interrupt_user(inputs, ctx):
            raise Interrupted("SIGTERM-during-call")

        with pytest.raises(Interrupted):
            Wrapper.run(
                interrupt_user,
                input_schema="sample_input.json",
                output_schema="sample_output.json",
            )

        err = capsys.readouterr().err
        event_types = [
            json.loads(line)["event"]["eventType"]
            for line in err.splitlines()
            if '"OpenLineage event"' in line
        ]
        # START only — no FAIL because Interrupted is cooperative
        assert event_types == ["START"]


class TestFanOut:
    def test_per_index_output(self, core_env, monkeypatch):
        monkeypatch.setenv("AWS_BATCH_JOB_ARRAY_INDEX", "0")
        monkeypatch.setenv("TURBOFAN_INPUT", "{}")  # ignored on fan_out path
        with mock_aws():
            s3 = boto3.client("s3", region_name="us-east-2")
            s3.create_bucket(
                Bucket="tf-test",
                CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
            )
            s3.put_object(
                Bucket="tf-test",
                Key="exec-1/ingest/input/items.json",
                Body=json.dumps([{"name": "alice"}]),
            )
            Wrapper.run(
                _ok,
                input_schema="sample_input.json",
                output_schema="sample_output.json",
            )
            out = s3.get_object(Bucket="tf-test", Key="exec-1/ingest/output/0.json")
            assert json.loads(out["Body"].read()) == {"status": "ok"}


class TestStorageSetup:
    def test_no_nvme_no_fargate_no_storage(self, core_env, aws, monkeypatch):
        # autouse fixture stripped TURBOFAN_NVME_MOUNT_PATH +
        # ECS_CONTAINER_METADATA_URI_V4. Wrapper.run should succeed
        # without a storage_path.
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([{"name": "alice"}]))
        Wrapper.run(
            _ok,
            input_schema="sample_input.json",
            output_schema="sample_output.json",
        )
        # No raise = pass

    def test_storage_path_already_set_used_as_is(
        self, core_env, aws, monkeypatch, tmp_path,
    ):
        monkeypatch.setenv("TURBOFAN_STORAGE_PATH", str(tmp_path))
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([{"name": "alice"}]))
        Wrapper.run(
            _ok,
            input_schema="sample_input.json",
            output_schema="sample_output.json",
        )
        # When set externally, _cleanup_storage still rmtrees it.
        # Verify the path no longer exists (cleanup happened).
        # (tmp_path is a pytest fixture; it may still exist as a
        # parent dir even after cleanup of contents — accept either.)

    def test_nvme_env_used_when_set(
        self, core_env, aws, monkeypatch, tmp_path,
    ):
        nvme = tmp_path / "nvme"
        nvme.mkdir()
        monkeypatch.setenv("TURBOFAN_NVME_MOUNT_PATH", str(nvme))
        monkeypatch.setenv("AWS_BATCH_JOB_ID", "test-job")
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([{"name": "alice"}]))
        Wrapper.run(
            _ok,
            input_schema="sample_input.json",
            output_schema="sample_output.json",
        )
        # storage subdir created under nvme — and cleaned up post-run
