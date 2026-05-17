import json

import boto3
import pytest
from moto import mock_aws

from turbofan_runtime.context import Context
from turbofan_runtime.output_serializer import OutputSerializer


def _ctx(**overrides):
    defaults = dict(
        execution_id="exec-1",
        attempt_number=1,
        step_name="ingest",
        stage="dev",
        pipeline_name="p",
        array_index=None,
        storage_path=None,
        size=None,
    )
    defaults.update(overrides)
    return Context(**defaults)


@pytest.fixture
def aws():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-2")
        s3.create_bucket(
            Bucket="tf-test",
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        yield s3


class TestNonFanOut:
    def test_writes_payload_and_returns_envelope(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_BUCKET", "tf-test")
        ctx = _ctx()
        ctx._s3 = aws  # inject mocked client

        returned = OutputSerializer.call({"hello": "world"}, ctx)

        # Returned value is the envelope JSON, not the raw payload
        envelope = json.loads(returned)
        assert envelope == {
            "__turbofan_s3_ref": "s3://tf-test/exec-1/ingest/output.json",
        }
        # Payload was written to S3
        obj = aws.get_object(Bucket="tf-test", Key="exec-1/ingest/output.json")
        assert json.loads(obj["Body"].read()) == {"hello": "world"}


class TestFanOut:
    def test_per_index_key(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_BUCKET", "tf-test")
        monkeypatch.setenv("TURBOFAN_STEP_NAME", "ingest")
        ctx = _ctx(array_index=3)
        ctx._s3 = aws

        returned = OutputSerializer.call({"i": 3}, ctx)
        # Fan_out returns raw JSON (downstream reads via S3, not stdout)
        assert json.loads(returned) == {"i": 3}
        obj = aws.get_object(Bucket="tf-test", Key="exec-1/ingest/output/3.json")
        assert json.loads(obj["Body"].read()) == {"i": 3}

    def test_size_segment(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_BUCKET", "tf-test")
        monkeypatch.setenv("TURBOFAN_STEP_NAME", "ingest")
        ctx = _ctx(array_index=1, size="small")
        ctx._s3 = aws

        OutputSerializer.call({"i": 1}, ctx)
        obj = aws.get_object(Bucket="tf-test", Key="exec-1/ingest/output/small/1.json")
        assert json.loads(obj["Body"].read()) == {"i": 1}

    def test_size_and_parent_segment(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_BUCKET", "tf-test")
        monkeypatch.setenv("TURBOFAN_STEP_NAME", "ingest")
        monkeypatch.setenv("TURBOFAN_PARENT_INDEX", "4")
        ctx = _ctx(array_index=2, size="small")
        ctx._s3 = aws

        OutputSerializer.call({"i": 2}, ctx)
        obj = aws.get_object(
            Bucket="tf-test", Key="exec-1/ingest/output/small/parent4/2.json"
        )
        assert json.loads(obj["Body"].read()) == {"i": 2}

    def test_parent_only_segment(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_BUCKET", "tf-test")
        monkeypatch.setenv("TURBOFAN_STEP_NAME", "ingest")
        monkeypatch.setenv("TURBOFAN_PARENT_INDEX", "9")
        ctx = _ctx(array_index=0)
        ctx._s3 = aws

        OutputSerializer.call({"i": 0}, ctx)
        obj = aws.get_object(Bucket="tf-test", Key="exec-1/ingest/output/parent9/0.json")
        assert json.loads(obj["Body"].read()) == {"i": 0}

    def test_empty_parent_index_treated_as_unset(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_BUCKET", "tf-test")
        monkeypatch.setenv("TURBOFAN_STEP_NAME", "ingest")
        monkeypatch.setenv("TURBOFAN_PARENT_INDEX", "")
        ctx = _ctx(array_index=5)
        ctx._s3 = aws

        OutputSerializer.call({"i": 5}, ctx)
        # Should go to plain segment-less key, not parent/5.json
        obj = aws.get_object(Bucket="tf-test", Key="exec-1/ingest/output/5.json")
        assert json.loads(obj["Body"].read()) == {"i": 5}


class TestBucketPrefix:
    def test_prefix_applied_to_non_fan_out(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_BUCKET", "tf-test")
        monkeypatch.setenv("TURBOFAN_BUCKET_PREFIX", "tenant-a")
        ctx = _ctx()
        ctx._s3 = aws
        OutputSerializer.call({"x": 1}, ctx)
        obj = aws.get_object(Bucket="tf-test", Key="tenant-a/exec-1/ingest/output.json")
        assert json.loads(obj["Body"].read()) == {"x": 1}

    def test_prefix_applied_to_fan_out(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_BUCKET", "tf-test")
        monkeypatch.setenv("TURBOFAN_BUCKET_PREFIX", "tenant-a")
        monkeypatch.setenv("TURBOFAN_STEP_NAME", "ingest")
        ctx = _ctx(array_index=2)
        ctx._s3 = aws
        OutputSerializer.call({"i": 2}, ctx)
        obj = aws.get_object(
            Bucket="tf-test", Key="tenant-a/exec-1/ingest/output/2.json"
        )
        assert json.loads(obj["Body"].read()) == {"i": 2}
