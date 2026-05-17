import json

import boto3
import pytest
from moto import mock_aws

from turbofan_runtime.payload import Payload


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-2")
        client.create_bucket(
            Bucket="tf-test",
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        yield client


class TestSerializeEnvelopeContract:
    def test_writes_payload_to_s3(self, s3):
        Payload.serialize(
            {"foo": "bar", "n": 42},
            s3_client=s3, bucket="tf-test",
            execution_id="exec-1", step_name="ingest",
        )
        obj = s3.get_object(Bucket="tf-test", Key="exec-1/ingest/output.json")
        assert json.loads(obj["Body"].read()) == {"foo": "bar", "n": 42}

    def test_returns_envelope_not_raw_payload(self, s3):
        # This is THE contract change vs Ruby. Stdout sees only the
        # envelope pointer, never the raw payload.
        returned = Payload.serialize(
            {"foo": "bar"},
            s3_client=s3, bucket="tf-test",
            execution_id="exec-1", step_name="ingest",
        )
        envelope = json.loads(returned)
        assert set(envelope.keys()) == {"__turbofan_s3_ref"}
        assert envelope["__turbofan_s3_ref"] == "s3://tf-test/exec-1/ingest/output.json"

    def test_bucket_prefix_in_envelope_ref(self, s3, monkeypatch):
        monkeypatch.setenv("TURBOFAN_BUCKET_PREFIX", "tenant-a")
        returned = Payload.serialize(
            {"x": 1},
            s3_client=s3, bucket="tf-test",
            execution_id="e", step_name="s",
        )
        envelope = json.loads(returned)
        assert envelope["__turbofan_s3_ref"] == "s3://tf-test/tenant-a/e/s/output.json"
        # And the actual S3 write went to the prefixed key
        obj = s3.get_object(Bucket="tf-test", Key="tenant-a/e/s/output.json")
        assert json.loads(obj["Body"].read()) == {"x": 1}


class TestJsonFormatting:
    def test_no_whitespace_in_output(self, s3):
        # Ruby JSON.generate emits no spaces. We pin to separators=(',',':').
        returned = Payload.serialize(
            {"a": 1, "b": [2, 3]},
            s3_client=s3, bucket="tf-test",
            execution_id="e", step_name="s",
        )
        # No space-comma-space or colon-space anywhere in the output:
        assert ", " not in returned
        assert ": " not in returned

    def test_unicode_not_escaped(self, s3):
        # Ruby JSON.generate leaves non-ASCII as UTF-8. Python default
        # `ensure_ascii=True` would escape to \uXXXX — we pin False.
        Payload.serialize(
            {"name": "café"},
            s3_client=s3, bucket="tf-test",
            execution_id="e", step_name="s",
        )
        body = s3.get_object(Bucket="tf-test", Key="e/s/output.json")["Body"].read().decode()
        assert "café" in body
        assert "\\u00e9" not in body  # NOT escaped to \uXXXX

    def test_dict_key_order_preserved(self, s3):
        Payload.serialize(
            {"z_first": 1, "a_second": 2},
            s3_client=s3, bucket="tf-test",
            execution_id="e", step_name="s",
        )
        body = s3.get_object(Bucket="tf-test", Key="e/s/output.json")["Body"].read().decode()
        # z_first appears before a_second in the wire bytes (no sorting)
        assert body.index("z_first") < body.index("a_second")
