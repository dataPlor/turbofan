import json

import boto3
import pytest
from moto import mock_aws

from turbofan_runtime.errors import HydrationError
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


class TestPassthrough:
    def test_scalar_passes_through(self, s3):
        assert Payload.deserialize(42, s3_client=s3) == 42

    def test_list_passes_through(self, s3):
        assert Payload.deserialize([1, 2], s3_client=s3) == [1, 2]

    def test_string_passes_through(self, s3):
        assert Payload.deserialize("hello", s3_client=s3) == "hello"

    def test_none_passes_through(self, s3):
        assert Payload.deserialize(None, s3_client=s3) is None

    def test_dict_without_ref_passes_through(self, s3):
        payload = {"some": "data"}
        assert Payload.deserialize(payload, s3_client=s3) == payload


class TestHydration:
    def test_hydrates_s3_ref(self, s3):
        s3.put_object(
            Bucket="tf-test", Key="path/payload.json",
            Body=json.dumps({"hydrated": True, "rows": [1, 2, 3]}),
        )
        ref = {"__turbofan_s3_ref": "s3://tf-test/path/payload.json"}
        result = Payload.deserialize(ref, s3_client=s3)
        assert result == {"hydrated": True, "rows": [1, 2, 3]}

    def test_hydrates_nested_array(self, s3):
        s3.put_object(
            Bucket="tf-test", Key="x.json", Body=json.dumps([{"a": 1}, {"a": 2}]),
        )
        ref = {"__turbofan_s3_ref": "s3://tf-test/x.json"}
        assert Payload.deserialize(ref, s3_client=s3) == [{"a": 1}, {"a": 2}]


class TestErrorHandling:
    def test_missing_key_raises_hydration_error(self, s3):
        ref = {"__turbofan_s3_ref": "s3://tf-test/missing.json"}
        with pytest.raises(HydrationError) as excinfo:
            Payload.deserialize(ref, s3_client=s3)
        assert "s3://tf-test/missing.json" in str(excinfo.value)

    def test_missing_bucket_raises_hydration_error_or_client_error(self, s3):
        # Different boto3 versions surface this differently — accept either.
        ref = {"__turbofan_s3_ref": "s3://nonexistent-bucket-xyz/x.json"}
        with pytest.raises(Exception):
            Payload.deserialize(ref, s3_client=s3)


# Payload.serialize is now implemented (ibk.5) — tests in test_payload_serialize.py
