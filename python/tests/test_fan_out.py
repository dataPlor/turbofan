import json

import boto3
import pytest
from moto import mock_aws

from turbofan_runtime import fan_out


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-2")
        client.create_bucket(
            Bucket="tf-test",
            CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
        )
        yield client


class TestS3Key:
    def test_no_prefix(self):
        assert fan_out.s3_key("a", "b", "c") == "a/b/c"

    def test_with_prefix(self, monkeypatch):
        monkeypatch.setenv("TURBOFAN_BUCKET_PREFIX", "tenant-a")
        assert fan_out.s3_key("a", "b") == "tenant-a/a/b"

    def test_empty_prefix_treated_as_unset(self, monkeypatch):
        monkeypatch.setenv("TURBOFAN_BUCKET_PREFIX", "")
        assert fan_out.s3_key("a", "b") == "a/b"

    def test_int_parts_stringified(self):
        assert fan_out.s3_key("exec", "step", 3) == "exec/step/3"


class TestReadInput:
    def _put_items(self, s3, key, items):
        s3.put_object(Bucket="tf-test", Key=key, Body=json.dumps(items))

    def test_neither_chunk_nor_parent(self, s3):
        self._put_items(s3, "exec/step/input/items.json", [{"i": 0}, {"i": 1}])
        result = fan_out.read_input(
            array_index=1, s3_client=s3, bucket="tf-test",
            execution_id="exec", step_name="step",
        )
        assert result == {"i": 1}

    def test_chunk_only(self, s3):
        self._put_items(s3, "exec/step/input/small/items.json", [{"x": "a"}])
        result = fan_out.read_input(
            array_index=0, s3_client=s3, bucket="tf-test",
            execution_id="exec", step_name="step",
            chunk="small",
        )
        assert result == {"x": "a"}

    def test_parent_only(self, s3):
        self._put_items(s3, "exec/step/input/parent7/items.json", [{"p": 7}])
        result = fan_out.read_input(
            array_index=0, s3_client=s3, bucket="tf-test",
            execution_id="exec", step_name="step",
            parent_index="7",
        )
        assert result == {"p": 7}

    def test_chunk_and_parent(self, s3):
        self._put_items(s3, "exec/step/input/small/parent4/items.json",
                        [{"a": 1}, {"a": 2}])
        result = fan_out.read_input(
            array_index=0, s3_client=s3, bucket="tf-test",
            execution_id="exec", step_name="step",
            chunk="small", parent_index="4",
        )
        assert result == {"a": 1}

    def test_empty_string_chunk_treated_as_unset(self, s3):
        # Ruby treats "" as truthy; Python's truthy check differs. Our
        # fan_out.read_input uses explicit `is not None and != ""`.
        self._put_items(s3, "exec/step/input/items.json", [{"plain": True}])
        result = fan_out.read_input(
            array_index=0, s3_client=s3, bucket="tf-test",
            execution_id="exec", step_name="step",
            chunk="",
        )
        assert result == {"plain": True}

    def test_bucket_prefix_applied(self, s3, monkeypatch):
        monkeypatch.setenv("TURBOFAN_BUCKET_PREFIX", "tenant-a")
        self._put_items(s3, "tenant-a/exec/step/input/items.json", [{"y": "z"}])
        result = fan_out.read_input(
            array_index=0, s3_client=s3, bucket="tf-test",
            execution_id="exec", step_name="step",
        )
        assert result == {"y": "z"}
