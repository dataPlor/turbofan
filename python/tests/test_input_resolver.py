import json

import boto3
import pytest
from moto import mock_aws

from turbofan_runtime.context import Context
from turbofan_runtime.input_resolver import InputResolver, normalize_envelope


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
        for bucket in ("tf-test", "turbofan-data"):
            s3.create_bucket(
                Bucket=bucket,
                CreateBucketConfiguration={"LocationConstraint": "us-east-2"},
            )
        yield s3


class TestNormalizeEnvelope:
    def test_array_wrapped_in_inputs(self):
        assert normalize_envelope([1, 2]) == {"inputs": [1, 2]}

    def test_dict_with_inputs_passthrough(self):
        assert normalize_envelope({"inputs": [1], "trace": "x"}) == {
            "inputs": [1],
            "trace": "x",
        }

    def test_items_renamed_to_inputs(self):
        out = normalize_envelope({"items": [1, 2], "trace": "x"})
        assert out["inputs"] == [1, 2]
        assert out["trace"] == "x"
        assert "items" not in out

    def test_scalar_wrapped(self):
        assert normalize_envelope("hello") == {"inputs": ["hello"]}
        assert normalize_envelope({"foo": "bar"}) == {"inputs": [{"foo": "bar"}]}

    def test_inputs_non_list_does_not_pass_through(self):
        # If "inputs" exists but isn't a list, wrap the whole thing.
        # (Matches Ruby normalize_envelope's `raw["inputs"].is_a?(Array)`.)
        out = normalize_envelope({"inputs": "not-a-list"})
        assert out["inputs"] == [{"inputs": "not-a-list"}]


class TestEnvVarPath:
    def test_array_input(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_INPUT", json.dumps([{"x": 1}, {"x": 2}]))
        envelope = InputResolver.call(_ctx())
        assert envelope == {"inputs": [{"x": 1}, {"x": 2}]}

    def test_inputs_key_passthrough(self, monkeypatch, aws):
        monkeypatch.setenv(
            "TURBOFAN_INPUT",
            json.dumps({"inputs": [{"x": 1}], "trace_id": "abc"}),
        )
        envelope = InputResolver.call(_ctx())
        assert envelope == {"inputs": [{"x": 1}], "trace_id": "abc"}

    def test_items_renamed(self, monkeypatch, aws):
        monkeypatch.setenv(
            "TURBOFAN_INPUT", json.dumps({"items": [{"x": 1}]})
        )
        envelope = InputResolver.call(_ctx())
        assert envelope["inputs"] == [{"x": 1}]

    def test_s3_ref_hydrated(self, monkeypatch, aws):
        aws.put_object(
            Bucket="tf-test", Key="payload.json",
            Body=json.dumps([{"hydrated": True}]),
        )
        monkeypatch.setenv(
            "TURBOFAN_INPUT",
            json.dumps({"__turbofan_s3_ref": "s3://tf-test/payload.json"}),
        )
        envelope = InputResolver.call(_ctx())
        assert envelope == {"inputs": [{"hydrated": True}]}

    def test_items_s3_uri_fetched(self, monkeypatch, aws):
        aws.put_object(
            Bucket="turbofan-data", Key="some/items.json",
            Body=json.dumps([{"a": 1}, {"a": 2}]),
        )
        monkeypatch.setenv(
            "TURBOFAN_INPUT",
            json.dumps({
                "items_s3_uri": "s3://turbofan-data/some/items.json",
            }),
        )
        envelope = InputResolver.call(_ctx())
        assert envelope == {"inputs": [{"a": 1}, {"a": 2}]}

    def test_empty_input_default(self, aws):
        # No TURBOFAN_INPUT env → default "{}" → normalize → {"inputs": [{}]}
        envelope = InputResolver.call(_ctx())
        assert envelope == {"inputs": [{}]}


class TestFanOutPath:
    def test_reads_by_array_index(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_BUCKET", "turbofan-data")
        monkeypatch.setenv("TURBOFAN_STEP_NAME", "ingest")
        aws.put_object(
            Bucket="turbofan-data",
            Key="exec-1/ingest/input/items.json",
            Body=json.dumps([{"i": 0}, {"i": 1}, {"i": 2}]),
        )
        envelope = InputResolver.call(_ctx(array_index=1))
        # fan_out items return the array element (which can be any
        # shape); normalize_envelope wraps it.
        assert envelope == {"inputs": [{"i": 1}]}

    def test_with_chunk_size(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_BUCKET", "turbofan-data")
        monkeypatch.setenv("TURBOFAN_STEP_NAME", "ingest")
        aws.put_object(
            Bucket="turbofan-data",
            Key="exec-1/ingest/input/small/items.json",
            Body=json.dumps([{"x": "a"}, {"x": "b"}]),
        )
        envelope = InputResolver.call(_ctx(array_index=1, size="small"))
        assert envelope == {"inputs": [{"x": "b"}]}

    def test_with_chunk_and_parent(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_BUCKET", "turbofan-data")
        monkeypatch.setenv("TURBOFAN_STEP_NAME", "ingest")
        monkeypatch.setenv("TURBOFAN_PARENT_INDEX", "4")
        aws.put_object(
            Bucket="turbofan-data",
            Key="exec-1/ingest/input/small/parent4/items.json",
            Body=json.dumps([{"x": "a"}]),
        )
        envelope = InputResolver.call(_ctx(array_index=0, size="small"))
        assert envelope == {"inputs": [{"x": "a"}]}


class TestPrevStepDeferred:
    def test_prev_step_raises_not_implemented(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_PREV_STEP", "upstream")
        with pytest.raises(NotImplementedError, match="TURBOFAN_PREV_STEP"):
            InputResolver.call(_ctx())

    def test_prev_steps_raises_not_implemented(self, monkeypatch, aws):
        monkeypatch.setenv("TURBOFAN_PREV_STEPS", "u1,u2")
        with pytest.raises(NotImplementedError, match="TURBOFAN_PREV_STEPS"):
            InputResolver.call(_ctx())
