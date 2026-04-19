# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Turbofan::Runtime::Payload do
  let(:s3_client) { instance_double(Aws::S3::Client, put_object: nil, get_object: nil) }
  let(:bucket) { "turbofan-test-pipeline-production-bucket" }
  let(:execution_id) { "arn:aws:states:us-east-1:123456:execution:my-sfn:exec-abc123" }
  let(:step_name) { "process" }
  let(:s3_args) { {s3_client: s3_client, bucket: bucket, execution_id: execution_id, step_name: step_name} }

  def serialize(result)
    described_class.serialize(result, **s3_args)
  end

  describe ".serialize" do
    it "always writes output to S3" do
      serialize({count: 42, path: "s3://bucket/output"})

      expect(s3_client).to have_received(:put_object).with(
        hash_including(
          bucket: bucket,
          key: "#{execution_id}/#{step_name}/output.json"
        )
      )
    end

    it "returns raw JSON (not an S3 ref)" do
      serialized = serialize({count: 42, path: "s3://bucket/output"})

      parsed = JSON.parse(serialized)
      expect(parsed).to eq({"count" => 42, "path" => "s3://bucket/output"})
      expect(parsed).not_to have_key("__turbofan_s3_ref")
    end

    it "writes to S3 even for small payloads" do
      serialize({small: true})

      expect(s3_client).to have_received(:put_object)
    end

    it "handles an empty hash" do
      serialized = serialize({})

      expect(JSON.parse(serialized)).to eq({})
      expect(s3_client).to have_received(:put_object)
    end

    it "handles nil result" do
      serialized = serialize(nil)

      expect(serialized).to eq("null")
    end

    it "uses S3 path convention: {execution_id}/{step_name}/output.json" do
      serialize({data: "test"})

      expect(s3_client).to have_received(:put_object).with(
        hash_including(key: "#{execution_id}/#{step_name}/output.json")
      )
    end

    it "writes the full JSON body to S3" do
      result = {data: "test_value"}
      serialize(result)

      expect(s3_client).to have_received(:put_object).with(
        hash_including(body: JSON.generate(result))
      )
    end
  end

  describe ".deserialize" do
    context "when input is a plain JSON hash" do
      it "passes through unchanged" do
        input = {"count" => 42, "path" => "s3://bucket/key"}
        result = described_class.deserialize(input, s3_client: s3_client)

        expect(result).to eq({"count" => 42, "path" => "s3://bucket/key"})
      end

      it "passes through an empty hash" do
        result = described_class.deserialize({}, s3_client: s3_client)
        expect(result).to eq({})
      end
    end

    context "when input contains __turbofan_s3_ref" do
      let(:s3_key) { "#{execution_id}/#{step_name}/output.json" }
      let(:s3_ref) { "s3://#{bucket}/#{s3_key}" }
      let(:original_data) { {"count" => 42, "results" => [1, 2, 3]} }

      before do
        s3_body = instance_double(StringIO, read: JSON.generate(original_data))
        s3_response = instance_double(Aws::S3::Types::GetObjectOutput, body: s3_body)
        allow(s3_client).to receive(:get_object).and_return(s3_response)
      end

      it "detects the __turbofan_s3_ref and hydrates from S3" do
        input = {"__turbofan_s3_ref" => s3_ref}
        result = described_class.deserialize(input, s3_client: s3_client)

        expect(result).to eq(original_data)
      end

      it "calls S3 get_object with the correct bucket and key" do
        input = {"__turbofan_s3_ref" => s3_ref}
        described_class.deserialize(input, s3_client: s3_client)

        expect(s3_client).to have_received(:get_object).with(
          bucket: bucket,
          key: s3_key
        )
      end
    end

    context "when S3 object is missing" do
      before do
        allow(s3_client).to receive(:get_object).and_raise(
          Aws::S3::Errors::NoSuchKey.new(nil, "The specified key does not exist.")
        )
      end

      it "raises an error with a descriptive message" do
        input = {"__turbofan_s3_ref" => "s3://#{bucket}/missing/key.json"}

        expect {
          described_class.deserialize(input, s3_client: s3_client)
        }.to raise_error(Turbofan::Runtime::Payload::HydrationError)
      end
    end

    context "when input is nil" do
      it "returns nil" do
        result = described_class.deserialize(nil, s3_client: s3_client)
        expect(result).to be_nil
      end
    end

    context "when input is not a hash" do
      it "returns the value as-is for arrays" do
        input = [1, 2, 3]
        result = described_class.deserialize(input, s3_client: s3_client)
        expect(result).to eq([1, 2, 3])
      end

      it "returns the value as-is for strings" do
        result = described_class.deserialize("hello", s3_client: s3_client)
        expect(result).to eq("hello")
      end
    end
  end
end
