# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Runtime::OutputSerializer do
  let(:s3_client) { instance_double(Aws::S3::Client, put_object: nil) }
  let(:execution_id) { "exec-abc" }
  let(:step_name) { "process" }
  let(:bucket) { "turbofan-bucket" }

  def build_context(array_index: nil, size: nil, storage_path: nil)
    ctx = Turbofan::Runtime::Context.new(
      execution_id: execution_id,
      attempt_number: 1,
      step_name: step_name,
      stage: "dev",
      pipeline_name: "pipe",
      array_index: array_index,
      storage_path: storage_path,
      uses: [],
      writes_to: [],
      size: size
    )
    allow(ctx).to receive(:s3).and_return(s3_client)
    ctx
  end

  before do
    ENV["TURBOFAN_BUCKET"] = bucket
    ENV["TURBOFAN_STEP_NAME"] = step_name
    ENV["TURBOFAN_BUCKET_PREFIX"] = nil
  end

  after do
    ENV.delete("TURBOFAN_BUCKET")
    ENV.delete("TURBOFAN_STEP_NAME")
    ENV.delete("TURBOFAN_PARENT_INDEX")
  end

  describe "fan-out path (array_index present)" do
    let(:result) { {"output" => "value"} }

    it "writes to output/{array_index}.json when no size and no parent_index" do
      ctx = build_context(array_index: 0)
      described_class.call(result, ctx)
      expect(s3_client).to have_received(:put_object).with(
        bucket: bucket,
        key: "#{execution_id}/#{step_name}/output/0.json",
        body: JSON.generate(result)
      )
    end

    it "writes to output/{size}/{array_index}.json when size set, no parent_index" do
      ctx = build_context(array_index: 2, size: "large")
      described_class.call(result, ctx)
      expect(s3_client).to have_received(:put_object).with(
        bucket: bucket,
        key: "#{execution_id}/#{step_name}/output/large/2.json",
        body: JSON.generate(result)
      )
    end

    it "writes to output/parent{parent_index}/{array_index}.json when parent_index set, no size" do
      ENV["TURBOFAN_PARENT_INDEX"] = "5"
      ctx = build_context(array_index: 3)
      described_class.call(result, ctx)
      expect(s3_client).to have_received(:put_object).with(
        bucket: bucket,
        key: "#{execution_id}/#{step_name}/output/parent5/3.json",
        body: JSON.generate(result)
      )
    end

    it "writes to output/{size}/parent{parent_index}/{array_index}.json when both set" do
      ENV["TURBOFAN_PARENT_INDEX"] = "1"
      ctx = build_context(array_index: 7, size: "small")
      described_class.call(result, ctx)
      expect(s3_client).to have_received(:put_object).with(
        bucket: bucket,
        key: "#{execution_id}/#{step_name}/output/small/parent1/7.json",
        body: JSON.generate(result)
      )
    end

    it "respects TURBOFAN_BUCKET_PREFIX if set" do
      ENV["TURBOFAN_BUCKET_PREFIX"] = "tenant-a"
      ctx = build_context(array_index: 0)
      described_class.call(result, ctx)
      expect(s3_client).to have_received(:put_object).with(
        bucket: bucket,
        key: "tenant-a/#{execution_id}/#{step_name}/output/0.json",
        body: JSON.generate(result)
      )
      ENV.delete("TURBOFAN_BUCKET_PREFIX")
    end

    it "returns the JSON-serialized result as the output string" do
      ctx = build_context(array_index: 0)
      expect(described_class.call(result, ctx)).to eq(JSON.generate(result))
    end
  end

  describe "transient error retry (via Retryable)" do
    it "retries S3 put_object on SlowDown and eventually succeeds" do
      slowdown = Aws::S3::Errors::ServiceError.new(nil, "SlowDown")
      allow(slowdown).to receive(:code).and_return("SlowDown")
      attempts = 0
      allow(s3_client).to receive(:put_object) do
        attempts += 1
        raise slowdown if attempts < 3
        nil
      end

      allow(Turbofan::Retryable).to receive(:call).and_wrap_original do |m, **kwargs, &blk|
        m.call(sleeper: ->(_s) {}, **kwargs, &blk)
      end

      ctx = build_context(array_index: 0)
      described_class.call({"ok" => true}, ctx)

      expect(attempts).to eq(3)
    end
  end

  describe "non-fan-out path (array_index nil)" do
    it "delegates to Payload.serialize" do
      ctx = build_context(array_index: nil)
      result = {"data" => "value"}
      expect(Turbofan::Runtime::Payload).to receive(:serialize).with(
        result,
        s3_client: s3_client,
        bucket: bucket,
        execution_id: execution_id,
        step_name: step_name
      ).and_return("payload-output")
      expect(described_class.call(result, ctx)).to eq("payload-output")
    end
  end
end
