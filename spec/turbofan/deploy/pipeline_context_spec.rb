# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Deploy::PipelineContext do
  describe ".load" do
    it "resolves the pipeline file under the default turbofans_root and delegates to PipelineLoader" do
      fake_result = Turbofan::Deploy::PipelineLoader::LoadResult.new(
        pipeline: :pipe, steps: {}, step_dirs: {}
      )
      expect(Turbofan::Deploy::PipelineLoader).to receive(:load)
        .with("turbofans/pipelines/my_pipe.rb", turbofans_root: "turbofans")
        .and_return(fake_result)

      result = described_class.load(pipeline_name: "my_pipe")
      expect(result).to eq(fake_result)
    end

    it "accepts a custom turbofans_root for testing or non-default layouts" do
      fake_result = Turbofan::Deploy::PipelineLoader::LoadResult.new(
        pipeline: :p, steps: {}, step_dirs: {}
      )
      expect(Turbofan::Deploy::PipelineLoader).to receive(:load)
        .with("custom_root/pipelines/x.rb", turbofans_root: "custom_root")
        .and_return(fake_result)

      described_class.load(pipeline_name: "x", turbofans_root: "custom_root")
    end

    it "passes through PipelineLoader exceptions unchanged" do
      expect(Turbofan::Deploy::PipelineLoader).to receive(:load)
        .and_raise(LoadError, "file not found")

      expect {
        described_class.load(pipeline_name: "missing")
      }.to raise_error(LoadError, "file not found")
    end
  end

  describe "DEFAULT_ROOT" do
    it "is the turbofan project convention" do
      expect(described_class::DEFAULT_ROOT).to eq("turbofans")
    end
  end
end
