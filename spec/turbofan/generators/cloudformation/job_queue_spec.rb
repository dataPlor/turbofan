require "spec_helper"

RSpec.describe Turbofan::Generators::CloudFormation::JobQueue do
  let(:tags) do
    [
      {"Key" => "stack", "Value" => "turbofan"},
      {"Key" => "stack-type", "Value" => "production"},
      {"Key" => "stack-component", "Value" => "test-pipeline"}
    ]
  end

  describe ".generate with compute_environment_ref" do
    it "accepts a compute_environment_ref Hash" do
      result = described_class.generate(
        prefix: "turbofan-test-pipeline-production",
        step_name: :process,
        compute_environment_ref: {"Fn::ImportValue" => "turbofan-ce-house-stark-production-arn"},
        tags: tags
      )

      queue = result["JobQueueProcess"]
      expect(queue).not_to be_nil
    end

    it "uses Fn::ImportValue in ComputeEnvironmentOrder" do
      result = described_class.generate(
        prefix: "turbofan-test-pipeline-production",
        step_name: :process,
        compute_environment_ref: {"Fn::ImportValue" => "turbofan-ce-house-stark-production-arn"},
        tags: tags
      )

      queue = result["JobQueueProcess"]
      ce_order = queue["Properties"]["ComputeEnvironmentOrder"]
      expect(ce_order.first["ComputeEnvironment"]).to eq(
        {"Fn::ImportValue" => "turbofan-ce-house-stark-production-arn"}
      )
    end

    it "no longer accepts compute_environment_key as a String" do
      # The old interface used compute_environment_key: "ComputeEnvironment"
      # The new interface uses compute_environment_ref: { "Fn::ImportValue" => "..." }
      expect {
        described_class.generate(
          prefix: "turbofan-test-pipeline-production",
          step_name: :process,
          compute_environment_key: "ComputeEnvironment",
          tags: tags
        )
      }.to raise_error(ArgumentError)
    end
  end
end
