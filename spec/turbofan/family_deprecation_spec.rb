require "spec_helper"

RSpec.describe "Family removal" do # rubocop:disable RSpec/DescribeClass
  describe "family method is no longer defined" do
    it "raises NoMethodError when family is called on a Step class" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          family :c
        end
      }.to raise_error(NoMethodError)
    end

    it "raises NoMethodError for any family symbol" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          family :m
        end
      }.to raise_error(NoMethodError)
    end
  end

  describe "VALID_FAMILIES constant is removed" do
    it "does not define VALID_FAMILIES on Turbofan::Step" do
      expect(Turbofan::Step).not_to be_const_defined(:VALID_FAMILIES)
    end
  end

  describe "turbofan_family reader is removed" do
    it "does not expose turbofan_family on step classes" do
      ce = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::FamilyRemovedCe", ce)

      step_class = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :family_removed_ce
        cpu 1
      end

      expect(step_class).not_to respond_to(:turbofan_family)
    end
  end

  describe "validate_family_set! is removed" do
    it "cpu does not require compute_environment eagerly (A1: lazy validation)" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          cpu 2
        end
      }.not_to raise_error
    end

    it "ram does not require compute_environment eagerly (A1: lazy validation)" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          ram 4096
        end
      }.not_to raise_error
    end
  end

  describe "InstanceFamily.derive is not called by cpu/ram" do
    it "cpu sets the value directly without derivation" do
      ce = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::DirectCe", ce)

      step_class = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :direct_ce
        cpu 2
      end

      # cpu 2 should store exactly 2 — no ram auto-derivation
      expect(step_class.turbofan_default_cpu).to eq(2)
      expect(step_class.turbofan_default_ram).to be_nil
    end

    it "ram sets the value directly without derivation" do
      ce = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::DirectCe2", ce)

      step_class = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :direct_ce2
        ram 4096
      end

      # ram 4096 should store exactly 4096 — no cpu auto-derivation
      expect(step_class.turbofan_default_ram).to eq(4096)
      expect(step_class.turbofan_default_cpu).to be_nil
    end
  end

  describe "pipeline_check treats missing compute_environment as error", :schemas do
    let(:pipeline_class) do
      # This step has no compute_environment — should be an error, not a warning
      stub_const("NoCeStep", Class.new {
        include Turbofan::Step
        execution :batch
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "no-ce-pipeline"
      end
    end

    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    it "reports an error (not a warning) when step has no compute_environment" do
      result = Turbofan::Check::PipelineCheck.run(
        pipeline: pipeline_class,
        steps: {no_ce_step: step_class}
      )
      ce_errors = result.errors.select { |e| e.match?(/compute_environment/i) }
      expect(ce_errors).not_to be_empty
    end
  end
end
