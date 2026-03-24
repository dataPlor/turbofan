require "spec_helper"

RSpec.describe Turbofan::Generators::CloudFormation, "consumable resources", :schemas do # rubocop:disable RSpec/DescribeMethod
  before { Turbofan.config.bucket = "turbofan-shared-bucket" }

  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::ResourceCe", klass)
    klass
  end

  let(:resource_class) do
    Class.new do
      include Turbofan::Resource

      key :duckdb
      consumable 10
    end
  end

  let(:step_with_uses) do
    ce_class # ensure stub_const runs
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      cpu 2
      uses :duckdb
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  let(:step_without_uses) do
    ce_class # ensure stub_const runs
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      cpu 2
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  let(:config) do
    {
      vpc_id: "vpc-123",
      subnets: ["subnet-456"],
      security_groups: ["sg-abc"]
    }
  end

  # Consumable resources are deployed in a separate resources stack and referenced via Fn::ImportValue.

  describe "ConsumableResourceProperties on job definitions" do
    let(:pipeline_class) do
      step_klass = step_with_uses
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "resource-pipeline"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_with_uses},
        stage: "production",
        config: config,
        resources: {duckdb: resource_class}
      )
    end

    let(:template) { generator.generate }

    it "adds ConsumableResourceProperties as a top-level property of the job definition" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      props = template["Resources"][jd_key]["Properties"]
      expect(props).to have_key("ConsumableResourceProperties")
    end

    it "does not put ConsumableResourceProperties inside ContainerProperties" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      container = template["Resources"][jd_key]["Properties"]["ContainerProperties"]
      expect(container).not_to have_key("ConsumableResourceProperties")
    end

    it "wraps entries in a ConsumableResourceList array" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      crp = template["Resources"][jd_key]["Properties"]["ConsumableResourceProperties"]
      expect(crp).to have_key("ConsumableResourceList")
      expect(crp["ConsumableResourceList"]).to be_an(Array)
    end

    it "uses Fn::ImportValue for ConsumableResource and sets Quantity to 1" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      crp = template["Resources"][jd_key]["Properties"]["ConsumableResourceProperties"]
      entry = crp["ConsumableResourceList"].first
      expect(entry).to have_key("ConsumableResource")
      expect(entry["ConsumableResource"]).to have_key("Fn::ImportValue")
      expect(entry["Quantity"]).to eq(1)
    end
  end

  describe "step without uses does not get ConsumableResourceProperties" do
    let(:pipeline_class) do
      step_klass = step_without_uses
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "no-resource-pipeline"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_without_uses},
        stage: "production",
        config: config,
        resources: {duckdb: resource_class}
      )
    end

    let(:template) { generator.generate }

    it "does not add ConsumableResourceProperties to steps without uses" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      props = template["Resources"][jd_key]["Properties"]
      expect(props).not_to have_key("ConsumableResourceProperties")
    end
  end

  describe "no resources defined" do
    let(:pipeline_class) do
      step_klass = step_with_uses
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "no-resource-pipeline"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_with_uses},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "does not create any ConsumableResource when no resources are defined" do
      cr_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::Batch::ConsumableResource"
      }
      expect(cr_keys).to be_empty
    end
  end

  describe "pipeline template does NOT inline ConsumableResource definitions" do
    let(:pipeline_class) do
      step_klass = step_with_uses
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "resource-pipeline"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_with_uses},
        stage: "production",
        config: config,
        resources: {duckdb: resource_class}
      )
    end

    let(:template) { generator.generate }

    it "does NOT include AWS::Batch::ConsumableResource in the pipeline template" do
      cr_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::Batch::ConsumableResource"
      }
      expect(cr_keys).to be_empty
    end

    it "references consumable resources via Fn::ImportValue in job definitions" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      crp = template["Resources"][jd_key]["Properties"]["ConsumableResourceProperties"]
      expect(crp).not_to be_nil
      entry = crp["ConsumableResourceList"].first
      expect(entry["ConsumableResource"]).to have_key("Fn::ImportValue")
    end
  end

  describe "step with multiple consumable uses gets ALL resource refs" do
    let(:gpu_resource_class) do
      Class.new do
        include Turbofan::Resource

        key :gpu
        consumable 4
      end
    end

    let(:step_with_two_uses) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        compute_environment :test_ce
        cpu 2
        uses :duckdb
        uses :gpu
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      step_klass = step_with_two_uses
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "multi-uses-pipeline"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_with_two_uses},
        stage: "production",
        config: config,
        resources: {duckdb: resource_class, gpu: gpu_resource_class}
      )
    end

    let(:template) { generator.generate }

    it "includes both consumable resources in the job definition ConsumableResourceList" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      crp = template["Resources"][jd_key]["Properties"]["ConsumableResourceProperties"]
      expect(crp).not_to be_nil,
        "Expected ConsumableResourceProperties on job definition for step with multiple uses"
      resource_list = crp["ConsumableResourceList"]
      expect(resource_list.size).to eq(2),
        "Expected 2 entries in ConsumableResourceList (one per consumable use), got #{resource_list.size}"
    end

    it "references both duckdb and gpu consumable resources via Fn::ImportValue" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      crp = template["Resources"][jd_key]["Properties"]["ConsumableResourceProperties"]
      resource_list = crp["ConsumableResourceList"]
      resource_refs = resource_list.map { |entry| entry["ConsumableResource"] }
      expect(resource_refs).to include(
        {"Fn::ImportValue" => "turbofan-resources-production-duckdb"}
      )
      expect(resource_refs).to include(
        {"Fn::ImportValue" => "turbofan-resources-production-gpu"}
      )
    end
  end
end
