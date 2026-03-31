require "spec_helper"

RSpec.describe Turbofan::Generators::CloudFormation, "tag expansion", :schemas do # rubocop:disable RSpec/DescribeMethod
  before { Turbofan.config.bucket = "turbofan-shared-bucket" }

  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::TagCe", klass)
    klass
  end

  let(:step_class) do
    ce_class # ensure stub_const runs
    Class.new do
      include Turbofan::Step

      execution :batch
      compute_environment :test_ce
      cpu 2
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  let(:pipeline_class) do
    step_klass = step_class
    stub_const("Process", step_klass)
    Class.new do
      include Turbofan::Pipeline

      pipeline_name "tag-pipeline"
      pipeline do
        process(trigger_input)
      end
    end
  end

  let(:config) do
    {
      vpc_id: "vpc-123",
      subnets: ["subnet-456"],
      security_groups: ["sg-abc"]
    }
  end

  let(:generator) do
    described_class.new(
      pipeline: pipeline_class,
      steps: {process: step_class},
      stage: "production",
      config: config
    )
  end

  let(:template) { generator.generate }

  describe "turbofan:* namespace tags" do
    it "includes turbofan:managed = true on all resources" do
      template["Resources"].each do |key, resource|
        next unless resource.dig("Properties", "Tags")
        tags_hash = described_class.tags_hash(resource["Properties"]["Tags"])
        expect(tags_hash["turbofan:managed"]).to eq("true"),
          "Expected resource #{key} to have turbofan:managed=true tag"
      end
    end

    it "includes turbofan:pipeline = pipeline_name on all resources" do
      template["Resources"].each do |key, resource|
        next unless resource.dig("Properties", "Tags")
        tags_hash = described_class.tags_hash(resource["Properties"]["Tags"])
        expect(tags_hash["turbofan:pipeline"]).to eq("tag-pipeline"),
          "Expected resource #{key} to have turbofan:pipeline=tag-pipeline tag"
      end
    end

    it "includes turbofan:stage = stage on all resources" do
      template["Resources"].each do |key, resource|
        next unless resource.dig("Properties", "Tags")
        tags_hash = described_class.tags_hash(resource["Properties"]["Tags"])
        expect(tags_hash["turbofan:stage"]).to eq("production"),
          "Expected resource #{key} to have turbofan:stage=production tag"
      end
    end
  end

  describe "per-step turbofan:step tag" do
    it "includes turbofan:step on job definition resources" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      tags_hash = described_class.tags_hash(template["Resources"][jd_key]["Properties"]["Tags"])
      expect(tags_hash["turbofan:step"]).to eq("process")
    end

    it "includes turbofan:step on ECR repository resources" do
      ecr_key = template["Resources"].keys.find { |k| k.start_with?("ECR") }
      tags_hash = described_class.tags_hash(template["Resources"][ecr_key]["Properties"]["Tags"])
      expect(tags_hash["turbofan:step"]).to eq("process")
    end

    it "includes turbofan:step on log group resources" do
      log_key = template["Resources"].keys.find { |k| k.start_with?("LogGroup") }
      tags_hash = described_class.tags_hash(template["Resources"][log_key]["Properties"]["Tags"])
      expect(tags_hash["turbofan:step"]).to eq("process")
    end
  end

  describe "multi-step turbofan:step tags" do
    let(:step_a) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 2
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:step_b) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:multi_pipeline) do
      stub_const("Extract", step_a)
      stub_const("Transform", step_b)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "multi-tag"
        pipeline do
          results = extract(trigger_input)
          transform(results)
        end
      end
    end

    let(:multi_template) do
      described_class.new(
        pipeline: multi_pipeline,
        steps: {extract: step_a, transform: step_b},
        stage: "staging",
        config: config
      ).generate
    end

    it "tags extract job definition with turbofan:step = extract" do
      jd_key = multi_template["Resources"].keys.find { |k|
        k.start_with?("JobDef") && k.include?("Extract")
      }
      tags_hash = described_class.tags_hash(multi_template["Resources"][jd_key]["Properties"]["Tags"])
      expect(tags_hash["turbofan:step"]).to eq("extract")
    end

    it "tags transform job definition with turbofan:step = transform" do
      jd_key = multi_template["Resources"].keys.find { |k|
        k.start_with?("JobDef") && k.include?("Transform")
      }
      tags_hash = described_class.tags_hash(multi_template["Resources"][jd_key]["Properties"]["Tags"])
      expect(tags_hash["turbofan:step"]).to eq("transform")
    end
  end

  describe "custom tags from Step" do
    let(:tagged_step) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 2
        input_schema "passthrough.json"
        output_schema "passthrough.json"
        tags team: "data-eng", cost_center: "12345"
      end
    end

    let(:tagged_pipeline) do
      step_klass = tagged_step
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "custom-tags"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:tagged_template) do
      described_class.new(
        pipeline: tagged_pipeline,
        steps: {process: tagged_step},
        stage: "production",
        config: config
      ).generate
    end

    it "merges custom step tags into per-step resources" do
      jd_key = tagged_template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      tags_hash = described_class.tags_hash(tagged_template["Resources"][jd_key]["Properties"]["Tags"])
      expect(tags_hash["team"]).to eq("data-eng")
      expect(tags_hash["cost_center"]).to eq("12345")
    end
  end

  describe "custom tags from Pipeline" do
    let(:pipeline_with_tags) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "pipeline-tags"

        tags environment: "prod", owner: "platform-team"
        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:pipeline_tagged_template) do
      described_class.new(
        pipeline: pipeline_with_tags,
        steps: {process: step_class},
        stage: "production",
        config: config
      ).generate
    end

    it "merges custom pipeline tags into all resources" do
      sm_resource = pipeline_tagged_template["Resources"]["StateMachine"]
      tags_hash = described_class.tags_hash(sm_resource["Properties"]["Tags"])
      expect(tags_hash["environment"]).to eq("prod")
      expect(tags_hash["owner"]).to eq("platform-team")
    end
  end

  describe "legacy tags preserved" do
    it "preserves the stack tag" do
      sm_resource = template["Resources"]["StateMachine"]
      tags_hash = described_class.tags_hash(sm_resource["Properties"]["Tags"])
      expect(tags_hash["stack"]).to eq("turbofan")
    end

    it "preserves the stack-type tag" do
      sm_resource = template["Resources"]["StateMachine"]
      tags_hash = described_class.tags_hash(sm_resource["Properties"]["Tags"])
      expect(tags_hash["stack-type"]).to eq("production")
    end

    it "preserves the stack-component tag" do
      sm_resource = template["Resources"]["StateMachine"]
      tags_hash = described_class.tags_hash(sm_resource["Properties"]["Tags"])
      expect(tags_hash["stack-component"]).to eq("tag-pipeline")
    end
  end
end
