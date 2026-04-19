# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Generators::CloudFormation, "image_tags", :schemas do # rubocop:disable RSpec/DescribeMethod
  before { Turbofan.config.bucket = "turbofan-shared-bucket" }

  let(:step_class) do
    Class.new do
      include Turbofan::Step

      runs_on :batch
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

      pipeline_name "test-pipeline"
      pipeline do
        process(trigger_input)
      end
    end
  end

  let(:config) do
    {
      subnets: ["subnet-456"],
      security_groups: ["sg-abc"]
    }
  end

  describe "image_tags keyword" do
    let(:image_tags) { {process: "sha-abc123def456"} }

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_class},
        stage: "production",
        config: config,
        image_tags: image_tags
      )
    end

    let(:template) { generator.generate }

    it "accepts image_tags keyword in constructor" do
      expect { generator }.not_to raise_error
    end

    it "bakes the per-step image tag into the job definition image URI" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      image = template["Resources"][jd_key]["Properties"]["ContainerProperties"]["Image"]
      image_str = image["Fn::Sub"]
      expect(image_str).to include("sha-abc123def456")
    end

    it "does not use the ${ImageTag} parameter reference" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      image = template["Resources"][jd_key]["Properties"]["ContainerProperties"]["Image"]
      image_str = image["Fn::Sub"]
      expect(image_str).not_to include("${ImageTag}")
    end
  end

  describe "image_tags with multiple steps" do
    let(:step_a) do
      Class.new do
        include Turbofan::Step

        runs_on :batch
        compute_environment :test_ce
        cpu 2
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:step_b) do
      Class.new do
        include Turbofan::Step

        runs_on :batch
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

        pipeline_name "multi"
        pipeline do
          results = extract(trigger_input)
          transform(results)
        end
      end
    end

    let(:image_tags) { {extract: "sha-aaa111bbb222", transform: "sha-ccc333ddd444"} }

    let(:template) do
      described_class.new(
        pipeline: multi_pipeline,
        steps: {extract: step_a, transform: step_b},
        stage: "production",
        config: config,
        image_tags: image_tags
      ).generate
    end

    it "uses per-step image tags, not a shared tag" do
      jd_extract = template["Resources"]["JobDefExtract"]
      jd_transform = template["Resources"]["JobDefTransform"]

      extract_image = jd_extract["Properties"]["ContainerProperties"]["Image"]["Fn::Sub"]
      transform_image = jd_transform["Properties"]["ContainerProperties"]["Image"]["Fn::Sub"]

      expect(extract_image).to include("sha-aaa111bbb222")
      expect(transform_image).to include("sha-ccc333ddd444")
    end

    it "each step has a different tag in its image URI" do
      jd_extract = template["Resources"]["JobDefExtract"]
      jd_transform = template["Resources"]["JobDefTransform"]

      extract_image = jd_extract["Properties"]["ContainerProperties"]["Image"]["Fn::Sub"]
      transform_image = jd_transform["Properties"]["ContainerProperties"]["Image"]["Fn::Sub"]

      expect(extract_image).not_to eq(transform_image)
    end
  end

  describe "default behavior without image_tags" do
    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_class},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "falls back to 'latest' when no image_tags provided" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      image = template["Resources"][jd_key]["Properties"]["ContainerProperties"]["Image"]
      image_str = image["Fn::Sub"]
      expect(image_str).to end_with(":latest")
    end
  end

  describe "CF template Outputs section" do
    let(:template) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_class},
        stage: "production",
        config: config
      ).generate
    end

    it "has an Outputs section" do
      expect(template).to have_key("Outputs")
    end

    it "outputs StateMachineArn" do
      expect(template["Outputs"]).to have_key("StateMachineArn")
    end

    it "StateMachineArn references the StateMachine resource" do
      sm_output = template["Outputs"]["StateMachineArn"]
      expect(sm_output["Value"]).to eq({"Ref" => "StateMachine"})
    end

    it "does not output S3BucketName" do
      expect(template["Outputs"]).not_to have_key("S3BucketName")
    end
  end

  describe "no ImageTag parameter" do
    let(:template) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_class},
        stage: "production",
        config: config,
        image_tags: {process: "sha-abc123def456"}
      ).generate
    end

    it "does not have an ImageTag parameter" do
      params = template["Parameters"] || {}
      expect(params).not_to have_key("ImageTag")
    end
  end

end
