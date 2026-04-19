# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Generators::CloudFormation, "external container images", :schemas do # rubocop:disable RSpec/DescribeMethod
  before { Turbofan.config.bucket = "turbofan-shared-bucket" }

  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::ExternalCe", klass)
    klass
  end

  let(:external_step) do
    ce_class # ensure stub_const runs
    Class.new do
      include Turbofan::Step

      execution :batch
      compute_environment :test_ce
      cpu 2
      docker_image "123456789012.dkr.ecr.us-east-1.amazonaws.com/external-repo:v1.2.3"
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  let(:normal_step) do
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

  let(:config) do
    {
      vpc_id: "vpc-123",
      subnets: ["subnet-456"],
      security_groups: ["sg-abc"]
    }
  end

  describe "step with docker_image" do
    let(:pipeline_class) do
      step_klass = external_step
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "external-pipeline"
        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: external_step},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "uses the docker_image URI directly as the container image" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      container = template["Resources"][jd_key]["Properties"]["ContainerProperties"]
      image = container["Image"]
      expect(image).to eq("123456789012.dkr.ecr.us-east-1.amazonaws.com/external-repo:v1.2.3")
    end

    it "does not generate an ECR repository for the external step" do
      ecr_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::ECR::Repository"
      }
      expect(ecr_keys).to be_empty
    end

    it "still generates a job definition for the external step" do
      jd_keys = template["Resources"].keys.select { |k| k.start_with?("JobDef") }
      expect(jd_keys.size).to eq(1)
    end

    it "still generates a log group for the external step" do
      log_keys = template["Resources"].keys.select { |k| k.start_with?("LogGroup") }
      expect(log_keys.size).to eq(1)
    end
  end

  describe "external step still requires schemas" do
    it "step declares input_schema" do
      expect(external_step.turbofan_input_schema_file).to eq("passthrough.json")
    end

    it "step declares output_schema" do
      expect(external_step.turbofan_output_schema_file).to eq("passthrough.json")
    end

    it "step is marked as external" do
      expect(external_step.turbofan_external?).to be true
    end
  end

  describe "normal step is not external" do
    it "normal step is not marked as external" do
      expect(normal_step.turbofan_external?).to be false
    end
  end

  describe "mixed pipeline: external and normal steps" do
    let(:mixed_pipeline) do
      stub_const("Extract", normal_step)
      stub_const("Transform", external_step)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "mixed-external"
        pipeline do
          results = extract(trigger_input)
          transform(results)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: mixed_pipeline,
        steps: {extract: normal_step, transform: external_step},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "does not generate any ECR repositories (ECR is managed by image builder)" do
      ecr_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::ECR::Repository"
      }
      expect(ecr_keys).to be_empty
    end

    it "uses Fn::Sub ECR URI for the normal step" do
      jd_key = template["Resources"].keys.find { |k|
        k.start_with?("JobDef") && k.include?("Extract")
      }
      container = template["Resources"][jd_key]["Properties"]["ContainerProperties"]
      image = container["Image"]
      expect(image).to be_a(Hash)
      expect(image).to have_key("Fn::Sub")
    end

    it "uses the literal docker_image URI for the external step" do
      jd_key = template["Resources"].keys.find { |k|
        k.start_with?("JobDef") && k.include?("Transform")
      }
      container = template["Resources"][jd_key]["Properties"]["ContainerProperties"]
      image = container["Image"]
      expect(image).to eq("123456789012.dkr.ecr.us-east-1.amazonaws.com/external-repo:v1.2.3")
    end

    it "generates job definitions for both steps" do
      jd_keys = template["Resources"].keys.select { |k| k.start_with?("JobDef") }
      expect(jd_keys.size).to eq(2)
    end
  end

  describe "external step with different image URIs" do
    let(:dockerhub_step) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1
        docker_image "nginx:1.25-alpine"
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:dockerhub_pipeline) do
      step_klass = dockerhub_step
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "dockerhub-pipeline"
        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:template) do
      described_class.new(
        pipeline: dockerhub_pipeline,
        steps: {process: dockerhub_step},
        stage: "production",
        config: config
      ).generate
    end

    it "uses the Docker Hub image URI directly" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      container = template["Resources"][jd_key]["Properties"]["ContainerProperties"]
      image = container["Image"]
      expect(image).to eq("nginx:1.25-alpine")
    end
  end
end
