# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Turbofan::Generators::CloudFormation, :schemas do
  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::TestCe", klass)
    klass
  end

  describe "execution :lambda step" do
    let(:lambda_step) do
      ce_class
      Class.new do
        include Turbofan::Step
        execution :lambda
        compute_environment :test_ce
        ram 4
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      stub_const("FilterGkeys", lambda_step)
      Class.new do
        include Turbofan::Pipeline
        pipeline_name "lambda-cfn-test"
        pipeline do
          filter_gkeys(trigger_input)
        end
      end
    end

    let(:template) do
      Turbofan.config.bucket = "test-bucket"
      Turbofan.config.aws_account_id = "123456789012"
      Turbofan.config.default_region = "us-east-1"
      described_class.new(
        pipeline: pipeline_class, steps: {filter_gkeys: lambda_step},
        stage: "production", config: {}
      ).generate
    end

    it "generates a Lambda function resource" do
      expect(template["Resources"]).to have_key("LambdaStepFilterGkeys")
      lambda_fn = template["Resources"]["LambdaStepFilterGkeys"]
      expect(lambda_fn["Type"]).to eq("AWS::Lambda::Function")
    end

    it "uses PackageType Image" do
      lambda_fn = template["Resources"]["LambdaStepFilterGkeys"]
      expect(lambda_fn.dig("Properties", "PackageType")).to eq("Image")
    end

    it "sets ImageUri from ECR using default_region" do
      lambda_fn = template["Resources"]["LambdaStepFilterGkeys"]
      image_uri = lambda_fn.dig("Properties", "Code", "ImageUri")
      expect(image_uri).to include("123456789012.dkr.ecr.us-east-1.amazonaws.com")
      expect(image_uri).to include("filter_gkeys")
    end

    it "sets MemorySize from ram in MB" do
      lambda_fn = template["Resources"]["LambdaStepFilterGkeys"]
      expect(lambda_fn.dig("Properties", "MemorySize")).to eq(4096)
    end

    it "caps Timeout at 900 seconds" do
      lambda_fn = template["Resources"]["LambdaStepFilterGkeys"]
      expect(lambda_fn.dig("Properties", "Timeout")).to be <= 900
    end

    it "sets ImageConfig with aws_lambda_ric entrypoint" do
      lambda_fn = template["Resources"]["LambdaStepFilterGkeys"]
      image_config = lambda_fn.dig("Properties", "ImageConfig")
      expect(image_config["EntryPoint"]).to eq(["/usr/local/bin/aws_lambda_ric"])
      expect(image_config["Command"].first).to include("LambdaHandler")
    end

    it "sets _HANDLER-compatible environment" do
      lambda_fn = template["Resources"]["LambdaStepFilterGkeys"]
      env = lambda_fn.dig("Properties", "Environment", "Variables")
      expect(env["TURBOFAN_BUCKET"]).to eq(Turbofan.config.bucket)
    end

    it "generates a Lambda IAM role" do
      expect(template["Resources"]).to have_key("LambdaStepRoleFilterGkeys")
      role = template["Resources"]["LambdaStepRoleFilterGkeys"]
      expect(role["Type"]).to eq("AWS::IAM::Role")
    end

    it "does NOT generate a Batch job definition" do
      batch_keys = template["Resources"].keys.select { |k| k.start_with?("JobDef") }
      lambda_related = batch_keys.select { |k| k.include?("FilterGkeys") }
      expect(lambda_related).to be_empty
    end

    it "SFN role grants lambda:InvokeFunction on the Lambda step" do
      sfn_role = template["Resources"]["SfnRole"]
      policies = sfn_role.dig("Properties", "Policies")
      lambda_policy = policies.find { |p| p["PolicyName"] == "LambdaInvoke" }
      expect(lambda_policy).not_to be_nil

      resources = lambda_policy.dig("PolicyDocument", "Statement", 0, "Resource")
      resources = [resources] unless resources.is_a?(Array)
      lambda_ref = resources.find { |r| r.is_a?(Hash) && r.dig("Fn::GetAtt", 0) == "LambdaStepFilterGkeys" }
      expect(lambda_ref).not_to be_nil
    end
  end
end
