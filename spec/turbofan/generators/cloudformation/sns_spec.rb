# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Generators::CloudFormation, :schemas do
  before { Turbofan.config.bucket = "turbofan-shared-bucket" }

  describe "SNS topic (Task 18)" do
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
      stub_const("Process", step_class)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "sns-pipeline"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:config) do
      {
        vpc_id: "vpc-123",
        subnets: ["subnet-456", "subnet-789"],
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

    describe "SNS topic resource" do
      let(:sns_key) { template["Resources"].keys.find { |k| k.include?("SNS") || k.include?("Topic") || k.include?("Notification") } }
      let(:sns) { template["Resources"][sns_key] }

      it "generates an SNS topic resource" do
        expect(sns_key).not_to be_nil
      end

      it "creates an AWS::SNS::Topic resource type" do
        expect(sns["Type"]).to eq("AWS::SNS::Topic")
      end

      it "names the topic following convention: turbofan-{pipeline}-{stage}-notifications" do
        expect(sns["Properties"]["TopicName"]).to eq(
          "turbofan-sns-pipeline-production-notifications"
        )
      end

      it "has standard tags" do
        tags = sns["Properties"]["Tags"]
        expect(tags).to be_an(Array)
        tag_keys = tags.map { |t| t["Key"] }
        expect(tag_keys).to include("stack")
        expect(tag_keys).to include("stack-type")
        expect(tag_keys).to include("stack-component")
      end

      it "sets stack tag to turbofan" do
        tags = sns["Properties"]["Tags"]
        stack_tag = tags.find { |t| t["Key"] == "stack" }
        expect(stack_tag["Value"]).to eq("turbofan")
      end

      it "sets stack-type tag to the stage" do
        tags = sns["Properties"]["Tags"]
        stack_type = tags.find { |t| t["Key"] == "stack-type" }
        expect(stack_type["Value"]).to eq("production")
      end

      it "sets stack-component tag to the pipeline name" do
        tags = sns["Properties"]["Tags"]
        component = tags.find { |t| t["Key"] == "stack-component" }
        expect(component["Value"]).to eq("sns-pipeline")
      end
    end

    describe "staging stage SNS" do
      let(:staging_template) do
        described_class.new(
          pipeline: pipeline_class,
          steps: {process: step_class},
          stage: "staging",
          config: config
        ).generate
      end

      it "names the topic with staging" do
        sns_key = staging_template["Resources"].keys.find { |k| k.include?("SNS") || k.include?("Topic") || k.include?("Notification") }
        sns = staging_template["Resources"][sns_key]
        expect(sns["Properties"]["TopicName"]).to eq(
          "turbofan-sns-pipeline-staging-notifications"
        )
      end
    end
  end
end
