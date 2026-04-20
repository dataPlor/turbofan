# frozen_string_literal: true

require "spec_helper"

# Exercises the trigger feature across DSL + check + CloudFormation
# generator as a single pipeline definition. Complements the
# single-layer specs by catching integration regressions — e.g. a
# DSL kwarg name that the generator forgot to read, or a check that
# passes a valid pipeline that the generator then fails on.
RSpec.describe "trigger end-to-end (DSL + check + generator)", :schemas do # rubocop:disable RSpec/DescribeClass, RSpec/DescribeMethod
  before { Turbofan.config.bucket = "turbofan-shared-bucket" }

  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::TriggerE2eCe", klass)
    klass
  end

  let(:step_class) do
    ce_class
    Class.new do
      include Turbofan::Step
      runs_on :batch
      compute_environment :trigger_e2e_ce
      cpu 2
      ram 4
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  let(:config) do
    {vpc_id: "vpc-123", subnets: ["subnet-456"], security_groups: ["sg-abc"]}
  end

  let(:pipeline_class) do
    step_klass = step_class
    stub_const("Process", step_klass)
    Class.new do
      include Turbofan::Pipeline
      pipeline_name "trigger-e2e"
      trigger :schedule, cron: "0 5 * * ? *"
      trigger :event,
        source: "aws.s3",
        detail_type: "Object Created",
        detail: {"bucket" => {"name" => ["incoming"]}}
      trigger :event, source: "myapp", event_bus: "ops-bus"
      pipeline { process(trigger_input) }
    end
  end

  it "passes check-time validation" do
    result = Turbofan::Check::PipelineCheck.run(
      pipeline: pipeline_class, steps: {process: step_class}
    )
    expect(result.errors).to be_empty, "expected no errors, got: #{result.errors.inspect}"
    expect(result.passed?).to be true
  end

  describe "CloudFormation generation" do
    let(:template) do
      Turbofan::Generators::CloudFormation.new(
        pipeline: pipeline_class,
        steps: {process: step_class},
        stage: "production",
        config: config
      ).generate
    end

    it "emits exactly three AWS::Events::Rule resources" do
      rules = template["Resources"].select { |_, v| v["Type"] == "AWS::Events::Rule" }
      expect(rules.keys.sort).to eq(%w[TriggerRule0 TriggerRule1 TriggerRule2])
    end

    it "wires the schedule trigger to a cron expression" do
      expect(template["Resources"]["TriggerRule0"]["Properties"]["ScheduleExpression"]).to eq("cron(0 5 * * ? *)")
    end

    it "wires the S3 trigger to the expected EventPattern" do
      pattern = template["Resources"]["TriggerRule1"]["Properties"]["EventPattern"]
      expect(pattern["source"]).to eq(["aws.s3"])
      expect(pattern["detail-type"]).to eq(["Object Created"])
      expect(pattern["detail"]).to eq({"bucket" => {"name" => ["incoming"]}})
    end

    it "sets EventBusName only on the custom-bus trigger" do
      expect(template["Resources"]["TriggerRule0"]["Properties"]).not_to have_key("EventBusName")
      expect(template["Resources"]["TriggerRule1"]["Properties"]).not_to have_key("EventBusName")
      expect(template["Resources"]["TriggerRule2"]["Properties"]["EventBusName"]).to eq("ops-bus")
    end

    it "gives each rule a dedicated Lambda::Permission scoped by SourceArn" do
      %w[TriggerRule0 TriggerRule1 TriggerRule2].each do |rule|
        perm = template["Resources"]["#{rule}Permission"]
        expect(perm["Type"]).to eq("AWS::Lambda::Permission")
        expect(perm["Properties"]["SourceArn"]).to eq({"Fn::GetAtt" => [rule, "Arn"]})
      end
    end

    it "shares a single GuardLambda + Role across all rules" do
      expect(template["Resources"]["GuardLambda"]["Type"]).to eq("AWS::Lambda::Function")
      expect(template["Resources"]["GuardLambdaRole"]["Type"]).to eq("AWS::IAM::Role")
    end
  end
end
