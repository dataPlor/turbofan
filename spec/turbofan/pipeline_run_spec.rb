# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Pipeline.run", :schemas do # rubocop:disable RSpec/DescribeClass
  let(:cf_client) { instance_double(Aws::CloudFormation::Client) }
  let(:sfn_client) { instance_double(Aws::States::Client) }
  let(:state_machine_arn) { "arn:aws:states:us-east-1:123456789:stateMachine:turbofan-test-pipeline-production-statemachine" }
  let(:execution_arn) { "arn:aws:states:us-east-1:123456789:execution:sm:exec-abc123" }

  let(:step_class) do
    Class.new do
      include Turbofan::Step
      runs_on :batch
      compute_environment :test_ce
      cpu 1
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  let(:pipeline_class) do
    step_klass = step_class
    stub_const("RunTestStep", step_klass)
    Class.new do
      include Turbofan::Pipeline

      pipeline_name "test-pipeline"
      pipeline do
        run_test_step(trigger_input)
      end
    end
  end

  before do
    allow(Aws::CloudFormation::Client).to receive(:new).and_return(cf_client)
    allow(Aws::States::Client).to receive(:new).and_return(sfn_client)

    allow(cf_client).to receive(:describe_stacks).and_return(
      double(stacks: [double(
        outputs: [
          double(output_key: "StateMachineArn", output_value: state_machine_arn),
          double(output_key: "S3BucketName", output_value: "turbofan-test-pipeline-production-bucket")
        ]
      )])
    )

    allow(Turbofan::Deploy::Execution).to receive(:start).and_return(execution_arn)
  end

  describe ".run" do
    it "returns the execution ARN" do
      result = pipeline_class.run(stage: "production")
      expect(result).to eq(execution_arn)
    end

    it "derives the correct stack name from pipeline name" do
      pipeline_class.run(stage: "production")

      expect(cf_client).to have_received(:describe_stacks).with(
        stack_name: "turbofan-test-pipeline-production"
      )
    end

    it "calls Execution.start with the state machine ARN" do
      pipeline_class.run(stage: "production", input: {brand_id: 123})

      expect(Turbofan::Deploy::Execution).to have_received(:start).with(
        sfn_client,
        state_machine_arn: state_machine_arn,
        input: '{"brand_id":123}'
      )
    end

    it "defaults input to empty object" do
      pipeline_class.run(stage: "production")

      expect(Turbofan::Deploy::Execution).to have_received(:start).with(
        sfn_client,
        state_machine_arn: state_machine_arn,
        input: "{}"
      )
    end

    it "accepts an optional region parameter" do
      pipeline_class.run(stage: "production", region: "eu-west-1")

      expect(Aws::CloudFormation::Client).to have_received(:new).with(hash_including(region: "eu-west-1"))
      expect(Aws::States::Client).to have_received(:new).with(hash_including(region: "eu-west-1"))
    end
  end
end
