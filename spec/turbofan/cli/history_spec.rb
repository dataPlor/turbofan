# frozen_string_literal: true

require "spec_helper"

# B5 — History command
RSpec.describe Turbofan::CLI::History do
  let(:cf_client) { instance_double(Aws::CloudFormation::Client) }
  let(:sfn_client) { instance_double(Aws::States::Client) }
  let(:pipeline_name) { "my_pipeline" }
  let(:stage) { "production" }
  let(:stack_name) { "turbofan-my-pipeline-production" }
  let(:state_machine_arn) { "arn:aws:states:us-east-1:123:stateMachine:sm" }

  before do
    allow(Aws::CloudFormation::Client).to receive(:new).and_return(cf_client)
    allow(Aws::States::Client).to receive(:new).and_return(sfn_client)
    allow(Turbofan::Deploy::StackManager).to receive(:stack_output)
      .with(cf_client, stack_name, "StateMachineArn")
      .and_return(state_machine_arn)
  end

  describe "command registration" do
    it "is registered as a command on the CLI" do
      expect(Turbofan::CLI.commands).to have_key("history")
    end
  end

  describe ".call" do
    let(:start_time) { Time.now - 3600 }
    let(:stop_time) { Time.now - 1800 }

    context "with executions" do
      before do
        allow(sfn_client).to receive(:list_executions).and_return(
          double(executions: [
            double(
              execution_arn: "arn:exec:1",
              name: "run-abc123",
              status: "SUCCEEDED",
              start_date: start_time,
              stop_date: stop_time
            ),
            double(
              execution_arn: "arn:exec:2",
              name: "run-def456",
              status: "FAILED",
              start_date: start_time,
              stop_date: stop_time
            )
          ])
        )
      end

      it "outputs execution history" do
        output = capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(output).to include("run-abc123")
        expect(output).to include("SUCCEEDED")
        expect(output).to include("run-def456")
        expect(output).to include("FAILED")
      end
    end

    context "with empty execution list" do
      before do
        allow(sfn_client).to receive(:list_executions).and_return(
          double(executions: [])
        )
      end

      it "shows 'No executions found' message" do
        output = capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(output).to include("No executions found")
      end
    end

    context "with --limit flag" do
      before do
        allow(sfn_client).to receive(:list_executions).and_return(
          double(executions: [])
        )
      end

      it "passes limit to list_executions as max_results" do
        capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage, limit: 5)
        end

        expect(sfn_client).to have_received(:list_executions).with(
          hash_including(max_results: 5)
        )
      end
    end
  end
end
