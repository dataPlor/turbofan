# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Turbofan::CLI::Rollback" do
  let(:rollback_class) { Turbofan::CLI::Rollback }
  let(:pipeline_name) { "test_pipeline" }
  let(:stage) { "production" }
  let(:stack_name) { "turbofan-test-pipeline-production" }
  let(:cf_client) { instance_double(Aws::CloudFormation::Client) }

  before do
    allow(Aws::CloudFormation::Client).to receive(:new).and_return(cf_client)
  end

  describe ".call" do
    context "when stack is in UPDATE_COMPLETE state" do
      before do
        allow(Turbofan::Deploy::StackManager).to receive(:detect_state)
          .with(cf_client, stack_name)
          .and_return(:update_complete)

        allow(cf_client).to receive(:update_stack)
        allow(cf_client).to receive(:describe_stacks).and_return(
          double(stacks: [double(stack_status: "UPDATE_COMPLETE")])
        )
      end

      it "calls update_stack with use_previous_template" do
        capture_stdout do
          rollback_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(cf_client).to have_received(:update_stack).with(
          stack_name: stack_name,
          use_previous_template: true,
          capabilities: ["CAPABILITY_NAMED_IAM"]
        )
      end

      it "prints a success message" do
        output = capture_stdout do
          rollback_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(output).to match(/rollback.*complete|rolled back/i)
      end
    end

    context "when stack is in UPDATE_ROLLBACK_COMPLETE state" do
      before do
        allow(Turbofan::Deploy::StackManager).to receive(:detect_state)
          .with(cf_client, stack_name)
          .and_return(:update_rollback_complete)

        allow(cf_client).to receive(:update_stack)
        allow(cf_client).to receive(:describe_stacks).and_return(
          double(stacks: [double(stack_status: "UPDATE_COMPLETE")])
        )
      end

      it "calls update_stack with use_previous_template" do
        capture_stdout do
          rollback_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(cf_client).to have_received(:update_stack).with(
          stack_name: stack_name,
          use_previous_template: true,
          capabilities: ["CAPABILITY_NAMED_IAM"]
        )
      end
    end

    context "when stack does not exist" do
      before do
        allow(Turbofan::Deploy::StackManager).to receive(:detect_state)
          .with(cf_client, stack_name)
          .and_return(:does_not_exist)
      end

      it "raises an error" do
        expect {
          rollback_class.call(pipeline_name: pipeline_name, stage: stage)
        }.to raise_error(/stack does not exist/i)
      end
    end

    context "when stack is in CREATE_COMPLETE state (no previous deployment)" do
      before do
        allow(Turbofan::Deploy::StackManager).to receive(:detect_state)
          .with(cf_client, stack_name)
          .and_return(:create_complete)
      end

      it "raises an error about no previous deployment" do
        expect {
          rollback_class.call(pipeline_name: pipeline_name, stage: stage)
        }.to raise_error(/no previous deployment to rollback to/i)
      end
    end

    context "when stack is in an IN_PROGRESS state" do
      before do
        allow(Turbofan::Deploy::StackManager).to receive(:detect_state)
          .with(cf_client, stack_name)
          .and_return(:in_progress)
      end

      it "raises an error about operation in progress" do
        expect {
          rollback_class.call(pipeline_name: pipeline_name, stage: stage)
        }.to raise_error(/another operation is in progress/i)
      end
    end

    context "when stack is in ROLLBACK_COMPLETE state" do
      before do
        allow(Turbofan::Deploy::StackManager).to receive(:detect_state)
          .with(cf_client, stack_name)
          .and_return(:rollback_complete)
      end

      it "raises an error about unhandled state" do
        expect {
          rollback_class.call(pipeline_name: pipeline_name, stage: stage)
        }.to raise_error(/unhandled stack state/i)
      end
    end

    context "when stack enters failure state during rollback" do
      before do
        allow(Turbofan::Deploy::StackManager).to receive(:detect_state)
          .with(cf_client, stack_name)
          .and_return(:update_complete)

        allow(cf_client).to receive(:update_stack)
        allow(cf_client).to receive_messages(describe_stacks: double(stacks: [double(stack_status: "UPDATE_ROLLBACK_COMPLETE")]), describe_stack_events: double(stack_events: []))
      end

      it "raises an error about failure state" do
        expect {
          rollback_class.call(pipeline_name: pipeline_name, stage: stage)
        }.to raise_error(/failure state/)
      end
    end
  end
end
