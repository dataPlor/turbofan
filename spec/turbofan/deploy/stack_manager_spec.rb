# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Deploy::StackManager do
  before do
    Turbofan.config.bucket = "turbofan-shared-bucket"
  end

  let(:cf_client) { instance_double(Aws::CloudFormation::Client) }
  let(:stack_name) { "turbofan-test-pipeline-production" }
  let(:template_body) { '{"AWSTemplateFormatVersion": "2010-09-09"}' }

  describe ".detect_state" do
    it "returns :create_complete for CREATE_COMPLETE" do
      allow(cf_client).to receive(:describe_stacks).and_return(
        double(stacks: [double(stack_status: "CREATE_COMPLETE")])
      )
      expect(described_class.detect_state(cf_client, stack_name)).to eq(:create_complete)
    end

    it "returns :update_complete for UPDATE_COMPLETE" do
      allow(cf_client).to receive(:describe_stacks).and_return(
        double(stacks: [double(stack_status: "UPDATE_COMPLETE")])
      )
      expect(described_class.detect_state(cf_client, stack_name)).to eq(:update_complete)
    end

    it "returns :update_rollback_complete for UPDATE_ROLLBACK_COMPLETE" do
      allow(cf_client).to receive(:describe_stacks).and_return(
        double(stacks: [double(stack_status: "UPDATE_ROLLBACK_COMPLETE")])
      )
      expect(described_class.detect_state(cf_client, stack_name)).to eq(:update_rollback_complete)
    end

    it "returns :rollback_complete for ROLLBACK_COMPLETE" do
      allow(cf_client).to receive(:describe_stacks).and_return(
        double(stacks: [double(stack_status: "ROLLBACK_COMPLETE")])
      )
      expect(described_class.detect_state(cf_client, stack_name)).to eq(:rollback_complete)
    end

    it "returns :in_progress for CREATE_IN_PROGRESS" do
      allow(cf_client).to receive(:describe_stacks).and_return(
        double(stacks: [double(stack_status: "CREATE_IN_PROGRESS")])
      )
      expect(described_class.detect_state(cf_client, stack_name)).to eq(:in_progress)
    end

    it "returns :in_progress for UPDATE_IN_PROGRESS" do
      allow(cf_client).to receive(:describe_stacks).and_return(
        double(stacks: [double(stack_status: "UPDATE_IN_PROGRESS")])
      )
      expect(described_class.detect_state(cf_client, stack_name)).to eq(:in_progress)
    end

    it "returns :in_progress for DELETE_IN_PROGRESS" do
      allow(cf_client).to receive(:describe_stacks).and_return(
        double(stacks: [double(stack_status: "DELETE_IN_PROGRESS")])
      )
      expect(described_class.detect_state(cf_client, stack_name)).to eq(:in_progress)
    end

    it "returns :does_not_exist when stack is not found" do
      allow(cf_client).to receive(:describe_stacks).and_raise(
        Aws::CloudFormation::Errors::ValidationError.new(nil, "Stack with id #{stack_name} does not exist")
      )
      expect(described_class.detect_state(cf_client, stack_name)).to eq(:does_not_exist)
    end
  end

  describe ".deploy" do
    before do
      allow(cf_client).to receive(:create_change_set)
      allow(cf_client).to receive(:execute_change_set)
      allow(cf_client).to receive_messages(describe_change_set: double(status: "CREATE_COMPLETE", changes: []), describe_stacks: double(stacks: [double(stack_status: "CREATE_COMPLETE")]))
      allow(described_class).to receive(:sleep)
    end

    context "when stack does not exist" do
      before do
        allow(described_class).to receive(:detect_state).and_return(:does_not_exist)
      end

      it "creates a changeset with type CREATE and CAPABILITY_NAMED_IAM" do
        described_class.deploy(cf_client, stack_name: stack_name, template_body: template_body)

        expect(cf_client).to have_received(:create_change_set).with(
          hash_including(
            change_set_type: "CREATE",
            capabilities: ["CAPABILITY_NAMED_IAM"]
          )
        )
      end
    end

    context "when stack is in CREATE_COMPLETE state" do
      before do
        allow(described_class).to receive(:detect_state).and_return(:create_complete)
        allow(cf_client).to receive(:describe_stacks).and_return(
          double(stacks: [double(stack_status: "UPDATE_COMPLETE")])
        )
      end

      it "creates a changeset with type UPDATE" do
        described_class.deploy(cf_client, stack_name: stack_name, template_body: template_body)

        expect(cf_client).to have_received(:create_change_set).with(
          hash_including(change_set_type: "UPDATE")
        )
      end
    end

    context "when stack is in UPDATE_COMPLETE state" do
      before do
        allow(described_class).to receive(:detect_state).and_return(:update_complete)
        allow(cf_client).to receive(:describe_stacks).and_return(
          double(stacks: [double(stack_status: "UPDATE_COMPLETE")])
        )
      end

      it "creates a changeset with type UPDATE" do
        described_class.deploy(cf_client, stack_name: stack_name, template_body: template_body)

        expect(cf_client).to have_received(:create_change_set).with(
          hash_including(change_set_type: "UPDATE")
        )
      end
    end

    context "when stack is in UPDATE_ROLLBACK_COMPLETE state" do
      before do
        allow(described_class).to receive(:detect_state).and_return(:update_rollback_complete)
        allow(cf_client).to receive(:describe_stacks).and_return(
          double(stacks: [double(stack_status: "UPDATE_COMPLETE")])
        )
      end

      it "creates a changeset with type UPDATE" do
        described_class.deploy(cf_client, stack_name: stack_name, template_body: template_body)

        expect(cf_client).to have_received(:create_change_set).with(
          hash_including(change_set_type: "UPDATE")
        )
      end
    end

    context "when stack is in ROLLBACK_COMPLETE state" do
      before do
        allow(described_class).to receive(:detect_state).and_return(:rollback_complete)
        allow(cf_client).to receive(:delete_stack)

        # First call raises (stack deleted), subsequent calls return CREATE_COMPLETE
        call_count = 0
        allow(cf_client).to receive(:describe_stacks) do
          call_count += 1
          if call_count == 1
            raise Aws::CloudFormation::Errors::ValidationError.new(nil, "Stack does not exist")
          else
            double(stacks: [double(stack_status: "CREATE_COMPLETE")])
          end
        end
      end

      it "deletes the stack before creating" do
        described_class.deploy(cf_client, stack_name: stack_name, template_body: template_body)

        expect(cf_client).to have_received(:delete_stack).with(stack_name: stack_name)
      end

      it "creates a changeset with type CREATE after deletion" do
        described_class.deploy(cf_client, stack_name: stack_name, template_body: template_body)

        expect(cf_client).to have_received(:create_change_set).with(
          hash_including(change_set_type: "CREATE")
        )
      end
    end

    context "when stack is in_progress" do
      before do
        allow(described_class).to receive(:detect_state).and_return(:in_progress)
      end

      it "raises an error" do
        expect {
          described_class.deploy(cf_client, stack_name: stack_name, template_body: template_body)
        }.to raise_error(/in progress/i)
      end
    end

    context "when artifacts are provided" do
      let(:s3_client) { instance_double(Aws::S3::Client) }

      before do
        allow(described_class).to receive(:detect_state).and_return(:does_not_exist)
        allow(s3_client).to receive(:put_object)
      end

      it "uploads artifacts to S3 before creating the changeset" do
        artifacts = [{bucket: "my-bucket", key: "path/handler.zip", body: "zipdata"}]
        described_class.deploy(cf_client, stack_name: stack_name, template_body: template_body, s3_client: s3_client, artifacts: artifacts)

        expect(s3_client).to have_received(:put_object).with(bucket: "my-bucket", key: "path/handler.zip", body: "zipdata")
      end
    end

    context "when changeset has no changes" do
      before do
        allow(described_class).to receive(:detect_state).and_return(:update_complete)
        allow(cf_client).to receive(:describe_change_set).and_return(
          double(status: "FAILED", status_reason: "No changes to deploy", changes: [])
        )
        allow(cf_client).to receive(:delete_change_set)
      end

      it "reports already up to date without executing" do
        expect {
          described_class.deploy(cf_client, stack_name: stack_name, template_body: template_body)
        }.not_to raise_error

        expect(cf_client).not_to have_received(:execute_change_set)
      end
    end
  end

  describe ".wait_for_stack (private, tested via send)" do
    it "polls until target state is reached" do
      call_count = 0
      allow(cf_client).to receive(:describe_stacks) do
        call_count += 1
        if call_count < 3
          double(stacks: [double(stack_status: "CREATE_IN_PROGRESS")])
        else
          double(stacks: [double(stack_status: "CREATE_COMPLETE")])
        end
      end

      allow(described_class).to receive(:sleep)

      described_class.send(
        :wait_for_stack,
        cf_client,
        stack_name: stack_name,
        target_states: ["CREATE_COMPLETE"]
      )

      expect(cf_client).to have_received(:describe_stacks).at_least(3).times
    end

    it "raises on failure state" do
      allow(cf_client).to receive_messages(describe_stacks: double(stacks: [double(stack_status: "CREATE_FAILED")]), describe_stack_events: double(stack_events: []))
      allow(described_class).to receive(:sleep)

      expect {
        described_class.send(:wait_for_stack, cf_client, stack_name: stack_name, target_states: ["CREATE_COMPLETE"])
      }.to raise_error(/failure state/)
    end
  end

  describe ".describe_changes (private, tested via send)" do
    it "shows replacement status when a resource will be replaced" do
      changes = [
        double(resource_change: double(
          action: "Modify", resource_type: "AWS::Batch::ComputeEnvironment",
          logical_resource_id: "ComputeEnvironment", replacement: "True"
        ))
      ]
      allow(cf_client).to receive(:describe_change_set).and_return(
        double(changes: changes)
      )

      output = capture_stdout do
        described_class.send(:describe_changes, cf_client, stack_name: stack_name, changeset_name: "cs-1")
      end

      expect(output).to include("[REPLACEMENT: True]")
    end

    it "warns about dependent stacks when replacement is detected" do
      changes = [
        double(resource_change: double(
          action: "Modify", resource_type: "AWS::Batch::ComputeEnvironment",
          logical_resource_id: "ComputeEnvironment", replacement: "True"
        ))
      ]
      allow(cf_client).to receive(:describe_change_set).and_return(
        double(changes: changes)
      )

      stderr = capture_stderr do
        capture_stdout do
          described_class.send(:describe_changes, cf_client, stack_name: stack_name, changeset_name: "cs-1")
        end
      end

      expect(stderr).to include("WARNING")
      expect(stderr).to include("ImportValue")
    end

    it "warns when replacement is Conditional" do
      changes = [
        double(resource_change: double(
          action: "Modify", resource_type: "AWS::Batch::ComputeEnvironment",
          logical_resource_id: "ComputeEnvironment", replacement: "Conditional"
        ))
      ]
      allow(cf_client).to receive(:describe_change_set).and_return(
        double(changes: changes)
      )

      stderr = capture_stderr do
        capture_stdout do
          described_class.send(:describe_changes, cf_client, stack_name: stack_name, changeset_name: "cs-1")
        end
      end

      expect(stderr).to include("WARNING")
    end

    it "does not show replacement for normal updates" do
      changes = [
        double(resource_change: double(
          action: "Modify", resource_type: "AWS::Batch::ComputeEnvironment",
          logical_resource_id: "ComputeEnvironment", replacement: "False"
        ))
      ]
      allow(cf_client).to receive(:describe_change_set).and_return(
        double(changes: changes)
      )

      output = capture_stdout do
        described_class.send(:describe_changes, cf_client, stack_name: stack_name, changeset_name: "cs-1")
      end

      expect(output).not_to include("REPLACEMENT")
    end
  end

  describe ".backoff_sleep" do
    it "starts at base delay" do
      allow(described_class).to receive(:sleep)
      allow(described_class).to receive(:rand).and_return(0.5)

      described_class.backoff_sleep(0, base: 2, max: 10)

      expect(described_class).to have_received(:sleep).with(2.5)
    end

    it "doubles delay on each attempt" do
      allow(described_class).to receive(:sleep)
      allow(described_class).to receive(:rand).and_return(0.0)

      described_class.backoff_sleep(2, base: 2, max: 60)

      expect(described_class).to have_received(:sleep).with(8.0)
    end

    it "caps at max delay" do
      allow(described_class).to receive(:sleep)
      allow(described_class).to receive(:rand).and_return(0.0)

      described_class.backoff_sleep(10, base: 2, max: 10)

      expect(described_class).to have_received(:sleep).with(10.0)
    end

    it "skips jitter when jitter: false" do
      allow(described_class).to receive(:sleep)

      described_class.backoff_sleep(0, base: 5, max: 30, jitter: false)

      expect(described_class).to have_received(:sleep).with(5)
    end
  end
end
