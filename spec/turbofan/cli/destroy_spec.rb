require "spec_helper"
require "stringio"

RSpec.describe Turbofan::CLI::Destroy do
  let(:cf_client) { instance_double(Aws::CloudFormation::Client) }
  let(:ecr_client) { instance_double(Aws::ECR::Client) }
  let(:input) { StringIO.new }
  let(:output) { StringIO.new }

  let(:stack_resources) do
    [
      double(resource_type: "AWS::Batch::ComputeEnvironment", logical_resource_id: "ComputeEnvironment", physical_resource_id: "ce-123"),
      double(resource_type: "AWS::Batch::JobQueue", logical_resource_id: "JobQueueProcess", physical_resource_id: "jq-456"),
      double(resource_type: "AWS::StepFunctions::StateMachine", logical_resource_id: "StateMachine", physical_resource_id: "sm-789")
    ]
  end

  before do
    Turbofan::CLI::Prompt.input = input
    Turbofan::CLI::Prompt.output = output
    allow(ecr_client).to receive(:describe_repositories).and_return(
      double(repositories: [])
    )
    allow(cf_client).to receive(:describe_stacks)
      .and_return(double(stacks: [double(stack_status: "CREATE_COMPLETE")]))
    allow(cf_client).to receive(:describe_stack_resources)
      .and_return(double(stack_resources: stack_resources))
    allow(cf_client).to receive(:delete_stack) do
      # After deletion, describe_stacks should raise (stack gone)
      allow(cf_client).to receive(:describe_stacks)
        .and_raise(Aws::CloudFormation::Errors::ValidationError.new(nil, "does not exist"))
    end
  end

  def make_tty
    allow(input).to receive(:tty?).and_return(true)
  end

  def type(*lines)
    input.string = lines.join("\n") + "\n"
  end

  describe "protected stage (production)" do
    let(:stage) { "production" }
    let(:stack_name) { "turbofan-my-pipeline-production" }

    context "when TTY with correct confirmation and yes to delete" do
      before { make_tty }

      it "deletes the stack" do
        type(stack_name, "y")

        capture_stdout do
          described_class.call(pipeline_name: "my_pipeline", stage: stage, cf_client: cf_client, ecr_client: ecr_client)
        end

        expect(cf_client).to have_received(:delete_stack).with(stack_name: stack_name)
      end

      it "lists resources before asking to delete" do
        type(stack_name, "y")

        stdout = capture_stdout do
          described_class.call(pipeline_name: "my_pipeline", stage: stage, cf_client: cf_client, ecr_client: ecr_client)
        end

        expect(stdout).to include("ComputeEnvironment")
        expect(stdout).to include("JobQueueProcess")
        expect(stdout).to include("StateMachine")
      end
    end

    context "when TTY with wrong confirmation" do
      before { make_tty }

      it "does NOT delete the stack" do
        type("wrong-input")

        capture_stdout do
          described_class.call(pipeline_name: "my_pipeline", stage: stage, cf_client: cf_client, ecr_client: ecr_client)
        end

        expect(cf_client).not_to have_received(:delete_stack)
      end

      it "does NOT list resources" do
        type("wrong-input")

        capture_stdout do
          described_class.call(pipeline_name: "my_pipeline", stage: stage, cf_client: cf_client, ecr_client: ecr_client)
        end

        expect(cf_client).not_to have_received(:describe_stack_resources)
      end
    end

    context "when TTY with correct confirmation and no to delete" do
      before { make_tty }

      it "does NOT delete the stack" do
        type(stack_name, "n")

        capture_stdout do
          described_class.call(pipeline_name: "my_pipeline", stage: stage, cf_client: cf_client, ecr_client: ecr_client)
        end

        expect(cf_client).not_to have_received(:delete_stack)
      end
    end

    context "without --force in non-TTY" do
      it "raises an error about using --force" do
        expect {
          described_class.call(pipeline_name: "my_pipeline", stage: stage, cf_client: cf_client, ecr_client: ecr_client)
        }.to raise_error(Thor::Error, /--force/)
      end
    end

    context "with --force" do
      it "deletes without any prompts" do
        capture_stdout do
          described_class.call(pipeline_name: "my_pipeline", stage: stage, force: true, cf_client: cf_client, ecr_client: ecr_client)
        end

        expect(cf_client).to have_received(:delete_stack).with(stack_name: stack_name)
        expect(output.string).to be_empty
      end
    end
  end

  describe "protected stage (staging)" do
    let(:stage) { "staging" }
    let(:stack_name) { "turbofan-my-pipeline-staging" }

    context "without --force in non-TTY" do
      it "raises an error about using --force" do
        expect {
          described_class.call(pipeline_name: "my_pipeline", stage: stage, cf_client: cf_client, ecr_client: ecr_client)
        }.to raise_error(Thor::Error, /--force/)
      end
    end

    context "with --force" do
      it "deletes without any prompts" do
        capture_stdout do
          described_class.call(pipeline_name: "my_pipeline", stage: stage, force: true, cf_client: cf_client, ecr_client: ecr_client)
        end

        expect(cf_client).to have_received(:delete_stack).with(stack_name: stack_name)
      end
    end
  end

  describe "non-protected stage (dev)" do
    let(:stage) { "dev" }
    let(:stack_name) { "turbofan-my-pipeline-dev" }

    context "when TTY with yes to delete" do
      before { make_tty }

      it "skips confirm_destructive and deletes after yes" do
        type("y")

        capture_stdout do
          described_class.call(pipeline_name: "my_pipeline", stage: stage, cf_client: cf_client, ecr_client: ecr_client)
        end

        expect(cf_client).to have_received(:delete_stack).with(stack_name: stack_name)
        expect(output.string).not_to include("Type '")
      end
    end

    context "when TTY with no to delete" do
      before { make_tty }

      it "does NOT delete the stack" do
        type("n")

        capture_stdout do
          described_class.call(pipeline_name: "my_pipeline", stage: stage, cf_client: cf_client, ecr_client: ecr_client)
        end

        expect(cf_client).not_to have_received(:delete_stack)
      end
    end

    context "with --force" do
      it "deletes without any prompts" do
        capture_stdout do
          described_class.call(pipeline_name: "my_pipeline", stage: stage, force: true, cf_client: cf_client, ecr_client: ecr_client)
        end

        expect(cf_client).to have_received(:delete_stack).with(stack_name: stack_name)
        expect(output.string).to be_empty
      end
    end
  end

  describe "resource count in prompt" do
    before { make_tty }

    it "includes the resource count in the yes/no prompt" do
      type("y")

      capture_stdout do
        described_class.call(pipeline_name: "my_pipeline", stage: "dev", cf_client: cf_client, ecr_client: ecr_client)
      end

      expect(output.string).to include("Delete 3 resources?")
    end
  end

  describe "pipeline name with underscores" do
    it "converts underscores to dashes in the stack name" do
      capture_stdout do
        described_class.call(pipeline_name: "my_cool_pipeline", stage: "test1", force: true, cf_client: cf_client, ecr_client: ecr_client)
      end

      expect(cf_client).to have_received(:describe_stack_resources)
        .with(stack_name: "turbofan-my-cool-pipeline-test1")
    end
  end

  describe "CloudFormation stack deletion" do
    it "calls delete_stack with correct stack name" do
      capture_stdout do
        described_class.call(pipeline_name: "my_pipeline", stage: "test1", force: true, cf_client: cf_client, ecr_client: ecr_client)
      end

      expect(cf_client).to have_received(:delete_stack).with(
        stack_name: "turbofan-my-pipeline-test1"
      )
    end
  end
end
