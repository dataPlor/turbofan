require "spec_helper"
require "tmpdir"
require "fileutils"

RSpec.describe Turbofan::CLI::Deploy do
  let(:tmpdir) { Dir.mktmpdir("turbofan-deploy-test", SPEC_TMP_ROOT) }
  let(:pipeline_name) { "test_pipeline" }
  let(:stage) { "production" }
  let(:stack_name) { "turbofan-test-pipeline-production" }
  let(:pipeline_stack_name) { stack_name }

  let(:pipeline_class) do
    Class.new do
      include Turbofan::Pipeline

      pipeline_name "test-pipeline"
    end
  end

  let(:step_class) do
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      execution :batch
      cpu 2
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  let(:load_result) do
    double(
      pipeline: pipeline_class,
      steps: {process: step_class},
      step_dirs: {process: File.join(tmpdir, "steps", "process")}
    )
  end

  let(:cf_client) { instance_double(Aws::CloudFormation::Client) }
  let(:ecr_client) { instance_double(Aws::ECR::Client) }

  before do
    FileUtils.mkdir_p(File.join(tmpdir, "steps", "process"))
    FileUtils.mkdir_p(File.join(tmpdir, "schemas"))

    allow(Turbofan::Deploy::PipelineLoader).to receive(:load).and_return(load_result)
    allow(Turbofan::CLI::Check).to receive(:call)
    allow(Turbofan::CLI::Deploy::Preflight).to receive_messages(buildkit_available?: true, aws_credentials_valid?: true, git_clean?: true)
    allow(Turbofan::CLI::Deploy::Preflight).to receive(:warn_running_executions)
    allow(Turbofan::Deploy::ImageBuilder).to receive_messages(content_tag: "sha-abc123def456", authenticate_ecr: "123456789.dkr.ecr.us-east-1.amazonaws.com")
    allow(Turbofan::Deploy::ImageBuilder).to receive(:build_and_push)
    allow(Turbofan::Deploy::StackManager).to receive_messages(stack_output: "arn:aws:states:us-east-1:123:stateMachine:test", detect_state: :create_complete)
    allow(Turbofan::Deploy::StackManager).to receive(:deploy)
    allow(Turbofan::Generators::CloudFormation).to receive(:new).and_return(double(generate: {"Resources" => {}}, lambda_artifacts: []))
    allow(Aws::CloudFormation::Client).to receive(:new).and_return(cf_client)
    allow(Aws::ECR::Client).to receive(:new).and_return(ecr_client)
    allow(Aws::States::Client).to receive(:new).and_return(instance_double(Aws::States::Client))
  end

  after { FileUtils.rm_rf(tmpdir) }

  describe ".call" do
    it "loads the pipeline via PipelineLoader" do
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).with(anything, pipeline_stack_name).and_return(:does_not_exist)

      described_class.call(pipeline_name: pipeline_name, stage: stage)

      expect(Turbofan::Deploy::PipelineLoader).to have_received(:load)
    end

    it "runs pre-flight checks" do
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).with(anything, pipeline_stack_name).and_return(:does_not_exist)

      described_class.call(pipeline_name: pipeline_name, stage: stage)

      expect(Turbofan::CLI::Check).to have_received(:call)
    end

    it "computes image tags for each step" do
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).with(anything, pipeline_stack_name).and_return(:does_not_exist)

      described_class.call(pipeline_name: pipeline_name, stage: stage)

      expect(Turbofan::Deploy::ImageBuilder).to have_received(:content_tag)
    end
  end

  describe "first deploy flow (stack does not exist)" do
    before do
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).with(anything, pipeline_stack_name).and_return(:does_not_exist)
    end

    it "deploys stack first, then builds/pushes images" do
      deploy_order = []
      allow(Turbofan::Deploy::StackManager).to receive(:deploy) { deploy_order << :deploy_stack }
      allow(Turbofan::Deploy::ImageBuilder).to receive(:authenticate_ecr) do
        deploy_order << :authenticate
        "123456789.dkr.ecr.us-east-1.amazonaws.com"
      end
      allow(Turbofan::Deploy::ImageBuilder).to receive(:build_and_push) { deploy_order << :build_push }

      described_class.call(pipeline_name: pipeline_name, stage: stage)

      expect(deploy_order).to eq([:deploy_stack, :authenticate, :build_push])
    end
  end

  describe "subsequent deploy flow (stack exists)" do
    before do
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).and_return(:update_complete)
    end

    it "builds/pushes images first, then deploys stack" do
      deploy_order = []
      allow(Turbofan::Deploy::ImageBuilder).to receive(:authenticate_ecr) do
        deploy_order << :authenticate
        "123456789.dkr.ecr.us-east-1.amazonaws.com"
      end
      allow(Turbofan::Deploy::ImageBuilder).to receive(:build_and_push) { deploy_order << :build_push }
      allow(Turbofan::Deploy::StackManager).to receive(:deploy) { deploy_order << :deploy_stack }

      described_class.call(pipeline_name: pipeline_name, stage: stage)

      expect(deploy_order).to eq([:authenticate, :build_push, :deploy_stack])
    end
  end

  describe "pre-flight failure" do
    it "aborts early when checks fail" do
      allow(Turbofan::CLI::Check).to receive(:call).and_raise("Pre-flight check failed")

      expect {
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      }.to raise_error(/Pre-flight check failed/)

      expect(Turbofan::Deploy::StackManager).not_to have_received(:deploy)
      expect(Turbofan::Deploy::ImageBuilder).not_to have_received(:build_and_push)
    end
  end

  describe "dry-run mode" do
    before do
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).and_return(:update_complete)
      allow(cf_client).to receive(:create_change_set)
      allow(cf_client).to receive(:describe_change_set).and_return(changeset_description)
      allow(cf_client).to receive(:delete_change_set)
      allow(cf_client).to receive(:execute_change_set)
    end

    let(:changeset_description) do
      double(
        status: "CREATE_COMPLETE",
        status_reason: nil,
        changes: [
          double(resource_change: double(
            action: "Modify",
            resource_type: "AWS::Batch::JobDefinition",
            logical_resource_id: "ProcessJobDef",
            replacement: "False"
          ))
        ]
      )
    end

    it "does not call StackManager.deploy" do
      capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage, dry_run: true)
      end

      expect(Turbofan::Deploy::StackManager).not_to have_received(:deploy)
    end

    it "creates a changeset, describes it, then deletes it" do
      capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage, dry_run: true)
      end

      expect(cf_client).to have_received(:create_change_set)
      expect(cf_client).to have_received(:describe_change_set).at_least(:once)
      expect(cf_client).to have_received(:delete_change_set)
    end

    it "does not execute the changeset" do
      capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage, dry_run: true)
      end

      expect(cf_client).not_to have_received(:execute_change_set)
    end

    it "prints the changeset summary to stdout" do
      output = capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage, dry_run: true)
      end

      expect(output).to match(/modify|change/i)
    end

    it "does not build or push images" do
      capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage, dry_run: true)
      end

      expect(Turbofan::Deploy::ImageBuilder).not_to have_received(:authenticate_ecr)
      expect(Turbofan::Deploy::ImageBuilder).not_to have_received(:build_and_push)
    end
  end

  describe "dry-run on first deploy (stack does not exist)" do
    before do
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).with(anything, pipeline_stack_name).and_return(:does_not_exist)
      allow(cf_client).to receive_messages(create_change_set: double(id: "cs-1"), describe_change_set: changeset_description)
      allow(cf_client).to receive(:delete_change_set)
    end

    let(:changeset_description) do
      double(
        status: "CREATE_COMPLETE",
        status_reason: nil,
        changes: [
          double(resource_change: double(
            action: "Add",
            resource_type: "AWS::StepFunctions::StateMachine",
            logical_resource_id: "StateMachine",
            replacement: nil
          ))
        ]
      )
    end

    it "creates a CREATE changeset type" do
      capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage, dry_run: true)
      end

      expect(cf_client).to have_received(:create_change_set).with(
        hash_including(change_set_type: "CREATE")
      )
    end

    it "does not call StackManager.deploy" do
      capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage, dry_run: true)
      end

      expect(Turbofan::Deploy::StackManager).not_to have_received(:deploy)
    end
  end

  describe "CE stack pre-flight check" do
    let(:ce_class) do
      klass = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::TestCe", klass)
      klass
    end

    let(:step_with_ce) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        compute_environment :test_ce
        execution :batch
        cpu 2
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:load_result_with_ce) do
      double(
        pipeline: pipeline_class,
        steps: {process: step_with_ce},
        step_dirs: {process: File.join(tmpdir, "steps", "process")}
      )
    end

    it "verifies CE stacks exist before deploying pipeline" do
      allow(Turbofan::Deploy::PipelineLoader).to receive(:load).and_return(load_result_with_ce)
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state)
        .with(cf_client, ce_class.stack_name(stage))
        .and_return(:does_not_exist)

      expect {
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      }.to raise_error(/compute environment.*stack.*not found|deploy.*compute environment.*first/i)
    end

    it "errors when resources stack does not exist for steps with consumable resources" do
      resource_class = Class.new do
        include Turbofan::Resource

        key :duckdb
        consumable 10
      end
      stub_const("Resources::Duckdb", resource_class)

      step_with_resource = Class.new do
        include Turbofan::Step

        compute_environment :test_ce
        execution :batch
        cpu 2
        uses :duckdb
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end

      load_result_with_resource = double(
        pipeline: pipeline_class,
        steps: {process: step_with_resource},
        step_dirs: {process: File.join(tmpdir, "steps", "process")}
      )

      allow(Turbofan::Deploy::PipelineLoader).to receive(:load).and_return(load_result_with_resource)
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).and_return(:create_complete)
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state)
        .with(cf_client, "turbofan-resources-production")
        .and_return(:does_not_exist)

      expect {
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      }.to raise_error(/Resources stack.*not found.*Deploy resources first/i)
    end

    it "proceeds when CE stacks exist" do
      allow(Turbofan::Deploy::PipelineLoader).to receive(:load).and_return(load_result_with_ce)
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).and_return(:create_complete)

      expect {
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      }.not_to raise_error
    end
  end

  describe "first deploy rollback on image build failure" do
    before do
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state)
        .with(anything, pipeline_stack_name).and_return(:does_not_exist)
      allow(Turbofan::Deploy::StackManager).to receive(:wait_for_stack)
      allow(cf_client).to receive(:delete_stack)
      allow(Turbofan::Deploy::ImageBuilder).to receive(:build_and_push)
        .and_raise("Command failed: docker build")
    end

    it "deletes the stack when image build fails on first deploy" do
      expect {
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      }.to raise_error(/Command failed: docker build/)

      expect(cf_client).to have_received(:delete_stack).with(stack_name: stack_name)
    end

    it "waits for stack deletion to complete" do
      expect {
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      }.to raise_error(/Command failed/)

      expect(Turbofan::Deploy::StackManager).to have_received(:wait_for_stack).with(
        cf_client,
        stack_name: stack_name,
        target_states: ["DELETE_COMPLETE"]
      )
    end

    it "re-raises the original error after rollback" do
      expect {
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      }.to raise_error(RuntimeError, /Command failed: docker build/)
    end
  end
end
