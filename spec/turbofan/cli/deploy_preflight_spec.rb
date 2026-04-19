# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"
require "aws-sdk-states"

RSpec.describe Turbofan::CLI::Deploy::Preflight do # rubocop:disable RSpec/MultipleDescribes
  describe ".buildkit_available?" do
    let(:success_status) { instance_double(Process::Status, success?: true) }
    let(:failure_status) { instance_double(Process::Status, success?: false) }

    it "returns true when docker buildx is present" do
      allow(Turbofan::Subprocess).to receive(:capture)
        .with("docker", "buildx", "version", allow_failure: true)
        .and_return(["", "", success_status])

      expect(described_class.buildkit_available?).to be true
    end

    it "returns false when docker buildx is absent" do
      allow(Turbofan::Subprocess).to receive(:capture)
        .with("docker", "buildx", "version", allow_failure: true)
        .and_return(["", "", failure_status])

      expect(described_class.buildkit_available?).to be false
    end

    it "returns false when docker command is not found (ENOENT)" do
      allow(Turbofan::Subprocess).to receive(:capture)
        .with("docker", "buildx", "version", allow_failure: true)
        .and_raise(Errno::ENOENT)

      expect(described_class.buildkit_available?).to be false
    end
  end

  describe ".aws_credentials_valid?" do
    let(:sts_client) { instance_double(Aws::STS::Client) }

    before do
      allow(Aws::STS::Client).to receive(:new).and_return(sts_client)
    end

    it "returns true when get_caller_identity succeeds" do
      allow(sts_client).to receive(:get_caller_identity).and_return(double(account: "123456789"))

      expect(described_class.aws_credentials_valid?).to be true
    end

    it "returns false when get_caller_identity raises a ServiceError" do
      allow(sts_client).to receive(:get_caller_identity)
        .and_raise(Aws::STS::Errors::ServiceError.new(double, "Access denied"))

      expect(described_class.aws_credentials_valid?).to be false
    end
  end

  describe ".git_clean?" do
    let(:status) { instance_double(Process::Status, success?: true) }

    it "returns true when git status --porcelain output is empty" do
      allow(Turbofan::Subprocess).to receive(:capture)
        .with("git", "status", "--porcelain", allow_failure: true)
        .and_return(["", "", status])

      expect(described_class.git_clean?).to be true
    end

    it "returns false when git status --porcelain output is non-empty" do
      allow(Turbofan::Subprocess).to receive(:capture)
        .with("git", "status", "--porcelain", allow_failure: true)
        .and_return([" M some_file.rb\n", "", status])

      expect(described_class.git_clean?).to be false
    end

    it "returns false when there are untracked files" do
      allow(Turbofan::Subprocess).to receive(:capture)
        .with("git", "status", "--porcelain", allow_failure: true)
        .and_return(["?? new_file.rb\n", "", status])

      expect(described_class.git_clean?).to be false
    end

    it "raises when git fails (e.g. not a repo) instead of fail-open 'clean'" do
      failure_status = instance_double(Process::Status, success?: false)
      allow(Turbofan::Subprocess).to receive(:capture)
        .with("git", "status", "--porcelain", allow_failure: true)
        .and_return(["", "fatal: not a git repository\n", failure_status])

      expect { described_class.git_clean? }.to raise_error(/git status failed/)
    end
  end

  describe ".warn_running_executions" do
    let(:sfn_client) { instance_double(Aws::States::Client) }
    let(:state_machine_arn) { "arn:aws:states:us-east-1:123456789:stateMachine:my-pipeline-production" }

    it "prints a WARNING with count and guidance when running executions exist" do
      executions = [double(execution_arn: "arn:aws:states:..."), double(execution_arn: "arn:aws:states:...2")]
      allow(sfn_client).to receive(:list_executions)
        .with(state_machine_arn: state_machine_arn, status_filter: "RUNNING")
        .and_return(double(executions: executions))

      output = capture_stdout do
        described_class.warn_running_executions(sfn_client, state_machine_arn)
      end

      expect(output).to match(/WARNING.*2 execution/i)
      expect(output).to match(/previous job definitions/i)
    end

    it "prints nothing when no running executions" do
      allow(sfn_client).to receive(:list_executions)
        .with(state_machine_arn: state_machine_arn, status_filter: "RUNNING")
        .and_return(double(executions: []))

      output = capture_stdout do
        described_class.warn_running_executions(sfn_client, state_machine_arn)
      end

      expect(output).to be_empty
    end
  end
end

RSpec.describe "Deploy pre-flight integration", type: :integration do # rubocop:disable RSpec/DescribeClass
  let(:tmpdir) { Dir.mktmpdir("turbofan-preflight-test", SPEC_TMP_ROOT) }
  let(:pipeline_name) { "my_pipeline" }
  let(:stage) { "production" }

  let(:pipeline_class) do
    Class.new do
      include Turbofan::Pipeline

      pipeline_name "my-pipeline"
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
    allow(Turbofan::Deploy::ImageBuilder).to receive(:build_and_push_all)
    allow(Turbofan::Deploy::StackManager).to receive(:deploy)
    allow(Turbofan::Deploy::StackManager).to receive_messages(detect_state: :create_complete, stack_output: "arn:aws:states:us-east-1:123:stateMachine:my-pipeline-production")
    allow(Turbofan::Generators::CloudFormation).to receive(:new).and_return(double(generate: {"Resources" => {}}, lambda_artifacts: []))
    allow(Aws::CloudFormation::Client).to receive(:new).and_return(cf_client)
    allow(Aws::ECR::Client).to receive(:new).and_return(ecr_client)
    allow(Aws::States::Client).to receive(:new).and_return(instance_double(Aws::States::Client))
  end

  after { FileUtils.rm_rf(tmpdir) }

  describe "BuildKit pre-flight check" do
    it "raises with install guidance when BuildKit is not available" do
      allow(Turbofan::CLI::Deploy::Preflight).to receive(:buildkit_available?).and_return(false)

      expect {
        Turbofan::CLI::Deploy.call(pipeline_name: pipeline_name, stage: stage)
      }.to raise_error(RuntimeError, /BuildKit not available.*Docker 23\.0\+/i)

      expect(Turbofan::Deploy::ImageBuilder).not_to have_received(:build_and_push_all)
      expect(Turbofan::Deploy::StackManager).not_to have_received(:deploy)
    end
  end

  describe "AWS STS credential pre-flight check" do
    it "raises with debugging guidance when credentials are invalid" do
      allow(Turbofan::CLI::Deploy::Preflight).to receive(:aws_credentials_valid?).and_return(false)

      expect {
        Turbofan::CLI::Deploy.call(pipeline_name: pipeline_name, stage: stage)
      }.to raise_error(RuntimeError, /AWS credentials invalid.*aws sts get-caller-identity/i)

      expect(Turbofan::Deploy::StackManager).not_to have_received(:deploy)
      expect(Turbofan::Deploy::ImageBuilder).not_to have_received(:build_and_push_all)
    end
  end

  describe "dirty git pre-flight check" do
    context "when deploying to production" do
      let(:stage) { "production" }

      it "raises with guidance when working tree is dirty" do
        allow(Turbofan::CLI::Deploy::Preflight).to receive(:git_clean?).and_return(false)

        expect {
          Turbofan::CLI::Deploy.call(pipeline_name: pipeline_name, stage: stage)
        }.to raise_error(RuntimeError, /Uncommitted changes detected.*commit or stash/i)
      end
    end

    context "when deploying to staging" do
      let(:stage) { "staging" }

      it "raises when working tree is dirty" do
        allow(Turbofan::CLI::Deploy::Preflight).to receive(:git_clean?).and_return(false)

        expect {
          Turbofan::CLI::Deploy.call(pipeline_name: pipeline_name, stage: stage)
        }.to raise_error(/Uncommitted changes detected/i)
      end
    end

    context "when deploying to a non-protected stage" do
      let(:stage) { "dev" }

      it "does NOT raise when working tree is dirty" do
        allow(Turbofan::CLI::Deploy::Preflight).to receive(:git_clean?).and_return(false)

        expect {
          Turbofan::CLI::Deploy.call(pipeline_name: pipeline_name, stage: stage)
        }.not_to raise_error
      end
    end
  end

  describe "in-flight executions warning" do
    it "calls warn_running_executions with SFN client and ARN when stack exists" do
      sm_arn = "arn:aws:states:us-east-1:123456789:stateMachine:my-pipeline-production"
      sfn_client = instance_double(Aws::States::Client)
      allow(Turbofan::Deploy::StackManager).to receive_messages(detect_state: :update_complete, stack_output: sm_arn)
      allow(Aws::States::Client).to receive(:new).and_return(sfn_client)

      Turbofan::CLI::Deploy.call(pipeline_name: pipeline_name, stage: stage)

      expect(Turbofan::CLI::Deploy::Preflight).to have_received(:warn_running_executions)
        .with(sfn_client, sm_arn)
    end

    it "skips warn_running_executions on first deploy (stack does not exist)" do
      stack = Turbofan::Naming.stack_name("my-pipeline", stage)
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).and_return(:create_complete)
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).with(anything, stack).and_return(:does_not_exist)

      Turbofan::CLI::Deploy.call(pipeline_name: pipeline_name, stage: stage)

      expect(Turbofan::CLI::Deploy::Preflight).not_to have_received(:warn_running_executions)
    end

    it "does not block the deploy" do
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).and_return(:update_complete)

      expect {
        Turbofan::CLI::Deploy.call(pipeline_name: pipeline_name, stage: stage)
      }.not_to raise_error

      expect(Turbofan::Deploy::StackManager).to have_received(:deploy)
    end
  end

  describe "dry-run mode" do
    before do
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).and_return(:update_complete)
      allow(Turbofan::Deploy::StackManager).to receive(:dry_run)
    end

    it "still raises on pre-flight failure" do
      allow(Turbofan::CLI::Deploy::Preflight).to receive(:buildkit_available?).and_return(false)

      expect {
        Turbofan::CLI::Deploy.call(pipeline_name: pipeline_name, stage: stage, dry_run: true)
      }.to raise_error(/BuildKit not available/i)
    end
  end

  describe "pre-flight ordering" do
    it "runs checks in order: BuildKit, STS, git, CLI::Check, then build/deploy ops" do
      stack = Turbofan::Naming.stack_name("my-pipeline", stage)
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).and_return(:create_complete)
      allow(Turbofan::Deploy::StackManager).to receive(:detect_state).with(anything, stack).and_return(:does_not_exist)

      call_order = []
      allow(Turbofan::CLI::Deploy::Preflight).to receive(:buildkit_available?) do
        call_order << :buildkit
        true
      end
      allow(Turbofan::CLI::Deploy::Preflight).to receive(:aws_credentials_valid?) do
        call_order << :sts
        true
      end
      allow(Turbofan::CLI::Deploy::Preflight).to receive(:git_clean?) do
        call_order << :git
        true
      end
      allow(Turbofan::CLI::Check).to receive(:call) do
        call_order << :cli_check
      end
      allow(Turbofan::Deploy::StackManager).to receive(:deploy) do
        call_order << :stack_deploy
      end
      allow(Turbofan::Deploy::ImageBuilder).to receive(:authenticate_ecr) do
        call_order << :ecr_auth
        "123456789.dkr.ecr.us-east-1.amazonaws.com"
      end

      Turbofan::CLI::Deploy.call(pipeline_name: pipeline_name, stage: stage)

      expect(call_order).to eq([:buildkit, :sts, :git, :cli_check, :stack_deploy, :ecr_auth])
    end
  end
end
