# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"

RSpec.describe "turbofan ce" do # rubocop:disable RSpec/DescribeClass
  let(:tmpdir) { Dir.mktmpdir("turbofan-ce-test", SPEC_TMP_ROOT) }

  after { FileUtils.rm_rf(tmpdir) }

  describe "ce new" do
    before do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        Turbofan::CLI.start(["ce", "new", "house_stark"])
      end
    end

    let(:ce_file) { File.join(tmpdir, "turbofans", "compute_environments", "house_stark.rb") }

    it "creates the compute_environments directory" do
      expect(Dir.exist?(File.join(tmpdir, "turbofans", "compute_environments"))).to be true
    end

    it "creates house_stark.rb" do
      expect(File.exist?(ce_file)).to be true
    end

    describe "file content" do
      let(:content) { File.read(ce_file) }

      it "includes Turbofan::ComputeEnvironment" do
        expect(content).to include("Turbofan::ComputeEnvironment")
      end

      it "defines a class with correct name" do
        expect(content).to include("HouseStark")
      end

      it "includes DSL methods" do
        expect(content).to include("instance_types")
        expect(content).to include("max_vcpus")
      end
    end
  end

  describe "ce list" do
    it "discovers and lists CE classes" do
      ce_class = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::HouseStark", ce_class)

      output = capture_stdout do
        Turbofan::CLI.start(["ce", "list"])
      end

      expect(output).to include("HouseStark")
    end
  end

  describe "ce deploy" do
    let(:cf_client) { instance_double(Aws::CloudFormation::Client) }

    before do
      allow(Aws::CloudFormation::Client).to receive(:new).and_return(cf_client)
      Turbofan.config.aws_account_id = "123456789012"
    end

    it "calls StackManager with generated template" do
      ce_class = Class.new do
        include Turbofan::ComputeEnvironment
        instance_types %w[c7gd.large]
        max_vcpus 512
        subnets %w[subnet-aaa]
        security_groups %w[sg-xxx]
        container_insights false
      end
      stub_const("ComputeEnvironments::HouseStark", ce_class)

      allow(Turbofan::ComputeEnvironment).to receive(:discover).and_return([ce_class])
      allow(Turbofan::Deploy::StackManager).to receive(:deploy)

      Dir.chdir(tmpdir) do
        Turbofan::CLI.start(["ce", "deploy", "production"])
      end

      expect(Turbofan::Deploy::StackManager).to have_received(:deploy).with(
        cf_client,
        hash_including(
          stack_name: "turbofan-ce-house-stark-production",
          parameters: []
        )
      )
    end

    it "enables container insights when container_insights is true" do
      require "aws-sdk-ecs"

      ce_class = Class.new do
        include Turbofan::ComputeEnvironment
        instance_types %w[c7gd.large]
        subnets %w[subnet-aaa]
        security_groups %w[sg-xxx]
      end
      stub_const("ComputeEnvironments::InsightsTest", ce_class)

      allow(Turbofan::ComputeEnvironment).to receive(:discover).and_return([ce_class])
      allow(Turbofan::Deploy::StackManager).to receive(:deploy)
      allow(Turbofan::Deploy::StackManager).to receive(:stack_output).and_return("arn:aws:batch:us-east-1:123:compute-environment/test")

      batch_client = instance_double(Aws::Batch::Client)
      allow(Aws::Batch::Client).to receive(:new).and_return(batch_client)
      allow(batch_client).to receive(:describe_compute_environments).and_return(
        double(compute_environments: [double(ecs_cluster_arn: "arn:aws:ecs:us-east-1:123:cluster/test")])
      )

      ecs_client = instance_double(Aws::ECS::Client)
      allow(Aws::ECS::Client).to receive(:new).and_return(ecs_client)
      allow(ecs_client).to receive(:update_cluster_settings)

      Dir.chdir(tmpdir) do
        Turbofan::CLI.start(["ce", "deploy", "production"])
      end

      expect(ecs_client).to have_received(:update_cluster_settings).with(
        cluster: "arn:aws:ecs:us-east-1:123:cluster/test",
        settings: [{name: "containerInsights", value: "enabled"}]
      )
    end

    it "passes generated template body to StackManager" do
      ce_class = Class.new do
        include Turbofan::ComputeEnvironment
        instance_types %w[c7gd.large]
        subnets %w[subnet-aaa]
        security_groups %w[sg-xxx]
        container_insights false
      end
      stub_const("ComputeEnvironments::DeployTest", ce_class)

      allow(Turbofan::ComputeEnvironment).to receive(:discover).and_return([ce_class])
      allow(Turbofan::Deploy::StackManager).to receive(:deploy)

      Dir.chdir(tmpdir) do
        Turbofan::CLI.start(["ce", "deploy", "staging"])
      end

      expect(Turbofan::Deploy::StackManager).to have_received(:deploy) do |_client, **kwargs|
        parsed = YAML.safe_load(kwargs[:template_body])
        types = parsed.dig("Resources", "ComputeEnvironment", "Properties", "ComputeResources", "InstanceTypes")
        expect(types).to eq(%w[c7gd.large])
      end
    end
  end
end
