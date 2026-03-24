require "spec_helper"

RSpec.describe "Turbofan::ComputeEnvironment" do
  describe "DSL defaults" do
    let(:ce_class) do
      klass = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::DefaultCe", klass)
      klass
    end

    it "has default instance_types" do
      expect(ce_class.turbofan_instance_types).to eq(["optimal"])
    end

    it "has default max_vcpus" do
      expect(ce_class.turbofan_max_vcpus).to eq(256)
    end

    it "has default min_vcpus" do
      expect(ce_class.turbofan_min_vcpus).to eq(0)
    end

    it "has default allocation_strategy" do
      expect(ce_class.turbofan_allocation_strategy).to eq("SPOT_PRICE_CAPACITY_OPTIMIZED")
    end

    it "has nil subnets by default (falls back to config)" do
      expect(ce_class.turbofan_subnets).to be_nil
    end
  end

  describe "DSL setters" do
    let(:ce_class) do
      klass = Class.new do
        include Turbofan::ComputeEnvironment

        instance_types %w[c7gd.large c6gd.large]
        max_vcpus 512
        min_vcpus 2
        allocation_strategy "SPOT_CAPACITY_OPTIMIZED"
        subnets %w[subnet-aaa subnet-bbb]
        security_groups %w[sg-xxx]
      end
      stub_const("ComputeEnvironments::CustomCe", klass)
      klass
    end

    it "sets instance_types" do
      expect(ce_class.turbofan_instance_types).to eq(%w[c7gd.large c6gd.large])
    end

    it "sets max_vcpus" do
      expect(ce_class.turbofan_max_vcpus).to eq(512)
    end

    it "sets min_vcpus" do
      expect(ce_class.turbofan_min_vcpus).to eq(2)
    end

    it "sets allocation_strategy" do
      expect(ce_class.turbofan_allocation_strategy).to eq("SPOT_CAPACITY_OPTIMIZED")
    end

    it "sets subnets" do
      expect(ce_class.turbofan_subnets).to eq(%w[subnet-aaa subnet-bbb])
    end

    it "sets security_groups" do
      expect(ce_class.turbofan_security_groups).to eq(%w[sg-xxx])
    end
  end

  describe "resolved_subnets" do
    it "uses CE-level subnets when set" do
      klass = Class.new do
        include Turbofan::ComputeEnvironment
        subnets %w[subnet-ce]
      end
      stub_const("ComputeEnvironments::CeLevel", klass)

      Turbofan.config.subnets = %w[subnet-global]
      expect(klass.resolved_subnets).to eq(%w[subnet-ce])
    end

    it "falls back to config subnets when CE-level not set" do
      klass = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::GlobalLevel", klass)

      Turbofan.config.subnets = %w[subnet-global]
      expect(klass.resolved_subnets).to eq(%w[subnet-global])
    end
  end

  describe "generate_template" do
    let(:ce_class) do
      klass = Class.new do
        include Turbofan::ComputeEnvironment

        instance_types %w[c7gd.large c6gd.large]
        max_vcpus 512
        subnets %w[subnet-aaa]
        security_groups %w[sg-xxx]
      end
      stub_const("ComputeEnvironments::HouseStark", klass)
      klass
    end

    before do
      Turbofan.config.aws_account_id = "123456789012"
    end

    it "generates valid YAML" do
      template = ce_class.generate_template(stage: "production")
      parsed = YAML.safe_load(template)
      expect(parsed).to be_a(Hash)
      expect(parsed["AWSTemplateFormatVersion"]).to eq("2010-09-09")
    end

    it "includes instance types" do
      template = ce_class.generate_template(stage: "production")
      parsed = YAML.safe_load(template)
      types = parsed.dig("Resources", "ComputeEnvironment", "Properties", "ComputeResources", "InstanceTypes")
      expect(types).to eq(%w[c7gd.large c6gd.large])
    end

    it "includes max_vcpus" do
      template = ce_class.generate_template(stage: "production")
      parsed = YAML.safe_load(template)
      max = parsed.dig("Resources", "ComputeEnvironment", "Properties", "ComputeResources", "MaxvCpus")
      expect(max).to eq(512)
    end

    it "includes IAM roles with account ID" do
      template = ce_class.generate_template(stage: "production")
      expect(template).to include("arn:aws:iam::123456789012:instance-profile/ecsInstanceRole")
      expect(template).to include("arn:aws:iam::123456789012:role/AmazonEC2SpotFleetTaggingRole")
    end

    it "includes subnets" do
      template = ce_class.generate_template(stage: "production")
      parsed = YAML.safe_load(template)
      subs = parsed.dig("Resources", "ComputeEnvironment", "Properties", "ComputeResources", "Subnets")
      expect(subs).to eq(%w[subnet-aaa])
    end

    it "includes tags" do
      template = ce_class.generate_template(stage: "production")
      parsed = YAML.safe_load(template)
      tags = parsed.dig("Resources", "ComputeEnvironment", "Properties", "Tags")
      expect(tags["turbofan:managed"]).to eq("true")
      expect(tags["turbofan:compute-environment"]).to eq("house-stark")
    end

    it "includes export output" do
      template = ce_class.generate_template(stage: "production")
      parsed = YAML.safe_load(template)
      export_name = parsed.dig("Outputs", "ComputeEnvironmentArn", "Export", "Name")
      expect(export_name).to eq("turbofan-ce-house-stark-production-arn")
    end

    it "raises if aws_account_id not configured" do
      Turbofan.config.aws_account_id = nil
      expect { ce_class.generate_template(stage: "production") }.to raise_error(/aws_account_id/)
    end

    it "raises if no subnets configured" do
      klass = Class.new do
        include Turbofan::ComputeEnvironment
        security_groups %w[sg-xxx]
      end
      stub_const("ComputeEnvironments::NoSubnets", klass)
      Turbofan.config.subnets = []
      expect { klass.generate_template(stage: "staging") }.to raise_error(/subnets/i)
    end
  end

  describe "stack_name" do
    let(:ce_class) do
      klass = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::HouseStark", klass)
      klass
    end

    it "generates stack name from class name and stage" do
      expect(ce_class.stack_name("production")).to eq("turbofan-ce-house-stark-production")
    end

    it "converts CamelCase to kebab-case" do
      klass = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::MyFancyCe", klass)
      expect(klass.stack_name("staging")).to eq("turbofan-ce-my-fancy-ce-staging")
    end
  end

  describe "export_name" do
    let(:ce_class) do
      klass = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::HouseStark", klass)
      klass
    end

    it "appends -arn to the stack name" do
      expect(ce_class.export_name("production")).to eq("turbofan-ce-house-stark-production-arn")
    end
  end

  describe "discover" do
    it "finds all classes including ComputeEnvironment via ObjectSpace" do
      ce_a = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::Alpha", ce_a)

      ce_b = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::Beta", ce_b)

      discovered = Turbofan::ComputeEnvironment.discover
      expect(discovered).to include(ce_a)
      expect(discovered).to include(ce_b)
    end

    it "excludes stale constants (liveness guard)" do
      ce = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::Stale", ce)

      ComputeEnvironments.send(:remove_const, :Stale) # rubocop:disable RSpec/RemoveConst

      discovered = Turbofan::ComputeEnvironment.discover
      expect(discovered).not_to include(ce)

      ComputeEnvironments.const_set(:Stale, ce)
    end
  end
end
