# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Turbofan::Generators::CloudFormation, :schemas do
  before { Turbofan.config.bucket = "turbofan-shared-bucket" }

  describe "multi-size job definitions and queues (Task 15)" do
    let(:ce_class) do
      klass = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::MultiSizeCe", klass)
      klass
    end

    let(:multi_size_step) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        size :s, cpu: 1, ram: 2
        size :m, cpu: 2, ram: 4
        size :l, cpu: 4, ram: 8
        uses :duckdb
        batch_size 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      stub_const("Process", multi_size_step)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "multi-size"

        pipeline do
          fan_out(process(trigger_input))
        end
      end
    end

    let(:config) do
      {
        vpc_id: "vpc-123",
        subnets: ["subnet-456", "subnet-789"],
        security_groups: ["sg-abc"]
      }
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: multi_size_step},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    describe "job definitions per size" do
      it "generates one job definition per declared size" do
        jd_keys = template["Resources"].keys.select { |k| k.start_with?("JobDef") }
        expect(jd_keys.size).to eq(3)
      end

      it "names job definitions with size suffix: jobdef-{step}-{size}" do
        jd_keys = template["Resources"].keys.select { |k| k.start_with?("JobDef") }
        jd_names = jd_keys.map { |k| template["Resources"][k]["Properties"]["JobDefinitionName"] }

        expect(jd_names.any? { |n| n.start_with?("turbofan-multi-size-production-jobdef-process-s-") }).to be true
        expect(jd_names.any? { |n| n.start_with?("turbofan-multi-size-production-jobdef-process-m-") }).to be true
        expect(jd_names.any? { |n| n.start_with?("turbofan-multi-size-production-jobdef-process-l-") }).to be true
      end

      it "sets correct CPU for small size" do
        jd_key = template["Resources"].keys.find { |k|
          k.start_with?("JobDef") &&
            template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("-process-s-")
        }
        container = template["Resources"][jd_key]["Properties"]["ContainerProperties"]
        vcpu = container["ResourceRequirements"].find { |r| r["Type"] == "VCPU" }
        expect(vcpu["Value"]).to eq("1")
      end

      it "sets correct CPU for medium size" do
        jd_key = template["Resources"].keys.find { |k|
          k.start_with?("JobDef") &&
            template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("-process-m-")
        }
        container = template["Resources"][jd_key]["Properties"]["ContainerProperties"]
        vcpu = container["ResourceRequirements"].find { |r| r["Type"] == "VCPU" }
        expect(vcpu["Value"]).to eq("2")
      end

      it "sets correct CPU for large size" do
        jd_key = template["Resources"].keys.find { |k|
          k.start_with?("JobDef") &&
            template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("-process-l-")
        }
        container = template["Resources"][jd_key]["Properties"]["ContainerProperties"]
        vcpu = container["ResourceRequirements"].find { |r| r["Type"] == "VCPU" }
        expect(vcpu["Value"]).to eq("4")
      end

      it "sets correct RAM for small size (derived from c-family: 1 CPU * 2 GB = 2048 MB)" do
        jd_key = template["Resources"].keys.find { |k|
          k.start_with?("JobDef") &&
            template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("-process-s-")
        }
        container = template["Resources"][jd_key]["Properties"]["ContainerProperties"]
        memory = container["ResourceRequirements"].find { |r| r["Type"] == "MEMORY" }
        expect(memory["Value"]).to eq("2048")
      end

      it "sets correct RAM for medium size (derived from c-family: 2 CPU * 2 GB = 4096 MB)" do
        jd_key = template["Resources"].keys.find { |k|
          k.start_with?("JobDef") &&
            template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("-process-m-")
        }
        container = template["Resources"][jd_key]["Properties"]["ContainerProperties"]
        memory = container["ResourceRequirements"].find { |r| r["Type"] == "MEMORY" }
        expect(memory["Value"]).to eq("4096")
      end

      it "sets correct RAM for large size (derived from c-family: 4 CPU * 2 GB = 8192 MB)" do
        jd_key = template["Resources"].keys.find { |k|
          k.start_with?("JobDef") &&
            template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("-process-l-")
        }
        container = template["Resources"][jd_key]["Properties"]["ContainerProperties"]
        memory = container["ResourceRequirements"].find { |r| r["Type"] == "MEMORY" }
        expect(memory["Value"]).to eq("8192")
      end
    end

    describe "job queues per step" do
      it "does not generate queue resources (queues live in CE stacks)" do
        queue_keys = template["Resources"].keys.select { |k| k.start_with?("JobQueue") }
        expect(queue_keys.size).to eq(0)
      end
    end

    describe "shared compute environment" do
      it "does not create an inline compute environment resource" do
        ce_keys = template["Resources"].keys.select { |k| k.start_with?("ComputeEnvironment") }
        expect(ce_keys.size).to eq(0)
      end
    end

    describe "per-size job definition tagging" do
      it "includes turbofan:size tag on each sized job definition" do
        jd_keys = template["Resources"].keys.select { |k| k.start_with?("JobDef") }

        jd_keys.each do |key|
          tags = template["Resources"][key]["Properties"]["Tags"]
          expect(tags).to have_key("turbofan:size"), "expected #{key} to have turbofan:size tag"
          expect(tags["turbofan:size"]).to match(/\A(s|m|l)\z/)
        end
      end

      it "does not include turbofan:size tag on unsized job definitions" do
        single_size_step = Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 2
          batch_size 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
        stub_const("Single", single_size_step)

        single_pipeline = Class.new do
          include Turbofan::Pipeline
          pipeline_name "single-tag-test"
          pipeline do
            fan_out(single(trigger_input))
          end
        end

        single_template = described_class.new(
          pipeline: single_pipeline, steps: {single: single_size_step},
          stage: "production", config: {}
        ).generate

        jd_keys = single_template["Resources"].keys.select { |k| k.start_with?("JobDef") }
        jd_keys.each do |key|
          tags = single_template["Resources"][key]["Properties"]["Tags"]
          expect(tags).not_to have_key("turbofan:size"), "expected unsized job definition to NOT have turbofan:size tag"
        end
      end
    end

    describe "resource naming convention" do
      it "follows turbofan-{pipeline}-{stage}-jobdef-{step}-{size}-{hash} for job definitions" do
        jd_keys = template["Resources"].keys.select { |k| k.start_with?("JobDef") }

        jd_keys.each do |key|
          name = template["Resources"][key]["Properties"]["JobDefinitionName"]
          expect(name).to match(/\Aturbofan-multi-size-production-jobdef-process-(s|m|l)-[0-9a-f]{6}\z/)
        end
      end

      it "does not generate queue resources in the pipeline template" do
        queue_keys = template["Resources"].keys.select { |k| k.start_with?("JobQueue") }
        expect(queue_keys).to be_empty
      end
    end

    describe "backward compatibility: single-size step unchanged" do
      let(:single_size_step) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:single_pipeline) do
        stub_const("Process", single_size_step)
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "single-size"

          pipeline do
            process(trigger_input)
          end
        end
      end

      let(:single_template) do
        described_class.new(
          pipeline: single_pipeline,
          steps: {process: single_size_step},
          stage: "production",
          config: config
        ).generate
      end

      it "generates exactly one job definition for a single-size step" do
        jd_keys = single_template["Resources"].keys.select { |k| k.start_with?("JobDef") }
        expect(jd_keys.size).to eq(1)
      end

      it "does not include a size suffix in the job definition name" do
        jd_key = single_template["Resources"].keys.find { |k| k.start_with?("JobDef") }
        name = single_template["Resources"][jd_key]["Properties"]["JobDefinitionName"]
        expect(name).to start_with("turbofan-single-size-production-jobdef-process-")
        expect(name).not_to match(/-[sml]-[0-9a-f]+\z/)
      end

      it "does not generate queue resources for a single-size step" do
        queue_keys = single_template["Resources"].keys.select { |k| k.start_with?("JobQueue") }
        expect(queue_keys.size).to eq(0)
      end
    end

    describe "multi-size with r-family" do
      let(:r_family_step) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          size :s, cpu: 1, ram: 8
          size :l, cpu: 4, ram: 32
          batch_size 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:r_pipeline) do
        stub_const("Process", r_family_step)
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "r-multi"

          pipeline do
            fan_out(process(trigger_input))
          end
        end
      end

      let(:r_template) do
        described_class.new(
          pipeline: r_pipeline,
          steps: {process: r_family_step},
          stage: "production",
          config: config
        ).generate
      end

      it "generates one job definition per size" do
        jd_keys = r_template["Resources"].keys.select { |k| k.start_with?("JobDef") }
        expect(jd_keys.size).to eq(2)
      end

      it "sets correct RAM for small size (8 GB = 8192 MB)" do
        jd_key = r_template["Resources"].keys.find { |k|
          k.start_with?("JobDef") &&
            r_template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("-process-s-")
        }
        container = r_template["Resources"][jd_key]["Properties"]["ContainerProperties"]
        memory = container["ResourceRequirements"].find { |r| r["Type"] == "MEMORY" }
        expect(memory["Value"]).to eq("8192")
      end

      it "sets correct RAM for large size (32 GB = 32768 MB)" do
        jd_key = r_template["Resources"].keys.find { |k|
          k.start_with?("JobDef") &&
            r_template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("-process-l-")
        }
        container = r_template["Resources"][jd_key]["Properties"]["ContainerProperties"]
        memory = container["ResourceRequirements"].find { |r| r["Type"] == "MEMORY" }
        expect(memory["Value"]).to eq("32768")
      end

      it "derives correct CPU for small size (8 GB / 8 GB per vCPU = 1)" do
        jd_key = r_template["Resources"].keys.find { |k|
          k.start_with?("JobDef") &&
            r_template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("-process-s-")
        }
        container = r_template["Resources"][jd_key]["Properties"]["ContainerProperties"]
        vcpu = container["ResourceRequirements"].find { |r| r["Type"] == "VCPU" }
        expect(vcpu["Value"]).to eq("1")
      end

      it "derives correct CPU for large size (32 GB / 8 GB per vCPU = 4)" do
        jd_key = r_template["Resources"].keys.find { |k|
          k.start_with?("JobDef") &&
            r_template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("-process-l-")
        }
        container = r_template["Resources"][jd_key]["Properties"]["ContainerProperties"]
        vcpu = container["ResourceRequirements"].find { |r| r["Type"] == "VCPU" }
        expect(vcpu["Value"]).to eq("4")
      end
    end

    describe "mixed pipeline: single-size and multi-size steps" do
      let(:single_step) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:multi_step) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          size :s, cpu: 1, ram: 2
          size :l, cpu: 4, ram: 8
          batch_size 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:mixed_pipeline) do
        stub_const("Discover", single_step)
        stub_const("Process", multi_step)
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "mixed"

          pipeline do
            files = discover(trigger_input)
            fan_out(process(files))
          end
        end
      end

      let(:mixed_template) do
        described_class.new(
          pipeline: mixed_pipeline,
          steps: {discover: single_step, process: multi_step},
          stage: "production",
          config: config
        ).generate
      end

      it "generates 1 job def for single-size + 2 for multi-size = 3 total" do
        jd_keys = mixed_template["Resources"].keys.select { |k| k.start_with?("JobDef") }
        expect(jd_keys.size).to eq(3)
      end

      it "does not generate queue resources in a mixed pipeline" do
        queue_keys = mixed_template["Resources"].keys.select { |k| k.start_with?("JobQueue") }
        expect(queue_keys.size).to eq(0)
      end

      it "single-size step job def has no size suffix" do
        jd_keys = mixed_template["Resources"].keys.select { |k| k.start_with?("JobDef") }
        discover_jd = jd_keys.find { |k|
          mixed_template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("discover")
        }
        name = mixed_template["Resources"][discover_jd]["Properties"]["JobDefinitionName"]
        expect(name).to start_with("turbofan-mixed-production-jobdef-discover-")
      end

      it "multi-size step job defs have size suffixes" do
        jd_keys = mixed_template["Resources"].keys.select { |k| k.start_with?("JobDef") }
        process_jds = jd_keys.select { |k|
          mixed_template["Resources"][k]["Properties"]["JobDefinitionName"]&.include?("process")
        }
        names = process_jds.map { |k| mixed_template["Resources"][k]["Properties"]["JobDefinitionName"] }
        expect(names.any? { |n| n.start_with?("turbofan-mixed-production-jobdef-process-s-") }).to be true
        expect(names.any? { |n| n.start_with?("turbofan-mixed-production-jobdef-process-l-") }).to be true
      end
    end
  end
end
