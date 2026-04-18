require "spec_helper"
require "json"

RSpec.describe Turbofan::Generators::ASL, :schemas do
  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::TestCe", klass)
    klass
  end

  describe "execution :lambda step" do
    let(:lambda_step) do
      ce_class
      Class.new do
        include Turbofan::Step
        execution :lambda
        compute_environment :test_ce
        ram 4
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      stub_const("FilterGkeys", lambda_step)
      Class.new do
        include Turbofan::Pipeline
        pipeline_name "lambda-test"
        pipeline do
          filter_gkeys(trigger_input)
        end
      end
    end

    let(:asl) do
      described_class.new(
        pipeline: pipeline_class, stage: "production",
        steps: {filter_gkeys: lambda_step}
      ).generate
    end

    it "generates a lambda:invoke Task state" do
      state = asl["States"]["filter_gkeys"]
      expect(state["Type"]).to eq("Task")
      expect(state["Resource"]).to eq("arn:aws:states:::lambda:invoke")
    end

    it "references the correct Lambda function name" do
      state = asl["States"]["filter_gkeys"]
      fn_name = state.dig("Parameters", "FunctionName")
      expect(fn_name).to eq("turbofan-lambda-test-production-lambda-filter_gkeys")
    end

    it "passes Turbofan env vars as Payload" do
      state = asl["States"]["filter_gkeys"]
      payload = state.dig("Parameters", "Payload")
      expect(payload["TURBOFAN_STEP_NAME"]).to eq("filter_gkeys")
      expect(payload["TURBOFAN_STAGE"]).to eq("production")
      expect(payload["TURBOFAN_BUCKET"]).to eq(Turbofan.config.bucket)
    end

    it "does NOT generate a Batch submitJob state" do
      state = asl["States"]["filter_gkeys"]
      expect(state["Resource"]).not_to include("batch")
    end

    it "has Catch routing to NotifyFailure" do
      state = asl["States"]["filter_gkeys"]
      expect(state["Catch"].first["Next"]).to eq("NotifyFailure")
    end
  end

  describe "execution :fargate step" do
    let(:fargate_step) do
      ce_class
      Class.new do
        include Turbofan::Step
        execution :fargate
        compute_environment :test_ce
        cpu 2
        ram 4
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      stub_const("ExportResults", fargate_step)
      Class.new do
        include Turbofan::Pipeline
        pipeline_name "fargate-test"
        pipeline do
          export_results(trigger_input)
        end
      end
    end

    let(:asl) do
      described_class.new(
        pipeline: pipeline_class, stage: "production",
        steps: {export_results: fargate_step}
      ).generate
    end

    it "generates an ecs:runTask.sync Task state" do
      state = asl["States"]["export_results"]
      expect(state["Type"]).to eq("Task")
      expect(state["Resource"]).to eq("arn:aws:states:::ecs:runTask.sync")
    end

    it "sets LaunchType to FARGATE" do
      state = asl["States"]["export_results"]
      expect(state.dig("Parameters", "LaunchType")).to eq("FARGATE")
    end

    it "includes NetworkConfiguration with subnets and security groups" do
      state = asl["States"]["export_results"]
      network = state.dig("Parameters", "NetworkConfiguration", "AwsvpcConfiguration")
      expect(network).to have_key("Subnets")
      expect(network).to have_key("SecurityGroups")
    end

    it "passes env vars via ContainerOverrides" do
      state = asl["States"]["export_results"]
      overrides = state.dig("Parameters", "Overrides", "ContainerOverrides")
      expect(overrides.first["Name"]).to eq("worker")
      env = overrides.first["Environment"]
      step_var = env.find { |e| e["Name"] == "TURBOFAN_STEP_NAME" }
      expect(step_var["Value"]).to eq("export_results")
    end

    it "does NOT generate a Batch submitJob state" do
      state = asl["States"]["export_results"]
      expect(state["Resource"]).not_to include("batch")
    end

    it "has Catch routing to NotifyFailure" do
      state = asl["States"]["export_results"]
      expect(state["Catch"].first["Next"]).to eq("NotifyFailure")
    end

    it "includes turbofan:execution tag referencing the execution ID" do
      state = asl["States"]["export_results"]
      tags = state.dig("Parameters", "Tags")
      expect(tags).to eq([{"Key" => "turbofan:execution", "Value.$" => "$$.Execution.Id"}])
    end
  end

  describe "mixed pipeline with :batch, :lambda, and :fargate steps" do
    let(:batch_step) do
      ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 2
        ram 4
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:lambda_step) do
      ce_class
      Class.new do
        include Turbofan::Step
        execution :lambda
        compute_environment :test_ce
        ram 2
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:fargate_step) do
      ce_class
      Class.new do
        include Turbofan::Step
        execution :fargate
        compute_environment :test_ce
        cpu 1
        ram 2
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      stub_const("Preprocess", lambda_step)
      stub_const("Process", batch_step)
      stub_const("Export", fargate_step)
      Class.new do
        include Turbofan::Pipeline
        pipeline_name "mixed-execution"
        pipeline do
          a = preprocess(trigger_input)
          b = process(a)
          export(b)
        end
      end
    end

    let(:asl) do
      described_class.new(
        pipeline: pipeline_class, stage: "production",
        steps: {preprocess: lambda_step, process: batch_step, export: fargate_step}
      ).generate
    end

    it "generates different Task types per execution model" do
      expect(asl["States"]["preprocess"]["Resource"]).to include("lambda")
      expect(asl["States"]["process"]["Resource"]).to include("batch")
      expect(asl["States"]["export"]["Resource"]).to include("ecs")
    end

    it "chains states correctly" do
      expect(asl["States"]["preprocess"]["Next"]).to eq("process")
      expect(asl["States"]["process"]["Next"]).to eq("export")
      expect(asl["States"]["export"]["Next"]).to eq("NotifySuccess")
    end
  end

  describe "fargate networking resolution" do
    it "uses step-level subnets/security_groups when set" do
      ce_class
      step = Class.new do
        include Turbofan::Step
        execution :fargate
        cpu 1
        ram 2
        subnets ["subnet-step"]
        security_groups ["sg-step"]
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
      stub_const("RunTask", step)
      pipeline = Class.new do
        include Turbofan::Pipeline
        pipeline_name "fargate-step-net"
        pipeline do
          run_task(trigger_input)
        end
      end
      asl = described_class.new(pipeline: pipeline, stage: "production", steps: {run_task: step}).generate
      network = asl["States"]["run_task"].dig("Parameters", "NetworkConfiguration", "AwsvpcConfiguration")
      expect(network["Subnets"]).to eq(["subnet-step"])
      expect(network["SecurityGroups"]).to eq(["sg-step"])
    end

    it "falls back to CE networking when step has no subnets/security_groups" do
      ce = ce_class
      allow(ce).to receive(:resolved_subnets).and_return(["subnet-ce"])
      allow(ce).to receive(:resolved_security_groups).and_return(["sg-ce"])
      step = Class.new do
        include Turbofan::Step
        execution :fargate
        compute_environment :test_ce
        cpu 1
        ram 2
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
      stub_const("RunTask", step)
      pipeline = Class.new do
        include Turbofan::Pipeline
        pipeline_name "fargate-ce-net"
        pipeline do
          run_task(trigger_input)
        end
      end
      asl = described_class.new(pipeline: pipeline, stage: "production", steps: {run_task: step}).generate
      network = asl["States"]["run_task"].dig("Parameters", "NetworkConfiguration", "AwsvpcConfiguration")
      expect(network["Subnets"]).to eq(["subnet-ce"])
      expect(network["SecurityGroups"]).to eq(["sg-ce"])
    end

    it "falls back to Turbofan.config when no step subnets and no CE" do
      allow(Turbofan.config).to receive(:subnets).and_return(["subnet-config"])
      allow(Turbofan.config).to receive(:security_groups).and_return(["sg-config"])
      step = Class.new do
        include Turbofan::Step
        execution :fargate
        cpu 1
        ram 2
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
      stub_const("RunTask", step)
      pipeline = Class.new do
        include Turbofan::Pipeline
        pipeline_name "fargate-config-net"
        pipeline do
          run_task(trigger_input)
        end
      end
      asl = described_class.new(pipeline: pipeline, stage: "production", steps: {run_task: step}).generate
      network = asl["States"]["run_task"].dig("Parameters", "NetworkConfiguration", "AwsvpcConfiguration")
      expect(network["Subnets"]).to eq(["subnet-config"])
      expect(network["SecurityGroups"]).to eq(["sg-config"])
    end

    it "step-level subnets override CE subnets" do
      ce = ce_class
      allow(ce).to receive(:resolved_subnets).and_return(["subnet-ce"])
      allow(ce).to receive(:resolved_security_groups).and_return(["sg-ce"])
      step = Class.new do
        include Turbofan::Step
        execution :fargate
        compute_environment :test_ce
        cpu 1
        ram 2
        subnets ["subnet-override"]
        security_groups ["sg-override"]
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
      stub_const("RunTask", step)
      pipeline = Class.new do
        include Turbofan::Pipeline
        pipeline_name "fargate-override"
        pipeline do
          run_task(trigger_input)
        end
      end
      asl = described_class.new(pipeline: pipeline, stage: "production", steps: {run_task: step}).generate
      network = asl["States"]["run_task"].dig("Parameters", "NetworkConfiguration", "AwsvpcConfiguration")
      expect(network["Subnets"]).to eq(["subnet-override"])
      expect(network["SecurityGroups"]).to eq(["sg-override"])
    end
  end
end
