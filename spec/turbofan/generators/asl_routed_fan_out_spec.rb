# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Turbofan::Generators::ASL, :schemas do
  describe "routed fan-out with per-size branches" do
    let(:discover_step) do
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:process_step) do
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        size :s, cpu: 1
        size :m, cpu: 2
        size :l, cpu: 4
        batch_size 3
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:aggregate_step) do
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      stub_const("Discover", discover_step)
      stub_const("Process", process_step)
      stub_const("Aggregate", aggregate_step)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "routed-fan-out"

        pipeline do
          files = discover(trigger_input)
          results = fan_out(process(files))
          aggregate(results)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        stage: "production",
        steps: {process: process_step}
      )
    end
    let(:asl) { generator.generate }
    let(:prefix) { "turbofan-routed-fan-out-production" }

    it "generates a chunk state with routed flag" do
      chunk_state = asl["States"]["process_chunk"]
      expect(chunk_state.dig("Parameters", "Payload", "routed")).to be true
    end

    it "chunk state has sizes ResultSelector instead of chunk_count" do
      chunk_state = asl["States"]["process_chunk"]
      expect(chunk_state["ResultSelector"]).to eq({"sizes.$" => "$.Payload.sizes"})
      expect(chunk_state["ResultSelector"]).not_to have_key("chunk_count.$")
    end

    it "chunk state Next points to process_routed" do
      chunk_state = asl["States"]["process_chunk"]
      expect(chunk_state["Next"]).to eq("process_routed")
    end

    it "does not emit a separate routing state (routing merged into chunk Lambda)" do
      expect(asl["States"]).not_to have_key("process_route")
    end

    it "chunk state payload includes router_class for the bundled router" do
      chunk_state = asl["States"]["process_chunk"]
      expect(chunk_state.dig("Parameters", "Payload", "router_class")).to eq("ProcessRouter")
    end

    it "chunk state invokes the per-step ChunkingLambda when routed" do
      chunk_state = asl["States"]["process_chunk"]
      expect(chunk_state.dig("Parameters", "FunctionName")).to eq("#{prefix}-chunking-process")
    end

    it "generates a Parallel state for process_routed" do
      routed_state = asl["States"]["process_routed"]
      expect(routed_state).not_to be_nil
      expect(routed_state["Type"]).to eq("Parallel")
    end

    it "Parallel state has 3 branches (s, m, l)" do
      routed_state = asl["States"]["process_routed"]
      expect(routed_state["Branches"].size).to eq(3)
    end

    it "each branch references the correct sized job definition" do
      routed_state = asl["States"]["process_routed"]
      job_defs = routed_state["Branches"].map { |b|
        b["States"].values.first.dig("ItemProcessor", "States").values.first.dig("Parameters", "JobDefinition")
      }
      expect(job_defs.size).to eq(3)
      expect(job_defs.any? { |d| d.include?("-process-s-") }).to be true
      expect(job_defs.any? { |d| d.include?("-process-m-") }).to be true
      expect(job_defs.any? { |d| d.include?("-process-l-") }).to be true
    end

    it "each branch references the correct sized job queue" do
      routed_state = asl["States"]["process_routed"]
      queues = routed_state["Branches"].map { |b|
        b["States"].values.first.dig("ItemProcessor", "States").values.first.dig("Parameters", "JobQueue")
      }
      expect(queues).to all(eq("turbofan-ce-test-ce-production-queue"))
    end

    it "each branch sets TURBOFAN_SIZE env var" do
      routed_state = asl["States"]["process_routed"]
      sizes = routed_state["Branches"].map { |b|
        env = b["States"].values.first.dig("ItemProcessor", "States").values.first.dig("Parameters", "ContainerOverrides", "Environment")
        env.find { |e| e["Name"] == "TURBOFAN_SIZE" }&.dig("Value")
      }
      expect(sizes).to contain_exactly("s", "m", "l")
    end

    it "each branch has dynamic ArrayProperties from parent size" do
      routed_state = asl["States"]["process_routed"]
      array_props = routed_state["Branches"].map { |b|
        b["States"].values.first.dig("ItemProcessor", "States").values.first.dig("Parameters", "ArrayProperties", "Size.$")
      }
      expect(array_props).to contain_exactly(
        "$.size",
        "$.size",
        "$.size"
      )
    end

    it "includes turbofan:execution tag on routed parallel inner Batch tasks" do
      routed_state = asl["States"]["process_routed"]
      routed_state["Branches"].each do |branch|
        inner_task = branch["States"].values.first.dig("ItemProcessor", "States").values.first
        tags = inner_task.dig("Parameters", "Tags")
        expect(tags).to eq({"turbofan:execution.$" => "$$.Execution.Id"})
      end
    end

    it "aggregate step has TURBOFAN_PREV_FAN_OUT_SIZES env var" do
      aggregate_state = asl["States"]["aggregate"]
      env = aggregate_state.dig("Parameters", "ContainerOverrides", "Environment")
      sizes_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_FAN_OUT_SIZES" }
      expect(sizes_var).not_to be_nil
      expect(sizes_var["Value"]).to eq("s,m,l")
    end

    it "aggregate step has per-size count env vars" do
      aggregate_state = asl["States"]["aggregate"]
      env = aggregate_state.dig("Parameters", "ContainerOverrides", "Environment")

      %w[S M L].each do |size_upper|
        size_lower = size_upper.downcase
        size_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_FAN_OUT_SIZE_#{size_upper}" }
        expect(size_var).not_to be_nil,
          "expected aggregate step to have TURBOFAN_PREV_FAN_OUT_SIZE_#{size_upper} env var"
        expect(size_var["Value.$"]).to eq(
          "States.JsonToString($.chunking.process.sizes.#{size_lower}.parents)"
        )
      end
    end

    it "each branch Map ItemsPath references per-size parents (supports empty arrays for sizes with no items)" do
      routed = asl["States"]["process_routed"]
      routed["Branches"].each do |branch|
        map_state = branch["States"].values.first
        expect(map_state["Type"]).to eq("Map")
        items_path = map_state["ItemsPath"]
        expect(items_path).to match(/\$\.chunking\.process\.sizes\.(s|m|l)\.parents/)
        # Empty parents ([]) would result in 0 Map iterations = instant success
        expect(map_state["MaxConcurrency"]).to eq(0)
      end
    end

    it "aggregate step does NOT have TURBOFAN_PREV_FAN_OUT_SIZE (singular)" do
      aggregate_state = asl["States"]["aggregate"]
      env = aggregate_state.dig("Parameters", "ContainerOverrides", "Environment")
      singular_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_FAN_OUT_SIZE" }
      expect(singular_var).to be_nil,
        "expected aggregate step after routed fan-out to NOT have singular TURBOFAN_PREV_FAN_OUT_SIZE"
    end
  end

  describe "routed fan-out as last step" do
    let(:discover_step) do
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:process_step) do
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        size :s, cpu: 1
        size :m, cpu: 2
        batch_size 3
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      stub_const("Discover", discover_step)
      stub_const("Process", process_step)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "routed-last"

        pipeline do
          files = discover(trigger_input)
          fan_out(process(files))
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        stage: "production",
        steps: {process: process_step}
      )
    end
    let(:asl) { generator.generate }

    it "routed Parallel Next is NotifySuccess" do
      routed_state = asl["States"]["process_routed"]
      expect(routed_state["Next"]).to eq("NotifySuccess")
    end
  end

  describe "non-routed fan-out step (no sizes) is unchanged" do
    let(:pipeline_class) do
      stub_const("Discover", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      stub_const("Process", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1
        batch_size 3

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      stub_const("Aggregate", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "non-routed-fan-out"

        pipeline do
          files = discover(trigger_input)
          results = fan_out(process(files))
          aggregate(results)
        end
      end
    end

    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
    let(:asl) { generator.generate }

    it "generates a regular chunk state without routed flag" do
      chunk_state = asl["States"]["process_chunk"]
      payload = chunk_state.dig("Parameters", "Payload")
      expect(payload).not_to have_key("routed")
    end

    it "generates a Map state (not Parallel) for non-routed fan-out" do
      process_state = asl["States"]["process"]
      expect(process_state).not_to be_nil
      expect(process_state["Type"]).to eq("Map")
      inner_task = process_state.dig("ItemProcessor", "States", "process_batch")
      expect(inner_task["Resource"]).to eq("arn:aws:states:::batch:submitJob.sync")
      expect(asl["States"]).not_to have_key("process_routed")
    end

    it "aggregate step has TURBOFAN_PREV_FAN_OUT_PARENTS" do
      aggregate_state = asl["States"]["aggregate"]
      env = aggregate_state.dig("Parameters", "ContainerOverrides", "Environment")
      parents_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_FAN_OUT_PARENTS" }
      expect(parents_var).not_to be_nil,
        "expected aggregate step after non-routed fan-out to have TURBOFAN_PREV_FAN_OUT_PARENTS"
      expect(parents_var["Value.$"]).to eq("States.JsonToString($.chunking.process.parents)")
    end
  end
end
