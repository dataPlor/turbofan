require "spec_helper"

RSpec.describe Turbofan::Generators::ASL, :schemas do
  before do
    Turbofan.config.bucket = "turbofan-shared-bucket"
  end

  describe "single-step pipeline" do
    let(:pipeline_class) do
      stub_const("Process", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "test-pipeline"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
    let(:asl) { generator.generate }

    it "produces ASL with a StartAt field" do
      expect(asl["StartAt"]).to eq("process")
    end

    it "has a States section" do
      expect(asl["States"]).to be_a(Hash)
    end

    it "creates states for the step plus notification and failure states" do
      expect(asl["States"].size).to eq(4)
    end

    it "creates a Task state type" do
      state = asl["States"]["process"]
      expect(state["Type"]).to eq("Task")
    end

    it "uses Batch submitJob.sync resource" do
      state = asl["States"]["process"]
      expect(state["Resource"]).to eq("arn:aws:states:::batch:submitJob.sync")
    end

    it "chains the only step to success notification" do
      state = asl["States"]["process"]
      expect(state["Next"]).to match(/success/i)
      expect(state).not_to have_key("End")
    end

    it "references the correct job definition" do
      state = asl["States"]["process"]
      params = state["Parameters"]
      expect(params["JobDefinition"]).to include("process")
    end

    it "references the correct job queue" do
      state = asl["States"]["process"]
      params = state["Parameters"]
      expect(params["JobQueue"]).to eq("turbofan-ce-test-ce-production-queue")
    end

    it "sets a job name" do
      state = asl["States"]["process"]
      params = state["Parameters"]
      expect(params["JobName"]).to include("process")
    end
  end

  describe "two-step linear pipeline" do
    let(:pipeline_class) do
      stub_const("Extract", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      stub_const("Load", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "linear-pipeline"

        pipeline do
          results = extract(trigger_input)
          load(results)
        end
      end
    end

    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
    let(:asl) { generator.generate }

    it "starts at the first step" do
      expect(asl["StartAt"]).to eq("extract")
    end

    it "creates states for steps plus notification and failure states" do
      expect(asl["States"].size).to eq(5)
    end

    it "chains the first step to the second via Next" do
      extract_state = asl["States"]["extract"]
      expect(extract_state["Next"]).to eq("load")
      expect(extract_state).not_to have_key("End")
    end

    it "chains the last step to success notification" do
      load_state = asl["States"]["load"]
      expect(load_state["Next"]).to match(/success/i)
      expect(load_state).not_to have_key("End")
    end
  end

  describe "three-step linear pipeline" do
    let(:pipeline_class) do
      stub_const("Extract", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      stub_const("Transform", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      stub_const("Load", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "three-step"

        pipeline do
          a = extract(trigger_input)
          b = transform(a)
          load(b)
        end
      end
    end

    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "staging") }
    let(:asl) { generator.generate }

    it "chains all three steps in order" do
      expect(asl["StartAt"]).to eq("extract")
      expect(asl["States"]["extract"]["Next"]).to eq("transform")
      expect(asl["States"]["transform"]["Next"]).to eq("load")
      expect(asl["States"]["load"]["Next"]).to match(/success/i)
    end

    it "uses staging in resource references" do
      state = asl["States"]["extract"]
      params = state["Parameters"]
      expect(params["JobDefinition"]).to include("staging")
    end
  end

  describe "multi-size step job definition references (F-8)" do
    let(:multi_size_step) do
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        size :s, cpu: 1
        size :m, cpu: 2
        size :l, cpu: 4
        batch_size 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      stub_const("Process", multi_size_step)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "multi-size-asl"

        pipeline do
          fan_out(process(trigger_input))
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        stage: "production",
        steps: {process: multi_size_step}
      )
    end
    let(:asl) { generator.generate }

    it "generates a routed Parallel state with per-size branches" do
      routed_state = asl["States"]["process_routed"]
      expect(routed_state["Type"]).to eq("Parallel")
      expect(routed_state["Branches"].size).to eq(3)
    end

    it "each branch references the correct sized job definition" do
      routed_state = asl["States"]["process_routed"]
      job_defs = routed_state["Branches"].map { |b|
        map_state = b["States"].values.first
        inner_task = map_state.dig("ItemProcessor", "States").values.first
        inner_task.dig("Parameters", "JobDefinition")
      }
      expect(job_defs.size).to eq(3)
      expect(job_defs.any? { |d| d.include?("-process-s-") }).to be true
      expect(job_defs.any? { |d| d.include?("-process-m-") }).to be true
      expect(job_defs.any? { |d| d.include?("-process-l-") }).to be true
    end

    it "each branch references the correct sized job queue" do
      routed_state = asl["States"]["process_routed"]
      queues = routed_state["Branches"].map { |b|
        map_state = b["States"].values.first
        inner_task = map_state.dig("ItemProcessor", "States").values.first
        inner_task.dig("Parameters", "JobQueue")
      }
      expect(queues).to all(eq("turbofan-ce-test-ce-production-queue"))
    end

    it "each branch sets TURBOFAN_SIZE env var" do
      routed_state = asl["States"]["process_routed"]
      sizes = routed_state["Branches"].map { |b|
        map_state = b["States"].values.first
        inner_task = map_state.dig("ItemProcessor", "States").values.first
        env = inner_task.dig("Parameters", "ContainerOverrides", "Environment")
        env.find { |e| e["Name"] == "TURBOFAN_SIZE" }&.dig("Value")
      }
      expect(sizes).to contain_exactly("s", "m", "l")
    end

    it "chunk state passes routed flag" do
      chunk = asl["States"]["process_chunk"]
      expect(chunk.dig("Parameters", "Payload", "routed")).to be true
    end

    it "chunk state has sizes ResultSelector" do
      chunk = asl["States"]["process_chunk"]
      expect(chunk["ResultSelector"]).to eq({"sizes.$" => "$.Payload.sizes"})
    end

    it "chunk state Next points to process_routed" do
      chunk = asl["States"]["process_chunk"]
      expect(chunk["Next"]).to eq("process_routed")
    end
  end

  describe "single-size step job definition references (backward compat)" do
    let(:pipeline_class) do
      stub_const("Process", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "single-size-asl"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
    let(:asl) { generator.generate }

    it "references unsuffixed job definition when no sizes declared" do
      process_state = asl["States"]["process"]
      job_def = process_state.dig("Parameters", "JobDefinition")
      expect(job_def).to start_with("turbofan-single-size-asl-production-jobdef-process-")
    end

    it "references CE-based queue when no sizes declared" do
      process_state = asl["States"]["process"]
      job_queue = process_state.dig("Parameters", "JobQueue")
      expect(job_queue).to eq("turbofan-ce-test-ce-production-queue")
    end
  end

  describe "mixed pipeline with multi-size and single-size steps" do
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
        size :s, cpu: 1
        size :l, cpu: 4
        batch_size 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      stub_const("Discover", single_step)
      stub_const("Process", multi_step)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "mixed-asl"

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
        steps: {discover: single_step, process: multi_step}
      )
    end
    let(:asl) { generator.generate }

    it "single-size step uses unsuffixed job definition" do
      discover_state = asl["States"]["discover"]
      job_def = discover_state.dig("Parameters", "JobDefinition")
      expect(job_def).to start_with("turbofan-mixed-asl-production-jobdef-discover-")
    end

    it "multi-size step generates routed Parallel with per-size branches" do
      routed_state = asl["States"]["process_routed"]
      expect(routed_state["Type"]).to eq("Parallel")
      job_defs = routed_state["Branches"].map { |b|
        map_state = b["States"].values.first
        inner_task = map_state.dig("ItemProcessor", "States").values.first
        inner_task.dig("Parameters", "JobDefinition")
      }
      expect(job_defs.size).to eq(2)
      expect(job_defs.any? { |d| d.include?("-process-s-") }).to be true
      expect(job_defs.any? { |d| d.include?("-process-l-") }).to be true
    end
  end

  describe "ASL output structure" do
    let(:pipeline_class) do
      stub_const("Process", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "structure-test"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
    let(:asl) { generator.generate }

    it "is a valid Hash (JSON-serializable)" do
      expect(asl).to be_a(Hash)
      expect { JSON.generate(asl) }.not_to raise_error
    end

    it "has Comment field" do
      expect(asl["Comment"]).to be_a(String)
    end
  end

  # ---------------------------------------------------------------------------
  # B2 — Retry with error-type filtering in ASL output
  # ---------------------------------------------------------------------------
  describe "Retry field from turbofan_retry_on (B2)", :schemas do
    context "when turbofan_retry_on is set" do
      let(:step_with_retry_on) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1
          retries 3, on: ["States.TaskFailed"]

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:pipeline_class) do
        stub_const("Process", step_with_retry_on)
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "retry-on-pipeline"

          pipeline do
            process(trigger_input)
          end
        end
      end

      let(:generator) do
        described_class.new(
          pipeline: pipeline_class,
          stage: "production",
          steps: {process: step_with_retry_on}
        )
      end
      let(:asl) { generator.generate }

      it "generated Task state has a Retry field with the specified ErrorEquals" do
        process_state = asl["States"]["process"]
        expect(process_state).to have_key("Retry")
        retry_config = process_state["Retry"]
        expect(retry_config).to be_an(Array)
        expect(retry_config.first["ErrorEquals"]).to eq(["States.TaskFailed"])
      end

      it "sets IntervalSeconds to 2 in the Retry field" do
        retry_entry = asl["States"]["process"]["Retry"].first
        expect(retry_entry["IntervalSeconds"]).to eq(2)
      end

      it "sets BackoffRate to 2.0 in the Retry field" do
        retry_entry = asl["States"]["process"]["Retry"].first
        expect(retry_entry["BackoffRate"]).to eq(2.0)
      end

      it "sets MaxAttempts to match turbofan_retries" do
        retry_entry = asl["States"]["process"]["Retry"].first
        expect(retry_entry["MaxAttempts"]).to eq(3)
      end
    end

    context "when turbofan_retry_on has multiple error types" do
      let(:step_with_multi_retry) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1
          retries 2, on: ["States.Timeout", "Batch.ServerException"]

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:pipeline_class) do
        stub_const("Process", step_with_multi_retry)
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "multi-retry-on-pipeline"

          pipeline do
            process(trigger_input)
          end
        end
      end

      let(:generator) do
        described_class.new(
          pipeline: pipeline_class,
          stage: "production",
          steps: {process: step_with_multi_retry}
        )
      end
      let(:asl) { generator.generate }

      it "includes all error types in ErrorEquals" do
        retry_entry = asl["States"]["process"]["Retry"].first
        expect(retry_entry["ErrorEquals"]).to eq(["States.Timeout", "Batch.ServerException"])
      end

      it "sets MaxAttempts to the specified retries count" do
        retry_entry = asl["States"]["process"]["Retry"].first
        expect(retry_entry["MaxAttempts"]).to eq(2)
      end
    end

    context "when turbofan_retry_on is nil (catch-all)" do
      let(:step_without_retry_on) do
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1
          retries 3

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:pipeline_class) do
        stub_const("Process", step_without_retry_on)
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "no-retry-on-pipeline"

          pipeline do
            process(trigger_input)
          end
        end
      end

      let(:generator) do
        described_class.new(
          pipeline: pipeline_class,
          stage: "production",
          steps: {process: step_without_retry_on}
        )
      end
      let(:asl) { generator.generate }

      it "generated Task state does not have a Retry field" do
        process_state = asl["States"]["process"]
        expect(process_state).not_to have_key("Retry")
      end
    end
  end

  describe "diamond DAG pattern (A -> {B, C} -> D)", :schemas do
    let(:pipeline_class) { build_pipeline_for_dag(build_diamond_dag, pipeline_name: "diamond-pipeline") }

    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
    let(:asl) { generator.generate }

    it "starts at step_a" do
      expect(asl["StartAt"]).to eq("step_a")
    end

    it "creates a Parallel state after step_a" do
      expect(asl["States"]).to have_key("step_a_parallel")
      expect(asl["States"]["step_a_parallel"]["Type"]).to eq("Parallel")
    end

    it "has two branches in the Parallel state" do
      parallel = asl["States"]["step_a_parallel"]
      expect(parallel["Branches"].size).to eq(2)
    end

    it "step_d follows the Parallel state" do
      parallel = asl["States"]["step_a_parallel"]
      expect(parallel["Next"]).to eq("step_d")
    end

    it "step_d is a join step with TURBOFAN_PREV_STEPS" do
      step_d_state = asl["States"]["step_d"]
      env = step_d_state.dig("Parameters", "ContainerOverrides", "Environment")
      prev_steps = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEPS" }
      expect(prev_steps).not_to be_nil
    end
  end

  describe "multi-step branch DAG (A -> {B->B2, C->C2} -> D)", :schemas do
    let(:pipeline_class) { build_pipeline_for_dag(build_multi_step_branch_dag, pipeline_name: "multi-branch-pipeline") }

    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
    let(:asl) { generator.generate }

    it "creates a Parallel state with multi-step branches" do
      parallel = asl["States"]["step_a_parallel"]
      expect(parallel["Type"]).to eq("Parallel")
      expect(parallel["Branches"].size).to eq(2)
    end

    it "each branch contains two states (chained)" do
      parallel = asl["States"]["step_a_parallel"]
      parallel["Branches"].each do |branch|
        expect(branch["States"].size).to eq(2)
      end
    end

    it "branch states are chained with Next pointers" do
      parallel = asl["States"]["step_a_parallel"]
      parallel["Branches"].each do |branch|
        states = branch["States"]
        start = branch["StartAt"]
        first_state = states[start]
        # First state should have Next, not End
        expect(first_state).to have_key("Next")
        expect(first_state).not_to have_key("End")
        # Last state should have End
        last_name = first_state["Next"]
        last_state = states[last_name]
        expect(last_state["End"]).to be true
      end
    end

    it "step_d follows the Parallel state as join point" do
      parallel = asl["States"]["step_a_parallel"]
      expect(parallel["Next"]).to eq("step_d")
    end

    it "step_d is present as a regular task state" do
      expect(asl["States"]).to have_key("step_d")
      expect(asl["States"]["step_d"]["Type"]).to eq("Task")
    end
  end
end
