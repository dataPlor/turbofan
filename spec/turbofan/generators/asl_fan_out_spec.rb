# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Turbofan::Generators::ASL, :schemas do
  describe "fan-out with array jobs (Task 13)" do
    describe "basic fan-out step" do
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
          batch_size 100

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

          pipeline_name "fan-out-pipeline"

          pipeline do
            files = discover(trigger_input)
            results = fan_out(process(files))
            aggregate(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "starts at the first step (non-fan-out)" do
        expect(asl["StartAt"]).to eq("discover")
      end

      it "chains discover -> process_chunk -> process -> aggregate -> success notification" do
        expect(asl["States"]["discover"]["Next"]).to eq("process_chunk")
        expect(asl["States"]["process_chunk"]["Next"]).to eq("process")
        expect(asl["States"]["process"]["Next"]).to eq("aggregate")
        expect(asl["States"]["aggregate"]["Next"]).to match(/success/i)
      end

      it "generates a chunk Lambda state before the fan_out batch state" do
        chunk_state = asl["States"]["process_chunk"]
        expect(chunk_state["Type"]).to eq("Task")
        expect(chunk_state["Resource"]).to eq("arn:aws:states:::lambda:invoke")
        expect(chunk_state.dig("Parameters", "FunctionName")).to include("chunking")
      end

      it "sets chunk state payload with step_name and group_size" do
        payload = asl["States"]["process_chunk"].dig("Parameters", "Payload")
        expect(payload["step_name"]).to eq("process")
        expect(payload["group_size"]).to eq(100)
      end

      it "chunk state references prev_step when not first (reads from S3, not raw data)" do
        payload = asl["States"]["process_chunk"].dig("Parameters", "Payload")
        expect(payload["prev_step"]).to eq("discover")
        expect(payload).not_to have_key("items.$")
        expect(payload).not_to have_key("items")
      end

      it "chunk state has ResultSelector for parents" do
        chunk_state = asl["States"]["process_chunk"]
        expect(chunk_state["ResultSelector"]).to eq({"parents.$" => "$.Payload.parents"})
      end

      it "chunk state stores result in $.chunking.process" do
        chunk_state = asl["States"]["process_chunk"]
        expect(chunk_state["ResultPath"]).to eq("$.chunking.process")
      end

      it "chunk state has Catch clause" do
        chunk_state = asl["States"]["process_chunk"]
        expect(chunk_state["Catch"]).to eq([{"ErrorEquals" => ["States.ALL"], "Next" => "NotifyFailure"}])
      end

      it "generates a Map state for the fan_out step" do
        process_state = asl["States"]["process"]
        expect(process_state["Type"]).to eq("Map")
        expect(process_state["ItemsPath"]).to eq("$.chunking.process.parents")
      end

      it "Map inner task has dynamic ArrayProperties from parent size" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        array_props = inner_task.dig("Parameters", "ArrayProperties")
        expect(array_props).to eq({"Size.$" => "$.size"})
      end

      it "Map inner task has ResultSelector to trim Batch response" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        expect(inner_task["ResultSelector"]).to eq({
          "JobId.$" => "$.JobId",
          "JobName.$" => "$.JobName",
          "Status.$" => "$.Status"
        })
      end

      it "aggregate step has TURBOFAN_PREV_FAN_OUT_PARENTS env var" do
        aggregate_state = asl["States"]["aggregate"]
        env = aggregate_state.dig("Parameters", "ContainerOverrides", "Environment")
        parents_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_FAN_OUT_PARENTS" }
        expect(parents_var).not_to be_nil
        expect(parents_var["Value.$"]).to eq("States.JsonToString($.chunking.process.parents)")
      end

      it "references the correct job definition in Map inner task" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        job_def = inner_task.dig("Parameters", "JobDefinition")
        expect(job_def).to include("process")
      end

      it "references the correct job queue in Map inner task" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        job_queue = inner_task.dig("Parameters", "JobQueue")
        expect(job_queue).to eq("turbofan-ce-test-ce-production-queue")
      end

      it "includes turbofan:execution tag on the inner Batch task" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        tags = inner_task.dig("Parameters", "Tags")
        expect(tags).to eq({"turbofan:execution.$" => "$$.Execution.Id"})
      end
    end

    describe "chunk state retry configuration" do
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
          batch_size 100

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "retry-pipeline"

          pipeline do
            files = discover(trigger_input)
            fan_out(process(files))
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }
      let(:chunk_state) { asl["States"]["process_chunk"] }

      it "has a Retry key on the chunk state" do
        expect(chunk_state).to have_key("Retry"),
          "Expected chunk Lambda state to have a Retry clause for transient Lambda errors"
      end

      it "sets MaxAttempts to 3" do
        retry_config = chunk_state["Retry"]
        expect(retry_config).to be_an(Array)
        retry_entry = retry_config.first
        expect(retry_entry["MaxAttempts"]).to eq(3)
      end

      it "covers Lambda.ServiceException, Lambda.TooManyRequestsException, and States.TaskFailed" do
        retry_config = chunk_state["Retry"]
        error_equals = retry_config.first["ErrorEquals"]
        expect(error_equals).to include("Lambda.ServiceException")
        expect(error_equals).to include("Lambda.TooManyRequestsException")
        expect(error_equals).to include("States.TaskFailed")
      end

      it "has IntervalSeconds for backoff" do
        retry_config = chunk_state["Retry"]
        retry_entry = retry_config.first
        expect(retry_entry).to have_key("IntervalSeconds"),
          "Expected Retry to have IntervalSeconds for exponential backoff"
        expect(retry_entry["IntervalSeconds"]).to be_a(Integer)
      end

      it "has BackoffRate for exponential backoff" do
        retry_config = chunk_state["Retry"]
        retry_entry = retry_config.first
        expect(retry_entry).to have_key("BackoffRate"),
          "Expected Retry to have BackoffRate for exponential backoff"
        expect(retry_entry["BackoffRate"]).to be_a(Numeric)
        expect(retry_entry["BackoffRate"]).to be > 1
      end
    end

    describe "fan-out with group limit" do
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
          batch_size 50

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "concurrent-fan"

          pipeline do
            files = discover(trigger_input)
            fan_out(process(files))
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "generates a Map state with inner Batch task" do
        process_state = asl["States"]["process"]
        expect(process_state["Type"]).to eq("Map")
        inner_task = process_state.dig("ItemProcessor", "States", "process_batch")
        expect(inner_task.dig("Parameters", "ArrayProperties", "Size.$")).to eq("$.size")
      end

      it "chunk state has group_size of 50" do
        payload = asl["States"]["process_chunk"].dig("Parameters", "Payload")
        expect(payload["group_size"]).to eq(50)
      end
    end

    describe "fan-out > 10,000 items generates chunk + batch states" do
      let(:pipeline_class) do
        stub_const("Process", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1
          batch_size 500

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "big-fan"

          pipeline do
            fan_out(process(trigger_input))
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "starts at the chunk state when first step is fan-out" do
        expect(asl["StartAt"]).to eq("process_chunk")
      end

      it "chunk state passes entire trigger payload for first step" do
        payload = asl["States"]["process_chunk"].dig("Parameters", "Payload")
        expect(payload["trigger.$"]).to eq("$")
        expect(payload).not_to have_key("prev_step")
        expect(payload).not_to have_key("input.$")
      end

      it "generates a Map state with inner Batch task" do
        process_state = asl["States"]["process"]
        expect(process_state["Type"]).to eq("Map")
        inner_task = process_state.dig("ItemProcessor", "States", "process_batch")
        expect(inner_task.dig("Parameters", "ArrayProperties", "Size.$")).to eq("$.size")
      end

      it "each chunk is capped at 10,000 items" do
        expect(Turbofan::Generators::ASL::MAX_ARRAY_SIZE).to eq(10_000)
      end

      it "does NOT set TURBOFAN_INPUT on the fan-out Map inner task" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        env = inner_task.dig("Parameters", "ContainerOverrides", "Environment")
        input_var = env.find { |e| e["Name"] == "TURBOFAN_INPUT" }
        expect(input_var).to be_nil,
          "Fan-out steps should not set TURBOFAN_INPUT (raw data hits 8KB Batch limit)"
      end
    end

    describe "fan-out steps do not get SFN Retry (retries are per-child at Batch level)" do
      let(:pipeline_class) do
        stub_const("Process", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1
          retries 3, on: ["States.TaskFailed"]
          batch_size 100

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "fan-out-retry"

          pipeline do
            fan_out(process(trigger_input))
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "does NOT add SFN Retry to the fan-out batch state" do
        process_state = asl["States"]["process"]
        expect(process_state).not_to have_key("Retry"),
          "Fan-out steps must not have SFN Retry — it re-submits the entire array. " \
          "Retries are handled per-child at the Batch level via retryStrategy.attempts."
      end

    end

    describe "fan-out timeout behavior" do
      it "fan-out Map state gets TimeoutSeconds from fan_out_timeout" do
        step_class = stub_const("Process", Class.new {
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          timeout 7200
          batch_size 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        pipeline = Class.new do
          include Turbofan::Pipeline
          pipeline_name "fan-timeout"
          pipeline do
            fan_out(process(trigger_input), timeout: 86400)
          end
        end
        gen = described_class.new(pipeline: pipeline, stage: "production", steps: {process: step_class})
        asl = gen.generate
        map_state = asl["States"]["process"]
        expect(map_state["TimeoutSeconds"]).to eq(86400)
      end

      it "fan-out Map state has no TimeoutSeconds when fan_out_timeout omitted" do
        step_class = stub_const("Process", Class.new {
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          timeout 7200
          batch_size 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        pipeline = Class.new do
          include Turbofan::Pipeline
          pipeline_name "fan-no-timeout"
          pipeline do
            fan_out(process(trigger_input))
          end
        end
        gen = described_class.new(pipeline: pipeline, stage: "production", steps: {process: step_class})
        asl = gen.generate
        map_state = asl["States"]["process"]
        expect(map_state).not_to have_key("TimeoutSeconds")
      end

      it "fan-out inner Batch task has NO TimeoutSeconds (step timeout goes to Batch job def only)" do
        step_class = stub_const("Process", Class.new {
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          timeout 7200
          batch_size 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        pipeline = Class.new do
          include Turbofan::Pipeline
          pipeline_name "fan-inner-no-timeout"
          pipeline do
            fan_out(process(trigger_input))
          end
        end
        gen = described_class.new(pipeline: pipeline, stage: "production", steps: {process: step_class})
        asl = gen.generate
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        expect(inner_task).not_to have_key("TimeoutSeconds"),
          "Step timeout must not be applied to fan-out inner task — it kills the entire parent array"
      end
    end

    describe "pipeline-level timeout" do
      it "pipeline with timeout gets top-level ASL TimeoutSeconds" do
        step_class = stub_const("Process", Class.new {
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        pipeline = Class.new do
          include Turbofan::Pipeline
          pipeline_name "pipeline-timeout"
          timeout 43200
          pipeline do
            process(trigger_input)
          end
        end
        gen = described_class.new(pipeline: pipeline, stage: "production", steps: {process: step_class})
        asl = gen.generate
        expect(asl["TimeoutSeconds"]).to eq(43200)
      end

      it "pipeline without timeout has no top-level TimeoutSeconds" do
        step_class = stub_const("Process", Class.new {
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        pipeline = Class.new do
          include Turbofan::Pipeline
          pipeline_name "pipeline-no-timeout"
          pipeline do
            process(trigger_input)
          end
        end
        gen = described_class.new(pipeline: pipeline, stage: "production", steps: {process: step_class})
        asl = gen.generate
        expect(asl).not_to have_key("TimeoutSeconds")
      end
    end

    describe "fan-out steps do not get SFN Retry (retries are per-child at Batch level)" do
      it "non-fan-out steps with retries on: still get SFN Retry" do
        extract_class = stub_const("Extract", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1
          retries 2, on: ["States.TaskFailed"]

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        pipeline = Class.new do
          include Turbofan::Pipeline
          pipeline_name "non-fan-retry"
          pipeline do
            extract(trigger_input)
          end
        end
        gen = described_class.new(pipeline: pipeline, stage: "production", steps: {extract: extract_class})
        result = gen.generate
        extract_state = result["States"]["extract"]
        expect(extract_state).to have_key("Retry")
        expect(extract_state["Retry"].first["ErrorEquals"]).to eq(["States.TaskFailed"])
      end
    end

    describe "fan-out S3 input/output paths" do
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
          batch_size 1

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

          pipeline_name "s3-paths"

          pipeline do
            files = discover(trigger_input)
            results = fan_out(process(files))
            aggregate(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "fan_out Map inner task sets TURBOFAN_STEP_NAME for S3 path construction" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        env = inner_task.dig("Parameters", "ContainerOverrides", "Environment")
        step_name_var = env&.find { |e| e["Name"] == "TURBOFAN_STEP_NAME" }
        expect(step_name_var).not_to be_nil,
          "expected fan_out Map inner task to set TURBOFAN_STEP_NAME for S3 path construction"
      end

      it "fan_out Map inner task sets TURBOFAN_EXECUTION_ID for S3 path construction" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        env = inner_task.dig("Parameters", "ContainerOverrides", "Environment")
        exec_id_var = env&.find { |e| e["Name"] == "TURBOFAN_EXECUTION_ID" }
        expect(exec_id_var).not_to be_nil
      end

      it "fan_out Map inner task sets TURBOFAN_BUCKET for S3 access" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        env = inner_task.dig("Parameters", "ContainerOverrides", "Environment")
        bucket_var = env&.find { |e| e["Name"] == "TURBOFAN_BUCKET" }
        expect(bucket_var).not_to be_nil
      end
    end

    describe "fan-out followed by regular step" do
      let(:pipeline_class) do
        stub_const("Process", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1
          batch_size 1

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

          pipeline_name "fan-then-step"

          pipeline do
            results = fan_out(process(trigger_input))
            aggregate(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "starts at the chunk state" do
        expect(asl["StartAt"]).to eq("process_chunk")
      end

      it "chains process_chunk -> process -> aggregate -> success" do
        expect(asl["States"]["process_chunk"]["Next"]).to eq("process")
        expect(asl["States"]["process"]["Next"]).to eq("aggregate")
        expect(asl["States"]["aggregate"]["Next"]).to match(/success/i)
      end

      it "aggregate step has TURBOFAN_PREV_FAN_OUT_PARENTS env var" do
        aggregate_state = asl["States"]["aggregate"]
        env = aggregate_state.dig("Parameters", "ContainerOverrides", "Environment")
        parents_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_FAN_OUT_PARENTS" }
        expect(parents_var).not_to be_nil
      end
    end

    describe "TURBOFAN_STEP_NAME for all steps" do
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
          batch_size 1

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

          pipeline_name "step-name-pipeline"

          pipeline do
            files = discover(trigger_input)
            results = fan_out(process(files))
            aggregate(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "sets TURBOFAN_STEP_NAME on non-fan-out steps too" do
        discover_state = asl["States"]["discover"]
        env = discover_state.dig("Parameters", "ContainerOverrides", "Environment")
        step_name_var = env&.find { |e| e["Name"] == "TURBOFAN_STEP_NAME" }
        expect(step_name_var).not_to be_nil,
          "expected non-fan-out step 'discover' to also have TURBOFAN_STEP_NAME"
        expect(step_name_var["Value"]).to eq("discover")
      end

      it "sets TURBOFAN_STEP_NAME on the aggregate step" do
        aggregate_state = asl["States"]["aggregate"]
        env = aggregate_state.dig("Parameters", "ContainerOverrides", "Environment")
        step_name_var = env&.find { |e| e["Name"] == "TURBOFAN_STEP_NAME" }
        expect(step_name_var).not_to be_nil,
          "expected non-fan-out step 'aggregate' to also have TURBOFAN_STEP_NAME"
        expect(step_name_var["Value"]).to eq("aggregate")
      end
    end

    describe "chained fan-out prev_fan_out_size in chunking Lambda" do
      let(:pipeline_class) do
        stub_const("StepA", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1
          batch_size 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("StepB", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1
          batch_size 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "chained-fan-outs"

          pipeline do
            a = fan_out(step_a(trigger_input))
            fan_out(step_b(a))
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "passes prev_fan_out_parents to the second chunking Lambda" do
        chunk_b = asl["States"]["step_b_chunk"]
        payload = chunk_b.dig("Parameters", "Payload")
        expect(payload).to have_key("prev_fan_out_parents.$"),
          "expected step_b_chunk Lambda payload to include prev_fan_out_parents.$ when prev step was fan-out"
        expect(payload["prev_fan_out_parents.$"]).to eq("States.JsonToString($.chunking.step_a.parents)")
      end

      it "does not pass prev_fan_out_size to the first chunking Lambda" do
        chunk_a = asl["States"]["step_a_chunk"]
        payload = chunk_a.dig("Parameters", "Payload")
        expect(payload).not_to have_key("prev_fan_out_size.$")
      end
    end

    describe "fan_in: false suppresses output collection" do
      let(:pipeline_class) do
        stub_const("Export", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Compute", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1
          batch_size 100

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("LoadObs", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "fan-in-false"

          pipeline do
            ex = export(trigger_input)
            computed = fan_out(compute(ex), fan_in: false)
            load_obs(computed)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "preserves DAG dependency (Map state runs before load_obs)" do
        expect(asl["States"]["compute"]["Next"]).to eq("load_obs")
      end

      it "does NOT set TURBOFAN_PREV_FAN_OUT_PARENTS on the fan_in: false step" do
        env = asl["States"]["load_obs"].dig("Parameters", "ContainerOverrides", "Environment")
        parents_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_FAN_OUT_PARENTS" }
        expect(parents_var).to be_nil
      end

      it "does NOT set TURBOFAN_PREV_STEP on the fan_in: false step" do
        env = asl["States"]["load_obs"].dig("Parameters", "ContainerOverrides", "Environment")
        prev_step_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEP" }
        expect(prev_step_var).to be_nil
      end

      it "sets TURBOFAN_INPUT to pass trigger input instead" do
        env = asl["States"]["load_obs"].dig("Parameters", "ContainerOverrides", "Environment")
        input_var = env.find { |e| e["Name"] == "TURBOFAN_INPUT" }
        expect(input_var).not_to be_nil
        expect(input_var["Value.$"]).to eq("States.JsonToString($.input)")
      end

      it "still chains the full pipeline correctly" do
        expect(asl["States"]["export"]["Next"]).to eq("compute_chunk")
        expect(asl["States"]["compute_chunk"]["Next"]).to eq("compute")
        expect(asl["States"]["compute"]["Next"]).to eq("load_obs")
        expect(asl["States"]["load_obs"]["Next"]).to match(/success/i)
      end
    end

    describe "fan-out as only step" do
      let(:pipeline_class) do
        stub_const("Process", Class.new {
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 1
          batch_size 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "fan-only"

          pipeline do
            fan_out(process(trigger_input))
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "starts at the chunk state when fan_out is the only step" do
        expect(asl["StartAt"]).to eq("process_chunk")
      end

      it "chunk state chains to the batch state" do
        expect(asl["States"]["process_chunk"]["Next"]).to eq("process")
      end

      it "Map state chains to success notification" do
        process_state = asl["States"]["process"]
        expect(process_state["Type"]).to eq("Map")
        expect(process_state["Next"]).to match(/success/i)
      end
    end
  end
end
