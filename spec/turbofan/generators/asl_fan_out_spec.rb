require "spec_helper"
require "json"

RSpec.describe Turbofan::Generators::ASL, :schemas do
  describe "fan-out with array jobs (Task 13)" do
    describe "basic fan-out step" do
      let(:pipeline_class) do
        stub_const("Discover", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Process", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Aggregate", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "fan-out-pipeline"

          pipeline do
            files = discover(trigger_input)
            results = fan_out(process(files), batch_size: 100)
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

      it "chunk state references prev_step when not first" do
        payload = asl["States"]["process_chunk"].dig("Parameters", "Payload")
        expect(payload["prev_step"]).to eq("discover")
        expect(payload).not_to have_key("items.$")
      end

      it "chunk state has ResultSelector for chunk_count" do
        chunk_state = asl["States"]["process_chunk"]
        expect(chunk_state["ResultSelector"]).to eq({"chunk_count.$" => "$.Payload.chunk_count"})
      end

      it "chunk state stores result in $.chunking.process" do
        chunk_state = asl["States"]["process_chunk"]
        expect(chunk_state["ResultPath"]).to eq("$.chunking.process")
      end

      it "chunk state has Catch clause" do
        chunk_state = asl["States"]["process_chunk"]
        expect(chunk_state["Catch"]).to eq([{"ErrorEquals" => ["States.ALL"], "Next" => "NotifyFailure"}])
      end

      it "generates a Batch state with dynamic ArrayProperties for the fan_out step" do
        process_state = asl["States"]["process"]
        array_props = process_state.dig("Parameters", "ArrayProperties")
        expect(array_props).to eq({"Size.$" => "$.chunking.process.chunk_count"})
      end

      it "aggregate step has TURBOFAN_PREV_FAN_OUT_SIZE env var" do
        aggregate_state = asl["States"]["aggregate"]
        env = aggregate_state.dig("Parameters", "ContainerOverrides", "Environment")
        fan_out_size_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_FAN_OUT_SIZE" }
        expect(fan_out_size_var).not_to be_nil
        expect(fan_out_size_var["Value.$"]).to eq("States.JsonToString($.chunking.process.chunk_count)")
      end

      it "references the correct job definition for the fan_out step" do
        process_state = asl["States"]["process"]
        job_def = process_state.dig("Parameters", "JobDefinition")
        expect(job_def).to include("process")
      end

      it "references the correct job queue for the fan_out step" do
        process_state = asl["States"]["process"]
        job_queue = process_state.dig("Parameters", "JobQueue")
        expect(job_queue).to include("process")
      end
    end

    describe "chunk state retry configuration" do
      let(:pipeline_class) do
        stub_const("Discover", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Process", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "retry-pipeline"

          pipeline do
            files = discover(trigger_input)
            fan_out(process(files), batch_size: 100)
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

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Process", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "concurrent-fan"

          pipeline do
            files = discover(trigger_input)
            fan_out(process(files), batch_size: 50)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "generates a valid fan_out state with dynamic ArrayProperties" do
        process_state = asl["States"]["process"]
        expect(process_state["Type"]).to eq("Task")
        expect(process_state.dig("Parameters", "ArrayProperties", "Size.$")).to eq("$.chunking.process.chunk_count")
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

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "big-fan"

          pipeline do
            fan_out(process(trigger_input), batch_size: 500)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "starts at the chunk state when first step is fan-out" do
        expect(asl["StartAt"]).to eq("process_chunk")
      end

      it "chunk state has items.$ for first step" do
        payload = asl["States"]["process_chunk"].dig("Parameters", "Payload")
        expect(payload["items.$"]).to eq("$.input")
        expect(payload).not_to have_key("prev_step")
      end

      it "generates a Task state with dynamic ArrayProperties" do
        process_state = asl["States"]["process"]
        expect(process_state["Type"]).to eq("Task")
        expect(process_state.dig("Parameters", "ArrayProperties", "Size.$")).to eq("$.chunking.process.chunk_count")
      end

      it "each chunk is capped at 10,000 items" do
        expect(Turbofan::Generators::ASL::MAX_ARRAY_SIZE).to eq(10_000)
      end
    end

    describe "fan-out S3 input/output paths" do
      let(:pipeline_class) do
        stub_const("Discover", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Process", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Aggregate", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "s3-paths"

          pipeline do
            files = discover(trigger_input)
            results = fan_out(process(files), batch_size: 1)
            aggregate(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "fan_out step sets TURBOFAN_STEP_NAME for S3 path construction" do
        process_state = asl["States"]["process"]
        env = process_state.dig("Parameters", "ContainerOverrides", "Environment")
        step_name_var = env&.find { |e| e["Name"] == "TURBOFAN_STEP_NAME" }
        expect(step_name_var).not_to be_nil,
          "expected fan_out step to set TURBOFAN_STEP_NAME for S3 path construction"
      end

      it "fan_out step sets TURBOFAN_EXECUTION_ID for S3 path construction" do
        process_state = asl["States"]["process"]
        env = process_state.dig("Parameters", "ContainerOverrides", "Environment")
        exec_id_var = env&.find { |e| e["Name"] == "TURBOFAN_EXECUTION_ID" }
        expect(exec_id_var).not_to be_nil
      end

      it "fan_out step sets TURBOFAN_BUCKET for S3 access" do
        process_state = asl["States"]["process"]
        env = process_state.dig("Parameters", "ContainerOverrides", "Environment")
        bucket_var = env&.find { |e| e["Name"] == "TURBOFAN_BUCKET" }
        expect(bucket_var).not_to be_nil
      end
    end

    describe "fan-out followed by regular step" do
      let(:pipeline_class) do
        stub_const("Process", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Aggregate", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "fan-then-step"

          pipeline do
            results = fan_out(process(trigger_input), batch_size: 1)
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

      it "aggregate step has TURBOFAN_PREV_FAN_OUT_SIZE env var" do
        aggregate_state = asl["States"]["aggregate"]
        env = aggregate_state.dig("Parameters", "ContainerOverrides", "Environment")
        fan_out_size_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_FAN_OUT_SIZE" }
        expect(fan_out_size_var).not_to be_nil
      end
    end

    describe "TURBOFAN_STEP_NAME for all steps" do
      let(:pipeline_class) do
        stub_const("Discover", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Process", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Aggregate", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "step-name-pipeline"

          pipeline do
            files = discover(trigger_input)
            results = fan_out(process(files), batch_size: 1)
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

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("StepB", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "chained-fan-outs"

          pipeline do
            a = fan_out(step_a(trigger_input), batch_size: 1)
            fan_out(step_b(a), batch_size: 1)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "passes prev_fan_out_size to the second chunking Lambda" do
        chunk_b = asl["States"]["step_b_chunk"]
        payload = chunk_b.dig("Parameters", "Payload")
        expect(payload).to have_key("prev_fan_out_size.$"),
          "expected step_b_chunk Lambda payload to include prev_fan_out_size.$ when prev step was fan-out"
        expect(payload["prev_fan_out_size.$"]).to eq("States.JsonToString($.chunking.step_a.chunk_count)")
      end

      it "does not pass prev_fan_out_size to the first chunking Lambda" do
        chunk_a = asl["States"]["step_a_chunk"]
        payload = chunk_a.dig("Parameters", "Payload")
        expect(payload).not_to have_key("prev_fan_out_size.$")
      end
    end

    describe "fan-out as only step" do
      let(:pipeline_class) do
        stub_const("Process", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "fan-only"

          pipeline do
            fan_out(process(trigger_input), batch_size: 1)
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

      it "batch state chains to success notification" do
        process_state = asl["States"]["process"]
        expect(process_state["Type"]).to eq("Task")
        expect(process_state["Next"]).to match(/success/i)
      end
    end
  end
end
