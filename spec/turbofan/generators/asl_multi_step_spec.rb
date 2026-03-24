require "spec_helper"
require "json"

RSpec.describe Turbofan::Generators::ASL, :schemas do
  describe "multi-step data flow (Task 11)" do
    describe "two-step pipeline environment variables" do
      let(:pipeline_class) do
        stub_const("Extract", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Load", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "multi-step"

          pipeline do
            results = extract(trigger_input)
            load(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      let(:batch_states) do
        asl["States"].select { |_name, state| state["Resource"]&.include?("batch") }
      end

      it "sets TURBOFAN_EXECUTION_ID on all batch steps" do
        batch_states.each do |_name, state|
          env = state.dig("Parameters", "ContainerOverrides", "Environment")
          exec_var = env.find { |e| e["Name"] == "TURBOFAN_EXECUTION_ID" }
          expect(exec_var).not_to be_nil
          expect(exec_var["Value.$"]).to eq("$$.Execution.Id")
        end
      end

      it "sets TURBOFAN_STAGE on all batch steps" do
        batch_states.each do |_name, state|
          env = state.dig("Parameters", "ContainerOverrides", "Environment")
          stage_var = env.find { |e| e["Name"] == "TURBOFAN_STAGE" }
          expect(stage_var).not_to be_nil
        end
      end

      it "sets TURBOFAN_PIPELINE on all batch steps" do
        batch_states.each do |_name, state|
          env = state.dig("Parameters", "ContainerOverrides", "Environment")
          pipeline_var = env.find { |e| e["Name"] == "TURBOFAN_PIPELINE" }
          expect(pipeline_var).not_to be_nil
        end
      end

      it "sets TURBOFAN_BUCKET on all batch steps" do
        batch_states.each do |_name, state|
          env = state.dig("Parameters", "ContainerOverrides", "Environment")
          bucket_var = env.find { |e| e["Name"] == "TURBOFAN_BUCKET" }
          expect(bucket_var).not_to be_nil
        end
      end

      it "sets TURBOFAN_INPUT on the first step from execution input" do
        first_state = asl["States"]["extract"]
        env = first_state.dig("Parameters", "ContainerOverrides", "Environment")
        input_var = env.find { |e| e["Name"] == "TURBOFAN_INPUT" }
        expect(input_var).not_to be_nil
        expect(input_var["Value.$"]).to include("$.input")
      end

      it "does not set TURBOFAN_PREV_STEP on the first step" do
        first_state = asl["States"]["extract"]
        env = first_state.dig("Parameters", "ContainerOverrides", "Environment")
        prev_step_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEP" }
        expect(prev_step_var).to be_nil
      end

      it "sets TURBOFAN_PREV_STEP on the second step" do
        second_state = asl["States"]["load"]
        env = second_state.dig("Parameters", "ContainerOverrides", "Environment")
        prev_step_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEP" }
        expect(prev_step_var).not_to be_nil
        expect(prev_step_var["Value"]).to eq("extract")
      end
    end

    describe "three-step pipeline chaining" do
      let(:pipeline_class) do
        stub_const("Extract", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Transform", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Load", Class.new {
          include Turbofan::Step

          compute_environment TestCe
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

      it "sets TURBOFAN_PREV_STEP on the second step to the first step name" do
        env = asl["States"]["transform"].dig("Parameters", "ContainerOverrides", "Environment")
        prev_step_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEP" }
        expect(prev_step_var).not_to be_nil
        expect(prev_step_var["Value"]).to eq("extract")
      end

      it "sets TURBOFAN_PREV_STEP on the third step to the second step name" do
        env = asl["States"]["load"].dig("Parameters", "ContainerOverrides", "Environment")
        prev_step_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEP" }
        expect(prev_step_var).not_to be_nil
        expect(prev_step_var["Value"]).to eq("transform")
      end

      it "only the first step reads from execution input" do
        extract_env = asl["States"]["extract"].dig("Parameters", "ContainerOverrides", "Environment")
        input_var = extract_env.find { |e| e["Name"] == "TURBOFAN_INPUT" }
        expect(input_var).not_to be_nil

        %w[transform load].each do |step_name|
          env = asl["States"][step_name].dig("Parameters", "ContainerOverrides", "Environment")
          input_from_exec = env.find { |e| e["Name"] == "TURBOFAN_INPUT" && e["Value.$"]&.include?("$.input") }
          expect(input_from_exec).to be_nil,
            "expected #{step_name} not to read TURBOFAN_INPUT from execution input"
        end
      end
    end

    describe "ResultSelector for extracting Batch response metadata" do
      let(:pipeline_class) do
        stub_const("Extract", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Load", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "result-select"

          pipeline do
            results = extract(trigger_input)
            load(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "includes ResultSelector on non-terminal steps" do
        extract_state = asl["States"]["extract"]
        expect(extract_state).to have_key("ResultSelector"),
          "expected extract state to have ResultSelector for extracting step output"
      end

      it "extracts useful metadata from Batch response via ResultSelector" do
        extract_state = asl["States"]["extract"]
        selector = extract_state["ResultSelector"]
        expect(selector).to be_a(Hash)
      end
    end

    describe "ResultPath for data flow between steps" do
      let(:pipeline_class) do
        stub_const("Extract", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Load", Class.new {
          include Turbofan::Step

          compute_environment TestCe
          cpu 1

          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "result-path"

          pipeline do
            results = extract(trigger_input)
            load(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "uses ResultPath to place step output in state" do
        extract_state = asl["States"]["extract"]
        expect(extract_state).to have_key("ResultPath"),
          "expected extract state to have ResultPath for placing output in state"
      end
    end
  end
end
