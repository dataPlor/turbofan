require "spec_helper"
require "json"

RSpec.describe Turbofan::Generators::ASL, :schemas do
  describe "SNS notification states (Task 18)" do
    describe "single-step pipeline with notifications" do
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

          pipeline_name "notify-pipeline"

          pipeline do
            process(trigger_input)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }
      let(:states) { asl["States"] }

      it "has a failure notification state" do
        failure_state = states.find { |name, _| name.match?(/fail/i) }
        expect(failure_state).not_to be_nil
      end

      it "has a success notification state" do
        success_state = states.find { |name, _| name.match?(/success/i) }
        expect(success_state).not_to be_nil
      end

      it "last step has a Catch clause" do
        process_state = states["process"]
        expect(process_state).to have_key("Catch")
      end

      it "Catch clause points to failure notification state" do
        process_state = states["process"]
        catch_clauses = process_state["Catch"]
        failure_state_name = states.keys.find { |k| k.match?(/fail/i) }
        catch_targets = catch_clauses.map { |c| c["Next"] }
        expect(catch_targets).to include(failure_state_name)
      end

      it "failure notification state publishes to SNS topic" do
        failure_state_name = states.keys.find { |k| k.match?(/fail/i) }
        failure_state = states[failure_state_name]
        expect(failure_state["Type"]).to eq("Task")
        expect(failure_state["Resource"]).to match(/sns/)
      end

      it "success notification state is at end of pipeline" do
        success_state_name = states.keys.find { |k| k.match?(/success/i) }
        success_state = states[success_state_name]
        expect(success_state["End"]).to be true
      end

      it "success notification state publishes to SNS topic" do
        success_state_name = states.keys.find { |k| k.match?(/success/i) }
        success_state = states[success_state_name]
        expect(success_state["Type"]).to eq("Task")
        expect(success_state["Resource"]).to match(/sns/)
      end

      it "success notification includes execution summary in message" do
        success_state_name = states.keys.find { |k| k.match?(/success/i) }
        success_state = states[success_state_name]
        params = success_state["Parameters"]
        expect(params).not_to be_nil
        # Message should reference the execution or include summary info
        expect(params["Message"] || params["Message.$"]).not_to be_nil
      end

      it "process step chains to success notification (not End)" do
        process_state = states["process"]
        success_state_name = states.keys.find { |k| k.match?(/success/i) }
        expect(process_state["Next"]).to eq(success_state_name)
        expect(process_state).not_to have_key("End")
      end
    end

    describe "multi-step pipeline with notifications" do
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

          pipeline_name "multi-notify"

          pipeline do
            results = extract(trigger_input)
            load(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }
      let(:states) { asl["States"] }

      it "last step (load) has a Catch clause for failure notification" do
        load_state = states["load"]
        expect(load_state).to have_key("Catch")
      end

      it "last step chains to success notification" do
        load_state = states["load"]
        success_state_name = states.keys.find { |k| k.match?(/success/i) }
        expect(load_state["Next"]).to eq(success_state_name)
      end

      it "extract step chains to load step (not directly to notification)" do
        extract_state = states["extract"]
        expect(extract_state["Next"]).to eq("load")
      end

      it "ALL steps have a Catch clause for failure notification, not just the last" do
        batch_states = states.select { |_name, state| state["Resource"]&.include?("batch") }
        batch_states.each do |name, state|
          expect(state).to have_key("Catch"),
            "expected step '#{name}' to have a Catch clause"
          catch_targets = state["Catch"].map { |c| c["Next"] }
          failure_state_name = states.keys.find { |k| k.match?(/fail/i) }
          expect(catch_targets).to include(failure_state_name),
            "expected step '#{name}' Catch to route to failure notification"
        end
      end
    end

    describe "three-step pipeline catch clauses" do
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

          pipeline_name "three-catch"

          pipeline do
            a = extract(trigger_input)
            b = transform(a)
            load(b)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }
      let(:states) { asl["States"] }

      it "every batch step has a Catch clause routing to NotifyFailure" do
        %w[extract transform load].each do |step_name|
          state = states[step_name]
          expect(state).to have_key("Catch"),
            "expected step '#{step_name}' to have a Catch clause"
          expect(state["Catch"].first["ErrorEquals"]).to eq(["States.ALL"])
          expect(state["Catch"].first["Next"]).to eq("NotifyFailure")
        end
      end

      it "intermediate steps still chain via Next on the success path" do
        expect(states["extract"]["Next"]).to eq("transform")
        expect(states["transform"]["Next"]).to eq("load")
        expect(states["load"]["Next"]).to eq("NotifySuccess")
      end
    end

    describe "SNS topic ARN reference" do
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

          pipeline_name "arn-ref"

          pipeline do
            process(trigger_input)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }
      let(:states) { asl["States"] }

      it "notification states reference an SNS TopicArn" do
        success_state_name = states.keys.find { |k| k.match?(/success/i) }
        success_state = states[success_state_name]
        params = success_state["Parameters"]
        topic_arn = params["TopicArn"] || params["TopicArn.$"]
        expect(topic_arn).not_to be_nil
      end

      it "uses CloudFormation pseudo-parameter placeholders in TopicArn" do
        success_state_name = states.keys.find { |k| k.match?(/success/i) }
        success_state = states[success_state_name]
        topic_arn = success_state["Parameters"]["TopicArn"]
        expect(topic_arn).to include("${AWS::Region}")
        expect(topic_arn).to include("${AWS::AccountId}")
        expect(topic_arn).not_to include("${region}")
        expect(topic_arn).not_to include("${account}")
      end
    end
  end
end
