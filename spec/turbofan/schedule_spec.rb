# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Schedule DSL" do # rubocop:disable RSpec/DescribeClass
  describe "Pipeline schedule" do
    let(:pipeline_class) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "scheduled-pipeline"
        schedule "0 6 * * ? *"
      end
    end

    it "stores the cron string" do
      expect(pipeline_class.turbofan_schedule).to eq("0 6 * * ? *")
    end
  end

  describe "Pipeline schedule default" do
    let(:pipeline_class) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "unscheduled-pipeline"
      end
    end

    it "defaults to nil when no schedule declared" do
      expect(pipeline_class.turbofan_schedule).to be_nil
    end
  end

  describe "schedule overwrite on second call" do
    let(:pipeline_class) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "overwritten-schedule"
        schedule "0 6 * * ? *"
        schedule "30 12 * * ? *"
      end
    end

    it "last schedule call wins" do
      expect(pipeline_class.turbofan_schedule).to eq("30 12 * * ? *")
    end
  end

  describe "schedule with empty string" do
    let(:pipeline_class) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "empty-schedule"
        schedule ""
      end
    end

    it "stores the empty string" do
      expect(pipeline_class.turbofan_schedule).to eq("")
    end
  end

  describe "turbofan check validates cron field count", :schemas do
    let(:pipeline_class) do
      stub_const("OnlyStep", Class.new {
        include Turbofan::Step
        runs_on :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "bad-cron"
        schedule "0 6 * *"  # only 4 fields, EventBridge requires 6

        pipeline do
          only_step(trigger_input)
        end
      end
    end

    let(:step_class) do
      Class.new do
        include Turbofan::Step
        runs_on :batch
        compute_environment :test_ce
        cpu 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    it "reports an error or warning about invalid cron field count" do
      result = Turbofan::Check::PipelineCheck.run(
        pipeline: pipeline_class,
        steps: {only_step: step_class}
      )
      all_messages = result.errors + result.warnings
      expect(all_messages).to include(
        a_string_matching(/cron|schedule|field/i)
      )
    end
  end

  describe "valid 6-field cron passes check", :schemas do
    let(:pipeline_class) do
      stub_const("OnlyStep", Class.new {
        include Turbofan::Step
        runs_on :batch
        compute_environment :test_ce
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "good-cron"
        schedule "0 6 * * ? *"

        pipeline do
          only_step(trigger_input)
        end
      end
    end

    let(:step_class) do
      Class.new do
        include Turbofan::Step
        runs_on :batch
        compute_environment :test_ce
        cpu 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    it "does not report schedule-related errors" do
      result = Turbofan::Check::PipelineCheck.run(
        pipeline: pipeline_class,
        steps: {only_step: step_class}
      )
      schedule_issues = (result.errors + result.warnings).select { |m| m.match?(/cron|schedule|field/i) }
      expect(schedule_issues).to be_empty
    end
  end

  describe "class isolation" do
    let(:pipeline_a) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "pipeline-a"
        schedule "0 6 * * ? *"
      end
    end

    let(:pipeline_b) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "pipeline-b"
      end
    end

    it "does not leak schedule between pipeline classes" do
      pipeline_a
      pipeline_b

      expect(pipeline_a.turbofan_schedule).to eq("0 6 * * ? *")
      expect(pipeline_b.turbofan_schedule).to be_nil
    end
  end
end
