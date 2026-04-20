# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Check::PipelineCheck, "trigger validation", :schemas do # rubocop:disable RSpec/DescribeMethod
  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::TrigCheckCe", klass)
    klass
  end

  let(:step_class) do
    ce_class
    Class.new do
      include Turbofan::Step
      runs_on :batch
      compute_environment :trig_check_ce
      cpu 1
      ram 2
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  def run_check(pipeline)
    described_class.run(pipeline: pipeline, steps: {my_step: step_class})
  end

  def build_pipeline(&block)
    Class.new do
      include Turbofan::Pipeline
      pipeline_name "my-pipeline"
      instance_eval(&block) if block
    end
  end

  describe "trigger :schedule" do
    it "passes for a valid 6-field cron" do
      pl = build_pipeline { trigger :schedule, cron: "0 5 * * ? *" }
      result = run_check(pl)
      expect(result.errors.select { |e| e.include?("cron") || e.include?("EventBridge") }).to be_empty
    end

    it "errors on a 5-field cron" do
      pl = build_pipeline { trigger :schedule, cron: "0 5 * * *" }
      result = run_check(pl)
      expect(result.errors).to include(a_string_matching(/5 fields.*EventBridge requires exactly 6/))
    end

    it "errors on a 7-field cron" do
      pl = build_pipeline { trigger :schedule, cron: "0 5 * * ? * *" }
      result = run_check(pl)
      expect(result.errors).to include(a_string_matching(/7 fields/))
    end
  end

  describe "trigger :event" do
    it "passes for a well-formed event trigger" do
      pl = build_pipeline do
        trigger :event, source: "aws.s3", detail_type: "Object Created",
          detail: {"bucket" => {"name" => ["my-bucket"]}}
      end
      result = run_check(pl)
      expect(result.errors).to be_empty
    end

    it "warns on an empty detail Hash (matches nothing)" do
      pl = build_pipeline { trigger :event, source: "aws.s3", detail: {} }
      result = run_check(pl)
      expect(result.warnings).to include(a_string_matching(/empty Hash — matches nothing/))
    end

    it "has no detail warning when detail kwarg is omitted entirely" do
      pl = build_pipeline { trigger :event, source: "aws.s3" }
      result = run_check(pl)
      expect(result.warnings.select { |w| w.include?("detail") }).to be_empty
    end

    it "passes with custom event_bus" do
      pl = build_pipeline { trigger :event, source: "myapp", event_bus: "ops-bus" }
      result = run_check(pl)
      expect(result.errors).to be_empty
    end
  end

  describe "multiple triggers" do
    it "reports cron errors with the index for disambiguation" do
      pl = build_pipeline do
        trigger :schedule, cron: "0 5 * * ? *"    # idx 0, OK
        trigger :event, source: "aws.s3"           # idx 1, OK
        trigger :schedule, cron: "0 5 * *"          # idx 2, BAD (4 fields)
      end
      result = run_check(pl)
      expect(result.errors).to include(a_string_matching(/4 fields/))
    end
  end

  describe "no triggers declared" do
    it "passes without emitting schedule/event-related errors" do
      pl = build_pipeline {}
      result = run_check(pl)
      expect(result.errors.select { |e| e.match?(/cron|EventBridge|trigger/i) }).to be_empty
    end
  end
end
