# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Turbofan::Pipeline #trigger macro" do
  def pipeline(&block)
    Class.new do
      include Turbofan::Pipeline
      instance_eval(&block) if block
    end
  end

  describe "type validation" do
    it "rejects unknown trigger types" do
      expect {
        pipeline { trigger :fax, number: "555-1212" }
      }.to raise_error(ArgumentError, /trigger type must be one of/)
    end

    it "rejects missing type" do
      expect {
        pipeline { trigger }
      }.to raise_error(ArgumentError)
    end
  end

  describe "trigger :schedule" do
    it "stores a schedule trigger with cron kwarg" do
      pl = pipeline { trigger :schedule, cron: "0 5 * * ? *" }
      expect(pl.turbofan_triggers).to eq([{type: :schedule, cron: "0 5 * * ? *"}])
    end

    it "requires a cron kwarg" do
      expect {
        pipeline { trigger :schedule }
      }.to raise_error(ArgumentError, /requires a `cron:` kwarg/)
    end

    it "rejects empty cron strings" do
      expect {
        pipeline { trigger :schedule, cron: "" }
      }.to raise_error(ArgumentError, /requires a `cron:` kwarg/)
    end

    it "rejects extraneous kwargs" do
      expect {
        pipeline { trigger :schedule, cron: "0 * * * ? *", source: "aws.s3" }
      }.to raise_error(ArgumentError, /does not accept \[:source\]/)
    end
  end

  describe "trigger :event" do
    it "stores an event trigger with a single source String" do
      pl = pipeline { trigger :event, source: "aws.s3" }
      expect(pl.turbofan_triggers).to eq([{type: :event, source: ["aws.s3"]}])
    end

    it "stores an event trigger with an Array source" do
      pl = pipeline { trigger :event, source: ["aws.s3", "aws.batch"] }
      expect(pl.turbofan_triggers).to eq([{type: :event, source: ["aws.s3", "aws.batch"]}])
    end

    it "accepts detail_type as String" do
      pl = pipeline {
        trigger :event, source: "aws.s3", detail_type: "Object Created"
      }
      expect(pl.turbofan_triggers.first[:detail_type]).to eq(["Object Created"])
    end

    it "accepts detail_type as Array of Strings" do
      pl = pipeline {
        trigger :event, source: "aws.batch", detail_type: ["Batch Job State Change", "Batch Job Queue State Change"]
      }
      expect(pl.turbofan_triggers.first[:detail_type]).to eq(["Batch Job State Change", "Batch Job Queue State Change"])
    end

    it "accepts detail pattern as Hash" do
      pl = pipeline {
        trigger :event,
          source: "aws.s3",
          detail_type: "Object Created",
          detail: {"bucket" => {"name" => ["my-bucket"]}}
      }
      expect(pl.turbofan_triggers.first[:detail]).to eq({"bucket" => {"name" => ["my-bucket"]}})
    end

    it "accepts event_bus" do
      pl = pipeline { trigger :event, source: "myapp", event_bus: "ops-bus" }
      expect(pl.turbofan_triggers.first[:event_bus]).to eq("ops-bus")
    end

    it "requires source" do
      expect {
        pipeline { trigger :event, detail_type: "Object Created" }
      }.to raise_error(ArgumentError, /requires a non-empty `source:` kwarg/)
    end

    it "rejects empty source String" do
      expect {
        pipeline { trigger :event, source: "" }
      }.to raise_error(ArgumentError, /requires a non-empty `source:` kwarg/)
    end

    it "rejects empty source Array" do
      expect {
        pipeline { trigger :event, source: [] }
      }.to raise_error(ArgumentError, /requires a non-empty `source:` kwarg/)
    end

    it "rejects non-String source entries" do
      expect {
        pipeline { trigger :event, source: [:aws_s3] }
      }.to raise_error(ArgumentError, /`source:` must be a String or Array of Strings/)
    end

    it "rejects non-Hash detail" do
      expect {
        pipeline { trigger :event, source: "aws.s3", detail: "not a hash" }
      }.to raise_error(ArgumentError, /`detail:` must be a Hash/)
    end

    it "rejects non-String detail_type entries" do
      expect {
        pipeline { trigger :event, source: "aws.s3", detail_type: [1, 2] }
      }.to raise_error(ArgumentError, /`detail_type:` must be a String or Array of Strings/)
    end

    it "rejects empty event_bus" do
      expect {
        pipeline { trigger :event, source: "myapp", event_bus: "" }
      }.to raise_error(ArgumentError, /`event_bus:` must be a non-empty String/)
    end

    it "rejects extraneous kwargs" do
      expect {
        pipeline { trigger :event, source: "aws.s3", cron: "0 * * * ? *" }
      }.to raise_error(ArgumentError, /does not accept \[:cron\]/)
    end
  end

  describe "multiple triggers" do
    it "accumulates into turbofan_triggers in declaration order" do
      pl = pipeline do
        trigger :schedule, cron: "0 5 * * ? *"
        trigger :event, source: "aws.s3", detail_type: "Object Created"
        trigger :event, source: "aws.batch"
      end
      expect(pl.turbofan_triggers.size).to eq(3)
      expect(pl.turbofan_triggers.map { |t| t[:type] }).to eq([:schedule, :event, :event])
    end
  end

  describe "no triggers declared" do
    it "has an empty triggers list (manual-invocation-only)" do
      pl = pipeline {}
      expect(pl.turbofan_triggers).to eq([])
    end
  end

  describe "trigger entries are frozen" do
    it "prevents downstream code from mutating stored entries" do
      pl = pipeline { trigger :event, source: "aws.s3" }
      expect(pl.turbofan_triggers.first).to be_frozen
    end

    it "subclass inherits fresh triggers array (not shared)" do
      parent = pipeline { trigger :schedule, cron: "0 5 * * ? *" }
      child = Class.new(parent)
      expect(child.turbofan_triggers).to eq([])
      expect(parent.turbofan_triggers.size).to eq(1)
    end
  end
end
