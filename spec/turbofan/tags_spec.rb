# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Tags DSL" do # rubocop:disable RSpec/DescribeClass
  describe "Step tags" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        tags stack: "geo", "stack-component": "validation"
      end
    end

    it "stores tags as a hash with string keys" do
      expect(step_class.turbofan_tags).to eq("stack" => "geo", "stack-component" => "validation")
    end

    it "converts symbol keys to strings" do
      expect(step_class.turbofan_tags.keys).to all(be_a(String))
    end
  end

  describe "Step tags default" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
      end
    end

    it "defaults to an empty hash when no tags declared" do
      expect(step_class.turbofan_tags).to eq({})
    end
  end

  describe "Pipeline tags" do
    let(:pipeline_class) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "tagged-pipeline"
        tags stack: "visitation"
      end
    end

    it "stores tags as a hash with string keys" do
      expect(pipeline_class.turbofan_tags).to eq("stack" => "visitation")
    end

    it "converts symbol keys to strings" do
      expect(pipeline_class.turbofan_tags.keys).to all(be_a(String))
    end
  end

  describe "Pipeline tags default" do
    let(:pipeline_class) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "untagged-pipeline"
      end
    end

    it "defaults to an empty hash when no tags declared" do
      expect(pipeline_class.turbofan_tags).to eq({})
    end
  end

  describe "Step tags with string keys passed directly" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        tags "already-string" => "value", :other => "sym"
      end
    end

    it "preserves string keys and converts symbol keys" do
      expect(step_class.turbofan_tags).to eq("already-string" => "value", "other" => "sym")
    end
  end

  describe "Pipeline tags with multiple entries" do
    let(:pipeline_class) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "multi-tagged"
        tags stack: "visitation", "stack-component": "visits_workers", team: "data"
      end
    end

    it "stores all tag pairs" do
      expect(pipeline_class.turbofan_tags).to eq(
        "stack" => "visitation",
        "stack-component" => "visits_workers",
        "team" => "data"
      )
    end
  end

  describe "tags overwrite on second call" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        tags stack: "geo"
        tags stack: "visitation"
      end
    end

    it "last tags call wins" do
      expect(step_class.turbofan_tags).to eq("stack" => "visitation")
    end
  end

  describe "tags with empty hash" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        tags({})
      end
    end

    it "stores an empty hash" do
      expect(step_class.turbofan_tags).to eq({})
    end
  end

  describe "Pipeline tags overwrite on second call" do
    let(:pipeline_class) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "overwritten-tags"
        tags stack: "geo"
        tags stack: "visitation", team: "data"
      end
    end

    it "last tags call wins and does not merge" do
      expect(pipeline_class.turbofan_tags).to eq("stack" => "visitation", "team" => "data")
    end

    it "does not retain keys from the first call" do
      # If first call had stack: "geo" and second has stack: "visitation", team: "data"
      # make sure there's no leftover from the first call beyond what the second provides
      expect(pipeline_class.turbofan_tags.keys).to contain_exactly("stack", "team")
    end
  end

  describe "tags with non-string values" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        tags version: 2, enabled: true
      end
    end

    it "stores non-string values as-is" do
      expect(step_class.turbofan_tags["version"]).to eq(2)
      expect(step_class.turbofan_tags["enabled"]).to be(true)
    end

    it "still converts keys to strings" do
      expect(step_class.turbofan_tags.keys).to all(be_a(String))
    end
  end

  describe "reserved turbofan: prefix validation" do
    it "raises ArgumentError when Step uses turbofan: prefix" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          tags "turbofan:foo" => "bar"
        end
      }.to raise_error(ArgumentError, /reserved.*turbofan:/)
    end

    it "raises ArgumentError when Pipeline uses turbofan: prefix" do
      expect {
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "bad-tags"
          tags "turbofan:foo" => "bar"
        end
      }.to raise_error(ArgumentError, /reserved.*turbofan:/)
    end

    it "raises ArgumentError when Step uses turbofan: prefix with symbol key" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          tags "turbofan:managed": "true"
        end
      }.to raise_error(ArgumentError, /reserved.*turbofan:/)
    end
  end

  describe "class isolation" do
    let(:step_a) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        tags stack: "geo"
      end
    end

    let(:step_b) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
      end
    end

    it "does not leak tags between step classes" do
      step_a
      step_b

      expect(step_a.turbofan_tags).to eq("stack" => "geo")
      expect(step_b.turbofan_tags).to eq({})
    end
  end
end
