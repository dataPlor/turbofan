# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Discovery do
  describe ".subclasses_of memoization" do
    it "returns the same array object across repeat calls (cache hit)" do
      first = described_class.subclasses_of(Turbofan::Step)
      second = described_class.subclasses_of(Turbofan::Step)
      expect(second).to equal(first)
    end

    it "does NOT touch ObjectSpace on cache hits" do
      described_class.subclasses_of(Turbofan::Step) # prime the cache

      expect(ObjectSpace).not_to receive(:each_object)
      described_class.subclasses_of(Turbofan::Step)
      described_class.subclasses_of(Turbofan::Step)
    end

    it "caches per module independently" do
      step_first = described_class.subclasses_of(Turbofan::Step)
      pipeline_first = described_class.subclasses_of(Turbofan::Pipeline)

      expect(ObjectSpace).not_to receive(:each_object)
      expect(described_class.subclasses_of(Turbofan::Step)).to equal(step_first)
      expect(described_class.subclasses_of(Turbofan::Pipeline)).to equal(pipeline_first)
    end

    it "picks up newly-defined subclasses automatically (Step.included hook resets cache)" do
      before_count = described_class.subclasses_of(Turbofan::Step).size

      new_step = Class.new { include Turbofan::Step }
      stub_const("MemoTestStep", new_step)

      # No explicit reset_cache! needed — Step.included already fired.
      after_classes = described_class.subclasses_of(Turbofan::Step)
      expect(after_classes).to include(new_step)
      expect(after_classes.size).to eq(before_count + 1)
    end
  end

  describe ".reset_cache!" do
    it "clears every module's cached entry" do
      step_first = described_class.subclasses_of(Turbofan::Step)
      described_class.reset_cache!
      step_second = described_class.subclasses_of(Turbofan::Step)
      # Same contents, but fresh array — proves compute_subclasses_of ran.
      expect(step_second).to eq(step_first)
      expect(step_second).not_to equal(step_first)
    end

    it "is idempotent — safe to call on an empty cache" do
      described_class.reset_cache!
      expect { described_class.reset_cache! }.not_to raise_error
    end
  end
end
