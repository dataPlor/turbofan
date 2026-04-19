# frozen_string_literal: true

require "spec_helper"
require "json"

RSpec.describe Turbofan::Runtime::InputResolver do
  describe ".normalize_envelope" do
    it "converts Array input to {inputs: array}" do
      result = described_class.send(:normalize_envelope, [{"a" => 1}, {"b" => 2}])
      expect(result).to eq({"inputs" => [{"a" => 1}, {"b" => 2}]})
    end

    it "passes through Hash with inputs key" do
      input = {"inputs" => [{"x" => 1}]}
      result = described_class.send(:normalize_envelope, input)
      expect(result).to eq({"inputs" => [{"x" => 1}]})
    end

    it "converts Hash with items key to inputs (backward compat)" do
      input = {"items" => [{"x" => 1}]}
      result = described_class.send(:normalize_envelope, input)
      expect(result).to eq({"inputs" => [{"x" => 1}]})
    end

    it "wraps a bare Hash into a single-element inputs array" do
      input = {"key" => "val"}
      result = described_class.send(:normalize_envelope, input)
      expect(result).to eq({"inputs" => [{"key" => "val"}]})
    end
  end
end
