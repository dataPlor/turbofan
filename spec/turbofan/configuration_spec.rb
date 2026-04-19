# frozen_string_literal: true

require "spec_helper"

# B8 — Turbofan.configure global config
RSpec.describe Turbofan::Configuration do
  after do
    # Reset global config between tests
    Turbofan.instance_variable_set(:@config, nil) if Turbofan.instance_variable_defined?(:@config)
  end

  describe "default values" do
    it "defaults bucket to nil" do
      config = described_class.new
      expect(config.bucket).to be_nil
    end

    it "defaults schemas_path to nil" do
      config = described_class.new
      expect(config.schemas_path).to be_nil
    end

    it "defaults default_region to nil" do
      config = described_class.new
      expect(config.default_region).to be_nil
    end

    it "defaults log_retention_days to 30" do
      config = described_class.new
      expect(config.log_retention_days).to eq(30)
    end
  end

  describe "Turbofan.configure" do
    it "sets configuration values via block" do
      Turbofan.configure { |c| c.bucket = "my-shared-bucket" }

      expect(Turbofan.config.bucket).to eq("my-shared-bucket")
    end
  end

  describe "Turbofan.config" do
    it "returns the configured value" do
      Turbofan.configure { |c| c.bucket = "another-bucket" }

      expect(Turbofan.config.bucket).to eq("another-bucket")
    end
  end
end
