# frozen_string_literal: true

require "spec_helper"

# Pins the Turbofan::Error hierarchy contract. The base class + mid-level
# groupings let users rescue at whatever granularity makes sense for
# their code: rescue Turbofan::Error for anything, rescue
# Turbofan::ValidationError for schema/check failures, or the specific
# subclass for targeted handling.
RSpec.describe "Turbofan error hierarchy" do
  it "Turbofan::Error is a StandardError subclass" do
    expect(Turbofan::Error).to be < StandardError
  end

  it "ConfigError and ValidationError are direct children of Turbofan::Error" do
    expect(Turbofan::ConfigError).to be < Turbofan::Error
    expect(Turbofan::ValidationError).to be < Turbofan::Error
  end

  it "reparents schema errors under ValidationError" do
    expect(Turbofan::SchemaIncompatibleError).to be < Turbofan::ValidationError
    expect(Turbofan::SchemaValidationError).to be < Turbofan::ValidationError
  end

  it "reparents resource/extension errors under ConfigError" do
    expect(Turbofan::ResourceUnavailableError).to be < Turbofan::ConfigError
    expect(Turbofan::ExtensionLoadError).to be < Turbofan::ConfigError
  end

  it "reparents Subprocess::Error under Turbofan::Error" do
    expect(Turbofan::Subprocess::Error).to be < Turbofan::Error
  end

  it "reparents Router::InvalidSizeError under ValidationError" do
    expect(Turbofan::Router::InvalidSizeError).to be < Turbofan::ValidationError
  end

  it "reparents Runtime::Payload::HydrationError under Turbofan::Error" do
    expect(Turbofan::Runtime::Payload::HydrationError).to be < Turbofan::Error
  end

  it "reparents FanOut WorkerError and WorkerErrors under Turbofan::Error" do
    expect(Turbofan::Runtime::FanOut::WorkerError).to be < Turbofan::Error
    expect(Turbofan::Runtime::FanOut::WorkerErrors).to be < Turbofan::Error
  end

  it "Turbofan::Interrupted is intentionally NOT a Turbofan::Error (SystemExit subclass)" do
    expect(Turbofan::Interrupted).to be < SystemExit
    expect(Turbofan::Interrupted).not_to be < Turbofan::Error
  end

  it "rescue Turbofan::Error catches schema, config, subprocess, and worker errors" do
    caught = []
    [
      Turbofan::SchemaIncompatibleError.new("schema"),
      Turbofan::ResourceUnavailableError.new("resource"),
      Turbofan::Subprocess::Error.new(command: ["foo"], exit_code: 1, stdout: "", stderr: ""),
      Turbofan::Router::InvalidSizeError.new("size")
    ].each do |err|
      begin
        raise err
      rescue Turbofan::Error => e
        caught << e.class
      end
    end
    expect(caught.size).to eq(4)
  end
end
