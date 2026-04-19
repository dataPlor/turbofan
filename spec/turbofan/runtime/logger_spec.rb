# frozen_string_literal: true

require "spec_helper"
require "json"
require "stringio"

RSpec.describe Turbofan::Runtime::Logger do
  let(:output) { StringIO.new }
  let(:logger) do
    described_class.new(
      output: output,
      execution_id: "exec-abc123",
      step_name: "generate_csvs",
      stage: "production",
      pipeline_name: "test-pipeline",
      array_index: nil
    )
  end

  def last_log_entry
    output.rewind
    lines = output.string.split("\n")
    JSON.parse(lines.last)
  end

  describe "#info" do
    it "writes a JSON log line" do
      logger.info("processing file")
      entry = last_log_entry
      expect(entry).to be_a(Hash)
    end

    it "includes the log level" do
      logger.info("processing file")
      entry = last_log_entry
      expect(entry["level"]).to eq("info")
    end

    it "includes the message" do
      logger.info("processing file")
      entry = last_log_entry
      expect(entry["message"]).to eq("processing file")
    end

    it "includes execution metadata" do
      logger.info("processing file")
      entry = last_log_entry
      expect(entry["execution_id"]).to eq("exec-abc123")
      expect(entry["step"]).to eq("generate_csvs")
      expect(entry["stage"]).to eq("production")
      expect(entry["pipeline"]).to eq("test-pipeline")
    end

    it "includes a timestamp" do
      logger.info("processing file")
      entry = last_log_entry
      expect(entry["timestamp"]).not_to be_nil
    end

    it "accepts additional key-value pairs" do
      logger.info("processing file", file: "/path/to/file", rows: 1000)
      entry = last_log_entry
      expect(entry["file"]).to eq("/path/to/file")
      expect(entry["rows"]).to eq(1000)
    end
  end

  describe "#warn" do
    it "logs at warn level" do
      logger.warn("slow query detected")
      entry = last_log_entry
      expect(entry["level"]).to eq("warn")
      expect(entry["message"]).to eq("slow query detected")
    end
  end

  describe "#error" do
    it "logs at error level" do
      logger.error("query failed", error: "timeout")
      entry = last_log_entry
      expect(entry["level"]).to eq("error")
      expect(entry["message"]).to eq("query failed")
      expect(entry["error"]).to eq("timeout")
    end
  end

  describe "#debug" do
    it "logs at debug level" do
      logger.debug("detailed trace")
      entry = last_log_entry
      expect(entry["level"]).to eq("debug")
    end
  end

  describe "array_index in metadata" do
    let(:array_logger) do
      described_class.new(
        output: output,
        execution_id: "exec-abc123",
        step_name: "process",
        stage: "production",
        pipeline_name: "test-pipeline",
        array_index: 42
      )
    end

    it "includes array_index when present" do
      array_logger.info("processing item")
      entry = last_log_entry
      expect(entry["array_index"]).to eq(42)
    end
  end

  describe "array_index absent" do
    it "does not include array_index when nil" do
      logger.info("processing")
      entry = last_log_entry
      expect(entry).not_to have_key("array_index")
    end
  end

  describe "JSON output format" do
    it "writes one JSON object per log call, one per line" do
      logger.info("first")
      logger.info("second")
      output.rewind
      lines = output.string.strip.split("\n")
      expect(lines.size).to eq(2)
      lines.each do |line|
        expect { JSON.parse(line) }.not_to raise_error
      end
    end
  end
end
