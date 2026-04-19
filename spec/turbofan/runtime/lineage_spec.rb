# frozen_string_literal: true

require "spec_helper"

# B7 — OpenLineage event emission
RSpec.describe Turbofan::Runtime::Lineage do
  include WrapperTestHelper

  let(:context) do
    Turbofan::Runtime::Context.new(
      execution_id: "exec-123",
      attempt_number: 1,
      step_name: "process",
      stage: "production",
      pipeline_name: "test-pipeline",
      array_index: nil,
      storage_path: nil,
      uses: [],
      writes_to: []
    )
  end

  describe ".start_event" do
    it "returns a hash with eventType START" do
      event = described_class.start_event(context: context)

      expect(event).to be_a(Hash)
      expect(event[:eventType]).to eq("START")
    end

    it "includes correct job namespace and name" do
      event = described_class.start_event(context: context)

      expect(event.dig(:job, :namespace)).to eq("test-pipeline")
      expect(event.dig(:job, :name)).to eq("process")
    end

    it "includes correct run ID based on execution_id" do
      event = described_class.start_event(context: context)

      expect(event.dig(:run, :runId)).to eq("exec-123")
    end

    it "includes step_class name in job facets when provided" do
      step_class = stub_const("MyApp::ProcessStep", Class.new { include Turbofan::Step; runs_on :batch })
      event = described_class.start_event(context: context, step_class: step_class)

      expect(event.dig(:job, :facets, :sourceCodeLocation)).to eq(type: "ruby", name: "MyApp::ProcessStep")
    end

    it "omits job facets when step_class is nil" do
      event = described_class.start_event(context: context)

      expect(event.dig(:job, :facets)).to be_nil
    end
  end

  describe ".complete_event" do
    it "returns a hash with eventType COMPLETE" do
      event = described_class.complete_event(context: context)

      expect(event).to be_a(Hash)
      expect(event[:eventType]).to eq("COMPLETE")
    end
  end

  describe ".fail_event" do
    it "returns a hash with eventType FAIL" do
      event = described_class.fail_event(context: context)

      expect(event[:eventType]).to eq("FAIL")
    end

    it "includes error facet when error is provided" do
      error = RuntimeError.new("something went wrong")
      event = described_class.fail_event(context: context, error: error)

      expect(event[:eventType]).to eq("FAIL")
      expect(event.dig(:run, :facets, :errorMessage)).to include("something went wrong")
    end

    it "has no errorMessage facet when error is not provided" do
      event = described_class.fail_event(context: context)

      error_facet = event.dig(:run, :facets, :errorMessage)
      expect(error_facet).to be_nil
    end
  end

  describe "inputs dataset list" do
    let(:context_with_uses) do
      Turbofan::Runtime::Context.new(
        execution_id: "exec-123",
        attempt_number: 1,
        step_name: "process",
        stage: "production",
        pipeline_name: "test-pipeline",
        array_index: nil,
        storage_path: nil,
        uses: [{type: :s3, uri: "s3://input-bucket/data/*"}, {type: :resource, key: :places_read}],
        writes_to: []
      )
    end

    it "builds inputs from step's uses" do
      event = described_class.start_event(context: context_with_uses)

      expect(event[:inputs]).to be_an(Array)
      expect(event[:inputs].size).to eq(2)
    end

    it "S3 dependencies produce datasets with namespace s3" do
      event = described_class.start_event(context: context_with_uses)

      s3_input = event[:inputs].find { |i| i[:namespace] == "s3" }
      expect(s3_input).not_to be_nil
    end

    it "resource dependencies produce datasets with namespace postgres" do
      event = described_class.start_event(context: context_with_uses)

      pg_input = event[:inputs].find { |i| i[:namespace] == "postgres" }
      expect(pg_input).not_to be_nil
    end
  end

  describe "outputs dataset list" do
    let(:context_with_writes) do
      Turbofan::Runtime::Context.new(
        execution_id: "exec-123",
        attempt_number: 1,
        step_name: "process",
        stage: "production",
        pipeline_name: "test-pipeline",
        array_index: nil,
        storage_path: nil,
        uses: [],
        writes_to: [{type: :s3, uri: "s3://output-bucket/results/"}, {type: :resource, key: :places_write}]
      )
    end

    it "builds outputs from step's writes_to" do
      event = described_class.start_event(context: context_with_writes)

      expect(event[:outputs]).to be_an(Array)
      expect(event[:outputs].size).to eq(2)
    end

    it "S3 write dependencies produce datasets with namespace s3" do
      event = described_class.start_event(context: context_with_writes)

      s3_output = event[:outputs].find { |o| o[:namespace] == "s3" }
      expect(s3_output).not_to be_nil
    end

    it "resource write dependencies produce datasets with namespace postgres" do
      event = described_class.start_event(context: context_with_writes)

      pg_output = event[:outputs].find { |o| o[:namespace] == "postgres" }
      expect(pg_output).not_to be_nil
    end
  end
end
