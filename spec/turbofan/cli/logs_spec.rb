require "spec_helper"

RSpec.describe "turbofan logs" do # rubocop:disable RSpec/DescribeClass
  let(:logs_client) { instance_double(Aws::CloudWatchLogs::Client) }

  before do
    allow(Aws::CloudWatchLogs::Client).to receive(:new).and_return(logs_client)
    allow(logs_client).to receive_messages(start_query: double(query_id: "query-123"), get_query_results: double(
      status: "Complete",
      results: [
        [
          double(field: "@timestamp", value: "2026-02-16T10:00:00Z"),
          double(field: "@message", value: '{"level":"info","step":"process","message":"Processing item 1"}')
        ],
        [
          double(field: "@timestamp", value: "2026-02-16T10:00:01Z"),
          double(field: "@message", value: '{"level":"info","step":"process","message":"Processing item 2"}')
        ]
      ]
    ))
  end

  context "when constructing log group name" do
    it "constructs per-step log group name matching CF naming convention" do
      Turbofan::CLI::Logs.call(pipeline_name: "my_pipeline", stage: "production", step: "process")
      expect(logs_client).to have_received(:start_query) do |args|
        expect(args[:log_group_name]).to eq("turbofan-my-pipeline-production-logs-process")
      end
    end

    it "converts underscores in pipeline name to dashes" do
      Turbofan::CLI::Logs.call(pipeline_name: "my_pipeline", stage: "staging", step: "step1")
      expect(logs_client).to have_received(:start_query) do |args|
        expect(args[:log_group_name]).to eq("turbofan-my-pipeline-staging-logs-step1")
      end
    end
  end

  context "with --execution filter" do
    it "generates CloudWatch Insights query for --execution filter" do
      Turbofan::CLI::Logs.call(pipeline_name: "my_pipeline", stage: "production", step: "process", execution: "exec-abc-123")
      expect(logs_client).to have_received(:start_query) do |args|
        expect(args[:query_string]).to include("exec-abc-123")
      end
    end
  end

  context "with --item filter" do
    it "generates query for --item filter" do
      Turbofan::CLI::Logs.call(pipeline_name: "my_pipeline", stage: "production", step: "process", item: "42")
      expect(logs_client).to have_received(:start_query) do |args|
        expect(args[:query_string]).to include("42")
      end
    end
  end

  context "with --query expression" do
    it "generates query for --query expression" do
      Turbofan::CLI::Logs.call(pipeline_name: "my_pipeline", stage: "production", step: "process", query: "level = 'ERROR'")
      expect(logs_client).to have_received(:start_query) do |args|
        expect(args[:query_string]).to include("ERROR")
      end
    end
  end

  context "when combining multiple filters" do
    it "combines --execution and --step filters" do
      Turbofan::CLI::Logs.call(pipeline_name: "my_pipeline", stage: "production", step: "process", execution: "exec-abc")
      expect(logs_client).to have_received(:start_query) do |args|
        expect(args[:query_string]).to include("exec-abc")
        expect(args[:log_group_name]).to include("process")
      end
    end

    it "combines --execution, --step, and --item filters" do
      Turbofan::CLI::Logs.call(pipeline_name: "my_pipeline", stage: "production", step: "process", execution: "exec-abc", item: "5")
      expect(logs_client).to have_received(:start_query) do |args|
        expect(args[:query_string]).to include("exec-abc")
        expect(args[:log_group_name]).to include("process")
        expect(args[:query_string]).to include("5")
      end
    end
  end

  context "when formatting output" do
    it "formats output as structured log lines" do
      output = capture_stdout do
        Turbofan::CLI::Logs.call(pipeline_name: "my_pipeline", stage: "production", step: "process", execution: "exec-abc")
      end
      expect(output).to include("2026-02-16")
    end
  end

  # ---------------------------------------------------------------------------
  # B4 — Logs --step optional
  # ---------------------------------------------------------------------------
  context "when --step is omitted (B4)" do
    it "does not raise Thor::RequiredArgumentMissingError" do
      expect {
        Turbofan::CLI::Logs.call(pipeline_name: "my_pipeline", stage: "production", step: nil)
      }.not_to raise_error
    end

    it "queries all steps or finds failed step when --step is omitted" do
      Turbofan::CLI::Logs.call(pipeline_name: "my_pipeline", stage: "production", step: nil)

      expect(logs_client).to have_received(:start_query)
    end
  end
end
