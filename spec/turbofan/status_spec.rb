require "spec_helper"
require "aws-sdk-batch"

RSpec.describe Turbofan::Status do
  let(:sfn_client) { instance_double(Aws::States::Client) }
  let(:batch_client) { instance_double(Aws::Batch::Client) }
  let(:execution_arn) { "arn:aws:states:us-east-1:123456789:execution:my-state-machine:run-abc123" }
  let(:pipeline_name) { "my_pipeline" }
  let(:stage) { "production" }
  let(:steps) { [:extract, :transform, :load] }
  let(:start_date) { Time.parse("2026-02-17T10:00:00Z") }

  let(:describe_execution_response) do
    double(
      "DescribeExecutionResponse",
      status: "RUNNING",
      start_date: start_date,
      stop_date: nil,
      name: "run-abc123"
    )
  end

  before do
    allow(sfn_client).to receive(:describe_execution)
      .with(execution_arn: execution_arn)
      .and_return(describe_execution_response)
  end

  describe ".fetch" do
    context "with a running execution" do
      before do
        allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
      end

      it "returns a hash with pipeline name" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: steps
        )

        expect(result[:pipeline]).to eq("my_pipeline")
      end

      it "returns a hash with stage" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: steps
        )

        expect(result[:stage]).to eq("production")
      end

      it "returns the execution_id from the SFN execution name" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: steps
        )

        expect(result[:execution_id]).to eq("run-abc123")
      end

      it "returns the execution status" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: steps
        )

        expect(result[:status]).to eq("RUNNING")
      end

      it "returns started_at as an ISO8601 timestamp string" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: steps
        )

        expect(result[:started_at]).to eq(start_date.iso8601)
      end

      it "returns a steps array" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: steps
        )

        expect(result[:steps]).to be_an(Array)
        expect(result[:steps].length).to eq(3)
      end
    end

    context "with a succeeded execution" do
      let(:describe_execution_response) do
        double(
          "DescribeExecutionResponse",
          status: "SUCCEEDED",
          start_date: start_date,
          stop_date: start_date + 300,
          name: "run-abc123"
        )
      end

      before do
        allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
      end

      it "returns SUCCEEDED status" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: steps
        )

        expect(result[:status]).to eq("SUCCEEDED")
      end
    end

    context "with a failed execution" do
      let(:describe_execution_response) do
        double(
          "DescribeExecutionResponse",
          status: "FAILED",
          start_date: start_date,
          stop_date: start_date + 120,
          name: "run-abc123"
        )
      end

      before do
        allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
      end

      it "returns FAILED status" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: steps
        )

        expect(result[:status]).to eq("FAILED")
      end
    end
  end

  describe "step status hashes" do
    let(:job_queue_arn) { "arn:aws:batch:us-east-1:123456789:job-queue/turbofan-my-pipeline-production-extract" }

    before do
      allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
    end

    it "includes the step name as a string" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: steps
      )

      step_names = result[:steps].map { |s| s[:name] }
      expect(step_names).to eq(%w[extract transform load])
    end

    it "includes step-level status" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: steps
      )

      result[:steps].each do |step|
        expect(step).to have_key(:status)
        expect(step[:status]).to be_a(String)
      end
    end

    it "includes jobs hash with pending, running, succeeded, failed counts" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: steps
      )

      result[:steps].each do |step|
        expect(step[:jobs]).to be_a(Hash)
        expect(step[:jobs]).to have_key(:pending)
        expect(step[:jobs]).to have_key(:running)
        expect(step[:jobs]).to have_key(:succeeded)
        expect(step[:jobs]).to have_key(:failed)
      end
    end

    it "returns integer counts for all job fields" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: steps
      )

      result[:steps].each do |step|
        step[:jobs].each_value do |count|
          expect(count).to be_an(Integer)
        end
      end
    end
  end

  describe "job count rules" do
    context "with SUBMITTED jobs" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "SUBMITTED"
            double(job_summary_list: [
              double(job_id: "job-1", array_properties: nil),
              double(job_id: "job-2", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "counts SUBMITTED jobs as pending" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:pending]).to eq(2)
      end
    end

    context "with PENDING jobs" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "PENDING"
            double(job_summary_list: [
              double(job_id: "job-1", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "counts PENDING jobs as pending" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:pending]).to eq(1)
      end
    end

    context "with RUNNABLE jobs" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "RUNNABLE"
            double(job_summary_list: [
              double(job_id: "job-1", array_properties: nil),
              double(job_id: "job-2", array_properties: nil),
              double(job_id: "job-3", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "counts RUNNABLE jobs as pending" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:pending]).to eq(3)
      end
    end

    context "with STARTING jobs" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "STARTING"
            double(job_summary_list: [
              double(job_id: "job-1", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "counts STARTING jobs as pending" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:pending]).to eq(1)
      end
    end

    context "with RUNNING jobs" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "RUNNING"
            double(job_summary_list: [
              double(job_id: "job-1", array_properties: nil),
              double(job_id: "job-2", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "counts RUNNING jobs as running" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:running]).to eq(2)
      end
    end

    context "with SUCCEEDED jobs" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "SUCCEEDED"
            double(job_summary_list: [
              double(job_id: "job-1", array_properties: nil),
              double(job_id: "job-2", array_properties: nil),
              double(job_id: "job-3", array_properties: nil),
              double(job_id: "job-4", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "counts SUCCEEDED jobs as succeeded" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:succeeded]).to eq(4)
      end
    end

    context "with FAILED jobs" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "FAILED"
            double(job_summary_list: [
              double(job_id: "job-1", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "counts FAILED jobs as failed" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:failed]).to eq(1)
      end
    end

    context "with mixed job statuses across all categories" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "SUBMITTED"
            double(job_summary_list: [double(job_id: "j-sub1", array_properties: nil)], next_token: nil)
          when "PENDING"
            double(job_summary_list: [double(job_id: "j-pend1", array_properties: nil)], next_token: nil)
          when "RUNNABLE"
            double(job_summary_list: [double(job_id: "j-run1", array_properties: nil)], next_token: nil)
          when "STARTING"
            double(job_summary_list: [double(job_id: "j-start1", array_properties: nil)], next_token: nil)
          when "RUNNING"
            double(job_summary_list: [
              double(job_id: "j-running1", array_properties: nil),
              double(job_id: "j-running2", array_properties: nil)
            ], next_token: nil)
          when "SUCCEEDED"
            double(job_summary_list: [
              double(job_id: "j-succ1", array_properties: nil),
              double(job_id: "j-succ2", array_properties: nil),
              double(job_id: "j-succ3", array_properties: nil)
            ], next_token: nil)
          when "FAILED"
            double(job_summary_list: [double(job_id: "j-fail1", array_properties: nil)], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "aggregates SUBMITTED, PENDING, RUNNABLE, STARTING into pending count" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:pending]).to eq(4)
        expect(extract[:jobs][:running]).to eq(2)
        expect(extract[:jobs][:succeeded]).to eq(3)
        expect(extract[:jobs][:failed]).to eq(1)
      end
    end
  end

  describe "array job children counting" do
    context "when a parent array job appears" do
      let(:array_parent) do
        double(
          job_id: "array-job-1",
          array_properties: double(size: 10, index: nil)
        )
      end

      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "RUNNING"
            double(job_summary_list: [array_parent], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "counts array job parent by its size, not as 1" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:running]).to eq(10)
      end
    end

    context "when array job children appear without parent" do
      before do
        child_running = (0..2).map do |i|
          double(job_id: "array-job-1:#{i}", array_properties: double(size: nil, index: i))
        end

        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "RUNNING"
            double(job_summary_list: child_running, next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "skips children since parent size already accounts for them" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:running]).to eq(0)
      end
    end
  end

  describe "list_jobs pagination" do
    context "when list_jobs returns multiple pages for a single status" do
      before do
        call_count = {}
        allow(batch_client).to receive(:list_jobs) do |args|
          status = args[:job_status]
          call_count[status] ||= 0
          call_count[status] += 1

          if status == "RUNNING" && call_count[status] == 1
            # First page: 2 jobs + next_token
            double(
              job_summary_list: [
                double(job_id: "j-run1", array_properties: nil),
                double(job_id: "j-run2", array_properties: nil)
              ],
              next_token: "page2-token"
            )
          elsif status == "RUNNING" && call_count[status] == 2
            # Second page: 1 job, no next_token
            double(
              job_summary_list: [
                double(job_id: "j-run3", array_properties: nil)
              ],
              next_token: nil
            )
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "fetches all pages and sums the job counts" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:running]).to eq(3)
      end

      it "passes next_token to subsequent list_jobs calls" do
        described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        expect(batch_client).to have_received(:list_jobs).with(
          job_queue: "turbofan-my_pipeline-production-queue-extract",
          job_status: "RUNNING",
          next_token: nil
        )
        expect(batch_client).to have_received(:list_jobs).with(
          job_queue: "turbofan-my_pipeline-production-queue-extract",
          job_status: "RUNNING",
          next_token: "page2-token"
        )
      end
    end
  end

  describe "array job child deduplication" do
    context "when both parent and child array jobs appear in the same response" do
      before do
        parent_job = double(
          job_id: "array-job-1",
          array_properties: double(size: 10, index: nil)
        )
        child_job = double(
          job_id: "array-job-1:0",
          array_properties: double(size: nil, index: 0)
        )

        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "RUNNING"
            double(job_summary_list: [parent_job, child_job], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "counts only the parent size and skips children to avoid double-counting" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        # Parent (size=10) counts as 10, child (index=0) counts as 0 => total 10, not 11
        expect(extract[:jobs][:running]).to eq(10)
      end
    end
  end

  describe "multiple steps" do
    before do
      allow(batch_client).to receive(:list_jobs) do |args|
        job_queue = args[:job_queue]
        if job_queue&.include?("extract")
          case args[:job_status]
          when "SUCCEEDED"
            double(job_summary_list: [
              double(job_id: "ext-1", array_properties: nil),
              double(job_id: "ext-2", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        elsif job_queue&.include?("transform")
          case args[:job_status]
          when "RUNNING"
            double(job_summary_list: [
              double(job_id: "trans-1", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        elsif job_queue&.include?("load")
          double(job_summary_list: [], next_token: nil)
        else
          double(job_summary_list: [], next_token: nil)
        end
      end
    end

    it "returns separate job counts per step" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: steps
      )

      extract = result[:steps].find { |s| s[:name] == "extract" }
      transform = result[:steps].find { |s| s[:name] == "transform" }
      load_step = result[:steps].find { |s| s[:name] == "load" }

      expect(extract[:jobs][:succeeded]).to eq(2)
      expect(transform[:jobs][:running]).to eq(1)
      expect(load_step[:jobs][:pending]).to eq(0)
      expect(load_step[:jobs][:running]).to eq(0)
      expect(load_step[:jobs][:succeeded]).to eq(0)
      expect(load_step[:jobs][:failed]).to eq(0)
    end
  end

  describe "return value structure" do
    before do
      allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
    end

    it "returns a hash with all required top-level keys" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: steps
      )

      expect(result).to be_a(Hash)
      expect(result).to have_key(:pipeline)
      expect(result).to have_key(:stage)
      expect(result).to have_key(:execution_id)
      expect(result).to have_key(:status)
      expect(result).to have_key(:started_at)
      expect(result).to have_key(:steps)
    end

    it "returns steps as an array of hashes with name, status, and jobs" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: steps
      )

      result[:steps].each do |step|
        expect(step).to have_key(:name)
        expect(step).to have_key(:status)
        expect(step).to have_key(:jobs)
        expect(step[:jobs].keys).to contain_exactly(:pending, :running, :succeeded, :failed)
      end
    end
  end

  describe "calls describe_execution" do
    before do
      allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
    end

    it "calls sfn_client.describe_execution with the provided execution_arn" do
      described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: steps
      )

      expect(sfn_client).to have_received(:describe_execution)
        .with(execution_arn: execution_arn)
    end
  end

  describe "derive_step_status" do
    def fetch_with_jobs(**overrides)
      described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: [:extract],
        **overrides
      )
    end

    context "when no jobs exist for a step" do
      before do
        allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
      end

      it "derives step status as PENDING" do
        result = fetch_with_jobs
        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:status]).to eq("PENDING")
      end
    end

    context "when only running jobs exist" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "RUNNING"
            double(job_summary_list: [double(job_id: "j1", array_properties: nil)], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "derives step status as RUNNING" do
        result = fetch_with_jobs
        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:status]).to eq("RUNNING")
      end
    end

    context "when running and failed jobs coexist" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "RUNNING"
            double(job_summary_list: [double(job_id: "j1", array_properties: nil)], next_token: nil)
          when "FAILED"
            double(job_summary_list: [double(job_id: "j2", array_properties: nil)], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "derives step status as RUNNING (running takes priority over failed)" do
        result = fetch_with_jobs
        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:status]).to eq("RUNNING")
      end
    end

    context "when only failed jobs exist (no running)" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "FAILED"
            double(job_summary_list: [
              double(job_id: "j1", array_properties: nil),
              double(job_id: "j2", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "derives step status as FAILED" do
        result = fetch_with_jobs
        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:status]).to eq("FAILED")
      end
    end

    context "when failed and succeeded jobs coexist (no running)" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "SUCCEEDED"
            double(job_summary_list: [
              double(job_id: "j1", array_properties: nil),
              double(job_id: "j2", array_properties: nil)
            ], next_token: nil)
          when "FAILED"
            double(job_summary_list: [double(job_id: "j3", array_properties: nil)], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "derives step status as FAILED" do
        result = fetch_with_jobs
        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:status]).to eq("FAILED")
      end
    end

    context "when all jobs succeeded" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "SUCCEEDED"
            double(job_summary_list: [
              double(job_id: "j1", array_properties: nil),
              double(job_id: "j2", array_properties: nil),
              double(job_id: "j3", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "derives step status as SUCCEEDED" do
        result = fetch_with_jobs
        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:status]).to eq("SUCCEEDED")
      end
    end

    context "when pending and succeeded jobs coexist (no running, no failed)" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "SUBMITTED"
            double(job_summary_list: [double(job_id: "j1", array_properties: nil)], next_token: nil)
          when "SUCCEEDED"
            double(job_summary_list: [double(job_id: "j2", array_properties: nil)], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "derives step status as PENDING" do
        result = fetch_with_jobs
        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:status]).to eq("PENDING")
      end
    end
  end

  describe "job queue naming" do
    before do
      allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
    end

    it "queries the correct job queue name for each step" do
      described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: "my_pipeline",
        stage: "production",
        steps: [:extract]
      )

      expect(batch_client).to have_received(:list_jobs)
        .with(hash_including(job_queue: "turbofan-my_pipeline-production-queue-extract"))
        .at_least(:once)
    end

    it "queries all 7 batch statuses per step" do
      described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: [:extract]
      )

      %w[SUBMITTED PENDING RUNNABLE STARTING RUNNING SUCCEEDED FAILED].each do |status|
        expect(batch_client).to have_received(:list_jobs)
          .with(hash_including(job_status: status))
      end
    end
  end

  describe "input coercion" do
    before do
      allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
    end

    context "when pipeline_name is a symbol" do
      it "converts pipeline_name to a string in the result" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: :my_pipeline,
          stage: stage,
          steps: steps
        )

        expect(result[:pipeline]).to eq("my_pipeline")
      end
    end

    context "when stage is a symbol" do
      it "converts stage to a string in the result" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: :production,
          steps: steps
        )

        expect(result[:stage]).to eq("production")
      end
    end

    context "when step names are symbols" do
      it "converts step names to strings in the result" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        expect(result[:steps].first[:name]).to eq("extract")
      end
    end
  end

  describe "empty steps array" do
    before do
      allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
    end

    it "returns an empty steps array when no steps provided" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: []
      )

      expect(result[:steps]).to eq([])
    end

    it "still includes execution-level data with no steps" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: []
      )

      expect(result[:pipeline]).to eq("my_pipeline")
      expect(result[:status]).to eq("RUNNING")
      expect(result[:execution_id]).to eq("run-abc123")
    end
  end

  describe "step ordering" do
    before do
      allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
    end

    it "preserves the order of steps as provided" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: [:load, :extract, :transform]
      )

      names = result[:steps].map { |s| s[:name] }
      expect(names).to eq(%w[load extract transform])
    end
  end

  describe "SFN execution terminal statuses" do
    before do
      allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
    end

    context "with a TIMED_OUT execution" do
      let(:describe_execution_response) do
        double(
          "DescribeExecutionResponse",
          status: "TIMED_OUT",
          start_date: start_date,
          stop_date: start_date + 3600,
          name: "run-abc123"
        )
      end

      it "returns TIMED_OUT status" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: steps
        )

        expect(result[:status]).to eq("TIMED_OUT")
      end
    end

    context "with an ABORTED execution" do
      let(:describe_execution_response) do
        double(
          "DescribeExecutionResponse",
          status: "ABORTED",
          start_date: start_date,
          stop_date: start_date + 60,
          name: "run-abc123"
        )
      end

      it "returns ABORTED status" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: steps
        )

        expect(result[:status]).to eq("ABORTED")
      end
    end
  end

  describe "large array job counts" do
    context "with a 10,000-child array job" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "RUNNING"
            double(job_summary_list: [
              double(job_id: "big-array-1", array_properties: double(size: 10_000, index: nil))
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "counts all 10,000 children from the parent array_properties.size" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:running]).to eq(10_000)
      end
    end

    context "with multiple large array jobs in different statuses" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "RUNNING"
            double(job_summary_list: [
              double(job_id: "array-a", array_properties: double(size: 5_000, index: nil)),
              double(job_id: "array-b", array_properties: double(size: 3_000, index: nil))
            ], next_token: nil)
          when "SUCCEEDED"
            double(job_summary_list: [
              double(job_id: "array-c", array_properties: double(size: 2_000, index: nil))
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "sums array sizes across multiple parent jobs" do
        result = described_class.fetch(
          sfn_client: sfn_client,
          batch_client: batch_client,
          execution_arn: execution_arn,
          pipeline_name: pipeline_name,
          stage: stage,
          steps: [:extract]
        )

        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:jobs][:running]).to eq(8_000)
        expect(extract[:jobs][:succeeded]).to eq(2_000)
      end
    end
  end

  describe "derive_step_status additional edge cases" do
    def fetch_single_step
      described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: [:extract]
      )
    end

    context "when jobs are FAILED + PENDING but no RUNNING" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "SUBMITTED"
            double(job_summary_list: [
              double(job_id: "j1", array_properties: nil),
              double(job_id: "j2", array_properties: nil)
            ], next_token: nil)
          when "FAILED"
            double(job_summary_list: [double(job_id: "j3", array_properties: nil)], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "derives step status as FAILED (failed takes priority over pending)" do
        result = fetch_single_step
        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:status]).to eq("FAILED")
      end
    end

    context "when all jobs are in PENDING statuses only" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "SUBMITTED"
            double(job_summary_list: [double(job_id: "j1", array_properties: nil)], next_token: nil)
          when "RUNNABLE"
            double(job_summary_list: [double(job_id: "j2", array_properties: nil)], next_token: nil)
          when "STARTING"
            double(job_summary_list: [double(job_id: "j3", array_properties: nil)], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "derives step status as PENDING" do
        result = fetch_single_step
        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:status]).to eq("PENDING")
        expect(extract[:jobs][:pending]).to eq(3)
      end
    end

    context "when all jobs are FAILED" do
      before do
        allow(batch_client).to receive(:list_jobs) do |args|
          case args[:job_status]
          when "FAILED"
            double(job_summary_list: [
              double(job_id: "j1", array_properties: nil),
              double(job_id: "j2", array_properties: nil),
              double(job_id: "j3", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        end
      end

      it "derives step status as FAILED" do
        result = fetch_single_step
        extract = result[:steps].find { |s| s[:name] == "extract" }
        expect(extract[:status]).to eq("FAILED")
        expect(extract[:jobs][:failed]).to eq(3)
      end
    end
  end

  describe "multiple steps with different derived statuses" do
    before do
      allow(batch_client).to receive(:list_jobs) do |args|
        job_queue = args[:job_queue]
        if job_queue&.include?("extract")
          case args[:job_status]
          when "SUCCEEDED"
            double(job_summary_list: [
              double(job_id: "e1", array_properties: nil),
              double(job_id: "e2", array_properties: nil)
            ], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        elsif job_queue&.include?("transform")
          case args[:job_status]
          when "RUNNING"
            double(job_summary_list: [double(job_id: "t1", array_properties: nil)], next_token: nil)
          when "SUCCEEDED"
            double(job_summary_list: [double(job_id: "t2", array_properties: nil)], next_token: nil)
          else
            double(job_summary_list: [], next_token: nil)
          end
        elsif job_queue&.include?("load")
          double(job_summary_list: [], next_token: nil)
        else
          double(job_summary_list: [], next_token: nil)
        end
      end
    end

    it "derives independent statuses for each step" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: [:extract, :transform, :load]
      )

      extract = result[:steps].find { |s| s[:name] == "extract" }
      transform = result[:steps].find { |s| s[:name] == "transform" }
      load_step = result[:steps].find { |s| s[:name] == "load" }

      expect(extract[:status]).to eq("SUCCEEDED")
      expect(transform[:status]).to eq("RUNNING")
      expect(load_step[:status]).to eq("PENDING")
    end
  end

  describe "error propagation" do
    context "when describe_execution raises an error" do
      before do
        allow(sfn_client).to receive(:describe_execution)
          .and_raise(Aws::States::Errors::ExecutionDoesNotExist.new(nil, "Execution does not exist"))
      end

      it "propagates the SFN error" do
        expect {
          described_class.fetch(
            sfn_client: sfn_client,
            batch_client: batch_client,
            execution_arn: execution_arn,
            pipeline_name: pipeline_name,
            stage: stage,
            steps: steps
          )
        }.to raise_error(Aws::States::Errors::ExecutionDoesNotExist)
      end
    end

    context "when list_jobs raises an error" do
      before do
        allow(batch_client).to receive(:list_jobs)
          .and_raise(Aws::Batch::Errors::ClientException.new(nil, "Job queue not found"))
      end

      it "propagates the Batch error" do
        expect {
          described_class.fetch(
            sfn_client: sfn_client,
            batch_client: batch_client,
            execution_arn: execution_arn,
            pipeline_name: pipeline_name,
            stage: stage,
            steps: [:extract]
          )
        }.to raise_error(Aws::Batch::Errors::ClientException)
      end
    end
  end

  # Bug 1: Status#fetch builds queue name as "turbofan-{pipeline}-{stage}-{step}"
  # but CloudFormation creates queues named "turbofan-{pipeline}-{stage}-queue-{step}".
  # The missing "-queue-" segment means Status#fetch queries the wrong queue.
  describe "queue name format includes -queue- segment" do
    before do
      allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
    end

    it "queries Batch using the correct queue name: turbofan-{pipeline}-{stage}-queue-{step}" do
      described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: [:extract]
      )

      expect(batch_client).to have_received(:list_jobs).with(
        hash_including(job_queue: "turbofan-my_pipeline-production-queue-extract")
      ).at_least(:once)
    end
  end

  describe "single step with all jobs in one state" do
    before do
      allow(batch_client).to receive(:list_jobs) do |args|
        case args[:job_status]
        when "RUNNING"
          jobs = (1..5).map { |i| double(job_id: "j#{i}", array_properties: nil) }
          double(job_summary_list: jobs, next_token: nil)
        else
          double(job_summary_list: [], next_token: nil)
        end
      end
    end

    it "returns zero for all other count buckets" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: [:extract]
      )

      extract = result[:steps].find { |s| s[:name] == "extract" }
      expect(extract[:jobs][:running]).to eq(5)
      expect(extract[:jobs][:pending]).to eq(0)
      expect(extract[:jobs][:succeeded]).to eq(0)
      expect(extract[:jobs][:failed]).to eq(0)
    end
  end

  describe "Hash steps with sized queues" do
    let(:sized_step_class) do
      Class.new do
        include Turbofan::Step

        compute_environment TestCe
        size :s, cpu: 1
        size :m, cpu: 2
        size :l, cpu: 4
      end
    end

    before do
      stub_const("SizedStep", sized_step_class)
      allow(batch_client).to receive(:list_jobs).and_return(double(job_summary_list: [], next_token: nil))
    end

    it "queries each size-specific queue when steps is a Hash with turbofan_sizes" do
      result = described_class.fetch(
        sfn_client: sfn_client,
        batch_client: batch_client,
        execution_arn: execution_arn,
        pipeline_name: pipeline_name,
        stage: stage,
        steps: {process: sized_step_class}
      )

      expect(result[:steps].length).to eq(1)
      expect(result[:steps].first[:name]).to eq("process")

      # Should have queried per-size queues: queue-process-s, queue-process-m, queue-process-l
      %w[s m l].each do |size|
        expect(batch_client).to have_received(:list_jobs).with(
          hash_including(job_queue: "turbofan-my_pipeline-production-queue-process-#{size}")
        ).at_least(:once)
      end
    end
  end
end
