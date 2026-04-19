# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Runtime::Metrics do
  let(:cloudwatch_client) { instance_double("Aws::CloudWatch::Client") } # rubocop:disable RSpec/VerifiedDoubleReference
  let(:metrics) do
    described_class.new(
      cloudwatch_client: cloudwatch_client,
      pipeline_name: "test-pipeline",
      stage: "production",
      step_name: "generate_csvs",
      size: nil
    )
  end

  describe "#emit" do
    it "records a metric value" do
      expect { metrics.emit("rows_processed", 1000) }.not_to raise_error
    end

    it "accepts numeric values" do
      expect { metrics.emit("processing_speed", 42.5) }.not_to raise_error
    end
  end

  describe "#flush" do
    before do
      allow(cloudwatch_client).to receive(:put_metric_data)
    end

    it "sends recorded metrics to CloudWatch" do
      metrics.emit("rows_processed", 1000)
      metrics.emit("files_generated", 42)

      metrics.flush

      expect(cloudwatch_client).to have_received(:put_metric_data).with(
        hash_including(
          namespace: "Turbofan/test-pipeline"
        )
      )
    end

    it "includes pipeline, stage, step dimensions" do
      metrics.emit("rows_processed", 1000)
      metrics.flush

      expect(cloudwatch_client).to have_received(:put_metric_data) do |args|
        metric_data = args[:metric_data]
        dims = metric_data.first[:dimensions]
        dim_names = dims.map { |d| d[:name] }
        expect(dim_names).to include("Pipeline")
        expect(dim_names).to include("Stage")
        expect(dim_names).to include("Step")
      end
    end

    it "sets the correct CloudWatch namespace" do
      metrics.emit("rows_processed", 1000)
      metrics.flush

      expect(cloudwatch_client).to have_received(:put_metric_data).with(
        hash_including(namespace: "Turbofan/test-pipeline")
      )
    end

    it "includes size dimension when present" do
      sized_metrics = described_class.new(
        cloudwatch_client: cloudwatch_client,
        pipeline_name: "test-pipeline",
        stage: "production",
        step_name: "generate_csvs",
        size: :l
      )

      sized_metrics.emit("rows_processed", 1000)
      sized_metrics.flush

      expect(cloudwatch_client).to have_received(:put_metric_data) do |args|
        metric_data = args[:metric_data]
        dims = metric_data.first[:dimensions]
        size_dim = dims.find { |d| d[:name] == "Size" }
        expect(size_dim).not_to be_nil
        expect(size_dim[:value]).to eq("l")
      end
    end
  end

  # A7: Metrics type check
  describe "emit type validation" do
    it "raises ArgumentError when value is not Numeric" do
      expect { metrics.emit("Foo", "bar") }
        .to raise_error(ArgumentError, /Numeric/)
    end

    it "succeeds with an integer value" do
      expect { metrics.emit("Foo", 42) }.not_to raise_error
    end

    it "succeeds with a float value" do
      expect { metrics.emit("Foo", 3.14) }.not_to raise_error
    end
  end

  describe "auto-metrics (emitted by wrapper)" do
    before do
      allow(cloudwatch_client).to receive(:put_metric_data)
    end

    %w[JobDuration JobSuccess JobFailure PeakMemoryMB CpuUtilization MemoryUtilization].each do |metric_name|
      it "can emit #{metric_name}" do
        metrics.emit(metric_name, 1)
        metrics.flush
        expect(cloudwatch_client).to have_received(:put_metric_data)
      end
    end
  end

  # Bug 3: Metrics#flush can raise when put_metric_data fails.
  # When flush is called in an ensure block (as the runtime wrapper does),
  # a flush error will mask the original exception from the step.
  # flush should not raise even when CloudWatch put_metric_data fails.
  describe "flush resilience when put_metric_data fails" do
    before do
      allow(cloudwatch_client).to receive(:put_metric_data)
        .and_raise(Aws::CloudWatch::Errors::InternalServiceFault.new(nil, "CloudWatch is down"))
    end

    it "does not raise when put_metric_data fails" do
      metrics.emit("rows_processed", 1000)

      expect { metrics.flush }.not_to raise_error
    end
  end

  describe "no metrics emitted" do
    before do
      allow(cloudwatch_client).to receive(:put_metric_data)
    end

    it "does not call CloudWatch when no metrics were emitted" do
      metrics.flush
      expect(cloudwatch_client).not_to have_received(:put_metric_data)
    end
  end

  describe "flush batching for CloudWatch limit" do
    before do
      allow(cloudwatch_client).to receive(:put_metric_data)
    end

    # Batch size is 100 (CloudWatch limit is 1000 per call; 100 gives
    # payload-size headroom while cutting 5x the API calls vs the old 20).
    {100 => 1, 101 => 2, 200 => 2, 201 => 3}.each do |count, expected_batches|
      it "sends #{count} metrics in #{expected_batches} batch(es)" do
        count.times { |i| metrics.emit("metric_#{i}", i) }
        metrics.flush

        expect(cloudwatch_client).to have_received(:put_metric_data).exactly(expected_batches).times
      end
    end
  end

  describe "flush error logging" do
    before do
      allow(cloudwatch_client).to receive(:put_metric_data)
        .and_raise(Aws::CloudWatch::Errors::InternalServiceFault.new(nil, "CloudWatch is down"))
    end

    it "logs a warning to stderr on flush failure" do
      metrics.emit("rows_processed", 1000)

      expect { metrics.flush }.to output(/WARNING.*Failed to flush.*metrics/).to_stderr
    end

    it "includes the error message in the warning" do
      metrics.emit("rows_processed", 1000)

      expect { metrics.flush }.to output(/CloudWatch is down/).to_stderr
    end

    it "preserves pending metrics after failure so a subsequent flush can retry" do
      metrics.emit("rows_processed", 1000)
      metrics.flush  # first flush raises, gets rescued, metric stays in @pending

      allow(cloudwatch_client).to receive(:put_metric_data)
      metrics.flush  # second flush succeeds, drains @pending

      # First flush: 1 call (attempt before Retryable gives up on
      # InternalServiceFault — note: non-transient for our predicate since
      # no HTTP status). Second flush: 1 more call. Total: 2.
      expect(cloudwatch_client).to have_received(:put_metric_data).at_least(2).times
    end
  end
end
