# frozen_string_literal: true

require "spec_helper"
require "aws-sdk-s3"
require "aws-sdk-cloudwatch"
require "seahorse/client/networking_error"

RSpec.describe Turbofan::Retryable do
  # Injectable seams for deterministic tests.
  let(:sleeps) { [] }
  let(:sleeper) { ->(s) { sleeps << s } }
  let(:jitter_rand) { -> { 0.0 } }  # no jitter → backoff is deterministic

  def call_with_deterministic_seams(**opts, &block)
    described_class.call(
      sleeper: sleeper,
      jitter_rand: jitter_rand,
      **opts,
      &block
    )
  end

  def build_aws_error(klass, code: nil, status: nil)
    err = klass.new(nil, code || klass.to_s.split("::").last)
    allow(err).to receive(:code).and_return(code) if code
    if status
      http_response = Struct.new(:status_code).new(status)
      context = Struct.new(:http_response).new(http_response)
      allow(err).to receive(:context).and_return(context)
    end
    err
  end

  describe "happy path" do
    it "returns the block's value on first successful attempt" do
      result = call_with_deterministic_seams { 42 }
      expect(result).to eq(42)
    end

    it "does not sleep when no error raised" do
      call_with_deterministic_seams { 42 }
      expect(sleeps).to be_empty
    end

    it "raises ArgumentError when no block given" do
      expect { described_class.call }.to raise_error(ArgumentError, /block/i)
    end
  end

  describe "transient error retry" do
    it "retries on Aws::S3::Errors::ServiceError with transient code (SlowDown)" do
      attempts = 0
      result = call_with_deterministic_seams do
        attempts += 1
        raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown") if attempts < 3
        "ok"
      end
      expect(result).to eq("ok")
      expect(attempts).to eq(3)
    end

    it "retries on ServiceError with 503 HTTP status regardless of code" do
      attempts = 0
      result = call_with_deterministic_seams do
        attempts += 1
        raise build_aws_error(Aws::S3::Errors::ServiceError, code: "UnknownCode", status: 503) if attempts < 2
        :done
      end
      expect(result).to eq(:done)
    end

    it "retries on 429 HTTP status (throttle)" do
      attempts = 0
      call_with_deterministic_seams do
        attempts += 1
        raise build_aws_error(Aws::S3::Errors::ServiceError, code: "Unknown", status: 429) if attempts < 2
      end
      expect(attempts).to eq(2)
    end

    it "retries on 500 HTTP status (server error)" do
      attempts = 0
      call_with_deterministic_seams do
        attempts += 1
        raise build_aws_error(Aws::S3::Errors::ServiceError, code: "Unknown", status: 500) if attempts < 2
      end
      expect(attempts).to eq(2)
    end

    it "retries on CloudWatch ThrottlingException code" do
      attempts = 0
      call_with_deterministic_seams do
        attempts += 1
        raise build_aws_error(Aws::CloudWatch::Errors::ServiceError, code: "ThrottlingException") if attempts < 2
      end
      expect(attempts).to eq(2)
    end

    it "retries on Seahorse::Client::NetworkingError (any wrapped origin)" do
      attempts = 0
      call_with_deterministic_seams do
        attempts += 1
        raise Seahorse::Client::NetworkingError.new(SocketError.new("connection reset")) if attempts < 2
      end
      expect(attempts).to eq(2)
    end
  end

  describe "non-transient errors pass through" do
    it "does not retry Aws::S3::Errors::NoSuchKey (4xx, non-transient code)" do
      attempts = 0
      expect {
        call_with_deterministic_seams do
          attempts += 1
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "NoSuchKey", status: 404)
        end
      }.to raise_error(Aws::S3::Errors::ServiceError)
      expect(attempts).to eq(1)  # no retry
      expect(sleeps).to be_empty
    end

    it "does not retry AccessDenied (4xx permission)" do
      attempts = 0
      expect {
        call_with_deterministic_seams do
          attempts += 1
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "AccessDenied", status: 403)
        end
      }.to raise_error(Aws::S3::Errors::ServiceError)
      expect(attempts).to eq(1)
    end

    it "does not retry arbitrary StandardError (non-AWS)" do
      attempts = 0
      expect {
        call_with_deterministic_seams do
          attempts += 1
          raise RuntimeError, "boom"
        end
      }.to raise_error(RuntimeError, "boom")
      expect(attempts).to eq(1)
    end
  end

  describe "max attempts" do
    it "raises the last transient error after max retries exhausted" do
      attempts = 0
      expect {
        call_with_deterministic_seams(max: 3) do
          attempts += 1
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown")
        end
      }.to raise_error(Aws::S3::Errors::ServiceError)
      expect(attempts).to eq(4)  # 1 initial + 3 retries
    end

    it "sleeps exactly max times between retries" do
      attempts = 0
      begin
        call_with_deterministic_seams(max: 3) do
          attempts += 1
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown")
        end
      rescue Aws::Errors::ServiceError
        # expected
      end
      expect(sleeps.size).to eq(3)
    end
  end

  describe "exponential backoff" do
    # jitter_rand returns 0.0 so delay = 0 (full-jitter formula: rand(0,backoff)*0).
    # To test the cap/base math, set jitter_rand to return 1.0 so delay = backoff.
    let(:jitter_rand) { -> { 1.0 } }

    it "follows exponential schedule (base * 2^(attempt-1)) when jitter returns 1.0" do
      attempts = 0
      begin
        call_with_deterministic_seams(max: 4, base: 0.5, cap: 30) do
          attempts += 1
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown")
        end
      rescue Aws::Errors::ServiceError
      end
      # attempt 1 fails → sleep backoff=0.5 * 2^0 = 0.5
      # attempt 2 fails → sleep backoff=0.5 * 2^1 = 1.0
      # attempt 3 fails → sleep backoff=0.5 * 2^2 = 2.0
      # attempt 4 fails → sleep backoff=0.5 * 2^3 = 4.0
      expect(sleeps).to eq([0.5, 1.0, 2.0, 4.0])
    end

    it "caps backoff at cap value" do
      attempts = 0
      begin
        call_with_deterministic_seams(max: 6, base: 1.0, cap: 8) do
          attempts += 1
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown")
        end
      rescue Aws::Errors::ServiceError
      end
      # 1, 2, 4, 8, 8 (capped), 8 (capped)
      expect(sleeps).to eq([1.0, 2.0, 4.0, 8.0, 8.0, 8.0])
    end
  end

  describe "full jitter" do
    it "delay is uniform(0, backoff) — jitter_rand=0 gives 0 delay" do
      call_with_deterministic_seams(jitter_rand: -> { 0.0 }, max: 1) do
        @attempts ||= 0
        @attempts += 1
        raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown") if @attempts < 2
      end
      expect(sleeps).to eq([0.0])
    end

    it "delay is uniform(0, backoff) — jitter_rand=0.5 gives half of backoff" do
      attempts = 0
      begin
        described_class.call(max: 1, base: 2.0, cap: 10, sleeper: sleeper, jitter_rand: -> { 0.5 }) do
          attempts += 1
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown")
        end
      rescue Aws::Errors::ServiceError
      end
      # backoff = 2.0 * 2^0 = 2.0; delay = 0.5 * 2.0 = 1.0
      expect(sleeps).to eq([1.0])
    end
  end

  describe "config validation" do
    it "rejects max=0" do
      expect {
        described_class.call(max: 0) { raise "x" }
      }.to raise_error(ArgumentError, /max/)
    end

    it "rejects negative max" do
      expect {
        described_class.call(max: -1) { raise "x" }
      }.to raise_error(ArgumentError, /max/)
    end

    it "rejects max > 20 (hard cap)" do
      expect {
        described_class.call(max: 21) { raise "x" }
      }.to raise_error(ArgumentError, /max/)
    end

    it "rejects base <= 0" do
      expect {
        described_class.call(base: 0) { raise "x" }
      }.to raise_error(ArgumentError, /base/)
    end

    it "rejects cap <= 0" do
      expect {
        described_class.call(cap: 0) { raise "x" }
      }.to raise_error(ArgumentError, /cap/)
    end
  end

  describe ".transient?" do
    it "returns true for Seahorse::Client::NetworkingError" do
      err = Seahorse::Client::NetworkingError.new(RuntimeError.new("oops"))
      expect(described_class.transient?(err)).to be(true)
    end

    it "returns true for AWS ServiceError with transient code" do
      err = build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown")
      expect(described_class.transient?(err)).to be(true)
    end

    it "returns true for AWS ServiceError with 503 HTTP status" do
      err = build_aws_error(Aws::S3::Errors::ServiceError, code: "Unknown", status: 503)
      expect(described_class.transient?(err)).to be(true)
    end

    it "returns true for 429 status" do
      err = build_aws_error(Aws::S3::Errors::ServiceError, code: "Unknown", status: 429)
      expect(described_class.transient?(err)).to be(true)
    end

    it "returns false for AWS ServiceError with permanent code + 403 status" do
      err = build_aws_error(Aws::S3::Errors::ServiceError, code: "AccessDenied", status: 403)
      expect(described_class.transient?(err)).to be(false)
    end

    it "returns false for NoSuchKey (404)" do
      err = build_aws_error(Aws::S3::Errors::ServiceError, code: "NoSuchKey", status: 404)
      expect(described_class.transient?(err)).to be(false)
    end

    it "returns false for plain StandardError" do
      expect(described_class.transient?(RuntimeError.new("x"))).to be(false)
    end
  end

  describe "observability via logger: kwarg" do
    let(:log_io) { StringIO.new }
    let(:logger) do
      Turbofan::Runtime::Logger.new(
        execution_id: "test-exec", step_name: "step", stage: "dev",
        pipeline_name: "pipe", array_index: nil, output: log_io
      )
    end

    def log_entries
      log_io.string.split("\n").reject(&:empty?).map { |l| JSON.parse(l) }
    end

    it "logs one info entry per retry attempt when logger is passed" do
      attempts = 0
      described_class.call(max: 3, sleeper: ->(_) {}, jitter_rand: -> { 0.0 }, logger: logger) do
        attempts += 1
        raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown") if attempts < 3
        :ok
      end

      entries = log_entries
      expect(entries.size).to eq(2)  # 2 retries (after attempts 1 and 2)
      entries.each do |entry|
        expect(entry["message"]).to eq("Retryable: transient error, retrying")
        expect(entry["max"]).to eq(3)
        expect(entry["error_class"]).to eq("Aws::S3::Errors::ServiceError")
        expect(entry["code"]).to eq("SlowDown")
        expect(entry["delay_ms"]).to be_a(Integer)
      end
      expect(entries.map { |e| e["attempt"] }).to eq([1, 2])
    end

    it "logs nothing when logger is nil (default, backward-compat)" do
      attempts = 0
      begin
        described_class.call(max: 1, sleeper: ->(_) {}, jitter_rand: -> { 0.0 }) do
          attempts += 1
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown")
        end
      rescue Aws::Errors::ServiceError
        # expected after max exhausted
      end
      expect(log_io.string).to eq("")  # logger not touched
    end

    it "logs nothing for non-transient errors (no retry happens)" do
      begin
        described_class.call(sleeper: ->(_) {}, jitter_rand: -> { 0.0 }, logger: logger) do
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "AccessDenied", status: 403)
        end
      rescue Aws::Errors::ServiceError
        # expected
      end
      expect(log_entries).to be_empty
    end

    it "does NOT log after final attempt exhausts max (failure not a retry)" do
      attempts = 0
      begin
        described_class.call(max: 2, sleeper: ->(_) {}, jitter_rand: -> { 0.0 }, logger: logger) do
          attempts += 1
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown")
        end
      rescue Aws::Errors::ServiceError
        # expected
      end
      # 3 total attempts (1 initial + 2 retries); logs should cover the 2 retries only
      expect(log_entries.size).to eq(2)
      expect(log_entries.map { |e| e["attempt"] }).to eq([1, 2])
    end
  end

  describe "observability via metrics: kwarg" do
    let(:recorder) do
      Class.new do
        attr_reader :emitted

        def initialize
          @emitted = []
        end

        def emit(name, value, unit: nil)
          @emitted << {name: name, value: value, unit: unit}
        end
      end.new
    end

    it "emits a RetryAttempt metric for each retry when eventually succeeding" do
      attempts = 0
      described_class.call(max: 3, sleeper: ->(_) {}, jitter_rand: -> { 0.0 }, metrics: recorder) do
        attempts += 1
        raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown") if attempts < 3
        :ok
      end

      retry_attempts = recorder.emitted.select { |e| e[:name] == "RetryAttempt" }
      expect(retry_attempts.size).to eq(2)
      expect(retry_attempts.map { |e| e[:value] }).to eq([1, 1])
    end

    it "emits a single RetriesExhausted metric when max is hit" do
      begin
        described_class.call(max: 2, sleeper: ->(_) {}, jitter_rand: -> { 0.0 }, metrics: recorder) do
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown")
        end
      rescue Aws::Errors::ServiceError
        # expected
      end

      retry_attempts = recorder.emitted.select { |e| e[:name] == "RetryAttempt" }
      retries_exhausted = recorder.emitted.select { |e| e[:name] == "RetriesExhausted" }
      expect(retry_attempts.size).to eq(2) # 2 retries (after attempt 1 and 2)
      expect(retries_exhausted.size).to eq(1) # exactly one "we gave up" signal
      expect(retries_exhausted.first[:value]).to eq(1)
    end

    it "does not emit RetriesExhausted when the block eventually succeeds" do
      attempts = 0
      described_class.call(max: 5, sleeper: ->(_) {}, jitter_rand: -> { 0.0 }, metrics: recorder) do
        attempts += 1
        raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown") if attempts < 2
        :ok
      end

      expect(recorder.emitted.map { |e| e[:name] }).not_to include("RetriesExhausted")
    end

    it "does not emit on non-transient errors (no retry happens)" do
      begin
        described_class.call(sleeper: ->(_) {}, jitter_rand: -> { 0.0 }, metrics: recorder) do
          raise build_aws_error(Aws::S3::Errors::ServiceError, code: "AccessDenied", status: 403)
        end
      rescue Aws::Errors::ServiceError
        # expected
      end

      expect(recorder.emitted).to be_empty
    end

    it "does nothing when metrics is nil (default, backward-compat)" do
      attempts = 0
      described_class.call(max: 2, sleeper: ->(_) {}, jitter_rand: -> { 0.0 }) do
        attempts += 1
        raise build_aws_error(Aws::S3::Errors::ServiceError, code: "SlowDown") if attempts < 2
        :ok
      end
      # No assertion beyond "did not raise" — nil metrics means no-op.
    end
  end
end
