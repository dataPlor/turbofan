# frozen_string_literal: true

require "aws-sdk-cloudwatch"

module Turbofan
  module Runtime
    class Metrics
      def initialize(pipeline_name:, stage:, step_name:, cloudwatch_client: nil, size: nil)
        @cloudwatch_client = cloudwatch_client
        @pipeline_name = pipeline_name
        @stage = stage
        @step_name = step_name
        @size = size
        @pending = []
        # fan_out workers may call emit() concurrently. Array#<< is not
        # thread-safe under MRI — two racing appends can drop an entry
        # or leave the array in an inconsistent state. flush() reads and
        # drains @pending, so it also needs mutex protection or a
        # concurrent emit() can land in a batch that's already been
        # serialized to CloudWatch payload form but not yet shifted off.
        @mutex = Mutex.new
      end

      def emit(name, value, unit: nil)
        raise ArgumentError, "metric value must be Numeric, got #{value.class}" unless value.is_a?(Numeric)
        entry = {name: name, value: value}
        entry[:unit] = unit if unit
        @mutex.synchronize { @pending << entry }
      end

      # CloudWatch PutMetricData supports up to 1000 metrics per call (1 MB
      # payload cap). Previously we used 20, causing 50× more API calls than
      # needed and increased throttle exposure at high fan-out scale.
      BATCH_SIZE = 100

      def flush
        return if @pending.empty?

        # Clear each batch only after its PUT succeeds. If a later batch fails
        # after retry exhaustion, the unsent remainder stays in @pending so a
        # subsequent flush call (if the container lives long enough) can retry.
        until @pending.empty?
          batch = @mutex.synchronize { @pending.first(BATCH_SIZE) }
          metric_data = batch.map { |entry| build_metric_datum(entry) }
          Turbofan::Retryable.call do
            cloudwatch_client.put_metric_data(
              namespace: "Turbofan/#{@pipeline_name}",
              metric_data: metric_data
            )
          end
          @mutex.synchronize { @pending.shift(batch.size) }
        end
      rescue Aws::Errors::ServiceError => e
        remaining = @mutex.synchronize { @pending.size }
        warn("[Turbofan] WARNING: Failed to flush #{remaining} remaining metrics: #{e.message}")
        # Intentionally do NOT clear @pending — if this Metrics instance is
        # flushed again, we get another chance. Container teardown is the
        # only path that truly drops them.
      end

      private

      # Disable SDK's built-in retry so Turbofan::Retryable owns all retry
      # decisions when flush wraps put_metric_data. See lib/turbofan/retryable.rb.
      # `max_attempts: 1` works across standard/adaptive/legacy modes; the
      # legacy-only `retry_limit: 0` would be ignored in modern modes.
      def cloudwatch_client
        return @cloudwatch_client if defined?(@cloudwatch_client) && @cloudwatch_client
        @mutex.synchronize do
          @cloudwatch_client ||= Aws::CloudWatch::Client.new(retry_mode: "standard", max_attempts: 1)
        end
      end

      def build_metric_datum(entry)
        datum = {
          metric_name: entry[:name],
          value: entry[:value],
          dimensions: dimensions
        }
        datum[:unit] = entry[:unit] if entry[:unit]
        datum
      end

      def dimensions
        dims = [
          {name: "Pipeline", value: @pipeline_name},
          {name: "Stage", value: @stage},
          {name: "Step", value: @step_name}
        ]
        dims << {name: "Size", value: @size.to_s} if @size
        dims
      end
    end
  end
end
