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
      end

      def emit(name, value, unit: nil)
        raise ArgumentError, "metric value must be Numeric, got #{value.class}" unless value.is_a?(Numeric)
        entry = {name: name, value: value}
        entry[:unit] = unit if unit
        @pending << entry
      end

      def flush
        return if @pending.empty?

        @pending.each_slice(20) do |batch|
          metric_data = batch.map { |entry| build_metric_datum(entry) }
          cloudwatch_client.put_metric_data(
            namespace: "Turbofan/#{@pipeline_name}",
            metric_data: metric_data
          )
        end
        @pending.clear
      rescue Aws::Errors::ServiceError => e
        warn("[Turbofan] WARNING: Failed to flush #{@pending.size} metrics: #{e.message}")
        @pending.clear
      end

      private

      # Disable SDK's built-in retry so Turbofan::Retryable owns all retry
      # decisions when flush wraps put_metric_data. See lib/turbofan/retryable.rb.
      # `max_attempts: 1` works across standard/adaptive/legacy modes; the
      # legacy-only `retry_limit: 0` would be ignored in modern modes.
      def cloudwatch_client
        @cloudwatch_client ||= Aws::CloudWatch::Client.new(retry_mode: "standard", max_attempts: 1)
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
