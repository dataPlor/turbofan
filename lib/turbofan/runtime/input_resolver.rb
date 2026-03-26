require "json"

module Turbofan
  module Runtime
    module InputResolver
      def self.call(context)
        raw = deserialize(context)
        normalize_envelope(raw)
      end

      def self.deserialize(context)
        if context.array_index
          bucket = ENV.fetch("TURBOFAN_BUCKET", "turbofan-data")
          step_name = ENV.fetch("TURBOFAN_STEP_NAME")
          FanOut.read_input(
            array_index: context.array_index,
            s3_client: context.s3,
            bucket: bucket,
            execution_id: context.execution_id,
            step_name: step_name,
            chunk: context.size,
            parent_index: ENV["TURBOFAN_PARENT_INDEX"]
          )
        elsif ENV.key?("TURBOFAN_PREV_STEPS")
          fetch_parallel_outputs(context)
        elsif ENV.key?("TURBOFAN_PREV_STEP")
          fetch_previous_step_output(context)
        else
          raw = ENV.fetch("TURBOFAN_INPUT", "{}")
          parsed = JSON.parse(raw)
          Payload.deserialize(parsed, s3_client: context.s3)
        end
      end
      private_class_method :deserialize

      def self.fetch_previous_step_output(context)
        prev_step = ENV["TURBOFAN_PREV_STEP"]
        bucket = ENV.fetch("TURBOFAN_BUCKET", "turbofan-data")

        if ENV.key?("TURBOFAN_PREV_FAN_OUT_SIZES")
          size_names = ENV["TURBOFAN_PREV_FAN_OUT_SIZES"].split(",")
          chunks = size_names.each_with_object({}) do |size, h|
            h[size] = ENV["TURBOFAN_PREV_FAN_OUT_SIZE_#{size.upcase}"].to_i
          end
          FanOut.collect_outputs(
            s3_client: context.s3,
            bucket: bucket,
            execution_id: context.execution_id,
            step_name: prev_step,
            chunks: chunks
          )
        elsif ENV.key?("TURBOFAN_PREV_FAN_OUT_SIZE")
          count = ENV["TURBOFAN_PREV_FAN_OUT_SIZE"].to_i
          FanOut.collect_outputs(
            count: count,
            s3_client: context.s3,
            bucket: bucket,
            execution_id: context.execution_id,
            step_name: prev_step
          )
        else
          key = FanOut.s3_key(context.execution_id, prev_step, "output.json")
          response = context.s3.get_object(bucket: bucket, key: key)
          JSON.parse(response.body.read)
        end
      end
      private_class_method :fetch_previous_step_output

      def self.fetch_parallel_outputs(context)
        prev_steps = ENV["TURBOFAN_PREV_STEPS"].split(",")
        bucket = ENV.fetch("TURBOFAN_BUCKET", "turbofan-data")
        prev_steps.map do |prev_step|
          key = FanOut.s3_key(context.execution_id, prev_step, "output.json")
          response = context.s3.get_object(bucket: bucket, key: key)
          JSON.parse(response.body.read)
        end
      end
      private_class_method :fetch_parallel_outputs

      def self.normalize_envelope(raw)
        if raw.is_a?(Array)
          {"inputs" => raw}
        elsif raw.is_a?(Hash) && raw.key?("inputs") && raw["inputs"].is_a?(Array)
          raw
        elsif raw.is_a?(Hash) && raw.key?("items") && raw["items"].is_a?(Array)
          {"inputs" => raw["items"]}
        else
          {"inputs" => [raw]}
        end
      end
      private_class_method :normalize_envelope
    end
  end
end
