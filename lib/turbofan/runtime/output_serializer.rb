require "json"

module Turbofan
  module Runtime
    module OutputSerializer
      def self.call(result, context)
        bucket = ENV.fetch("TURBOFAN_BUCKET", "turbofan-data")
        if context.array_index
          step_name = ENV.fetch("TURBOFAN_STEP_NAME")
          parent_index = ENV["TURBOFAN_PARENT_INDEX"]
          segment = if context.size
            "#{context.size}/"
          elsif parent_index
            "parent#{parent_index}/"
          else
            ""
          end
          key = FanOut.s3_key(context.execution_id, step_name, "output", "#{segment}#{context.array_index}.json")
          context.s3.put_object(bucket: bucket, key: key, body: JSON.generate(result))
          JSON.generate(result)
        else
          Payload.serialize(
            result,
            s3_client: context.s3,
            bucket: bucket,
            execution_id: context.execution_id,
            step_name: context.step_name
          )
        end
      end
    end
  end
end
