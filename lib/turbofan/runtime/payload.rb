require "json"
require "uri"
require "aws-sdk-s3"

module Turbofan
  module Runtime
    module Payload
      class HydrationError < StandardError; end

      def self.serialize(result, s3_client:, bucket:, execution_id:, step_name:)
        json = JSON.generate(result)
        key = FanOut.s3_key(execution_id, step_name, "output.json")
        s3_client.put_object(bucket: bucket, key: key, body: json)
        json
      end

      def self.deserialize(input, s3_client:)
        return input unless input.is_a?(Hash)
        return input unless input.key?("_turbofan_s3_ref")

        ref = input["_turbofan_s3_ref"]
        parsed = URI.parse(ref)
        bucket = parsed.host
        key = parsed.path.delete_prefix("/")

        response = s3_client.get_object(bucket: bucket, key: key)
        JSON.parse(response.body.read)
      rescue Aws::S3::Errors::NoSuchKey => e
        raise HydrationError, "Failed to hydrate from #{ref}: #{e.message}"
      end
    end
  end
end
