require "uri"

module Turbofan
  class S3Uri
    attr_reader :bucket, :key

    def initialize(uri)
      raise ArgumentError, "Invalid S3 URI: #{uri.inspect}" unless uri.is_a?(String) && uri.start_with?("s3://")
      parsed = URI.parse(uri)
      @bucket = parsed.host
      @key = parsed.path&.delete_prefix("/") || ""
    end

    def to_bucket_arn
      "arn:aws:s3:::#{@bucket}"
    end

    def to_object_arn
      if @key.empty?
        "arn:aws:s3:::#{@bucket}/*"
      elsif @key.end_with?("*")
        "arn:aws:s3:::#{@bucket}/#{@key}"
      else
        "arn:aws:s3:::#{@bucket}/#{@key}*"
      end
    end

    def to_arns
      [to_bucket_arn, to_object_arn]
    end

    def to_s
      "s3://#{@bucket}/#{@key}"
    end
  end
end
