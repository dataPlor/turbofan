require "zlib"
require "stringio"

module Turbofan
  module Generators
    class CloudFormation
      module ChunkingLambda
        HANDLER = <<~'RUBY'
          require 'json'
          require 'aws-sdk-s3'

          S3 = Aws::S3::Client.new

          def chunk(items, group_size)
            items.each_slice(group_size).to_a
          end

          def s3_key(*parts)
            prefix = ENV['TURBOFAN_BUCKET_PREFIX']
            key = parts.join('/')
            prefix && !prefix.empty? ? "#{prefix}/#{key}" : key
          end

          def read_items(event, bucket, execution_id)
            if event.key?('prev_step')
              prev_step = event['prev_step']
              if event.key?('prev_fan_out_size')
                count = event['prev_fan_out_size'].to_i
                threads = (0...count).map do |i|
                  Thread.new(i) do |idx|
                    key = s3_key(execution_id, prev_step, 'output', "#{idx}.json")
                    response = S3.get_object(bucket: bucket, key: key)
                    JSON.parse(response.body.read)
                  end
                end
                threads.map(&:value)
              else
                key = s3_key(execution_id, prev_step, 'output.json')
                response = S3.get_object(bucket: bucket, key: key)
                data = JSON.parse(response.body.read)
                data.is_a?(Hash) && data.key?('items') ? data['items'] : [data]
              end
            else
              event['items']
            end
          end

          def handler(event:, context:)
            bucket = ENV['TURBOFAN_BUCKET']
            execution_id = event['execution_id'] || context.aws_request_id
            step_name = event['step_name']
            group_size = event['group_size']
            routed = event.fetch('routed', false)

            items = read_items(event, bucket, execution_id)

            if routed
              groups = {}
              items.each do |item|
                size = item.fetch('_turbofan_size', 'default')
                (groups[size] ||= []) << item
              end

              sizes = {}
              groups.each do |size_name, size_items|
                chunks = chunk(size_items, group_size)
                chunks.each_with_index do |chunk_data, idx|
                  key = s3_key(execution_id, step_name, 'input', size_name, "#{idx}.json")
                  S3.put_object(bucket: bucket, key: key, body: JSON.generate(chunk_data))
                end
                sizes[size_name] = { 'count' => chunks.size }
              end

              { 'sizes' => sizes }
            else
              chunks = chunk(items, group_size)

              chunks.each_with_index do |chunk_data, idx|
                key = s3_key(execution_id, step_name, 'input', "#{idx}.json")
                S3.put_object(bucket: bucket, key: key, body: JSON.generate(chunk_data))
              end

              { 'chunk_count' => chunks.size }
            end
          end
        RUBY

        LAMBDA_RUNTIME = "ruby3.3"

        # Returns the S3 key where the handler zip should be uploaded
        def self.handler_s3_key(bucket_prefix)
          "#{bucket_prefix}/chunking-lambda/handler.zip"
        end

        # Builds a minimal zip file containing index.rb with the handler code
        def self.handler_zip
          content = HANDLER.b
          name = "index.rb".b
          crc = Zlib.crc32(content)
          size = content.bytesize

          buf = StringIO.new
          buf.set_encoding(Encoding::BINARY)

          # Local file header
          buf.write([0x04034b50, 20, 0, 0, 0, 0].pack("Vvvvvv"))
          buf.write([crc, size, size, name.bytesize, 0].pack("VVVvv"))
          buf.write(name)
          buf.write(content)

          # Central directory entry
          cd_offset = buf.pos
          buf.write([0x02014b50, 20, 20, 0, 0, 0, 0].pack("Vvvvvvv"))
          buf.write([crc, size, size, name.bytesize, 0, 0, 0, 0, 0, 0].pack("VVVvvvvvVV"))
          buf.write(name)
          cd_size = buf.pos - cd_offset

          # End of central directory
          buf.write([0x06054b50, 0, 0, 1, 1].pack("Vvvvv"))
          buf.write([cd_size, cd_offset, 0].pack("VVv"))

          buf.string
        end

        def self.generate(prefix:, bucket_prefix:, tags:)
          resources = {}
          resources.merge!(lambda_role(prefix, tags))
          resources.merge!(lambda_function(prefix, bucket_prefix, tags))
          resources
        end

        def self.lambda_function(prefix, bucket_prefix, tags)
          {
            "ChunkingLambda" => {
              "Type" => "AWS::Lambda::Function",
              "Properties" => {
                "FunctionName" => "#{prefix}-chunking",
                "Runtime" => LAMBDA_RUNTIME,
                "Handler" => "index.handler",
                "Timeout" => 300,
                "Role" => {"Fn::GetAtt" => ["ChunkingLambdaRole", "Arn"]},
                "Code" => {
                  "S3Bucket" => Turbofan.config.bucket,
                  "S3Key" => handler_s3_key(bucket_prefix)
                },
                "Environment" => {
                  "Variables" => {
                    "TURBOFAN_BUCKET" => Turbofan.config.bucket,
                    "TURBOFAN_BUCKET_PREFIX" => bucket_prefix
                  }
                },
                "Tags" => tags
              }
            }
          }
        end
        private_class_method :lambda_function

        def self.lambda_role(prefix, tags)
          {
            "ChunkingLambdaRole" => {
              "Type" => "AWS::IAM::Role",
              "Properties" => {
                "RoleName" => "#{prefix}-chunking-lambda-role",
                "Tags" => tags,
                "AssumeRolePolicyDocument" => {
                  "Version" => "2012-10-17",
                  "Statement" => [
                    {
                      "Effect" => "Allow",
                      "Principal" => {"Service" => "lambda.amazonaws.com"},
                      "Action" => "sts:AssumeRole"
                    }
                  ]
                },
                "ManagedPolicyArns" => [
                  "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
                ],
                "Policies" => [
                  {
                    "PolicyName" => "S3Access",
                    "PolicyDocument" => {
                      "Version" => "2012-10-17",
                      "Statement" => [
                        {
                          "Effect" => "Allow",
                          "Action" => ["s3:GetObject", "s3:PutObject"],
                          "Resource" => "arn:aws:s3:::#{Turbofan.config.bucket}/*"
                        }
                      ]
                    }
                  }
                ]
              }
            }
          }
        end
        private_class_method :lambda_role
      end
    end
  end
end
