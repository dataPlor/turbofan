require "digest"
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
                unless data.is_a?(Hash) && data.key?('items') && data['items'].is_a?(Array)
                  raise "Invalid input from step '#{prev_step}': expected {\"items\": [...]} in output.json. " \
                        "Got: #{data.is_a?(Hash) ? data.keys.inspect : data.class}"
                end
                data['items']
              end
            elsif event.key?('trigger')
              read_trigger_input(event['trigger'], bucket)
            else
              raise "No input source: expected 'prev_step' or 'trigger' in event"
            end
          end

          def read_trigger_input(input, bucket)
            unless input.is_a?(Hash) && input.key?('items_s3_uri')
              raise "Invalid trigger input for fan-out: expected {\"items_s3_uri\": \"s3://...\"}. " \
                    "Fan-out items must be stored on S3 in {\"items\": [...]} format. " \
                    "Got: #{input.class}"
            end

            uri = input['items_s3_uri']
            unless uri.is_a?(String) && uri.start_with?('s3://')
              raise "Invalid items_s3_uri: expected s3:// URI, got: #{uri.inspect}"
            end

            key = uri.sub("s3://#{bucket}/", '')
            if key == uri
              raise "items_s3_uri must reference the pipeline bucket (s3://#{bucket}/...). " \
                    "Got: #{uri}"
            end

            response = S3.get_object(bucket: bucket, key: key)
            data = JSON.parse(response.body.read)

            unless data.is_a?(Hash) && data.key?('items') && data['items'].is_a?(Array)
              raise "Invalid S3 input format: expected {\"items\": [...]} in #{uri}. " \
                    "Got: #{data.is_a?(Hash) ? data.keys.inspect : data.class}"
            end

            data['items']
          end

          MAX_ARRAY_SIZE = 10_000
          MIN_ARRAY_SIZE = 2

          def split_into_parents(chunks, execution_id, step_name, bucket)
            parent_count = [(chunks.size.to_f / MAX_ARRAY_SIZE).ceil, 1].max
            base = chunks.size / parent_count
            remainder = chunks.size % parent_count

            parents = []
            offset = 0
            parent_count.times do |i|
              real_size = base + (i < remainder ? 1 : 0)
              parent_chunks = chunks[offset, real_size]

              # Batch requires ArrayProperties.Size >= 2. Pad with null sentinel.
              if parent_chunks.size < MIN_ARRAY_SIZE
                parent_chunks += [nil] * (MIN_ARRAY_SIZE - parent_chunks.size)
              end

              key = s3_key(execution_id, step_name, 'input', "parent#{i}", 'items.json')
              S3.put_object(bucket: bucket, key: key, body: JSON.generate(parent_chunks))
              parents << { 'index' => i, 'size' => [real_size, MIN_ARRAY_SIZE].max }
              offset += real_size
            end

            parents
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
                if chunks.size > MAX_ARRAY_SIZE
                  raise "Routed fan-out size '#{size_name}' has #{chunks.size} chunks " \
                        "(max #{MAX_ARRAY_SIZE}). Reduce batch_size or split the size."
                end
                key = s3_key(execution_id, step_name, 'input', size_name, 'items.json')
                S3.put_object(bucket: bucket, key: key, body: JSON.generate(chunks))
                sizes[size_name] = { 'count' => chunks.size }
              end

              { 'sizes' => sizes }
            else
              chunks = chunk(items, group_size)
              parents = split_into_parents(chunks, execution_id, step_name, bucket)
              { 'parents' => parents }
            end
          end
        RUBY

        LAMBDA_RUNTIME = "ruby3.3"

        # Returns the S3 key where the handler zip should be uploaded.
        # Includes the code hash so CloudFormation detects code changes
        # (S3Key change forces Lambda to re-download the zip).
        def self.handler_s3_key(bucket_prefix)
          code_hash = Digest::SHA256.hexdigest(HANDLER)[0, 12]
          "#{bucket_prefix}/chunking-lambda/handler-#{code_hash}.zip"
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
                    "TURBOFAN_BUCKET_PREFIX" => bucket_prefix,
                    "TURBOFAN_CODE_HASH" => Digest::SHA256.hexdigest(HANDLER)[0, 12]
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
