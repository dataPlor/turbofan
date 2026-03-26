require "digest"
require "zlib"
require "stringio"

module Turbofan
  module Generators
    class CloudFormation
      module ToleranceLambda
        HANDLER = <<~'RUBY'
          require 'json'
          require 'aws-sdk-batch'
          require 'aws-sdk-s3'

          BATCH = Aws::Batch::Client.new
          S3 = Aws::S3::Client.new

          def s3_key(*parts)
            prefix = ENV['TURBOFAN_BUCKET_PREFIX']
            key = parts.join('/')
            prefix && !prefix.empty? ? "#{prefix}/#{key}" : key
          end

          def find_job_id(event)
            # Primary: parse JobId from Batch error Cause
            if event['error'] && event['error']['Cause']
              cause = begin
                JSON.parse(event['error']['Cause'])
              rescue JSON::ParserError
                nil
              end
              return cause['JobId'] if cause&.key?('JobId')
            end

            # Fallback: find by job name via listJobs
            job_name = event['job_name']
            job_queue = event['job_queue']
            return nil unless job_name && job_queue

            response = BATCH.list_jobs(job_queue: job_queue, job_status: 'FAILED')
            job = response.job_summary_list.find { |j| j.job_name == job_name }
            job&.job_id
          end

          def handler(event:, context:)
            bucket = ENV['TURBOFAN_BUCKET']
            job_id = find_job_id(event)
            raise "Could not determine Batch job ID from error or job name" unless job_id

            step_name = event['step_name']
            parent_index = event['parent_index']
            real_size = event['parent_real_size']
            threshold = event['tolerated_failure_rate']
            execution_id = event['execution_id']

            # Get status summary
            desc = BATCH.describe_jobs(jobs: [job_id])
            job = desc.jobs.first
            raise "Job #{job_id} not found" unless job

            summary = job.array_properties&.status_summary || {}
            succeeded = summary['SUCCEEDED'] || 0
            failed = summary['FAILED'] || 0
            total = real_size || (succeeded + failed)
            total = [total, 1].max # avoid division by zero

            failure_rate = failed.to_f / total

            if failure_rate > threshold
              raise "Failure rate #{(failure_rate * 100).round(2)}% exceeds tolerance " \
                    "#{(threshold * 100).round(2)}% (#{failed}/#{total} failed)"
            end

            # Within tolerance — collect failed child indices and inputs
            failed_items = []
            if failed > 0
              next_token = nil
              loop do
                response = BATCH.list_jobs(
                  array_job_id: job_id,
                  job_status: 'FAILED',
                  next_token: next_token
                )
                response.job_summary_list.each do |child|
                  failed_items << { 'index' => child.array_properties.index }
                end
                next_token = response.next_token
                break unless next_token
              end

              # Read input file to get the original items for failed indices
              begin
                input_key = s3_key(execution_id, step_name, 'input', "parent#{parent_index}", 'items.json')
                response = S3.get_object(bucket: bucket, key: input_key)
                input_chunks = JSON.parse(response.body.read)
                failed_items.each do |item|
                  idx = item['index']
                  item['input'] = input_chunks[idx] if idx && idx < input_chunks.size
                end
              rescue Aws::S3::Errors::NoSuchKey
                # Input file not found — indices only
              end
            end

            # Write manifest
            manifest = {
              'parent_index' => parent_index,
              'total' => total,
              'succeeded' => succeeded,
              'failed' => failed,
              'failure_rate' => failure_rate.round(6),
              'threshold' => threshold,
              'failed_items' => failed_items
            }

            manifest_key = s3_key(
              execution_id, 'tolerated_failures', step_name, "parent#{parent_index}.json"
            )
            S3.put_object(
              bucket: bucket,
              key: manifest_key,
              body: JSON.generate(manifest),
              content_type: 'application/json'
            )

            { 'status' => 'tolerated', 'failure_rate' => failure_rate, 'manifest_key' => manifest_key }
          end
        RUBY

        LAMBDA_RUNTIME = "ruby3.3"

        def self.handler_s3_key(bucket_prefix)
          code_hash = Digest::SHA256.hexdigest(HANDLER)[0, 12]
          "#{bucket_prefix}/tolerance-lambda/handler-#{code_hash}.zip"
        end

        def self.handler_zip
          content = HANDLER.b
          name = "index.rb".b
          crc = Zlib.crc32(content)
          size = content.bytesize

          buf = StringIO.new
          buf.set_encoding(Encoding::BINARY)

          # Local file header
          buf.write(["PK\x03\x04", 20, 0, 0, 0, 0, 0, 0, crc, size, size, name.bytesize, 0].pack("a4vvvvvvVVVvv"))
          buf.write(name)
          buf.write(content)
          offset = 0

          # Central directory
          cd_start = buf.pos
          buf.write(["PK\x01\x02", 20, 20, 0, 0, 0, 0, 0, 0, crc, size, size, name.bytesize, 0, 0, 0, 0, 0x20, offset].pack("a4vvvvvvVVVvvvvvVV"))
          buf.write(name)
          cd_size = buf.pos - cd_start

          # End of central directory
          buf.write(["PK\x05\x06", 0, 0, 1, 1, cd_size, cd_start, 0].pack("a4vvvvVVv"))

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
            "ToleranceLambda" => {
              "Type" => "AWS::Lambda::Function",
              "Properties" => {
                "FunctionName" => "#{prefix}-tolerance-check",
                "Runtime" => LAMBDA_RUNTIME,
                "Handler" => "index.handler",
                "Timeout" => 300,
                "MemorySize" => 512,
                "Role" => {"Fn::GetAtt" => ["ToleranceLambdaRole", "Arn"]},
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
            "ToleranceLambdaRole" => {
              "Type" => "AWS::IAM::Role",
              "Properties" => {
                "RoleName" => "#{prefix}-tolerance-lambda-role",
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
                  },
                  {
                    "PolicyName" => "BatchAccess",
                    "PolicyDocument" => {
                      "Version" => "2012-10-17",
                      "Statement" => [
                        {
                          "Effect" => "Allow",
                          "Action" => ["batch:DescribeJobs", "batch:ListJobs"],
                          "Resource" => "*"
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

        def self.lambda_artifacts(bucket_prefix)
          [{
            bucket: Turbofan.config.bucket,
            key: handler_s3_key(bucket_prefix),
            body: handler_zip
          }]
        end
      end
    end
  end
end
