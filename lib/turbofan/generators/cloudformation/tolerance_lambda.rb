# frozen_string_literal: true

module Turbofan
  module Generators
    class CloudFormation
      module ToleranceLambda
        # Handler executed by Step Functions when a fan-out branch fails —
        # checks the Batch job status summary against the tolerated failure
        # rate, writes a manifest of failed items to S3, and either returns
        # a "tolerated" result or re-raises to fail the execution.
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

            # Fallback: find by job name via listJobs (paginated)
            job_name = event['job_name']
            job_queue = event['job_queue']
            return nil unless job_name && job_queue

            next_token = nil
            loop do
              response = BATCH.list_jobs(job_queue: job_queue, job_status: 'FAILED', next_token: next_token)
              job = response.job_summary_list.find { |j| j.job_name == job_name }
              return job.job_id if job
              next_token = response.next_token
              break unless next_token
            end
            nil
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

        SUBDIR = "tolerance-lambda"
        LOGICAL_ID = "ToleranceLambda"
        ROLE_LOGICAL_ID = "ToleranceLambdaRole"
        MEMORY_SIZE = 512
        TIMEOUT = 300

        BATCH_ACCESS_POLICY = {
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
        }.freeze

        def self.generate(prefix:, bucket_prefix:, tags:)
          config = build_config(prefix: prefix, bucket_prefix: bucket_prefix, tags: tags)
          Lambdas::Packager.lambda_role(config).merge(Lambdas::Packager.lambda_function(config))
        end

        def self.handler_s3_key(bucket_prefix)
          Lambdas::Packager.handler_s3_key(
            bucket_prefix: bucket_prefix,
            subdir: SUBDIR,
            code_hash: Lambdas::Packager.code_hash(HANDLER)
          )
        end

        def self.handler_zip
          Lambdas::Packager.build_handler_zip(HANDLER)
        end

        def self.lambda_artifacts(bucket_prefix)
          [{
            bucket: Turbofan.config.bucket,
            key: handler_s3_key(bucket_prefix),
            body: handler_zip
          }]
        end

        def self.build_config(prefix:, bucket_prefix:, tags:)
          Lambdas::Packager::LambdaConfig.new(
            logical_id: LOGICAL_ID,
            role_logical_id: ROLE_LOGICAL_ID,
            function_name: "#{prefix}-tolerance-check",
            role_name: Naming.iam_role_name("#{prefix}-tolerance-lambda-role"),
            s3_key: handler_s3_key(bucket_prefix),
            memory_size: MEMORY_SIZE,
            timeout: TIMEOUT,
            code_hash: Lambdas::Packager.code_hash(HANDLER),
            bucket_prefix: bucket_prefix,
            tags: tags,
            extra_policies: [BATCH_ACCESS_POLICY]
          )
        end
        private_class_method :build_config
      end
    end
  end
end
