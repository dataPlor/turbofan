# frozen_string_literal: true

module Turbofan
  class Status
    PENDING_STATUSES = %w[SUBMITTED PENDING RUNNABLE STARTING].freeze
    BATCH_STATUSES = (PENDING_STATUSES + %w[RUNNING SUCCEEDED FAILED]).freeze

    def self.fetch(sfn_client:, batch_client:, execution_arn:, pipeline_name:, stage:, steps:)
      execution = sfn_client.describe_execution(execution_arn: execution_arn)
      started_at = execution.start_date

      step_results = steps_entries(steps).map do |step_name, step_class|
        if step_class&.respond_to?(:turbofan_sizes) && step_class.turbofan_sizes.any?
          counts = {pending: 0, running: 0, succeeded: 0, failed: 0}
          step_class.turbofan_sizes.each_key do |size|
            job_queue = "turbofan-#{pipeline_name}-#{stage}-queue-#{step_name}-#{size}"
            size_counts = count_jobs(batch_client, job_queue, after: started_at)
            counts.each_key { |k| counts[k] += size_counts[k] }
          end
        else
          job_queue = "turbofan-#{pipeline_name}-#{stage}-queue-#{step_name}"
          counts = count_jobs(batch_client, job_queue, after: started_at)
        end
        {
          name: step_name.to_s,
          status: derive_step_status(counts),
          jobs: counts
        }
      end

      {
        pipeline: pipeline_name.to_s,
        stage: stage.to_s,
        execution_id: execution.name,
        status: execution.status,
        started_at: execution.start_date.iso8601,
        steps: step_results
      }
    end

    def self.count_jobs(batch_client, job_queue, after: nil)
      counts = {pending: 0, running: 0, succeeded: 0, failed: 0}

      filters = []
      if after
        filters << {name: "AFTER_CREATED_AT", values: [(after.to_i * 1000).to_s]}
      end

      BATCH_STATUSES.each do |batch_status|
        next_token = nil
        loop do
          params = {job_queue: job_queue, job_status: batch_status, next_token: next_token}
          params[:filters] = filters if filters.any?
          response = batch_client.list_jobs(**params)
          job_count = response.job_summary_list.sum do |job|
            if job.array_properties
              if job.array_properties.index
                # Child of array job — parent already counted via size
                0
              elsif job.array_properties.size && job.array_properties.size > 0
                job.array_properties.size
              else
                1
              end
            else
              1
            end
          end

          bucket = PENDING_STATUSES.include?(batch_status) ? :pending : batch_status.downcase.to_sym
          counts[bucket] += job_count
          next_token = response.next_token
          break unless next_token
        end
      end

      counts
    end

    def self.derive_step_status(counts)
      total = counts.values.sum
      return "PENDING" if total.zero?

      if counts[:running] > 0
        "RUNNING"
      elsif counts[:failed] > 0
        "FAILED"
      elsif counts[:succeeded] == total
        "SUCCEEDED"
      else
        "PENDING"
      end
    end

    def self.steps_entries(steps)
      case steps
      when Hash
        steps.map { |name, klass| [name, klass] }
      else
        steps.map { |name| [name, nil] }
      end
    end

    private_class_method :count_jobs, :derive_step_status, :steps_entries
  end
end
