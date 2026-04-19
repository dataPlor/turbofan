# frozen_string_literal: true

require "aws-sdk-batch"
require "aws-sdk-cloudformation"
require "aws-sdk-states"

module Turbofan
  class CLI < Thor
    module Status
      STATUS_INDICATORS = {
        "SUCCEEDED" => "✓",
        "RUNNING" => "⟳",
        "FAILED" => "✗",
        "PENDING" => "·"
      }.freeze

      def self.call(pipeline_name:, stage:, watch: false)
        cf = Aws::CloudFormation::Client.new
        sfn = Aws::States::Client.new
        stack_name = Turbofan::Naming.stack_name(pipeline_name, stage)
        sm_arn = Turbofan::Deploy::StackManager.stack_output(cf, stack_name, "StateMachineArn")

        steps = load_pipeline_steps(pipeline_name)

        loop do
          executions = sfn.list_executions(
            state_machine_arn: sm_arn,
            status_filter: "RUNNING"
          ).executions

          if executions.empty?
            $stdout.puts "No active executions for #{stack_name}."
            break
          end

          $stdout.puts "Active executions for #{stack_name}:"
          $stdout.puts ""

          has_fetch = false
          executions.each do |exec|
            status = begin
              Turbofan::Status.fetch(
                sfn_client: sfn,
                batch_client: Aws::Batch::Client.new,
                execution_arn: exec.execution_arn,
                pipeline_name: pipeline_name,
                stage: stage,
                steps: steps
              )
            rescue StandardError => e
              warn("[Turbofan] Status.fetch failed, falling back to execution history: #{e.message}")
              nil
            end

            if status
              has_fetch = true
              print_fetch_status(status)
            else
              info = Turbofan::Deploy::Execution.describe(sfn, execution_arn: exec.execution_arn)
              steps = Turbofan::Deploy::Execution.step_statuses(sfn, execution_arn: exec.execution_arn)

              started_ago = time_ago(info[:start_date])
              $stdout.puts "#{info[:name]} (#{info[:status]}, started #{started_ago})"

              steps.each do |step_name, detail|
                indicator = STATUS_INDICATORS.fetch(detail[:status], "?")
                $stdout.puts "  #{indicator} #{step_name} #{detail[:status]}"
              end
              $stdout.puts ""
            end
          end

          break unless watch

          sleep(has_fetch ? 10 : 5)
        end
      end

      def self.print_fetch_status(status)
        started = status[:started_at] ? Time.parse(status[:started_at]) : nil
        started_ago = time_ago(started)
        $stdout.puts "#{status[:execution_id]} (#{status[:status]}, started #{started_ago})"

        status[:steps].each do |step|
          indicator = STATUS_INDICATORS.fetch(step[:status], "?")
          jobs = step[:jobs]
          total = jobs.values.sum
          parts = []
          parts << "#{jobs[:succeeded]} succeeded" if jobs[:succeeded] > 0
          parts << "#{jobs[:running]} running" if jobs[:running] > 0
          parts << "#{jobs[:failed]} failed" if jobs[:failed] > 0
          parts << "#{jobs[:pending]} pending" if jobs[:pending] > 0
          parts << "#{total} total" if total > 0
          job_str = parts.empty? ? "" : "  #{parts.join(", ")}"
          $stdout.puts "  #{indicator} #{step[:name]} #{step[:status]}#{job_str}"
        end
        $stdout.puts ""
      end
      private_class_method :print_fetch_status

      def self.time_ago(time)
        return "just now" unless time
        seconds = (Time.now - time).to_i
        if seconds < 60
          "#{seconds}s ago"
        elsif seconds < 3600
          "#{seconds / 60}m ago"
        else
          "#{seconds / 3600}h ago"
        end
      end
      private_class_method :time_ago

      def self.load_pipeline_steps(pipeline_name)
        pipeline_file = File.join(
          Turbofan::Deploy::PipelineContext::DEFAULT_ROOT,
          "pipelines", "#{pipeline_name}.rb"
        )
        return [] unless File.exist?(pipeline_file)

        load_result = Turbofan::Deploy::PipelineContext.load(pipeline_name: pipeline_name)
        load_result.steps
      rescue StandardError => e
        warn("[Turbofan] WARNING: Could not load pipeline steps: #{e.message}")
        []
      end
      private_class_method :load_pipeline_steps
    end
  end
end
