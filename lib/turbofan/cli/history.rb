require "aws-sdk-cloudformation"
require "aws-sdk-states"

module Turbofan
  class CLI < Thor
    module History
      def self.call(pipeline_name:, stage:, limit: 20)
        cf = Aws::CloudFormation::Client.new
        sfn = Aws::States::Client.new
        stack_name = Turbofan::Naming.stack_name(pipeline_name, stage)

        sm_arn = Turbofan::Deploy::StackManager.stack_output(cf, stack_name, "StateMachineArn")

        response = sfn.list_executions(
          state_machine_arn: sm_arn,
          max_results: limit
        )

        executions = response.executions

        if executions.empty?
          puts "No executions found for #{pipeline_name} (#{stage})"
          return
        end

        executions.each do |exec|
          duration = if exec.stop_date && exec.start_date
            elapsed = exec.stop_date - exec.start_date
            format_duration(elapsed)
          else
            "running"
          end

          puts "#{exec.name}  #{exec.status}  #{exec.start_date}  #{duration}"
        end
      end

      def self.format_duration(seconds)
        if seconds < 60
          "#{seconds.round(1)}s"
        elsif seconds < 3600
          "#{(seconds / 60).round(1)}m"
        else
          "#{(seconds / 3600).round(1)}h"
        end
      end
      private_class_method :format_duration
    end
  end
end
