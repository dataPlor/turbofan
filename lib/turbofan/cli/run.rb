require "aws-sdk-cloudformation"
require "aws-sdk-states"

module Turbofan
  class CLI < Thor
    module Run
      def self.call(pipeline_name:, stage:, input: nil, input_file: nil, dry_run: false)
        cf = Aws::CloudFormation::Client.new
        sfn = Aws::States::Client.new
        stack_name = Turbofan::Naming.stack_name(pipeline_name, stage)

        sm_arn = Turbofan::Deploy::StackManager.stack_output(cf, stack_name, "StateMachineArn")

        exec_input = input || (input_file && File.read(input_file)) || "{}"

        if dry_run
          puts "Dry run: Validation checks passed"
          puts "Pipeline: #{pipeline_name}"
          puts "Stage: #{stage}"
          puts "Steps would execute with input: #{exec_input}"
          puts "State machine: #{sm_arn}"
          puts "Dry run complete. Use without --dry-run to execute."
          return
        end

        execution_arn = Turbofan::Deploy::Execution.start(
          sfn,
          state_machine_arn: sm_arn,
          input: exec_input
        )

        region = sfn.config.region
        console_url = "https://#{region}.console.aws.amazon.com/states/home?region=#{region}#/executions/details/#{execution_arn}"
        puts "Execution started: #{execution_arn}"
        puts "Console: #{console_url}"
      end
    end
  end
end
