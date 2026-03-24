require "aws-sdk-cloudformation"

module Turbofan
  class CLI < Thor
    module Destroy
      def self.call(pipeline_name:, stage:, force: false, cf_client: Aws::CloudFormation::Client.new)
        dash_name = pipeline_name.tr("_", "-")
        stack_name = "turbofan-#{dash_name}-#{stage}"

        if CLI::PROTECTED_STAGES.include?(stage) && !force
          unless Turbofan::CLI::Prompt.confirm_destructive(
            "WARNING: '#{stage}' is a protected stage.\nStack '#{stack_name}' will be permanently deleted.",
            expected_input: stack_name
          )
            raise Thor::Error, "Use --force to destroy protected stacks in non-interactive mode." unless Turbofan::CLI::Prompt.tty?
            return
          end
        end

        resources = cf_client.describe_stack_resources(stack_name: stack_name).stack_resources
        $stdout.puts "Resources in #{stack_name}:"
        resources.each do |r|
          $stdout.puts "  #{r.resource_type}  #{r.logical_resource_id}  #{r.physical_resource_id}"
        end

        unless force
          return unless Turbofan::CLI::Prompt.yes?("Delete #{resources.size} resources?", default: false)
        end

        cf_client.delete_stack(stack_name: stack_name)
        $stdout.puts "Stack #{stack_name} deletion initiated."
      end
    end
  end
end
