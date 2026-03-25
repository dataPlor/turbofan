require "aws-sdk-cloudformation"
require "aws-sdk-ecr"

module Turbofan
  class CLI < Thor
    module Destroy
      def self.call(pipeline_name:, stage:, force: false, cf_client: Aws::CloudFormation::Client.new, ecr_client: nil)
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

        state = Turbofan::Deploy::StackManager.detect_state(cf_client, stack_name)
        if state == :does_not_exist
          $stdout.puts "Stack #{stack_name} does not exist."
          return
        end

        resources = cf_client.describe_stack_resources(stack_name: stack_name).stack_resources
        $stdout.puts "Resources in #{stack_name}:"
        resources.each do |r|
          $stdout.puts "  #{r.resource_type}  #{r.logical_resource_id}  #{r.physical_resource_id}"
        end

        unless force
          return unless Turbofan::CLI::Prompt.yes?("Delete #{resources.size} resources?", default: false)
        end

        # Empty ECR repos before deletion (CloudFormation can't delete non-empty repos)
        ecr_repos = resources.select { |r| r.resource_type == "AWS::ECR::Repository" }
        ecr_client ||= Aws::ECR::Client.new if ecr_repos.any?
        ecr_repos.each do |r|
          $stdout.puts "  Emptying ECR repository: #{r.physical_resource_id}"
          Turbofan::Deploy::ImageBuilder.empty_repository(ecr_client, r.physical_resource_id)
        end

        cf_client.delete_stack(stack_name: stack_name)
        $stdout.puts "Waiting for stack deletion..."
        Turbofan::Deploy::StackManager.wait_for_stack(
          cf_client, stack_name: stack_name, target_states: ["DELETE_COMPLETE"]
        )
        $stdout.puts "Stack #{stack_name} deleted."
      end
    end
  end
end
