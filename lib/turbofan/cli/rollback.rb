module Turbofan
  class CLI < Thor
    module Rollback
      def self.call(pipeline_name:, stage:)
        cf_client = Aws::CloudFormation::Client.new
        # Note: uses directory name directly. Assumes it matches pipeline's turbofan_name.
        stack_name = Turbofan::Naming.stack_name(pipeline_name, stage)
        state = Turbofan::Deploy::StackManager.detect_state(cf_client, stack_name)

        case state
        when :does_not_exist
          raise "Stack does not exist: #{stack_name}"
        when :create_complete
          raise "No previous deployment to rollback to for #{stack_name}"
        when :in_progress
          raise "Another operation is in progress on #{stack_name}"
        when :update_complete, :update_rollback_complete
          # Reapplies the previous template version. Effective for reverting
          # image tag changes since tags are baked into the CF template.
          cf_client.update_stack(
            stack_name: stack_name,
            use_previous_template: true,
            capabilities: ["CAPABILITY_NAMED_IAM"]
          )
          Turbofan::Deploy::StackManager.wait_for_stack(cf_client, stack_name: stack_name, target_states: ["UPDATE_COMPLETE"])
          puts "Rollback complete: #{stack_name}"
        else
          raise "Unhandled stack state: #{state}"
        end
      end
    end
  end
end
