require "aws-sdk-cloudformation"

module Turbofan
  module Deploy
    class StackManager
      UPDATABLE_STATES = %i[create_complete update_complete update_rollback_complete].freeze
      CHANGESET_TIMEOUT = 300
      STACK_TIMEOUT = 1800

      def self.backoff_sleep(attempt, base:, max:, jitter: true)
        delay = [base * (2**attempt), max].min
        delay += rand(0.0..1.0) if jitter
        sleep(delay)
      end

      def self.stack_output(cf_client, stack_name, output_key)
        outputs = cf_client.describe_stacks(stack_name: stack_name).stacks.first.outputs
        entry = outputs.find { |o| o.output_key == output_key }
        raise "Output #{output_key} not found on stack #{stack_name}" unless entry
        entry.output_value
      end

      def self.detect_state(cf_client, stack_name)
        response = cf_client.describe_stacks(stack_name: stack_name)
        status = response.stacks.first.stack_status

        if status.end_with?("_IN_PROGRESS")
          :in_progress
        else
          status.downcase.to_sym
        end
      rescue Aws::CloudFormation::Errors::ValidationError
        :does_not_exist
      end

      def self.deploy(cf_client, stack_name:, template_body:, parameters: [], s3_client: nil, artifacts: [])
        state = detect_state(cf_client, stack_name)

        case state
        when :does_not_exist
          changeset_type = "CREATE"
        when *UPDATABLE_STATES
          changeset_type = "UPDATE"
        when :rollback_complete, :delete_failed
          cf_client.delete_stack(stack_name: stack_name)
          wait_for_stack(cf_client, stack_name: stack_name, target_states: ["DELETE_COMPLETE"])
          changeset_type = "CREATE"
        when :in_progress
          raise "Another operation is in progress on stack #{stack_name}"
        else
          raise "Unhandled stack state: #{state} for #{stack_name}"
        end

        unless artifacts.empty?
          s3 = s3_client || Aws::S3::Client.new
          artifacts.each { |a| s3.put_object(bucket: a[:bucket], key: a[:key], body: a[:body]) }
        end

        changeset_name = create_changeset(cf_client, stack_name: stack_name, template_body: template_body, type: changeset_type, parameters: parameters, s3_client: s3_client)
        result = wait_for_changeset(cf_client, stack_name: stack_name, changeset_name: changeset_name)

        if result == :failed
          reason = cf_client.describe_change_set(stack_name: stack_name, change_set_name: changeset_name).status_reason
          if reason&.match?(/no changes|didn't contain changes/i)
            cf_client.delete_change_set(stack_name: stack_name, change_set_name: changeset_name)
            return
          else
            raise "Changeset failed: #{reason}"
          end
        end

        describe_changes(cf_client, stack_name: stack_name, changeset_name: changeset_name)
        cf_client.execute_change_set(stack_name: stack_name, change_set_name: changeset_name)

        target = (changeset_type == "CREATE") ? ["CREATE_COMPLETE"] : ["UPDATE_COMPLETE"]
        wait_for_stack(cf_client, stack_name: stack_name, target_states: target)
      end

      def self.create_changeset(cf_client, stack_name:, template_body:, type:, parameters: [], s3_client: nil)
        changeset_name = "turbofan-deploy-#{Time.now.to_i}"
        template_param = if template_body.bytesize > 51_200
          s3 = s3_client || Aws::S3::Client.new
          bucket = Turbofan.config.bucket
          key = "turbofan-cfn-templates/#{stack_name}/#{changeset_name}.json"
          s3.put_object(bucket: bucket, key: key, body: template_body)
          region = cf_client.config.region
          {template_url: "https://#{bucket}.s3.#{region}.amazonaws.com/#{key}"}
        else
          {template_body: template_body}
        end

        cf_client.create_change_set(
          **template_param,
          stack_name: stack_name,
          change_set_name: changeset_name,
          change_set_type: type,
          capabilities: ["CAPABILITY_NAMED_IAM"],
          parameters: parameters
        )
        changeset_name
      end

      def self.wait_for_changeset(cf_client, stack_name:, changeset_name:)
        deadline = Time.now + CHANGESET_TIMEOUT
        attempt = 0
        loop do
          raise "Timed out waiting for changeset #{changeset_name}" if Time.now > deadline
          response = cf_client.describe_change_set(stack_name: stack_name, change_set_name: changeset_name)
          case response.status
          when "CREATE_COMPLETE"
            return :create_complete
          when "FAILED"
            return :failed
          end
          backoff_sleep(attempt, base: 2, max: 10)
          attempt += 1
        end
      end

      def self.wait_for_stack(cf_client, stack_name:, target_states:)
        deadline = Time.now + STACK_TIMEOUT
        attempt = 0
        loop do
          raise "Timed out waiting for stack #{stack_name}" if Time.now > deadline
          response = cf_client.describe_stacks(stack_name: stack_name)
          status = response.stacks.first.stack_status
          return if target_states.include?(status)
          if status.match?(/_FAILED|ROLLBACK_COMPLETE/)
            reason = failure_reason(cf_client, stack_name)
            raise "Stack #{stack_name} entered failure state: #{status}#{reason}"
          end
          backoff_sleep(attempt, base: 5, max: 30)
          attempt += 1
        end
      rescue Aws::CloudFormation::Errors::ValidationError
        return if target_states.include?("DELETE_COMPLETE")
        raise
      end

      def self.failure_reason(cf_client, stack_name)
        events = cf_client.describe_stack_events(stack_name: stack_name).stack_events
        failed = events.select { |e|
          e.resource_status&.include?("_FAILED") &&
            e.resource_status_reason && !e.resource_status_reason.match?(/cancelled/i)
        }
        return "" if failed.empty?
        reasons = failed.first(5).map { |e| "#{e.logical_resource_id}: #{e.resource_status_reason}" }
        "\n  #{reasons.join("\n  ")}"
      rescue StandardError
        ""
      end
      private_class_method :failure_reason

      def self.dry_run(cf_client, stack_name:, template_body:)
        state = detect_state(cf_client, stack_name)
        changeset_type = (state == :does_not_exist) ? "CREATE" : "UPDATE"
        changeset_name = create_changeset(cf_client, stack_name: stack_name, template_body: template_body, type: changeset_type)
        wait_for_changeset(cf_client, stack_name: stack_name, changeset_name: changeset_name)
        describe_changes(cf_client, stack_name: stack_name, changeset_name: changeset_name)
        cf_client.delete_change_set(stack_name: stack_name, change_set_name: changeset_name)
      end

      def self.wait_for_changeset(cf_client, stack_name:, changeset_name:)
        loop do
          resp = cf_client.describe_change_set(stack_name: stack_name, change_set_name: changeset_name)
          case resp.status
          when "CREATE_COMPLETE"
            return
          when "FAILED"
            puts "  Changeset failed: #{resp.status_reason}"
            return
          when "CREATE_PENDING", "CREATE_IN_PROGRESS"
            sleep 1
          else
            return
          end
        end
      end
      private_class_method :wait_for_changeset

      def self.describe_changes(cf_client, stack_name:, changeset_name:)
        response = cf_client.describe_change_set(stack_name: stack_name, change_set_name: changeset_name)
        response.changes.each do |change|
          rc = change.resource_change
          puts "  #{rc.action}  #{rc.resource_type}  #{rc.logical_resource_id}"
        end
      end

      private_class_method :create_changeset, :wait_for_changeset, :describe_changes
    end
  end
end
