require "aws-sdk-states"

module Turbofan
  module Deploy
    module Execution
      def self.start(sfn_client, state_machine_arn:, input:)
        response = sfn_client.start_execution(
          state_machine_arn: state_machine_arn,
          input: input
        )
        response.execution_arn
      end

      def self.describe(sfn_client, execution_arn:)
        response = sfn_client.describe_execution(execution_arn: execution_arn)
        {
          status: response.status,
          start_date: response.start_date,
          stop_date: response.stop_date,
          name: response.name
        }
      end

      def self.wait_for_completion(sfn_client, execution_arn:, timeout: 600, poll_interval: 10)
        deadline = Time.now + timeout
        attempt = 0
        loop do
          info = describe(sfn_client, execution_arn: execution_arn)
          case info[:status]
          when "SUCCEEDED" then return info
          when "FAILED", "TIMED_OUT", "ABORTED"
            raise "Execution #{info[:status]}: #{execution_arn}"
          end
          raise "Timed out after #{timeout}s" if Time.now > deadline
          delay = [poll_interval * (2**attempt), 60].min
          delay += rand(0.0..1.0)
          sleep delay
          attempt += 1
        end
      end

      def self.step_statuses(sfn_client, execution_arn:)
        events = collect_events(sfn_client, execution_arn)
        statuses = {}
        current_running = nil

        events.each do |event|
          case event.type
          when "TaskStateEntered"
            name = event.state_entered_event_details.name
            statuses[name] = {
              status: "RUNNING",
              started_at: event.timestamp
            }
            current_running = name
          when "TaskFailed"
            statuses[current_running][:status] = "FAILED" if current_running && statuses[current_running]
          when "TaskStateExited"
            name = event.state_exited_event_details.name
            next unless statuses.key?(name)

            statuses[name][:status] = "SUCCEEDED" unless statuses[name][:status] == "FAILED"
            statuses[name][:ended_at] = event.timestamp
            current_running = nil if current_running == name
          end
        end

        statuses
      end

      def self.collect_events(sfn_client, execution_arn)
        events = []
        params = {execution_arn: execution_arn, reverse_order: false}
        loop do
          response = sfn_client.get_execution_history(**params)
          events.concat(response.events)
          break unless response.next_token
          params[:next_token] = response.next_token
        end
        events
      end
      private_class_method :collect_events
    end
  end
end
