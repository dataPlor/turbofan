# frozen_string_literal: true

require "aws-sdk-cloudwatchlogs"

module Turbofan
  class CLI < Thor
    module Logs
      def self.call(pipeline_name:, stage:, step:, execution: nil, item: nil, query: nil, logs_client: Aws::CloudWatchLogs::Client.new)
        dash_name = pipeline_name.tr("_", "-")

        log_group = if step.nil? || step.empty?
          # When no step is specified, query a general log group for the pipeline
          "turbofan-#{dash_name}-#{stage}-logs"
        else
          "turbofan-#{dash_name}-#{stage}-logs-#{step}"
        end

        insights = Turbofan::Observability::InsightsQuery.new(log_group: log_group)
        insights = insights.execution(execution) if execution
        insights = insights.item(item) if item
        insights = insights.expression(query) if query

        query_string = insights.build

        now = Time.now
        response = logs_client.start_query(
          log_group_name: log_group,
          start_time: (now - 86_400).to_i,
          end_time: now.to_i,
          query_string: query_string
        )

        results = poll_results(logs_client, response.query_id)
        format_results(results)
      end

      POLL_TIMEOUT = 60

      def self.poll_results(logs_client, query_id)
        deadline = Time.now + POLL_TIMEOUT
        loop do
          response = logs_client.get_query_results(query_id: query_id)
          case response.status
          when "Complete" then return response.results
          when "Failed", "Cancelled", "Timeout"
            raise "CloudWatch Insights query #{response.status}"
          end
          raise "Query timed out after #{POLL_TIMEOUT}s" if Time.now > deadline
          sleep 0.5
        end
      end
      private_class_method :poll_results

      def self.format_results(results)
        results.each do |row|
          fields = row.each_with_object({}) { |f, h| h[f.field] = f.value }
          timestamp = fields["@timestamp"] || ""
          message = fields["@message"] || ""
          puts "#{timestamp}  #{message}"
        end
      end
      private_class_method :format_results
    end
  end
end
