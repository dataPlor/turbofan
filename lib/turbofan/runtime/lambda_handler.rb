require "json"

module Turbofan
  module Runtime
    # Lambda handler shim that adapts Lambda's (event:, context:) interface
    # to Turbofan's Wrapper.run. Used for execution :lambda steps.
    #
    # The Lambda event contains execution context (execution_id, step_name,
    # prev_step, etc.) that would normally come from Batch env vars.
    # This handler sets them as ENV vars and delegates to Wrapper.
    module LambdaHandler
      def self.process(event:, context:)
        # Set env vars from the Lambda event payload (same as Batch env vars)
        event.each do |key, value|
          next unless key.start_with?("TURBOFAN_") || key == "AWS_REGION" || key == "AWS_DEFAULT_REGION"
          ENV[key] = value.to_s
        end

        # Discover and run the step
        step_name = ENV["TURBOFAN_STEP_NAME"]
        raise "TURBOFAN_STEP_NAME not set in Lambda event" unless step_name

        components = Turbofan.discover_components
        step_class = components[:steps][step_name.to_sym]
        raise "Step class not found for :#{step_name}" unless step_class

        wrapper = Wrapper.new(step_class)
        wrapper.run

        {statusCode: 200, body: "OK"}
      rescue => e
        {statusCode: 500, body: e.message}
      end
    end
  end
end
