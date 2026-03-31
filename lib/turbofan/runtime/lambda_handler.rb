require "json"

# Load the step's entrypoint (worker.rb + dependencies).
# The RIC's WorkingDirectory is /app, so require_relative finds entrypoint.rb.
# This must happen at load time (not inside process) so the step class is
# registered before the first invocation.
$LOAD_PATH.unshift("/app") unless $LOAD_PATH.include?("/app")
require "turbofan"
require "turbofan/runtime/wrapper"

# Load worker.rb — the step class self-registers via include Turbofan::Step
Dir.glob("/app/worker.rb").each { |f| require f }

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

        step_name = ENV["TURBOFAN_STEP_NAME"]
        raise "TURBOFAN_STEP_NAME not set in Lambda event" unless step_name

        # Find the step class — it was loaded at require time above
        step_class = ObjectSpace.each_object(Class).find do |klass|
          klass < Turbofan::Step && Turbofan.snake_case(klass.name).to_s == step_name
        end
        raise "Step class not found for :#{step_name}" unless step_class

        Wrapper.new(step_class).run

        {statusCode: 200, body: "OK"}
      rescue => e
        raise  # Let Lambda report the real error instead of swallowing it
      end
    end
  end
end
