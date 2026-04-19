# frozen_string_literal: true

require "json"
require "time"

module Turbofan
  module Runtime
    class Logger
      def initialize(execution_id:, step_name:, stage:, pipeline_name:, output: $stdout, array_index: nil)
        @output = output
        @metadata = {
          execution_id: execution_id,
          step: step_name,
          stage: stage,
          pipeline: pipeline_name
        }
        @metadata[:array_index] = array_index unless array_index.nil?
      end

      %w[info warn error debug].each do |level|
        define_method(level) do |message, **extra|
          write_entry(level, message, extra)
        end
      end

      private

      def write_entry(level, message, extra)
        entry = {
          level: level,
          message: message,
          **@metadata,
          timestamp: Time.now.utc.iso8601,
          **extra
        }
        @output.puts(JSON.generate(entry))
        @output.flush
      end
    end
  end
end
