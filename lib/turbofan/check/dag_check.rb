# frozen_string_literal: true

require "tsort"

module Turbofan
  module Check
    module DagCheck
      def self.run(pipeline:)
        dag = pipeline.turbofan_dag
        dag.sorted_steps
        Result.new(passed: true, errors: [], warnings: [], report: nil)
      rescue ArgumentError => e
        Result.new(passed: false, errors: [e.message], warnings: [], report: nil)
      rescue TSort::Cyclic => e
        Result.new(passed: false, errors: ["Cyclic dependency detected: #{e.message}"], warnings: [], report: nil)
      end
    end
  end
end
