# frozen_string_literal: true

module Turbofan
  module Check
    module ResourceCheck
      # Resource keys that are provided by the framework and do not require a
      # matching Resource class. :duckdb is automatically available whenever any
      # resource key is declared on a step (see Step#turbofan_needs_duckdb?).
      BUILT_IN_RESOURCES = %i[duckdb].freeze

      def self.run(pipeline:, steps:, resources:)
        errors = []
        warnings = []

        fan_out_step_names = detect_fan_out_steps(pipeline)

        steps.each do |step_name, step_class|
          keys = step_class.turbofan.resource_keys
          next if keys.empty?

          keys.each do |key|
            next if BUILT_IN_RESOURCES.include?(key)

            unless resources[key]
              errors << "Step :#{step_name} uses :#{key} but no matching Resource with `key :#{key}` was found"
              next
            end

            resource = resources[key]
            if fan_out_step_names.include?(step_name) &&
                resource.respond_to?(:turbofan_resource_type) &&
                resource.turbofan_resource_type == :postgres
              warnings << "Step :#{step_name} is a fan_out step using Postgres resource :#{key} -- risk of connection storm or database overload"
            end
          end
        end

        Result.new(passed: errors.empty?, errors: errors, warnings: warnings, report: nil)
      end

      def self.detect_fan_out_steps(pipeline)
        dag = pipeline.turbofan_dag
        dag.steps.select(&:fan_out?).map(&:name).to_set
      rescue ArgumentError, Turbofan::SchemaIncompatibleError
        Set.new
      end

      private_class_method :detect_fan_out_steps
    end
  end
end
