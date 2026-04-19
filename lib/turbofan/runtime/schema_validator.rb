# frozen_string_literal: true

require "json_schemer"

module Turbofan
  module Runtime
    module SchemaValidator
      def self.validate_input!(step_class, inputs)
        schema = step_class.turbofan_input_schema
        unless schema
          raise Turbofan::SchemaValidationError,
            "#{step_class} has no input_schema declared"
        end

        schemer = JSONSchemer.schema(schema)
        inputs.each do |item|
          clean = item.is_a?(Hash) ? item.reject { |k, _| k.start_with?("__") } : item
          errors = schemer.validate(clean).to_a
          next if errors.empty?

          raise Turbofan::SchemaValidationError,
            "Input validation failed for #{step_class}: #{errors.map { |e| e["error"] }.join(", ")}"
        end
      end

      def self.validate_output!(step_class, output)
        schema = step_class.turbofan_output_schema
        unless schema
          raise Turbofan::SchemaValidationError,
            "#{step_class} has no output_schema declared"
        end

        schemer = JSONSchemer.schema(schema)
        errors = schemer.validate(output).to_a
        return if errors.empty?

        raise Turbofan::SchemaValidationError,
          "Output validation failed for #{step_class}: #{errors.map { |e| e["error"] }.join(", ")}"
      end
    end
  end
end
