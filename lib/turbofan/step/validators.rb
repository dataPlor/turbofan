# frozen_string_literal: true

require "json"

module Turbofan
  module Step
    # Shared validation / parsing helpers used by the DSL macros in
    # Step::ClassMethods. Kept as module_function so ClassMethods can
    # call them as Validators.foo without mixing them into user step
    # classes' public surface.
    module Validators
      module_function

      # :duckdb is a reserved/built-in resource key. DuckDB is automatically
      # available when any resource key is declared (see BUILT_IN_RESOURCES in
      # Check::ResourceCheck). Declaring `uses :duckdb` is accepted but
      # unnecessary — DuckDB availability is inferred from the presence of
      # other resource keys.
      def parse_dependency(target)
        case target
        when Symbol
          unless target.to_s.match?(/\A[a-z_][a-z0-9_]*\z/)
            raise ArgumentError, "resource key must be a valid identifier (lowercase, underscores), got #{target.inspect}"
          end
          {type: :resource, key: target}
        when String
          unless target.start_with?("s3://")
            raise ArgumentError, "string arguments must be S3 URIs (s3://...), got #{target.inspect}"
          end
          {type: :s3, uri: target}
        else
          raise ArgumentError, "expected a Symbol (resource key) or S3 URI string, got #{target.inspect}"
        end
      end

      def load_schema(filename)
        raise "No schemas_path configured" unless Turbofan.config.schemas_path
        path = File.join(Turbofan.config.schemas_path, filename)
        JSON.parse(File.read(path))
      end

      def validate_positive!(name, value)
        unless value.is_a?(Numeric) && value > 0
          raise ArgumentError, "#{name} must be a positive number, got #{value.inspect}"
        end
      end
    end
  end
end
