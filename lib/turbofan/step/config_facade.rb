# frozen_string_literal: true

module Turbofan
  module Step
    # Read-only façade exposing a step's declared DSL state through a
    # single public seam. Replaces the 20+ `turbofan_*` attr_readers
    # (removed in 0.7) that previously polluted each user Step class's
    # public API.
    #
    # Use:
    #
    #   class MyStep
    #     include Turbofan::Step
    #     runs_on :batch
    #     uses :postgres
    #   end
    #
    #   MyStep.turbofan.uses         # => [{type: :resource, key: :postgres}]
    #   MyStep.turbofan.execution    # => :batch
    #   MyStep.turbofan.inspect      # walks all fields — useful in pry/irb
    class ConfigFacade
      FIELDS = %i[
        uses writes_to secrets sizes batch_size execution timeout
        retries retry_on default_cpu default_ram compute_environment
        input_schema_file output_schema_file tags docker_image
        duckdb_extensions subnets security_groups storage
      ].freeze

      def initialize(step_class)
        @step_class = step_class
      end

      FIELDS.each do |field|
        define_method(field) do
          @step_class.instance_variable_get(:"@turbofan_#{field}")
        end
      end

      # Computed readers. The `turbofan_*` methods on Step::ClassMethods
      # they delegate to are private — the façade is the only public
      # seam, reached internally via #send.
      def input_schema = @step_class.send(:turbofan_input_schema)
      def output_schema = @step_class.send(:turbofan_output_schema)
      def resource_keys = @step_class.send(:turbofan_resource_keys)
      def needs_duckdb? = @step_class.send(:turbofan_needs_duckdb?)
      def lambda? = @step_class.send(:turbofan_lambda?)
      def fargate? = @step_class.send(:turbofan_fargate?)
      def external? = @step_class.send(:turbofan_external?)

      # S3 dependency filters — private on Step::ClassMethods (Jeremy
      # Evans's audit flag), routed exclusively through the façade.
      def uses_s3 = @step_class.send(:uses_s3)
      def writes_to_s3 = @step_class.send(:writes_to_s3)

      # Per-size batch_size resolution. Accepts a size symbol (matching
      # a `size` macro declaration) or nil. Returns per-size override
      # when present, else the step-level default.
      def batch_size_for(size_name)
        per_size = @step_class.instance_variable_get(:@turbofan_sizes).dig(size_name, :batch_size)
        per_size || @step_class.instance_variable_get(:@turbofan_batch_size)
      end

      # Network placement resolution — falls back to Turbofan.config
      # defaults when the step hasn't overridden.
      def resolved_subnets
        @step_class.instance_variable_get(:@turbofan_subnets) || Turbofan.config.subnets
      end

      def resolved_security_groups
        @step_class.instance_variable_get(:@turbofan_security_groups) || Turbofan.config.security_groups
      end

      def inspect
        attrs = FIELDS.map { |f| "#{f}=#{public_send(f).inspect}" }.join(" ")
        "#<Turbofan::Step::ConfigFacade #{Turbofan::Discovery.class_name_of(@step_class)} #{attrs}>"
      end
    end
  end
end
