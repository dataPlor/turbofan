# frozen_string_literal: true

module Turbofan
  module Pipeline
    DEFAULT_STATE = {
      turbofan_name: nil,
      turbofan_metrics: [],
      turbofan_pipeline_block: nil,
      turbofan_compute_environment: nil,
      turbofan_tags: {},
      turbofan_schedule: nil,
      turbofan_timeout: nil
    }.freeze
    private_constant :DEFAULT_STATE

    def self.init_state(klass)
      DEFAULT_STATE.each do |key, value|
        klass.instance_variable_set(:"@#{key}", value.dup)
      end
    end

    def self.included(base)
      base.extend(ClassMethods)
      init_state(base)
      Turbofan::Discovery.reset_cache!
    end

    module ClassMethods
      # Mirror of Step#inherited: re-initialize per-class DSL state so a
      # subclass of a Pipeline-including class gets its own ivar slots.
      # Call super to preserve downstream inheritance hooks.
      def inherited(subclass)
        super
        Turbofan::Pipeline.init_state(subclass)
      end

      attr_reader :turbofan_name, :turbofan_metrics,
        :turbofan_compute_environment, :turbofan_tags, :turbofan_schedule,
        :turbofan_timeout

      def pipeline_name(value)
        @turbofan_name = value
      end

      def metric(name, stat: :sum, display: :line, unit: nil, step: nil)
        @turbofan_metrics << {name: name, stat: stat, display: display, unit: unit, step: step}
      end

      def tags(hash)
        hash.each_key do |k|
          raise ArgumentError, "Tag key '#{k}' uses reserved 'turbofan:' prefix" if k.to_s.start_with?("turbofan:")
        end
        @turbofan_tags = hash.transform_keys(&:to_s)
      end

      def schedule(cron_string)
        @turbofan_schedule = cron_string
      end

      def timeout(value)
        @turbofan_timeout = value
      end

      def compute_environment(sym)
        raise ArgumentError, "compute_environment must be a Symbol, got #{sym.class}" unless sym.is_a?(Symbol)
        @turbofan_compute_environment = sym
      end

      def pipeline(&block)
        @turbofan_pipeline_block = block
      end

      def run(stage:, input: {}, region: nil)
        require "aws-sdk-cloudformation"
        require "aws-sdk-states"
        opts = region ? {region: region} : {}
        cf = Aws::CloudFormation::Client.new(**opts)
        sfn = Aws::States::Client.new(**opts)
        stack_name = Turbofan::Naming.stack_name(turbofan_name, stage)
        sm_arn = Turbofan::Deploy::StackManager.stack_output(cf, stack_name, "StateMachineArn")
        json_input = input.is_a?(String) ? input : JSON.generate(input)
        Turbofan::Deploy::Execution.start(sfn, state_machine_arn: sm_arn, input: json_input)
      end

      def turbofan_dag
        raise ArgumentError, "no pipeline block defined" unless @turbofan_pipeline_block

        builder = DagBuilder.new
        define_component_methods!(builder)
        if @turbofan_pipeline_block.arity == 0
          builder.instance_eval(&@turbofan_pipeline_block)
        else
          builder.instance_exec(builder.trigger_input, &@turbofan_pipeline_block)
        end
        builder.dag.freeze!
        builder.dag
      end

      private

      RESERVED_DAG_METHODS = %i[fan_out trigger_input].freeze
      private_constant :RESERVED_DAG_METHODS

      def define_component_methods!(builder)
        components = Turbofan.discover_components
        components[:steps].each do |method_name, klass|
          if RESERVED_DAG_METHODS.include?(method_name)
            raise ArgumentError,
              "Step class #{Turbofan::Discovery.class_name_of(klass)} maps to :#{method_name}, which conflicts " \
              "with a reserved DagBuilder method. Rename the step class."
          end
          builder.define_singleton_method(method_name) do |input_proxy = nil|
            input_proxy ||= trigger_input
            validate_schema_edge!(input_proxy, klass)
            validate_unique_name!(method_name)
            @dag.add_step(method_name)
            @dag.add_edge(from: input_proxy.step_name, to: method_name)
            DagProxy.new(method_name, schema: klass.turbofan.output_schema)
          end
        end
        components[:pipelines].each do |method_name, klass|
          next if klass == self  # skip self-reference
          if RESERVED_DAG_METHODS.include?(method_name)
            raise ArgumentError,
              "Pipeline class #{Turbofan::Discovery.class_name_of(klass)} maps to :#{method_name}, which conflicts " \
              "with a reserved DagBuilder method. Rename the pipeline class."
          end

          builder.define_singleton_method(method_name) do |input_proxy = nil|
            input_proxy ||= trigger_input
            sub_block = klass.instance_variable_get(:@turbofan_pipeline_block)
            raise ArgumentError, "Pipeline #{klass} has no pipeline block" unless sub_block

            with_trigger_override(input_proxy) do
              if sub_block.arity == 0
                instance_eval(&sub_block)
              else
                instance_exec(input_proxy, &sub_block)
              end
            end
          end
        end
      end
    end
  end
end
