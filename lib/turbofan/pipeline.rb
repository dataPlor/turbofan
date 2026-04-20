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
      turbofan_triggers: [],
      turbofan_timeout: nil
    }.freeze
    private_constant :DEFAULT_STATE

    VALID_TRIGGER_TYPES = %i[schedule event].freeze
    private_constant :VALID_TRIGGER_TYPES

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
        :turbofan_triggers, :turbofan_timeout

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

      # Declare an EventBridge-backed pipeline trigger. Rails-style:
      # the first positional is the type Symbol, the rest are type-
      # specific kwargs.
      #
      #   trigger :schedule, cron: "0 5 * * ? *"
      #
      #   trigger :event,
      #     source: "aws.s3",
      #     detail_type: "Object Created",
      #     detail: {"bucket" => {"name" => ["my-bucket"]}}
      #
      #   trigger :event, source: "myapp.custom", event_bus: "ops-bus"
      #
      # Multiple trigger declarations are allowed — each generates its
      # own AWS::Events::Rule sharing the pipeline's GuardLambda target.
      # No triggers declared = manual-invocation-only (same as a
      # pipeline with no schedule today).
      #
      # Validation:
      # - type must be :schedule or :event
      # - :schedule requires `cron:`
      # - :event requires `source:` (String or Array<String>)
      # - :event optionally accepts `detail_type:` (String or Array),
      #   `detail:` (Hash — EventBridge pattern for detail matching),
      #   `event_bus:` (String — custom bus name, defaults to the
      #   account's default bus)
      def trigger(type, **kwargs)
        unless VALID_TRIGGER_TYPES.include?(type)
          raise ArgumentError, "trigger type must be one of #{VALID_TRIGGER_TYPES.inspect}, got #{type.inspect}"
        end

        entry = {type: type}
        case type
        when :schedule
          cron = kwargs[:cron]
          raise ArgumentError, "trigger :schedule requires a `cron:` kwarg" if cron.nil? || cron.to_s.empty?
          extra = kwargs.keys - [:cron]
          unless extra.empty?
            raise ArgumentError, "trigger :schedule does not accept #{extra.inspect}"
          end
          entry[:cron] = cron
        when :event
          source = kwargs[:source]
          if source.nil? || (source.is_a?(Array) && source.empty?) || (source.is_a?(String) && source.empty?)
            raise ArgumentError, "trigger :event requires a non-empty `source:` kwarg (String or Array of Strings)"
          end
          unless source.is_a?(String) || (source.is_a?(Array) && source.all? { |s| s.is_a?(String) })
            raise ArgumentError, "trigger :event `source:` must be a String or Array of Strings, got #{source.inspect}"
          end
          entry[:source] = Array(source)

          if (dt = kwargs[:detail_type])
            unless dt.is_a?(String) || (dt.is_a?(Array) && dt.all? { |s| s.is_a?(String) })
              raise ArgumentError, "trigger :event `detail_type:` must be a String or Array of Strings, got #{dt.inspect}"
            end
            entry[:detail_type] = Array(dt)
          end

          if (detail = kwargs[:detail])
            unless detail.is_a?(Hash)
              raise ArgumentError, "trigger :event `detail:` must be a Hash (EventBridge pattern), got #{detail.class}"
            end
            entry[:detail] = detail
          end

          if (bus = kwargs[:event_bus])
            unless bus.is_a?(String) && !bus.empty?
              raise ArgumentError, "trigger :event `event_bus:` must be a non-empty String, got #{bus.inspect}"
            end
            entry[:event_bus] = bus
          end

          allowed = %i[source detail_type detail event_bus]
          extra = kwargs.keys - allowed
          unless extra.empty?
            raise ArgumentError, "trigger :event does not accept #{extra.inspect} (allowed: #{allowed.inspect})"
          end
        end

        @turbofan_triggers << entry.freeze
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
