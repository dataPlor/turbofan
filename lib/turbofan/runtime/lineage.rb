# frozen_string_literal: true

require "json"

module Turbofan
  module Runtime
    module Lineage
      module_function

      def start_event(context:, step_class: nil)
        build_event("START", context: context, step_class: step_class)
      end

      def complete_event(context:, step_class: nil)
        build_event("COMPLETE", context: context, step_class: step_class)
      end

      def fail_event(context:, step_class: nil, error: nil)
        event = build_event("FAIL", context: context, step_class: step_class)
        if error
          event[:run][:facets] = {
            errorMessage: "#{error.class}: #{error.message}"
          }
        end
        event
      end

      def emit(event, context:)
        entry = {
          level: "info",
          message: "OpenLineage event",
          event: event,
          timestamp: Time.now.utc.iso8601
        }
        warn(JSON.generate(entry))
      end

      def build_event(type, context:, step_class: nil)
        job = {namespace: context.pipeline_name, name: context.step_name}
        if step_class&.name
          job[:facets] = {sourceCodeLocation: {type: "ruby", name: step_class.name}}
        end
        {
          eventType: type,
          eventTime: Time.now.utc.iso8601,
          producer: "https://github.com/dataplor/turbofan",
          schemaURL: "https://openlineage.io/spec/2-0-2/OpenLineage.json",
          run: {runId: context.execution_id.to_s},
          job: job,
          inputs: build_datasets(context.uses),
          outputs: build_datasets(context.writes_to)
        }
      end

      def build_datasets(deps)
        return [] unless deps

        deps.filter_map do |dep|
          case dep[:type]
          when :s3
            {namespace: "s3", name: dep[:uri]}
          when :resource
            {namespace: "postgres", name: dep[:key].to_s}
          end
        end
      end

      private_class_method :build_event, :build_datasets
    end
  end
end
