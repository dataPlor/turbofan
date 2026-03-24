module Turbofan
  module Check
    module PipelineCheck
      def self.run(pipeline:, steps:)
        errors = []
        warnings = []

        # 1. Pipeline name must be present
        name = pipeline.turbofan_name
        if name.nil? || name.to_s.strip.empty?
          errors << "Pipeline name is not set (turbofan_name is blank)"
        end

        # 2. Validate schedule cron field count (EventBridge requires 6 fields)
        if pipeline.turbofan_schedule
          field_count = pipeline.turbofan_schedule.strip.split(/\s+/).size
          unless field_count == 6
            errors << "Schedule cron expression has #{field_count} fields, but EventBridge requires exactly 6"
          end
        end

        # 3. Per-step validation
        steps.each do |step_name, step_class|
          unless step_class.turbofan_compute_environment
            errors << "Step :#{step_name} has no compute_environment (must be set on each step)"
          end

          has_sizes = step_class.turbofan_sizes.any?
          has_cpu = step_class.turbofan_default_cpu
          has_ram = step_class.turbofan_default_ram

          if !has_sizes && !(has_cpu && has_ram)
            if has_cpu && !has_ram
              errors << "Step :#{step_name} is missing ram (cpu is set but ram is also required)"
            elsif has_ram && !has_cpu
              errors << "Step :#{step_name} is missing cpu (ram is set but cpu is also required)"
            elsif !has_sizes
              errors << "Step :#{step_name} has neither sizes nor default cpu/ram set"
            end
          end

          if has_sizes
            step_class.turbofan_sizes.each do |size_name, config|
              missing = []
              missing << "cpu" unless config[:cpu]
              missing << "ram" unless config[:ram]
              if missing.any?
                errors << "Step :#{step_name} size :#{size_name} is missing #{missing.join(" and ")}"
              end
            end
          end

          unless step_class.turbofan_input_schema_file
            errors << "Step :#{step_name} missing input_schema declaration"
          end
          unless step_class.turbofan_output_schema_file
            errors << "Step :#{step_name} missing output_schema declaration"
          end
        end

        # 4. Schema files must exist and be valid JSON
        # Skipped when schemas_path is nil (e.g. unit tests). In the CLI path,
        # PipelineLoader.load always sets schemas_path before this runs.
        if Turbofan.schemas_path
          steps.each do |step_name, step_class|
            [
              [:input_schema, step_class.turbofan_input_schema_file],
              [:output_schema, step_class.turbofan_output_schema_file]
            ].each do |kind, filename|
              next unless filename
              path = File.join(Turbofan.schemas_path, filename)
              unless File.exist?(path)
                errors << "Step :#{step_name} #{kind} file not found: #{path}"
                next
              end
              begin
                JSON.parse(File.read(path))
              rescue JSON::ParserError => e
                errors << "Step :#{step_name} #{kind} file is not valid JSON: #{e.message}"
              end
            end
          end
        end

        # 5. DAG step names must match loaded Step class names
        # Note: turbofan_dag also validates schema edge compatibility at build time.
        # This section only cross-checks the steps hash passed by the caller (CLI path)
        # against the DAG's own step list.
        begin
          dag = pipeline.turbofan_dag
        rescue ArgumentError
          # No pipeline block defined - skip DAG checks
          return Result.new(passed: errors.empty?, errors: errors, warnings: warnings, report: nil)
        rescue Turbofan::SchemaIncompatibleError => e
          errors << "DAG schema edge validation failed: #{e.message}"
          return Result.new(passed: errors.empty?, errors: errors, warnings: warnings, report: nil)
        end

        dag_step_names = dag.steps.map(&:name).to_set
        step_keys = steps.keys.to_set

        missing_steps = dag_step_names - step_keys
        missing_steps.each do |name|
          errors << "DAG references step :#{name} but no Step class was loaded for it"
        end

        extra_steps = step_keys - dag_step_names
        extra_steps.each do |name|
          errors << "Step class :#{name} is loaded but not referenced in the DAG"
        end

        Result.new(passed: errors.empty?, errors: errors, warnings: warnings, report: nil)
      end
    end
  end
end
