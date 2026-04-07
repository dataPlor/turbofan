module Turbofan
  module Check
    module PipelineCheck
      def self.run(pipeline:, steps:, step_dirs: {})
        errors = []
        warnings = []

        validate_pipeline_name(pipeline, errors)
        validate_schedule(pipeline, errors)
        validate_steps(steps, errors)
        validate_schema_files(steps, errors)
        validate_dag_consistency(pipeline, steps, errors)
        validate_batch_size(pipeline, steps, errors, warnings)
        validate_execution(pipeline, steps, errors, warnings)
        validate_routers(pipeline, steps, step_dirs, errors, warnings)

        Result.new(passed: errors.empty?, errors: errors, warnings: warnings, report: nil)
      end

      def self.validate_batch_size(pipeline, steps, errors, _warnings)
        begin
          dag = pipeline.turbofan_dag
        rescue ArgumentError, Turbofan::SchemaIncompatibleError
          return
        end

        dag.steps.each do |dag_step|
          next unless dag_step.fan_out?
          step_class = steps[dag_step.name]
          next unless step_class
          next unless step_class.turbofan_sizes.any?

          # Routed fan-out: each size must resolve to a batch_size.
          # The step default is 1, so this only errors if someone
          # explicitly set the default to nil (shouldn't happen).
          step_class.turbofan_sizes.each do |size_name, _config|
            unless step_class.turbofan_batch_size_for(size_name)
              errors << "Step :#{dag_step.name} size :#{size_name} has no batch_size " \
                        "(set batch_size on the size or a default on the step)"
            end
          end
        end
      end
      private_class_method :validate_batch_size

      def self.validate_execution(pipeline, steps, errors, warnings)
        # Per-step checks (no DAG needed)
        steps.each do |step_name, step_class|
          unless step_class.turbofan_execution
            errors << "Step :#{step_name} has no execution model declared " \
                      "(add `execution :batch`, `execution :lambda`, or `execution :fargate`)"
            next
          end

          case step_class.turbofan_execution
          when :batch
            if step_class.turbofan_subnets
              errors << "Step :#{step_name} (execution :batch) declares subnets but Batch networking comes from the compute environment"
            end
            if step_class.turbofan_security_groups
              errors << "Step :#{step_name} (execution :batch) declares security_groups but Batch networking comes from the compute environment"
            end
            if step_class.turbofan_storage
              errors << "Step :#{step_name} (execution :batch) declares storage but storage is only valid for :fargate steps"
            end
          when :lambda
            unless step_class.turbofan_default_ram
              errors << "Step :#{step_name} (execution :lambda) requires `ram` declaration"
            end
            if step_class.turbofan_default_ram && step_class.turbofan_default_ram > 10
              errors << "Step :#{step_name} (execution :lambda) ram exceeds Lambda maximum of 10 GB"
            end
            if step_class.turbofan_default_cpu
              warnings << "Step :#{step_name} (execution :lambda) declares cpu but Lambda ignores it (cpu scales with ram)"
            end
            if step_class.turbofan_sizes.any?
              warnings << "Step :#{step_name} (execution :lambda) declares sizes but sizes are only used with execution :batch fan-out"
            end
            if step_class.turbofan_subnets
              errors << "Step :#{step_name} (execution :lambda) declares subnets but Lambda steps do not support VPC networking via the Step DSL"
            end
            if step_class.turbofan_security_groups
              errors << "Step :#{step_name} (execution :lambda) declares security_groups but Lambda steps do not support VPC networking via the Step DSL"
            end
            if step_class.turbofan_storage
              errors << "Step :#{step_name} (execution :lambda) declares storage but storage is only valid for :fargate steps"
            end
          when :fargate
            unless step_class.turbofan_default_cpu
              errors << "Step :#{step_name} (execution :fargate) requires `cpu` declaration"
            end
            unless step_class.turbofan_default_ram
              errors << "Step :#{step_name} (execution :fargate) requires `ram` declaration"
            end
          end
        end

        # Fan-out check (needs DAG)
        begin
          dag = pipeline.turbofan_dag
        rescue ArgumentError, Turbofan::SchemaIncompatibleError
          return
        end

        dag.steps.each do |dag_step|
          next unless dag_step.fan_out?
          step_class = steps[dag_step.name]
          next unless step_class&.turbofan_execution

          unless step_class.turbofan_execution == :batch
            errors << "Step :#{dag_step.name} is a fan-out step but uses execution :#{step_class.turbofan_execution} " \
                      "(fan-out requires execution :batch)"
          end
        end
      end
      private_class_method :validate_execution

      def self.validate_routers(pipeline, steps, step_dirs, _errors, warnings)
        begin
          dag = pipeline.turbofan_dag
        rescue ArgumentError, Turbofan::SchemaIncompatibleError
          return
        end

        dag.steps.each do |dag_step|
          next unless dag_step.fan_out?
          step_class = steps[dag_step.name]
          next unless step_class&.turbofan_sizes&.any?

          step_dir = step_dirs[dag_step.name]
          router_path = if step_dir
            File.join(step_dir, "router", "router.rb")
          else
            File.join("steps", dag_step.name.to_s, "router", "router.rb")
          end
          unless File.exist?(router_path)
            warnings << "Step :#{dag_step.name} is a routed fan-out but has no router at #{router_path}"
          end
        end
      end
      private_class_method :validate_routers

      def self.validate_dag_consistency(pipeline, steps, errors)
        begin
          dag = pipeline.turbofan_dag
        rescue ArgumentError
          return
        rescue Turbofan::SchemaIncompatibleError => e
          errors << "DAG schema edge validation failed: #{e.message}"
          return
        end

        dag_step_names = dag.steps.map(&:name).to_set
        step_keys = steps.keys.to_set

        (dag_step_names - step_keys).each do |name|
          errors << "DAG references step :#{name} but no Step class was loaded for it"
        end

        (step_keys - dag_step_names).each do |name|
          errors << "Step class :#{name} is loaded but not referenced in the DAG"
        end
      end
      private_class_method :validate_dag_consistency

      def self.validate_steps(steps, errors)
        steps.each do |step_name, step_class|
          if step_class.turbofan_fargate?
            # Fargate: CE is optional (networking comes from step-level or Turbofan.config)
            if step_class.turbofan_compute_environment
              begin
                Turbofan::ComputeEnvironment.resolve(step_class.turbofan_compute_environment)
              rescue ArgumentError => e
                errors << "Step :#{step_name}: #{e.message}"
              end
            end
          else
            unless step_class.turbofan_compute_environment
              errors << "Step :#{step_name} has no compute_environment (must be set on each step)"
            end

            if step_class.turbofan_compute_environment
              begin
                Turbofan::ComputeEnvironment.resolve(step_class.turbofan_compute_environment)
              rescue ArgumentError => e
                errors << "Step :#{step_name}: #{e.message}"
              end
            end
          end

          has_sizes = step_class.turbofan_sizes.any?
          has_cpu = step_class.turbofan_default_cpu
          has_ram = step_class.turbofan_default_ram
          is_lambda = step_class.turbofan_execution == :lambda

          # Lambda only needs ram (cpu scales with ram). Batch and Fargate need both.
          if !has_sizes && !is_lambda && !(has_cpu && has_ram)
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
      end
      private_class_method :validate_steps

      def self.validate_schema_files(steps, errors)
        return unless Turbofan.schemas_path

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
      private_class_method :validate_schema_files

      def self.validate_schedule(pipeline, errors)
        return unless pipeline.turbofan_schedule

        field_count = pipeline.turbofan_schedule.strip.split(/\s+/).size
        unless field_count == 6
          errors << "Schedule cron expression has #{field_count} fields, but EventBridge requires exactly 6"
        end
      end
      private_class_method :validate_schedule

      def self.validate_pipeline_name(pipeline, errors)
        name = pipeline.turbofan_name
        if name.nil? || name.to_s.strip.empty?
          errors << "Pipeline name is not set (turbofan_name is blank)"
        end
      end
      private_class_method :validate_pipeline_name
    end
  end
end
