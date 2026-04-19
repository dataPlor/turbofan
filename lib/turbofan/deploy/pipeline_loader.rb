# frozen_string_literal: true

module Turbofan
  module Deploy
    class PipelineLoader
      LoadResult = Struct.new(:pipeline, :steps, :step_dirs, keyword_init: true)

      def self.load(pipeline_file, turbofans_root:)
        Turbofan.schemas_path = File.join(turbofans_root, "schemas")

        config_file = File.join(turbofans_root, "config", "turbofan.rb")
        Kernel.load(File.expand_path(config_file)) if File.exist?(config_file)

        raise "Pipeline file not found: #{pipeline_file}" unless File.exist?(pipeline_file)

        before = Set.new(Turbofan::Discovery.subclasses_of(Pipeline))

        Kernel.load(File.expand_path(pipeline_file))

        components = Turbofan.discover_components
        new_pipelines = components[:pipelines].values.reject { |c| before.include?(c) }
        raise "No pipeline class found after loading #{pipeline_file}" if new_pipelines.empty?
        raise "Multiple pipeline classes found: #{new_pipelines.map { |c| Turbofan::Discovery.class_name_of(c) }}" if new_pipelines.size > 1
        pipeline_class = new_pipelines.first

        dag = pipeline_class.turbofan_dag
        steps, step_dirs = resolve_steps(dag, components, turbofans_root)

        LoadResult.new(pipeline: pipeline_class, steps: steps, step_dirs: step_dirs)
      end

      def self.resolve_steps(dag, components, turbofans_root)
        steps = {}
        step_dirs = {}
        dag.steps.each do |dag_step|
          step_dir = File.join(turbofans_root, "steps", dag_step.name.to_s)
          klass = components[:steps][dag_step.name]
          raise "No loaded class for step :#{dag_step.name}" unless klass
          raise "Step directory not found: #{step_dir}" unless klass.turbofan_external? || Dir.exist?(step_dir)
          steps[dag_step.name] = klass
          step_dirs[dag_step.name] = step_dir
        end
        [steps, step_dirs]
      end
      private_class_method :resolve_steps
    end
  end
end
