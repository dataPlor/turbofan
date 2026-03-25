module Turbofan
  class CLI < Thor
    module Check
      def self.call(pipeline_name:, stage:, load_result: nil)
        Turbofan::CLI::Ce.load_all_definitions
        turbofans_root = "turbofans"
        pipeline_file = File.join(turbofans_root, "pipelines", "#{pipeline_name}.rb")
        load_result ||= Turbofan::Deploy::PipelineLoader.load(pipeline_file, turbofans_root: turbofans_root)

        pipeline = load_result.pipeline
        steps = load_result.steps

        all_errors = []
        all_warnings = []

        # Run PipelineCheck
        pipeline_result = Turbofan::Check::PipelineCheck.run(pipeline: pipeline, steps: steps)
        all_errors.concat(pipeline_result.errors)
        all_warnings.concat(pipeline_result.warnings)

        # Run DagCheck
        dag_result = Turbofan::Check::DagCheck.run(pipeline: pipeline)
        all_errors.concat(dag_result.errors)
        all_warnings.concat(dag_result.warnings)

        # Run ResourceCheck
        resources = Turbofan.discover_components[:resources]
        resource_result = Turbofan::Check::ResourceCheck.run(pipeline: pipeline, steps: steps, resources: resources)
        all_errors.concat(resource_result.errors)
        all_warnings.concat(resource_result.warnings)

        # Only run InstanceCheck and RouterCheck if PipelineCheck passed
        # (InstanceCheck requires valid cpu/ram on every step)
        if pipeline_result.passed?
          # Run InstanceCheck
          instance_result = Turbofan::Check::InstanceCheck.run(steps: steps)
          all_errors.concat(instance_result.errors)
          all_warnings.concat(instance_result.warnings)

          # Run RouterCheck (routers are discovered from step directories)
          routers = discover_routers(steps)
          router_result = Turbofan::Check::RouterCheck.run(steps: steps, routers: routers)
          all_errors.concat(router_result.errors)
          all_warnings.concat(router_result.warnings)
        end

        # Print results
        if all_errors.any?
          all_errors.each { |e| warn "ERROR: #{e}" }
        end

        if all_warnings.any?
          all_warnings.each { |w| warn "WARNING: #{w}" }
        end

        if all_errors.empty? && all_warnings.empty?
          puts "All checks passed."
        elsif all_errors.empty?
          puts "Checks passed with #{all_warnings.size} warning(s)."
        end

        exit(1) if all_errors.any?
      end

      def self.discover_routers(steps)
        routers = {}
        steps.each_key do |step_name|
          router_path = File.join("turbofans", "steps", step_name.to_s, "router.rb")
          next unless File.exist?(router_path)
          Kernel.load(router_path)
        end
        ObjectSpace.each_object(Class).each do |c|
          class_name = Turbofan::GET_CLASS_NAME.bind_call(c)
          next unless class_name && begin; c < Turbofan::Router; rescue NoMethodError; false; end
          live = begin; Object.const_get(class_name); rescue NameError; nil; end
          next unless live == c
          key = Turbofan.snake_case(class_name).to_s.delete_suffix("_router").to_sym
          routers[key] = c if steps.key?(key)
        end
        routers
      end
      private_class_method :discover_routers
    end
  end
end
