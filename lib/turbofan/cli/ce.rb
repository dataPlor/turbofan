module Turbofan
  class CLI < Thor
    module Ce
      def self.new_ce(name)
        Dir.chdir(Turbofan::CLI.project_root) do
          class_name = Turbofan::Naming.pascal_case(name)
          ce_dir = File.join("turbofans", "compute_environments")
          FileUtils.mkdir_p(ce_dir)

          File.write(File.join(ce_dir, "#{name}.rb"), <<~RUBY)
            module ComputeEnvironments
              class #{class_name}
                include Turbofan::ComputeEnvironment

                instance_types %w[optimal]
                max_vcpus 256
              end
            end
          RUBY
        end
      end

      def self.deploy(stage:)
        require "aws-sdk-cloudformation"
        load_all_definitions
        cf_client = Aws::CloudFormation::Client.new
        Turbofan::ComputeEnvironment.discover.each do |ce_class|
          template_body = ce_class.generate_template(stage: stage)
          stack_name = ce_class.stack_name(stage)
          Turbofan::Deploy::StackManager.deploy(
            cf_client,
            stack_name: stack_name,
            template_body: template_body,
            parameters: []
          )
        end
      end

      def self.list
        load_all_definitions
        Turbofan::ComputeEnvironment.discover.each do |ce_class|
          puts ce_class.name
        end
      end

      def self.load_all_definitions
        Dir.glob(File.join("turbofans", "compute_environments", "*.rb")).each do |path|
          Kernel.load(File.expand_path(path))
        end
      end
      private_class_method :load_all_definitions
    end
  end
end
