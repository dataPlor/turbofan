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
        require "aws-sdk-batch"
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

          if ce_class.turbofan_container_insights
            enable_container_insights(cf_client, stack_name)
          end
        end
      end

      def self.enable_container_insights(cf_client, stack_name)
        require "aws-sdk-ecs"
        ce_arn = Turbofan::Deploy::StackManager.stack_output(cf_client, stack_name, "ComputeEnvironmentArn")
        batch_client = Aws::Batch::Client.new
        ce_desc = batch_client.describe_compute_environments(compute_environments: [ce_arn])
        ecs_cluster_arn = ce_desc.compute_environments.first.ecs_cluster_arn

        ecs_client = Aws::ECS::Client.new
        ecs_client.update_cluster_settings(
          cluster: ecs_cluster_arn,
          settings: [{name: "containerInsights", value: "enabled"}]
        )
        puts "  Container insights enabled on #{ecs_cluster_arn}"
      end

      private_class_method :enable_container_insights

      def self.list
        load_all_definitions
        Turbofan::ComputeEnvironment.discover.each do |ce_class|
          puts ce_class.name
        end
      end

      def self.load_all_definitions
        config_file = File.join("turbofans", "config", "turbofan.rb")
        Kernel.load(File.expand_path(config_file)) if File.exist?(config_file)

        Dir.glob(File.join("turbofans", "compute_environments", "*.rb")).each do |path|
          Kernel.load(File.expand_path(path))
        end
      end
    end
  end
end
