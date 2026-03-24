require "json"
require "yaml"
require "aws-sdk-sts"
require_relative "deploy/preflight"

module Turbofan
  class CLI < Thor
    module Deploy
      def self.call(pipeline_name:, stage:, dry_run: false)
        turbofans_root = "turbofans"
        pipeline_file = File.join(turbofans_root, "pipelines", "#{pipeline_name}.rb")
        schemas_dir = File.join(turbofans_root, "schemas")
        ce_dir = File.join(turbofans_root, "compute_environments")

        load_result = Turbofan::Deploy::PipelineLoader.load(pipeline_file, turbofans_root: turbofans_root)
        pipeline_class = load_result.pipeline
        steps = load_result.steps
        step_dirs = load_result.step_dirs

        # Load stage config
        config_path = File.join(turbofans_root, "config", "#{stage}.yml")
        config = File.exist?(config_path) ? YAML.safe_load_file(config_path, symbolize_names: true) : {}

        # Pre-flight checks
        raise "BuildKit not available. Install Docker 23.0+ or enable BuildKit." unless Preflight.buildkit_available?
        raise "AWS credentials invalid. Run 'aws sts get-caller-identity' to debug." unless Preflight.aws_credentials_valid?
        if CLI::PROTECTED_STAGES.include?(stage)
          raise "Uncommitted changes detected. Commit or stash before deploying." unless Preflight.git_clean?
        end
        CLI::Check.call(pipeline_name: pipeline_name, stage: stage, load_result: load_result)

        # Compute image tags per step
        image_tags = {}
        step_dirs.each do |step_name, step_dir|
          image_tags[step_name] = Turbofan::Deploy::ImageBuilder.content_tag(step_dir, schemas_dir, ce_dir)
        end

        cf_client = Aws::CloudFormation::Client.new
        ecr_client = Aws::ECR::Client.new

        # Verify CE stacks exist
        steps.each do |sname, sclass|
          ce_class = sclass.turbofan_compute_environment || pipeline_class.turbofan_compute_environment
          next unless ce_class
          ce_stack_name = ce_class.stack_name(stage)
          ce_state = Turbofan::Deploy::StackManager.detect_state(cf_client, ce_stack_name)
          if ce_state == :does_not_exist
            raise "Compute environment stack '#{ce_stack_name}' not found. Deploy compute environments first: turbofan ce deploy --stage #{stage}"
          end
        end

        # Discover resources used by this pipeline's steps
        resources = {}
        Turbofan::Resource.discover.each do |r|
          resources[r.turbofan_key] = r if r.turbofan_consumable
        end

        # Verify resources stack if any consumable resources are used
        step_resource_keys = steps.values.flat_map(&:turbofan_resource_keys).uniq
        used_resources = resources.slice(*step_resource_keys)
        unless used_resources.empty?
          resources_stack = "turbofan-resources-#{stage}"
          if Turbofan::Deploy::StackManager.detect_state(cf_client, resources_stack) == :does_not_exist
            raise "Resources stack '#{resources_stack}' not found. Deploy resources first: turbofan resources deploy #{stage}"
          end
        end

        pipeline_name = pipeline_class.turbofan_name
        stack_name = Turbofan::Naming.stack_name(pipeline_name, stage)
        cfn_prefix = "turbofan-#{pipeline_name}-#{stage}"
        state = Turbofan::Deploy::StackManager.detect_state(cf_client, stack_name)

        # In-flight executions warning (only when stack already exists)
        if state != :does_not_exist
          sfn_client = Aws::States::Client.new
          sm_arn = Turbofan::Deploy::StackManager.stack_output(cf_client, stack_name, "StateMachineArn")
          Preflight.warn_running_executions(sfn_client, sm_arn)
        end

        cfn_generator = Turbofan::Generators::CloudFormation.new(
          pipeline: pipeline_class, steps: steps, stage: stage, config: config, image_tags: image_tags, resources: used_resources
        )
        template_body = JSON.generate(cfn_generator.generate)
        artifacts = cfn_generator.lambda_artifacts

        if dry_run
          Turbofan::Deploy::StackManager.dry_run(cf_client, stack_name: stack_name, template_body: template_body)
          return
        end

        if state == :does_not_exist
          # First deploy: create stack first (ECR repos), then build/push images
          Turbofan::Deploy::StackManager.deploy(cf_client, stack_name: stack_name, template_body: template_body, artifacts: artifacts)
          begin
            registry = Turbofan::Deploy::ImageBuilder.authenticate_ecr(ecr_client)
            build_and_push_all(step_dirs: step_dirs, schemas_dir: schemas_dir, ce_dir: ce_dir, stack_name: cfn_prefix, registry: registry, ecr_client: ecr_client, image_tags: image_tags)
          rescue => e
            warn("Image build/push failed: #{e.message}")
            warn("Rolling back stack #{stack_name}...")
            cf_client.delete_stack(stack_name: stack_name)
            Turbofan::Deploy::StackManager.wait_for_stack(cf_client, stack_name: stack_name, target_states: ["DELETE_COMPLETE"])
            raise
          end
        else
          # Subsequent deploy: build/push images first, then update stack
          registry = Turbofan::Deploy::ImageBuilder.authenticate_ecr(ecr_client)
          build_and_push_all(step_dirs: step_dirs, schemas_dir: schemas_dir, ce_dir: ce_dir, stack_name: cfn_prefix, registry: registry, ecr_client: ecr_client, image_tags: image_tags)
          Turbofan::Deploy::StackManager.deploy(cf_client, stack_name: stack_name, template_body: template_body, artifacts: artifacts)
        end

        puts "Deploy complete: #{stack_name}"
      end

      def self.build_and_push_all(step_dirs:, schemas_dir:, ce_dir:, stack_name:, registry:, ecr_client:, image_tags:)
        configs = step_dirs.map do |step_name, step_dir|
          {
            step_dir: step_dir,
            schemas_dir: schemas_dir,
            ce_dir: ce_dir,
            ecr_client: ecr_client,
            repository_name: "#{stack_name}-ecr-#{step_name}",
            repository_uri: "#{registry}/#{stack_name}-ecr-#{step_name}",
            tag: image_tags[step_name]
          }
        end
        Turbofan::Deploy::ImageBuilder.build_and_push_all(step_configs: configs)
      end

      private_class_method :build_and_push_all
    end
  end
end
