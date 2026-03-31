require "digest"
require "base64"
require "open3"
require "pathname"
require "aws-sdk-ecr"
require_relative "dependency_resolver"

module Turbofan
  module Deploy
    class ImageBuilder
      # ECR repos are owned by the image builder, not CloudFormation.
      # This follows SAM's --resolve-image-repos pattern: repos are deployment
      # plumbing with near-zero config drift risk. Creating them here avoids
      # the chicken-and-egg problem where CFN can't create repos before images
      # are pushed, but images can't be pushed before repos exist.
      # turbofan destroy cleans up repos by naming convention.
      ECR_LIFECYCLE_POLICY = {
        "rules" => [
          {
            "rulePriority" => 1,
            "description" => "Keep last 30 tagged images",
            "selection" => {
              "tagStatus" => "tagged",
              "tagPrefixList" => ["sha-"],
              "countType" => "imageCountMoreThan",
              "countNumber" => 30
            },
            "action" => {"type" => "expire"}
          },
          {
            "rulePriority" => 2,
            "description" => "Expire untagged images after 7 days",
            "selection" => {
              "tagStatus" => "untagged",
              "countType" => "sinceImagePushed",
              "countUnit" => "days",
              "countNumber" => 7
            },
            "action" => {"type" => "expire"}
          }
        ]
      }.to_json.freeze
      def self.content_tag(step_dir, schemas_dir, external_deps: [], project_root: Dir.pwd)
        digest = Digest::SHA256.new
        [step_dir, schemas_dir].each do |dir|
          Dir.glob("#{dir}/**/*").select { |f| File.file?(f) }.sort.each do |f|
            digest.update(Pathname.new(f).relative_path_from(dir).to_s)
            digest.update(File.binread(f))
          end
        end

        pr = Pathname.new(project_root)
        external_deps.sort.each do |f|
          digest.update(Pathname.new(f).relative_path_from(pr).to_s)
          digest.update(File.binread(f))
        end

        "sha-#{digest.hexdigest[0, 12]}"
      end

      def self.image_exists?(ecr_client, repository_name, image_tag)
        ecr_client.describe_images(
          repository_name: repository_name,
          image_ids: [{image_tag: image_tag}]
        )
        true
      rescue Aws::ECR::Errors::ImageNotFoundException
        false
      rescue Aws::ECR::Errors::RepositoryNotFoundException
        ecr_client.create_repository(
          repository_name: repository_name,
          image_scanning_configuration: {scan_on_push: true}
        )
        ecr_client.put_lifecycle_policy(
          repository_name: repository_name,
          lifecycle_policy_text: ECR_LIFECYCLE_POLICY
        )
        puts "  Created ECR repository: #{repository_name}"
        false
      end

      # Wrapping Dockerfile for Lambda steps. Applied as a second build on top
      # of the user's image. Adds aws_lambda_ric and sets the Lambda entrypoint.
      # The user's Dockerfile stays execution-model-agnostic.
      LAMBDA_WRAPPER = <<~DOCKERFILE
        ARG BASE_IMAGE
        FROM ${BASE_IMAGE}
        RUN gem install aws_lambda_ric
        ENTRYPOINT ["/usr/local/bin/aws_lambda_ric"]
        CMD ["turbofan/runtime/lambda_handler.Turbofan::Runtime::LambdaHandler.process"]
      DOCKERFILE

      def self.wrap_for_lambda(repository_uri:, tag:)
        require "tempfile"
        base_image = "#{repository_uri}:#{tag}"
        wrapper = Tempfile.new(["lambda-wrapper", ".Dockerfile"])
        wrapper.write(LAMBDA_WRAPPER)
        wrapper.close
        run_cmd("docker", "build",
          "--provenance=false",
          "--build-arg", "BASE_IMAGE=#{base_image}",
          "-f", wrapper.path,
          "-t", base_image,
          ".")
      ensure
        wrapper&.unlink
      end

      def self.build(step_dir, schemas_dir, tag:, repository_uri:, external_deps: [], project_root: Dir.pwd)
        deps_dir = DependencyResolver.prepare_build_context(external_deps, project_root)

        cmd = ["docker", "build",
          "--provenance=false",
          "--build-context", "schemas=#{schemas_dir}",
          "--build-context", "deps=#{deps_dir}"]
        proxy_ca = ENV.fetch("TURBOFAN_PROXY_CA", "/usr/local/share/ca-certificates/proxy-ca.crt")
        if File.exist?(proxy_ca)
          cmd.push("--build-context", "proxy-ca=#{File.dirname(proxy_ca)}")
        end
        %w[HTTP_PROXY HTTPS_PROXY http_proxy https_proxy no_proxy NO_PROXY].each do |var|
          cmd.push("--build-arg", "#{var}=#{ENV[var]}") if ENV[var]
        end
        cmd.push("-t", "#{repository_uri}:#{tag}", step_dir)
        run_cmd(*cmd)
      ensure
        DependencyResolver.cleanup_build_context(deps_dir)
      end

      def self.push(tag:, repository_uri:)
        run_cmd("docker", "push", "#{repository_uri}:#{tag}")
      end

      def self.authenticate_ecr(ecr_client)
        auth = ecr_client.get_authorization_token.authorization_data.first
        password = Base64.decode64(auth.authorization_token).split(":").last
        registry = auth.proxy_endpoint

        _out, status = Open3.capture2(
          "docker", "login", "--username", "AWS", "--password-stdin", registry,
          stdin_data: password
        )
        raise "ECR authentication failed" unless status.success?

        registry.sub(%r{\Ahttps?://}, "")
      end

      def self.git_sha
        sha = `git rev-parse --short HEAD 2>/dev/null`.strip
        sha.empty? ? nil : "git-#{sha}"
      end

      def self.build_and_push(step_dir:, schemas_dir:, ecr_client:, repository_name:, repository_uri:, tag: nil, external_deps: [], project_root: Dir.pwd, lambda_wrap: false)
        tag ||= content_tag(step_dir, schemas_dir, external_deps: external_deps, project_root: project_root)

        if image_exists?(ecr_client, repository_name, tag)
          puts "Image #{repository_name}:#{tag} already exists, skipping build"
          return tag
        end

        build(step_dir, schemas_dir, tag: tag, repository_uri: repository_uri, external_deps: external_deps, project_root: project_root)
        wrap_for_lambda(repository_uri: repository_uri, tag: tag) if lambda_wrap
        push(tag: tag, repository_uri: repository_uri)

        git_tag = git_sha
        if git_tag
          run_cmd("docker", "tag", "#{repository_uri}:#{tag}", "#{repository_uri}:#{git_tag}")
          push(tag: git_tag, repository_uri: repository_uri)
        end

        tag
      end

      def self.build_and_push_all(step_configs:)
        results = {}
        threads = step_configs.map do |config|
          Thread.new do
            tag = build_and_push(**config)
            [config[:step_dir], tag]
          rescue => e
            step_name = File.basename(config[:step_dir])
            raise "Build failed for step '#{step_name}': #{e.message}"
          end
        end
        threads.each do |t|
          dir, tag = t.value
          results[dir] = tag
        end
        results
      end

      def self.empty_repository(ecr_client, repository_name)
        loop do
          response = ecr_client.list_images(repository_name: repository_name, max_results: 100)
          break if response.image_ids.empty?
          ecr_client.batch_delete_image(repository_name: repository_name, image_ids: response.image_ids)
        end
      rescue Aws::ECR::Errors::RepositoryNotFoundException
        nil
      end

      def self.garbage_collect(ecr_client, repository_name, keep:)
        images = []
        params = {repository_name: repository_name}
        loop do
          response = ecr_client.describe_images(**params)
          images.concat(response.image_details)
          break unless response.next_token
          params[:next_token] = response.next_token
        end

        sha_images = images.select { |img| img.image_tags&.any? { |t| t.start_with?("sha-") } }
        sha_images.sort_by! { |img| img.image_pushed_at }

        delete_count = sha_images.size - keep
        return if delete_count <= 0
        to_delete = sha_images.first(delete_count)

        ecr_client.batch_delete_image(
          repository_name: repository_name,
          image_ids: to_delete.map { |img| {image_digest: img.image_digest} }
        )
      end

      def self.run_cmd(*cmd)
        success = system(*cmd)
        raise "Command failed: #{cmd.first(3).join(" ")}" unless success
      end
      private_class_method :run_cmd
    end
  end
end
