require_relative "lambdas/packager"

module Turbofan
  module Generators
    class CloudFormation
      module ChunkingLambda
        # Handler code bundled into the Lambda zip as `index.rb`. Kept as a real
        # Ruby file so syntax/lint tools can check it; read at gem-load time.
        HANDLER = File.read(File.expand_path("chunking_handler.rb", __dir__))

        # Turbofan::Router source bundled into the zip as `turbofan_router.rb`
        # for the per-step routed variant. Read from the gem's canonical Router
        # module so there's no drift between the user-facing module and the
        # Lambda-bundled copy.
        ROUTER_MODULE = File.read(File.expand_path("../../router.rb", __dir__))

        LAMBDA_RUNTIME = "ruby3.3"

        SUBDIR = "chunking-lambda"
        LOGICAL_ID = "ChunkingLambda"
        ROLE_LOGICAL_ID = "ChunkingLambdaRole"
        MEMORY_SIZE = 1024
        TIMEOUT = 300

        # ── Shared variant (non-routed fan-outs) ────────────────────────
        #
        # Invoked by Step Functions as `{prefix}-chunking` for pipelines
        # with non-routed fan-outs. One shared Lambda per stack.

        def self.generate(prefix:, bucket_prefix:, tags:)
          config = build_shared_config(prefix: prefix, bucket_prefix: bucket_prefix, tags: tags)
          Lambdas::Packager.lambda_role(config).merge(Lambdas::Packager.lambda_function(config))
        end

        def self.handler_s3_key(bucket_prefix)
          Lambdas::Packager.handler_s3_key(
            bucket_prefix: bucket_prefix,
            subdir: SUBDIR,
            code_hash: Lambdas::Packager.code_hash(HANDLER)
          )
        end

        def self.handler_zip
          Lambdas::Packager.build_handler_zip(HANDLER)
        end

        def self.build_shared_config(prefix:, bucket_prefix:, tags:)
          Lambdas::Packager::LambdaConfig.new(
            logical_id: LOGICAL_ID,
            role_logical_id: ROLE_LOGICAL_ID,
            function_name: "#{prefix}-chunking",
            role_name: Naming.iam_role_name("#{prefix}-chunking-lambda-role"),
            s3_key: handler_s3_key(bucket_prefix),
            memory_size: MEMORY_SIZE,
            timeout: TIMEOUT,
            code_hash: Lambdas::Packager.code_hash(HANDLER),
            bucket_prefix: bucket_prefix,
            tags: tags
          )
        end
        private_class_method :build_shared_config

        # ── Per-step routed variant ─────────────────────────────────────
        #
        # One Lambda per routed fan-out step. The user's router.rb is
        # bundled into the zip alongside the framework handler so routing
        # + chunking happen in a single in-memory pass.

        def self.generate_per_step(prefix:, step_name:, bucket_prefix:, tags:, code_hash:)
          config = build_per_step_config(
            prefix: prefix, step_name: step_name,
            bucket_prefix: bucket_prefix, tags: tags, code_hash: code_hash
          )
          Lambdas::Packager.lambda_role(config).merge(Lambdas::Packager.lambda_function(config))
        end

        def self.lambda_artifacts_per_step(bucket_prefix:, step_name:, router_source:)
          code_hash = Lambdas::Packager.code_hash(HANDLER, ROUTER_MODULE, router_source)
          [{
            bucket: Turbofan.config.bucket,
            key: handler_s3_key_per_step(bucket_prefix, step_name, code_hash),
            body: Lambdas::Packager.build_zip_from_files(
              "index.rb" => HANDLER,
              "turbofan_router.rb" => ROUTER_MODULE,
              "router.rb" => router_source
            )
          }]
        end

        def self.handler_s3_key_per_step(bucket_prefix, step_name, code_hash)
          Lambdas::Packager.handler_s3_key(
            bucket_prefix: bucket_prefix,
            subdir: SUBDIR,
            code_hash: code_hash,
            basename: step_name
          )
        end
        private_class_method :handler_s3_key_per_step

        def self.build_per_step_config(prefix:, step_name:, bucket_prefix:, tags:, code_hash:)
          step_pascal = Naming.pascal_case(step_name)
          Lambdas::Packager::LambdaConfig.new(
            logical_id: "#{LOGICAL_ID}#{step_pascal}",
            role_logical_id: "#{ROLE_LOGICAL_ID}#{step_pascal}",
            function_name: "#{prefix}-chunking-#{step_name}",
            role_name: Naming.iam_role_name("#{prefix}-chunking-#{step_name}-role"),
            s3_key: handler_s3_key_per_step(bucket_prefix, step_name, code_hash),
            memory_size: MEMORY_SIZE,
            timeout: TIMEOUT,
            code_hash: code_hash,
            bucket_prefix: bucket_prefix,
            tags: tags
          )
        end
        private_class_method :build_per_step_config
      end
    end
  end
end
