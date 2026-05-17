# frozen_string_literal: true

require "fileutils"

module Turbofan
  class CLI < Thor
    module New
      def self.call(name)
        Dir.chdir(Turbofan::CLI.project_root) do
          class_name = name.split("_").map(&:capitalize).join

          pipelines_dir = File.join("turbofans", "pipelines")
          schemas_dir = File.join("turbofans", "schemas")
          config_dir = File.join("turbofans", "config")

          FileUtils.mkdir_p(pipelines_dir)
          FileUtils.mkdir_p(schemas_dir)
          FileUtils.mkdir_p(config_dir)

          write_pipeline_rb(pipelines_dir, name, class_name)
          write_config(config_dir, "production")
          write_config(config_dir, "staging")
        end
      end

      def self.write_pipeline_rb(dir, name, class_name)
        File.write(File.join(dir, "#{name}.rb"), <<~RUBY)
          class #{class_name}
            include Turbofan::Pipeline

            pipeline_name "#{name}"

            pipeline do |input|
              # Add steps with: turbofan step new STEP_NAME
            end
          end
        RUBY
      end

      def self.write_config(config_dir, stage)
        path = File.join(config_dir, "#{stage}.yml")
        return if File.exist?(path)

        File.write(path, <<~YAML)
          subnets: []
          security_groups: []
        YAML
      end

      def self.write_step(step_dir, class_name, duckdb:, step_name: nil, compute_environment: :compute, cpu: 1, extensions: [])
        step_name ||= File.basename(step_dir)
        write_worker(step_dir, class_name, duckdb: duckdb, step_name: step_name, compute_environment: compute_environment, cpu: cpu, lang: "ruby")
        write_gemfile(step_dir, duckdb: duckdb)
        write_dockerfile(step_dir, duckdb: duckdb, extensions: extensions)
        write_entrypoint(step_dir, class_name)
      end

      # Orchestrator for Python-language step scaffolding. Parallel to
      # write_step but emits Python runtime files instead of Ruby.
      # worker.rb is still produced (no `def call` body) — Turbofan's
      # ObjectSpace discovery + ASL/CFN generation require it regardless
      # of container language.
      def self.write_python_step(step_dir, class_name, duckdb:, step_name: nil, compute_environment: :compute, cpu: 1, extensions: [])
        step_name ||= File.basename(step_dir)
        write_worker(step_dir, class_name, duckdb: duckdb, step_name: step_name, compute_environment: compute_environment, cpu: cpu, lang: "python")
        write_main_py(step_dir, step_name)
        write_requirements_txt(step_dir, duckdb: duckdb)
        write_python_dockerfile(step_dir, duckdb: duckdb, extensions: extensions)
      end

      def self.write_worker(step_dir, class_name, duckdb:, step_name:, compute_environment: :compute, cpu: 1, lang: "ruby")
        File.write(File.join(step_dir, "worker.rb"), worker_template(
          class_name: class_name,
          step_name: step_name,
          compute_environment: compute_environment,
          cpu: cpu,
          lang: lang
        ))
      end

      # Shared template for both languages. Ruby gets a `def call` body,
      # Python gets only the metadata (since the implementation lives in
      # main.py and is invoked by turbofan_runtime.Wrapper).
      #
      # `ram` value is in GB across both flavors (matches the actual
      # convention used by examples/steps/hello_python/worker.rb and
      # lib/turbofan/generators/cloudformation/job_definition.rb which
      # multiplies by 1024 to compute MEMORY). Earlier Ruby template
      # had `ram 2048` (interpreted as GB → 2 TB) which was a unit-
      # confusion bug; corrected to `ram 2` here.
      def self.worker_template(class_name:, step_name:, compute_environment:, cpu:, lang:)
        case lang
        when "ruby"
          <<~RUBY
            class #{class_name}
              include Turbofan::Step

              runs_on :batch
              compute_environment :#{compute_environment}
              cpu #{cpu}
              ram 2
              input_schema "#{step_name}_input.json"
              output_schema "#{step_name}_output.json"

              def call(inputs, context)
                # TODO: implement
              end
            end
          RUBY
        when "python"
          <<~RUBY
            class #{class_name}
              include Turbofan::Step

              runs_on :batch
              compute_environment :#{compute_environment}
              cpu #{cpu}
              ram 2
              batch_size 1
              input_schema "#{step_name}_input.json"
              output_schema "#{step_name}_output.json"
            end
          RUBY
        else
          raise ArgumentError, "Unknown lang: #{lang.inspect} (expected ruby or python)"
        end
      end

      def self.write_main_py(step_dir, step_name)
        File.write(File.join(step_dir, "main.py"), <<~PYTHON)
          import sys

          from turbofan_runtime import Interrupted, Wrapper


          def call(inputs, context):
              # TODO: implement
              return {"status": "ok"}


          if __name__ == "__main__":
              try:
                  Wrapper.run(
                      call,
                      input_schema="#{step_name}_input.json",
                      output_schema="#{step_name}_output.json",
                  )
              except Interrupted:
                  # SIGTERM cooperative shutdown — exit 143 so AWS Batch
                  # classifies as signal-driven (not error).
                  sys.exit(143)
        PYTHON
      end

      RUNTIME_PACKAGE_SPEC = "turbofan-runtime @ git+https://github.com/dataplor/turbofan@main#subdirectory=python"

      def self.write_requirements_txt(step_dir, duckdb:)
        lines = [RUNTIME_PACKAGE_SPEC, "boto3"]
        if duckdb
          minor = Turbofan.config.duckdb_version.split(".")[0..1].join(".")
          lines << "duckdb~=#{minor}"
        end
        File.write(File.join(step_dir, "requirements.txt"), lines.join("\n") + "\n")
      end

      def self.write_python_dockerfile(step_dir, duckdb: false, extensions: [])
        all_extensions = duckdb ? ([:postgres_scanner] + extensions.map(&:to_sym)).uniq : []

        lines = []
        lines << "FROM --platform=linux/arm64 python:3.13-slim"
        lines << ""
        # git is required for pip's git+subdirectory installs (the
        # turbofan-runtime spec uses it).
        lines << "RUN apt-get update && apt-get install -y --no-install-recommends git \\"
        lines << "    && rm -rf /var/lib/apt/lists/*"
        lines << ""
        # PYTHONUNBUFFERED ensures stdout/stderr line-buffer correctly
        # under Docker — without it, CloudWatch Logs see step output
        # only after the process exits, blocking real-time monitoring.
        lines << "ENV PYTHONUNBUFFERED=1"
        lines << ""
        lines << "WORKDIR /app"
        lines << "COPY requirements.txt ."
        lines << "RUN pip install --no-cache-dir -r requirements.txt"

        if all_extensions.any?
          install_path = Turbofan::Extensions.install_path
          curl_lines = all_extensions.map do |ext|
            url = Turbofan::Extensions.repo_url(ext)
            "    curl -fsSL #{url} | \\\n      gunzip > #{install_path}/#{ext}.duckdb_extension"
          end
          lines << ""
          lines << "# Pre-download DuckDB extensions (no internet needed at runtime)"
          lines << "RUN mkdir -p #{install_path} && \\"
          lines << curl_lines.join(" && \\\n")
        end

        lines << ""
        lines << "COPY . ."
        lines << ""
        lines << "# Schemas injected via BuildKit named context (--build-context schemas=turbofans/schemas)"
        lines << "COPY --from=schemas . schemas/"
        lines << ""
        lines << "# External deps injected via BuildKit named context (--build-context deps=<tmpdir>)"
        lines << "COPY --from=deps . ."
        lines << ""
        lines << "ENV TURBOFAN_SCHEMAS_PATH=/app/schemas"
        lines << ""
        lines << 'CMD ["python", "main.py"]'

        File.write(File.join(step_dir, "Dockerfile"), lines.join("\n") + "\n")
      end

      def self.write_gemfile(step_dir, duckdb:)
        lines = ['source "https://rubygems.org"', "", 'gem "turbofan"']
        lines << 'gem "duckdb"' if duckdb
        File.write(File.join(step_dir, "Gemfile"), lines.join("\n") + "\n")
      end

      def self.write_dockerfile(step_dir, duckdb: false, extensions: [])
        all_extensions = if duckdb
          ([:postgres_scanner] + extensions.map(&:to_sym)).uniq
        else
          []
        end

        lines = []
        lines << "FROM --platform=linux/arm64 amazonlinux:2023"
        lines << ""
        lines << "RUN dnf install -y ruby ruby-devel gcc gcc-c++ make tar gzip && dnf clean all"
        lines << ""
        lines << "WORKDIR /app"
        lines << "COPY Gemfile Gemfile"
        lines << "RUN gem install bundler && bundle install"

        if all_extensions.any?
          install_path = Turbofan::Extensions.install_path
          curl_lines = all_extensions.map do |ext|
            url = Turbofan::Extensions.repo_url(ext)
            "    curl -fsSL #{url} | \\\n      gunzip > #{install_path}/#{ext}.duckdb_extension"
          end
          lines << ""
          lines << "# Pre-download DuckDB extensions (no internet needed at runtime)"
          lines << "RUN mkdir -p #{install_path} && \\"
          lines << curl_lines.join(" && \\\n")
        end

        lines << ""
        lines << "COPY . ."
        lines << ""
        lines << "# Schemas injected via BuildKit named context (--build-context schemas=turbofans/schemas)"
        lines << "COPY --from=schemas . schemas/"
        lines << ""
        lines << "# External deps injected via BuildKit named context (--build-context deps=<tmpdir>)"
        lines << "COPY --from=deps . ."
        lines << ""
        lines << "ENV TURBOFAN_SCHEMAS_PATH=/app/schemas"
        lines << ""
        lines << 'CMD ["ruby", "entrypoint.rb"]'

        File.write(File.join(step_dir, "Dockerfile"), lines.join("\n") + "\n")
      end

      def self.write_entrypoint(step_dir, class_name)
        File.write(File.join(step_dir, "entrypoint.rb"), <<~RUBY)
          $LOAD_PATH.unshift(__dir__) unless $LOAD_PATH.include?(__dir__)
          require "turbofan"
          require_relative "worker"

          Turbofan::Runtime::Wrapper.run(#{class_name})
        RUBY
      end

      def self.write_schema(schemas_dir, step_name)
        File.write(File.join(schemas_dir, "#{step_name}_input.json"), '{"type": "object"}' + "\n")
        File.write(File.join(schemas_dir, "#{step_name}_output.json"), '{"type": "object"}' + "\n")
      end
    end
  end
end
