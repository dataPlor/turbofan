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
        write_worker(step_dir, class_name, duckdb: duckdb, step_name: step_name, compute_environment: compute_environment, cpu: cpu)
        write_gemfile(step_dir, duckdb: duckdb)
        write_dockerfile(step_dir, duckdb: duckdb, extensions: extensions)
        write_entrypoint(step_dir, class_name)
      end

      def self.write_worker(step_dir, class_name, duckdb:, step_name:, compute_environment: :compute, cpu: 1)
        File.write(File.join(step_dir, "worker.rb"), <<~RUBY)
          class #{class_name}
            include Turbofan::Step

            runs_on :batch
            compute_environment :#{compute_environment}
            cpu #{cpu}
            ram 2048
            input_schema "#{step_name}_input.json"
            output_schema "#{step_name}_output.json"

            def call(inputs, context)
              # TODO: implement
            end
          end
        RUBY
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
