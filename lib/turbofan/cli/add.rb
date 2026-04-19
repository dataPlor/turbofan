# frozen_string_literal: true

require "fileutils"

module Turbofan
  class CLI < Thor
    module Add
      def self.call(step_name, duckdb: true, compute_environment: :compute, cpu: 1, extensions: [])
        Dir.chdir(Turbofan::CLI.project_root) do
          step_dir = File.join("turbofans", "steps", step_name)
          schemas_dir = File.join("turbofans", "schemas")
          class_name = step_name.split("_").map(&:capitalize).join

          FileUtils.mkdir_p(step_dir)
          FileUtils.mkdir_p(schemas_dir)
          CLI::New.write_step(step_dir, class_name, duckdb: duckdb, step_name: step_name, compute_environment: compute_environment, cpu: cpu, extensions: extensions)
          CLI::New.write_schema(schemas_dir, step_name)
        end
      end
    end
  end
end
