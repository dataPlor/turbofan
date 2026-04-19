# frozen_string_literal: true

require "thor"

module Turbofan
  class CLI < Thor
    PROTECTED_STAGES = %w[production staging].freeze

    map %w[-v --version] => :version
    desc "version", "Print the Turbofan gem version"
    def version
      $stdout.puts Turbofan::VERSION
    end

    desc "new NAME", "Create a new Turbofan pipeline"
    def new(name)
      Turbofan::CLI::New.call(name)
    end

    desc "check PIPELINE STAGE", "Run validation checks on a pipeline"
    def check(pipeline_name, stage)
      Turbofan::CLI::Check.call(
        pipeline_name: pipeline_name,
        stage: stage
      )
    end

    desc "destroy PIPELINE STAGE", "Destroy a deployed pipeline stack"
    option :force, type: :boolean, default: false
    def destroy(pipeline_name, stage)
      Turbofan::CLI::Destroy.call(
        pipeline_name: pipeline_name,
        stage: stage,
        force: options[:force]
      )
    end

    desc "logs PIPELINE STAGE", "Query CloudWatch logs for a pipeline"
    option :step, type: :string
    option :execution, type: :string
    option :item, type: :string
    option :query, type: :string
    def logs(pipeline_name, stage)
      Turbofan::CLI::Logs.call(
        pipeline_name: pipeline_name,
        stage: stage,
        step: options[:step],
        execution: options[:execution],
        item: options[:item],
        query: options[:query]
      )
    end

    desc "deploy PIPELINE STAGE", "Deploy a pipeline to AWS"
    option :dry_run, type: :boolean, default: false
    def deploy(pipeline_name, stage)
      Turbofan::CLI::Deploy.call(
        pipeline_name: pipeline_name,
        stage: stage,
        dry_run: options[:dry_run]
      )
    end

    desc "rollback PIPELINE STAGE", "Rollback a pipeline to its previous deployment"
    def rollback(pipeline_name, stage)
      Turbofan::CLI::Rollback.call(
        pipeline_name: pipeline_name,
        stage: stage
      )
    end

    desc "start PIPELINE STAGE", "Start a pipeline execution"
    map "run" => :start
    option :input, type: :string
    option :input_file, type: :string
    option :dry_run, type: :boolean, default: false
    def start(pipeline_name, stage)
      Turbofan::CLI::Run.call(
        pipeline_name: pipeline_name,
        stage: stage,
        input: options[:input],
        input_file: options[:input_file],
        dry_run: options[:dry_run]
      )
    end

    desc "history PIPELINE STAGE", "Show recent execution history"
    option :limit, type: :numeric, default: 20
    def history(pipeline_name, stage)
      Turbofan::CLI::History.call(
        pipeline_name: pipeline_name,
        stage: stage,
        limit: options[:limit]
      )
    end

    desc "cost PIPELINE STAGE", "Query CUR cost data from S3"
    option :days, type: :numeric, default: 60, desc: "Number of days to query (default: 60)"
    option :period, type: :string, default: "day", desc: "Time period: hour, day, week, month"
    def cost(pipeline_name, stage)
      Turbofan::CLI::Cost.call(
        pipeline_name: pipeline_name,
        stage: stage,
        days: options[:days],
        period: options[:period]
      )
    end

    desc "status PIPELINE STAGE", "List active executions for a pipeline"
    option :watch, type: :boolean, default: false
    def status(pipeline_name, stage)
      Turbofan::CLI::Status.call(
        pipeline_name: pipeline_name,
        stage: stage,
        watch: options[:watch]
      )
    end

    desc "ce SUBCOMMAND", "Manage compute environments"
    subcommand "ce", Class.new(Thor) {
      desc "new NAME", "Create a new compute environment"
      def new(name)
        Turbofan::CLI::Ce.new_ce(name)
      end

      desc "deploy STAGE", "Deploy all compute environment stacks"
      def deploy(stage)
        Turbofan::CLI::Ce.deploy(stage: stage)
      end

      desc "list", "List available compute environment profiles"
      def list
        Turbofan::CLI::Ce.list
      end

      desc "destroy STAGE", "Destroy all compute environment stacks"
      option :force, type: :boolean, default: false
      def destroy(stage)
        Turbofan::CLI::Ce.destroy(stage: stage, force: options[:force])
      end
    }

    desc "resources SUBCOMMAND", "Manage resources"
    subcommand "resources", Class.new(Thor) {
      desc "deploy STAGE", "Deploy all consumable resource stacks"
      def deploy(stage)
        Turbofan::CLI::Resources.deploy(stage: stage)
      end

      desc "list", "List available resources"
      def list
        Turbofan::CLI::Resources.list
      end
    }

    desc "step SUBCOMMAND", "Manage steps"
    subcommand "step", Class.new(Thor) {
      desc "new [NAME]", "Create a new step"
      option :duckdb, type: :boolean, default: nil
      option :compute_environment, type: :string, default: nil
      option :cpu, type: :numeric, default: nil
      def new(name = nil)
        if name.nil?
          name = Turbofan::CLI::Prompt.ask("Step name (snake_case)")
          raise Thor::Error, "Step name is required" if name.nil? || name.empty?
        end
        compute_environment = if options[:compute_environment]
          options[:compute_environment].to_sym
        else
          Turbofan::CLI::Prompt.select(
            "Compute environment",
            ["compute", "memory", "gpu"]
          ).then { |choice| choice.to_sym }
        end
        cpu = options[:cpu] || Turbofan::CLI::Prompt.select("CPU count", %w[1 2 4 8 16]).to_i
        duckdb = options[:duckdb].nil? ? Turbofan::CLI::Prompt.yes?("Include DuckDB?", default: true) : options[:duckdb]
        Turbofan::CLI::Add.call(name, duckdb: duckdb, compute_environment: compute_environment, cpu: cpu)
      end

      desc "router STEP_NAME", "Add a router to an existing step"
      def router(step_name)
        Turbofan::CLI::AddRouter.call(step_name)
      end
    }

    def self.project_root
      dir = Dir.pwd
      loop do
        return dir if Dir.exist?(File.join(dir, "turbofans"))
        return dir if File.exist?(File.join(dir, "Gemfile")) && !Dir.glob(File.join(dir, "*.gemspec")).any?
        parent = File.dirname(dir)
        return Dir.pwd if parent == dir
        dir = parent
      end
    end
  end
end

require_relative "cli/prompt"
require_relative "cli/new"
require_relative "cli/add"
require_relative "cli/add_router"
require_relative "cli/check"
require_relative "cli/destroy"
require_relative "cli/logs"
require_relative "cli/deploy"
require_relative "cli/run"
require_relative "cli/status"
require_relative "cli/rollback"
require_relative "cli/ce"
require_relative "cli/resources"
require_relative "cli/cost"
require_relative "cli/history"
