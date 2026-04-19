# frozen_string_literal: true

require "zeitwerk"

# Errors must exist before Zeitwerk resolves any gem file that references
# Turbofan::Error / Turbofan::ConfigError / Turbofan::ValidationError.
# Loading them manually here means subsequent autoload triggers find the
# base classes already defined.
require_relative "turbofan/errors"

module Turbofan
  class << self
    attr_reader :loader
  end

  @loader = Zeitwerk::Loader.for_gem
  loader.inflector.inflect(
    "asl" => "ASL",
    "cli" => "CLI",
    "cloudformation" => "CloudFormation"
  )

  # turbofan/resources/postgres.rb defines Turbofan::Postgres (a mixin
  # users include into their Resource classes), NOT Turbofan::Resources::
  # Postgres. Tell Zeitwerk the `resources` segment is collapsed so file
  # paths map to the parent namespace: lib/turbofan/resources/FOO.rb →
  # Turbofan::FOO.
  loader.collapse("#{__dir__}/turbofan/resources")

  # errors.rb defines multiple top-level error classes (Error,
  # ConfigError, ValidationError, plus the grouped subclasses) — it does
  # NOT map to a single constant named `Turbofan::Errors`. Since it's
  # loaded manually above to satisfy the other files' forward references,
  # Zeitwerk must be told to ignore it.
  loader.ignore("#{__dir__}/turbofan/errors.rb")

  # runtime.rb and deploy.rb are entry-point files that `require_relative`
  # back into this very file and then call eager_load_dir. They define
  # the Turbofan::Runtime / Turbofan::Deploy namespace modules explicitly
  # (standard Zeitwerk file+directory pattern), so Zeitwerk manages them
  # as normal — no ignore needed.

  # Lambda-bundle files are shipped INTO AWS Lambda zip archives at deploy
  # time; they are not Ruby constants in the gem's autoload tree. Their
  # `require_relative "router"` / `"turbofan_router"` lines only resolve
  # once the zip is deployed — autoloading them here would fail at boot.
  loader.ignore("#{__dir__}/turbofan/generators/cloudformation/chunking_handler.rb")

  # Optional autoload tracing for diagnosing inflector or file-layout
  # surprises. Enable with TURBOFAN_LOADER_LOG=1.
  loader.log! if ENV["TURBOFAN_LOADER_LOG"]

  loader.setup

  # Eager-load gem-internal Resource subclasses so
  # Discovery.subclasses_of(Resource) finds them via ObjectSpace. User
  # Step/Pipeline/Resource classes are loaded via PipelineLoader at a
  # later point (Kernel.load populates ObjectSpace synchronously), so
  # only the built-in resources directory needs eager-loading here.
  loader.eager_load_dir("#{__dir__}/turbofan/resources")

  # Raised on SIGTERM (Spot reclaim / Batch termination). Subclasses
  # SystemExit so `ensure` blocks still run, but the final process exit
  # status is 143 — which AWS Batch's retry strategy matches via
  # `onExitCode: 143 → RETRY` without counting against the retry limit.
  class Interrupted < SystemExit
    # Intentionally public: this constant is an AWS Batch contract
    # (matched by `onExitCode: 143 → RETRY` in the generated CloudFormation
    # retry strategy). External callers and tests legitimately reference
    # it to assert the contract, so it is NOT marked private_constant.
    EXIT_CODE = 143

    def initialize(message = "SIGTERM received")
      super(EXIT_CODE, message)
    end
  end

  def self.discover_components
    steps = {}
    Discovery.subclasses_of(Step).each do |c|
      steps[snake_case(Discovery.class_name_of(c))] = c
    end

    pipelines = {}
    Discovery.subclasses_of(Pipeline).each do |c|
      pipelines[snake_case(Discovery.class_name_of(c))] = c
    end

    resources = {}
    Discovery.subclasses_of(Resource).each do |c|
      resources[c.turbofan_key] = c if c.respond_to?(:turbofan_key) && c.turbofan_key
    end

    {steps: steps, pipelines: pipelines, resources: resources}
  end

  def self.snake_case(class_name)
    class_name
      .split("::")
      .map { |s| s.gsub(/([A-Z]+)([A-Z][a-z])/, '\1_\2').gsub(/([a-z\d])([A-Z])/, '\1_\2').downcase }
      .join("_")
      .to_sym
  end
end
