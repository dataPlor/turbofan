require_relative "turbofan/configuration"
require_relative "turbofan/discovery"
require_relative "turbofan/extensions"
require_relative "turbofan/version"
require_relative "turbofan/naming"
require_relative "turbofan/retryable"
require_relative "turbofan/subprocess"
require_relative "turbofan/instance_catalog"
require_relative "turbofan/instance_selector"
require_relative "turbofan/step"
require_relative "turbofan/dag"
require_relative "turbofan/pipeline"
require_relative "turbofan/compute_environment"
require_relative "turbofan/resource"
require_relative "turbofan/resources/postgres"
require_relative "turbofan/router"
require_relative "turbofan/s3_uri"
require_relative "turbofan/check/result"
require_relative "turbofan/check/dag_check"
require_relative "turbofan/check/router_check"
require_relative "turbofan/check/instance_check"
require_relative "turbofan/check/pipeline_check"
require_relative "turbofan/check/resource_check"
require_relative "turbofan/generators/cloudformation"
require_relative "turbofan/generators/asl"
require_relative "turbofan/runtime/logger"
require_relative "turbofan/runtime/metrics"
require_relative "turbofan/runtime/context"
require_relative "turbofan/runtime/payload"
require_relative "turbofan/runtime/input_resolver"
require_relative "turbofan/runtime/output_serializer"
require_relative "turbofan/runtime/schema_validator"
require_relative "turbofan/runtime/step_metrics"
require_relative "turbofan/runtime/wrapper"
require_relative "turbofan/runtime/resource_attacher"
require_relative "turbofan/runtime/fan_out"
require_relative "turbofan/runtime/lineage"
require_relative "turbofan/observability/insights_query"
require_relative "turbofan/cli"
require_relative "turbofan/deploy/pipeline_loader"
require_relative "turbofan/deploy/image_builder"
require_relative "turbofan/deploy/stack_manager"
require_relative "turbofan/deploy/execution"
require_relative "turbofan/deploy/pipeline_context"
require_relative "turbofan/status"

module Turbofan
  class SchemaIncompatibleError < StandardError; end
  class SchemaValidationError < StandardError; end
  class ResourceUnavailableError < StandardError; end
  class ExtensionLoadError < StandardError; end

  # Raised on SIGTERM (Spot reclaim / Batch termination). Subclasses
  # SystemExit so `ensure` blocks still run, but the final process exit
  # status is 143 — which AWS Batch's retry strategy matches via
  # `onExitCode: 143 → RETRY` without counting against the retry limit.
  class Interrupted < SystemExit
    EXIT_CODE = 143

    def initialize(message = "SIGTERM received")
      super(EXIT_CODE, message)
    end
  end

  def self.schemas_path
    config.schemas_path
  end

  def self.schemas_path=(path)
    config.schemas_path = path
  end

  GET_CLASS_NAME = Module.instance_method(:name)

  def self.discover_components
    steps = {}
    Discovery.subclasses_of(Step).each do |c|
      steps[snake_case(GET_CLASS_NAME.bind_call(c))] = c
    end

    pipelines = {}
    Discovery.subclasses_of(Pipeline).each do |c|
      pipelines[snake_case(GET_CLASS_NAME.bind_call(c))] = c
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
