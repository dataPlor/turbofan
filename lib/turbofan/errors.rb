# frozen_string_literal: true

module Turbofan
  # Base class for every error the gem raises. Lives in its own file so
  # it can be required before any other lib/turbofan/** file — those
  # files reparent their domain-specific errors under Turbofan::Error or
  # one of its mid-level groupings (ConfigError, ValidationError).
  #
  # Turbofan::Interrupted is NOT a Turbofan::Error — it's a SystemExit
  # subclass by design so `ensure` runs but the process exits with 143
  # for AWS Batch's retry strategy.
  class Error < StandardError; end

  # Umbrella for configuration/discovery failures: missing resources,
  # extension load problems, invalid DSL state, missing AWS account id.
  class ConfigError < Error; end

  # Umbrella for validation failures at DAG-build / schema-check time.
  class ValidationError < Error; end

  # Specific validation failures.
  class SchemaIncompatibleError < ValidationError; end
  class SchemaValidationError < ValidationError; end

  # Specific configuration failures.
  class ResourceUnavailableError < ConfigError; end
  class ExtensionLoadError < ConfigError; end
end
