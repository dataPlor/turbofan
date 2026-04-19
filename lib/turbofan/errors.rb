# frozen_string_literal: true

# ⚠ DO NOT CREATE A lib/turbofan/errors/ SUBDIRECTORY.
#
# This file is explicitly `loader.ignore`d by Zeitwerk in lib/turbofan.rb
# because it defines multiple top-level error classes (Error, ConfigError,
# ValidationError, SchemaIncompatibleError, etc.) rather than a single
# Turbofan::Errors constant. That design is intentional — operators need
# a flat, rescuable hierarchy in one place.
#
# If you add a lib/turbofan/errors/ subdirectory, Zeitwerk will demand
# a Turbofan::Errors parent module to house its contents (Zeitwerk's
# file+directory namespace pattern). But there is no Turbofan::Errors —
# we have Turbofan::Error (singular). The loader will raise at
# `eager_load(force: true)` during CI's spec_helper or during any first
# cold boot. New error classes belong in this file, not a subdirectory.
# (Flagged by Xavier Noria during the 0.6.0 final review.)

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

  # Raised by Retryable.call when the accumulated sleep time across
  # retries exceeds Turbofan.config.max_retry_seconds. Distinct from a
  # retry-exhausted-by-attempt-count case (which re-raises the original
  # transient error) because "we ran out of budget" is a different
  # operational signal than "we attempted 5 times and all 5 failed."
  class RetryBudgetExhausted < Error
    attr_reader :elapsed_seconds, :budget_seconds, :last_error

    def initialize(elapsed_seconds:, budget_seconds:, last_error:)
      @elapsed_seconds = elapsed_seconds
      @budget_seconds = budget_seconds
      @last_error = last_error
      super("Retry budget exhausted: slept #{elapsed_seconds.round(2)}s, " \
            "budget #{budget_seconds}s. Last error: #{last_error.class}: #{last_error.message}")
      set_backtrace(last_error.backtrace) if last_error.respond_to?(:backtrace) && last_error.backtrace
    end
  end
end
