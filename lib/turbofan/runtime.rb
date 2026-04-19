# frozen_string_literal: true

# Runtime-only entry point for containerized workers. Use this in your
# Dockerfile/worker.rb to avoid loading the deploy-side AWS SDK gems
# (cloudformation, batch, ec2, ecr, states, sts, ecs, cloudwatchlogs)
# that are only needed for `turbofan deploy` / `turbofan destroy` / CLI.
#
#   require "turbofan/runtime"
#
# Zeitwerk's lazy autoload does the heavy lifting: as long as the
# container's worker.rb never references deploy/CLI constants, those
# files and their AWS SDK requires never load.
#
# What this loads eagerly:
# * Turbofan base (Zeitwerk setup, errors, configuration, naming,
#   retryable, subprocess, compute_environment, resource, resources/*,
#   router, s3_uri, check/*, observability/*)
# * Turbofan::Step and Turbofan::Pipeline DSL
# * All Turbofan::Runtime::* (wrapper, context, metrics, logger, payload,
#   fan_out, input_resolver, output_serializer, schema_validator,
#   step_metrics, resource_attacher, lineage, lambda_handler)
#
# What this does NOT load:
# * Turbofan::CLI::* (use `require "turbofan/cli"` if you need the CLI)
# * Turbofan::Deploy::* and Turbofan::Generators::*
# * aws-sdk-cloudformation, aws-sdk-batch, aws-sdk-ec2, aws-sdk-ecr,
#   aws-sdk-states, aws-sdk-sts, aws-sdk-ecs, aws-sdk-cloudwatchlogs

# Signals to other parts of the gem that we're in runtime-only mode.
# If the worker accidentally references Turbofan::CLI or a Deploy::*
# constant, Zeitwerk would silently autoload those files + their
# deploy-side aws-sdk-* gems — introducing a surprise latency spike on
# whatever worker touched them. The tripwire in cli.rb / deploy/*
# catches this early so the violation surfaces at load-time (loud)
# rather than at the first fan-out (quiet but costly).
ENV["TURBOFAN_RUNTIME_ONLY"] ||= "1"

require_relative "../turbofan"

module Turbofan
  module Runtime
    # Zeitwerk autoloads the contents of lib/turbofan/runtime/ — this
    # module declaration just ensures the namespace exists so the
    # eager_load_dir below can do its job. Keeping the file name
    # matched to `Turbofan::Runtime` lets Zeitwerk treat it as the
    # canonical namespace-owning file (the file+directory pattern).
  end
end

# Eagerly load every runtime file so container boot surfaces any
# file-naming drift immediately (rather than at the first fan-out).
Turbofan.loader.eager_load_dir("#{__dir__}/runtime")
