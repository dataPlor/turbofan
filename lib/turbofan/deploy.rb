# frozen_string_literal: true

# Deploy-side entry point. Loads the full gem plus the Deploy::* and
# Generators::* modules, and forces their AWS SDK dependencies to
# resolve so a CI job that only builds CloudFormation can rely on them
# being present without a full gem boot.
#
#   require "turbofan/deploy"
#
# For the full gem including CLI, use `require "turbofan"`.

require_relative "../turbofan"

module Turbofan
  module Deploy
    # Zeitwerk autoloads the contents of lib/turbofan/deploy/. Module
    # declaration preserves the file+directory pattern convention.
  end
end

Turbofan.loader.eager_load_dir("#{__dir__}/deploy")
Turbofan.loader.eager_load_dir("#{__dir__}/generators")
