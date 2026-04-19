# frozen_string_literal: true

require "turbofan"
require_relative "worker"

Turbofan::Runtime::Wrapper.run(BuildItems)
