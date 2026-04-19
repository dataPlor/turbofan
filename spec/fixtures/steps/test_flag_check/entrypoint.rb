# frozen_string_literal: true

require "turbofan/runtime/wrapper"
require_relative "worker"

Turbofan::Runtime::Wrapper.run(TestFlagCheck)
