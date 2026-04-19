# frozen_string_literal: true

module Turbofan
  # Deprecation-warning emitter with two operational properties Matz and
  # Mike both asked for:
  #
  #   1. Quiet by default — only emits when $VERBOSE is true OR
  #      Turbofan.config.deprecations is truthy. A gem that dumps a
  #      deprecation warning on every class load is hostile to operators
  #      running at scale (10k-child fan-outs would drown the logs).
  #
  #   2. Memoized once per (class, key) pair so the same warning never
  #      prints twice for the same user class. A user running rspec with
  #      100 step classes using the deprecated API would otherwise see
  #      100 identical warnings.
  #
  # Usage:
  #
  #   Turbofan::Deprecations.warn_once(
  #     self,                         # the class raising the warning
  #     :uses_extensions_kwarg,       # stable key for this deprecation site
  #     "uses(:duckdb, extensions:) is deprecated; use block form."
  #   )
  module Deprecations
    @seen_mutex = Mutex.new
    @seen = {} # {class_name => Set of keys}

    def self.warn_once(context, key, message)
      return unless emit?

      class_name = if context.is_a?(Module)
        Turbofan::Discovery.class_name_of(context) || context.to_s
      else
        context.class.to_s
      end

      @seen_mutex.synchronize do
        seen_for_class = (@seen[class_name] ||= [])
        return if seen_for_class.include?(key)
        seen_for_class << key
      end

      Kernel.warn("[Turbofan Deprecation] #{class_name}: #{message}")
    end

    # Test-only reset hook so per-example state doesn't leak.
    def self.reset_seen!
      @seen_mutex.synchronize { @seen.clear }
    end

    def self.emit?
      return true if $VERBOSE
      Turbofan.config.respond_to?(:deprecations) && Turbofan.config.deprecations
    end
    private_class_method :emit?
  end
end
