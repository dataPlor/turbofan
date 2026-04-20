# frozen_string_literal: true

module Turbofan
  # User step classes `include Turbofan::Step` to get the DSL macros,
  # per-class state, and the `.turbofan` façade. The module itself is
  # intentionally tiny — the DSL lives in Step::ClassMethods, the
  # public seam in Step::ConfigFacade, and shared validation in
  # Step::Validators. Zeitwerk autoloads the split files.
  module Step
    # Per-class DSL state defaults. Frozen so the constant itself can't
    # be mutated; .dup'd per-class in initializers so each Step subclass
    # gets an independent mutable copy of the container values (Arrays,
    # Hashes). Shallow-dup is sufficient because the containers start
    # empty — if a default ever contained a nested collection, it would
    # need Marshal.load(Marshal.dump(DEFAULT)) to avoid aliasing.
    DEFAULT_STATE = {
      turbofan_uses: [],
      turbofan_writes_to: [],
      turbofan_secrets: [],
      turbofan_sizes: {},
      turbofan_batch_size: 1,
      turbofan_timeout: nil,
      turbofan_retries: 3,
      turbofan_retry_on: nil,
      turbofan_default_cpu: nil,
      turbofan_default_ram: nil,
      turbofan_compute_environment: nil,
      turbofan_tags: {},
      turbofan_execution: nil,
      turbofan_docker_image: nil,
      turbofan_duckdb_extensions: [],
      turbofan_subnets: nil,
      turbofan_security_groups: nil,
      turbofan_storage: nil
    }.freeze
    private_constant :DEFAULT_STATE

    def self.init_state(klass)
      DEFAULT_STATE.each do |key, value|
        # dup only containers (Arrays, Hashes); immutable defaults (nil,
        # Integer, Symbol) return themselves on .dup on modern Ruby.
        klass.instance_variable_set(:"@#{key}", value.dup)
      end
    end

    def self.included(base)
      base.extend(ClassMethods)
      init_state(base)
      Turbofan::Discovery.reset_cache!
    end
  end
end
