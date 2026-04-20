# frozen_string_literal: true

module Turbofan
  module Step
    # All of the Step DSL macros (`uses`, `writes_to`, `runs_on`, `cpu`,
    # `ram`, `tags`, `size`, etc.) plus the computed readers the façade
    # forwards to (`turbofan_input_schema`, `turbofan_external?`, etc.).
    #
    # Mixed into each Step subclass via Turbofan::Step.included, which
    # also seeds per-class DSL state from DEFAULT_STATE.
    module ClassMethods
      # Re-initialize DSL state for each subclass. Without this, `class B < A`
      # where A already includes Step inherits A's class-ivar lookups (nil for
      # B) and the first DSL macro (e.g. B.uses :foo) would NoMethodError on
      # `@turbofan_uses << dep`. Call super so we don't break inheritance
      # hooks installed by downstream gems (ActiveSupport, dry-rb, etc.).
      def inherited(subclass)
        super
        Turbofan::Step.init_state(subclass)
      end

      # Returns a read-only façade exposing this step's DSL state.
      # Replaces the 20+ `turbofan_*` attr_readers removed in 0.7 — all
      # DSL state is reached through `.turbofan.<field>`.
      def turbofan
        @turbofan_facade ||= ConfigFacade.new(self)
      end

      # Declare the step's input JSON schema. Accepts three shapes
      # (Matz's "the name should agree with what it takes" critique —
      # honest by broadening acceptance rather than renaming):
      #
      #   input_schema "hello_input.json"   # String: filename under
      #                                     # Turbofan.config.schemas_path
      #   input_schema({type: "object", ...}) # Hash: literal schema
      #   input_schema HelloInputSchema       # Class/Module responding
      #                                       # to .schema returning a Hash
      def input_schema(schema)
        @turbofan_input_schema_file = nil
        @turbofan_input_schema_parsed = nil
        case schema
        when String
          @turbofan_input_schema_file = schema
        when Hash
          @turbofan_input_schema_parsed = schema
        else
          raise ArgumentError, "input_schema expects a filename String, a Hash, or a Class/Module responding to .schema; got #{schema.class}" unless schema.respond_to?(:schema)
          resolved = schema.schema
          raise ArgumentError, "#{schema}.schema must return a Hash, got #{resolved.class}" unless resolved.is_a?(Hash)
          @turbofan_input_schema_parsed = resolved
        end
      end

      def output_schema(schema)
        @turbofan_output_schema_file = nil
        @turbofan_output_schema_parsed = nil
        case schema
        when String
          @turbofan_output_schema_file = schema
        when Hash
          @turbofan_output_schema_parsed = schema
        else
          raise ArgumentError, "output_schema expects a filename String, a Hash, or a Class/Module responding to .schema; got #{schema.class}" unless schema.respond_to?(:schema)
          resolved = schema.schema
          raise ArgumentError, "#{schema}.schema must return a Hash, got #{resolved.class}" unless resolved.is_a?(Hash)
          @turbofan_output_schema_parsed = resolved
        end
      end

      def turbofan_input_schema
        return @turbofan_input_schema_parsed if @turbofan_input_schema_parsed
        return nil unless @turbofan_input_schema_file
        @turbofan_input_schema_parsed = Validators.load_schema(@turbofan_input_schema_file)
      end

      def turbofan_output_schema
        return @turbofan_output_schema_parsed if @turbofan_output_schema_parsed
        return nil unless @turbofan_output_schema_file
        @turbofan_output_schema_parsed = Validators.load_schema(@turbofan_output_schema_file)
      end

      def tags(hash)
        hash.each_key do |k|
          raise ArgumentError, "Tag key '#{k}' uses reserved 'turbofan:' prefix" if k.to_s.start_with?("turbofan:")
        end
        @turbofan_tags = hash.transform_keys(&:to_s)
      end

      def docker_image(uri)
        @turbofan_docker_image = uri
      end

      def turbofan_external?
        !@turbofan_docker_image.nil? && !@turbofan_docker_image.empty?
      end

      VALID_EXECUTION_MODELS = %i[batch lambda fargate].freeze
      private_constant :VALID_EXECUTION_MODELS

      # Declare where this step runs: `runs_on :batch`, `runs_on :lambda`,
      # or `runs_on :fargate`. Pairs grammatically with
      # `compute_environment :foo` — both are nouns describing the step's
      # runtime environment.
      def runs_on(model)
        unless VALID_EXECUTION_MODELS.include?(model)
          raise ArgumentError, "runs_on must be one of #{VALID_EXECUTION_MODELS.inspect}, got #{model.inspect}"
        end
        @turbofan_execution = model
      end

      def turbofan_lambda?
        @turbofan_execution == :lambda
      end

      def turbofan_fargate?
        @turbofan_execution == :fargate
      end

      def cpu(value)
        Validators.validate_positive!(:cpu, value)
        @turbofan_default_cpu = value
      end

      def ram(value)
        Validators.validate_positive!(:ram, value)
        @turbofan_default_ram = value
      end

      def batch_size(value)
        unless value.is_a?(Integer) && value > 0
          raise ArgumentError, "batch_size must be a positive integer, got #{value.inspect}"
        end
        @turbofan_batch_size = value
      end

      def turbofan_batch_size_for(size_name)
        per_size = @turbofan_sizes.dig(size_name, :batch_size)
        per_size || @turbofan_batch_size
      end

      def size(name, cpu: nil, ram: nil, batch_size: nil)
        Validators.validate_positive!(:cpu, cpu) if cpu
        Validators.validate_positive!(:ram, ram) if ram
        if batch_size
          unless batch_size.is_a?(Integer) && batch_size > 0
            raise ArgumentError, "batch_size must be a positive integer, got #{batch_size.inspect}"
          end
        end
        @turbofan_sizes[name] = {cpu: cpu, ram: ram, batch_size: batch_size}
      end

      # Declare a step-level dependency. Accepts a resource Symbol
      # (`uses :postgres`) or an S3 URI String (`uses "s3://bucket/key"`).
      #
      # For DuckDB extensions, pass a block:
      #
      #   uses :duckdb do
      #     extensions :json, :parquet, :spatial
      #   end
      #
      # Only :duckdb accepts a block. The block form replaced the old
      # `extensions:` kwarg in 0.6.0; kwarg form was removed in 0.7.0.
      def uses(target, &block)
        dep = Validators.parse_dependency(target)
        @turbofan_uses << dep unless @turbofan_uses.include?(dep)

        if block
          raise ArgumentError, "uses block form is only supported for :duckdb" unless target == :duckdb
          UsesDuckdbDSL.new(self).instance_eval(&block)
        end
      end

      alias_method :reads_from, :uses

      # Internal: used by the block form of `uses :duckdb`. Adds each
      # argument to @turbofan_duckdb_extensions with the same validation
      # as the legacy kwarg path.
      def add_duckdb_extensions(names)
        Array(names).each do |ext|
          ext = ext.to_sym
          unless ext.to_s.match?(/\A[a-z][a-z0-9_]*\z/)
            raise ArgumentError, "invalid extension name: #{ext.inspect}"
          end
          @turbofan_duckdb_extensions << ext unless @turbofan_duckdb_extensions.include?(ext)
        end
      end

      def writes_to(target)
        dep = Validators.parse_dependency(target)
        @turbofan_writes_to << dep unless @turbofan_writes_to.include?(dep)
      end

      def timeout(value)
        @turbofan_timeout = value
      end

      def retries(value, on: nil)
        @turbofan_retries = value
        @turbofan_retry_on = Array(on) if on
      end

      def inject_secret(name, from:)
        @turbofan_secrets << {name: name, from: from}
      end

      alias_method :secret, :inject_secret

      def compute_environment(sym)
        raise ArgumentError, "compute_environment must be a Symbol, got #{sym.class}" unless sym.is_a?(Symbol)
        @turbofan_compute_environment = sym
      end

      def subnets(value)
        if @turbofan_execution && @turbofan_execution != :fargate
          raise ArgumentError, "subnets is only valid for runs_on :fargate steps (this step uses :#{@turbofan_execution})"
        end
        @turbofan_subnets = Array(value)
      end

      def security_groups(value)
        if @turbofan_execution && @turbofan_execution != :fargate
          raise ArgumentError, "security_groups is only valid for runs_on :fargate steps (this step uses :#{@turbofan_execution})"
        end
        @turbofan_security_groups = Array(value)
      end

      def storage(value)
        if @turbofan_execution && @turbofan_execution != :fargate
          raise ArgumentError, "storage is only valid for runs_on :fargate steps (this step uses :#{@turbofan_execution})"
        end
        unless value.is_a?(Integer) && value >= 21 && value <= 200
          raise ArgumentError, "storage must be an integer between 21 and 200 (GiB), got #{value.inspect}"
        end
        @turbofan_storage = value
      end

      def resolved_subnets
        @turbofan_subnets || Turbofan.config.subnets
      end

      def resolved_security_groups
        @turbofan_security_groups || Turbofan.config.security_groups
      end

      # Resource keys from both uses and writes_to (symbols only, no S3).
      # Public: documented reader + exposed on the .turbofan façade as
      # `resource_keys`. Multiple internal callers (generators,
      # resource_check, instance_check) depend on this.
      def turbofan_resource_keys
        @turbofan_resource_keys ||= (uses_resources + writes_to_resources).map { |d| d[:key] }.uniq.freeze
      end

      # Public: documented reader + .turbofan.needs_duckdb? façade entry.
      def turbofan_needs_duckdb?
        turbofan_resource_keys.any? || @turbofan_duckdb_extensions.any?
      end

      # add_duckdb_extensions is defined above (public order-of-declaration)
      # but is purely internal — called only by UsesDuckdbDSL and the uses
      # macro's kwarg path. Retroactively privatize.
      private :add_duckdb_extensions

      private

      # The four filter helpers are implementation details of the public
      # DSL / façade. Previously public-by-accident (no external docs, no
      # external lib/ callers except iam.rb's use of uses_s3/writes_to_s3
      # which is now routed through the façade). Made private to tighten
      # the public surface per Jeremy Evans's audit — "publishing an API
      # means declaring it."

      def uses_resources
        @turbofan_uses_resources ||= @turbofan_uses.select { |d| d[:type] == :resource }.freeze
      end

      def writes_to_resources
        @turbofan_writes_to_resources ||= @turbofan_writes_to.select { |d| d[:type] == :resource }.freeze
      end

      def uses_s3
        @turbofan_uses_s3 ||= @turbofan_uses.select { |d| d[:type] == :s3 }.freeze
      end

      def writes_to_s3
        @turbofan_writes_to_s3 ||= @turbofan_writes_to.select { |d| d[:type] == :s3 }.freeze
      end
    end
  end
end
