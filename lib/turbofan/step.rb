# frozen_string_literal: true

require "json"

module Turbofan
  module Step
    # Block-form receiver for `uses :duckdb do ... end`. Delegates the
    # single meaningful verb (extensions) back to the owning step class
    # so the public side of the DSL doesn't leak a private receiver type.
    class UsesDuckdbDSL
      def initialize(step_class)
        @step_class = step_class
      end

      def extensions(*names)
        @step_class.send(:add_duckdb_extensions, names)
      end
    end
    private_constant :UsesDuckdbDSL

    # Read-only façade exposing a step's declared DSL state through a
    # single public seam. Replaces the 20+ `turbofan_*` attr_readers
    # that previously polluted each user Step class's public API.
    #
    # Use:
    #
    #   class MyStep
    #     include Turbofan::Step
    #     runs_on :batch
    #     uses :postgres
    #   end
    #
    #   MyStep.turbofan.uses         # => [{type: :resource, key: :postgres}]
    #   MyStep.turbofan.execution    # => :batch
    #   MyStep.turbofan.inspect      # walks all fields — useful in pry/irb
    #
    # The legacy readers (MyStep.turbofan_uses, etc.) still exist through
    # 0.6.x as direct attr_readers and continue to work unchanged — this
    # façade is purely additive for 0.6. The legacy readers are slated
    # for removal in 1.0 per CHANGELOG [Unreleased].
    class ConfigFacade
      FIELDS = %i[
        uses writes_to secrets sizes batch_size execution timeout
        retries retry_on default_cpu default_ram compute_environment
        input_schema_file output_schema_file tags docker_image
        duckdb_extensions subnets security_groups storage
      ].freeze

      def initialize(step_class)
        @step_class = step_class
      end

      FIELDS.each do |field|
        define_method(field) do
          @step_class.instance_variable_get(:"@turbofan_#{field}")
        end
      end

      # Computed readers that forward to existing class methods so
      # callers see the same parsed/memoized values as the legacy API.
      def input_schema = @step_class.turbofan_input_schema
      def output_schema = @step_class.turbofan_output_schema
      def resource_keys = @step_class.turbofan_resource_keys
      def needs_duckdb? = @step_class.turbofan_needs_duckdb?
      def lambda? = @step_class.turbofan_lambda?
      def fargate? = @step_class.turbofan_fargate?
      def external? = @step_class.turbofan_external?

      # S3 dependency filters — previously public-by-accident on
      # Step::ClassMethods (Jeremy Evans's audit flag), now routed
      # exclusively through the façade. The private-on-ClassMethods
      # versions still exist and this delegates to them via send so
      # privatization doesn't break internal behavior.
      def uses_s3 = @step_class.send(:uses_s3)
      def writes_to_s3 = @step_class.send(:writes_to_s3)

      # Per-size batch_size resolution. Accepts a size symbol (matching
      # a `size` macro declaration) or nil. Returns per-size override
      # when present, else the step-level default.
      def batch_size_for(size_name)
        per_size = @step_class.instance_variable_get(:@turbofan_sizes).dig(size_name, :batch_size)
        per_size || @step_class.instance_variable_get(:@turbofan_batch_size)
      end

      # Network placement resolution — falls back to Turbofan.config
      # defaults when the step hasn't overridden.
      def resolved_subnets
        @step_class.instance_variable_get(:@turbofan_subnets) || Turbofan.config.subnets
      end

      def resolved_security_groups
        @step_class.instance_variable_get(:@turbofan_security_groups) || Turbofan.config.security_groups
      end

      def inspect
        attrs = FIELDS.map { |f| "#{f}=#{public_send(f).inspect}" }.join(" ")
        "#<Turbofan::Step::ConfigFacade #{Turbofan::Discovery.class_name_of(@step_class)} #{attrs}>"
      end
    end

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
    end

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
      # Preferred over the legacy `turbofan_*` attr_readers, which are
      # slated for removal in 1.0.
      def turbofan
        @turbofan_facade ||= ConfigFacade.new(self)
      end

      attr_reader :turbofan_uses, :turbofan_writes_to,
        :turbofan_secrets, :turbofan_sizes, :turbofan_batch_size,
        :turbofan_execution, :turbofan_timeout,
        :turbofan_retries, :turbofan_retry_on, :turbofan_default_cpu,
        :turbofan_default_ram,
        :turbofan_compute_environment,
        :turbofan_input_schema_file, :turbofan_output_schema_file,
        :turbofan_tags, :turbofan_docker_image,
        :turbofan_duckdb_extensions,
        :turbofan_subnets, :turbofan_security_groups, :turbofan_storage

      # Declare the step's input JSON schema. Accepts three shapes
      # (Matz's "the name should agree with what it takes" critique —
      # honest by broadening acceptance rather than renaming):
      #
      #   input_schema "hello_input.json"   # String: filename under
      #                                     # Turbofan.config.schemas_path
      #   input_schema({type: "object", ...}) # Hash: literal schema
      #   input_schema HelloInputSchema       # Class/Module responding
      #                                       # to .schema returning a Hash
      #
      # The filename path is the original behavior and remains
      # unchanged. Hash and Class paths are additive in 0.6.1.
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
        @turbofan_input_schema_parsed = load_schema(@turbofan_input_schema_file)
      end

      def turbofan_output_schema
        return @turbofan_output_schema_parsed if @turbofan_output_schema_parsed
        return nil unless @turbofan_output_schema_file
        @turbofan_output_schema_parsed = load_schema(@turbofan_output_schema_file)
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
        validate_positive!(:cpu, value)
        @turbofan_default_cpu = value
      end

      def ram(value)
        validate_positive!(:ram, value)
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
        validate_positive!(:cpu, cpu) if cpu
        validate_positive!(:ram, ram) if ram
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
        dep = parse_dependency(target)
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
        dep = parse_dependency(target)
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

      # :duckdb is a reserved/built-in resource key. DuckDB is automatically
      # available when any resource key is declared (see BUILT_IN_RESOURCES in
      # Check::ResourceCheck). Declaring `uses :duckdb` is accepted but
      # unnecessary — DuckDB availability is inferred from the presence of
      # other resource keys.
      def parse_dependency(target)
        case target
        when Symbol
          unless target.to_s.match?(/\A[a-z_][a-z0-9_]*\z/)
            raise ArgumentError, "resource key must be a valid identifier (lowercase, underscores), got #{target.inspect}"
          end
          {type: :resource, key: target}
        when String
          unless target.start_with?("s3://")
            raise ArgumentError, "string arguments must be S3 URIs (s3://...), got #{target.inspect}"
          end
          {type: :s3, uri: target}
        else
          raise ArgumentError, "expected a Symbol (resource key) or S3 URI string, got #{target.inspect}"
        end
      end

      def load_schema(filename)
        raise "No schemas_path configured" unless Turbofan.config.schemas_path
        path = File.join(Turbofan.config.schemas_path, filename)
        JSON.parse(File.read(path))
      end

      def validate_positive!(name, value)
        unless value.is_a?(Numeric) && value > 0
          raise ArgumentError, "#{name} must be a positive number, got #{value.inspect}"
        end
      end
    end
  end
end
