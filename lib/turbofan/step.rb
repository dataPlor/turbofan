require "json"

module Turbofan
  module Step
    def self.included(base)
      base.extend(ClassMethods)
      base.instance_variable_set(:@turbofan_uses, [])
      base.instance_variable_set(:@turbofan_writes_to, [])
      base.instance_variable_set(:@turbofan_secrets, [])
      base.instance_variable_set(:@turbofan_sizes, {})
      base.instance_variable_set(:@turbofan_batch_size, 1)
      base.instance_variable_set(:@turbofan_timeout, nil)
      base.instance_variable_set(:@turbofan_retries, 3)
      base.instance_variable_set(:@turbofan_retry_on, nil)
      base.instance_variable_set(:@turbofan_default_cpu, nil)
      base.instance_variable_set(:@turbofan_default_ram, nil)
      base.instance_variable_set(:@turbofan_compute_environment, nil)
      base.instance_variable_set(:@turbofan_tags, {})
      base.instance_variable_set(:@turbofan_execution, nil)
      base.instance_variable_set(:@turbofan_docker_image, nil)
      base.instance_variable_set(:@turbofan_duckdb_extensions, [])
      base.instance_variable_set(:@turbofan_subnets, nil)
      base.instance_variable_set(:@turbofan_security_groups, nil)
      base.instance_variable_set(:@turbofan_storage, nil)
    end

    module ClassMethods
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

      def input_schema(filename)
        @turbofan_input_schema_file = filename
      end

      def output_schema(filename)
        @turbofan_output_schema_file = filename
      end

      def turbofan_input_schema
        return nil unless @turbofan_input_schema_file
        @turbofan_input_schema_parsed ||= load_schema(@turbofan_input_schema_file)
      end

      def turbofan_output_schema
        return nil unless @turbofan_output_schema_file
        @turbofan_output_schema_parsed ||= load_schema(@turbofan_output_schema_file)
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

      def execution(model)
        unless VALID_EXECUTION_MODELS.include?(model)
          raise ArgumentError, "execution must be one of #{VALID_EXECUTION_MODELS.inspect}, got #{model.inspect}"
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

      def uses(target, extensions: nil)
        dep = parse_dependency(target)
        @turbofan_uses << dep unless @turbofan_uses.include?(dep)
        if extensions
          raise ArgumentError, "extensions: is only supported for :duckdb" unless target == :duckdb
          Array(extensions).each do |ext|
            ext = ext.to_sym
            unless ext.to_s.match?(/\A[a-z][a-z0-9_]*\z/)
              raise ArgumentError, "invalid extension name: #{ext.inspect}"
            end
            @turbofan_duckdb_extensions << ext unless @turbofan_duckdb_extensions.include?(ext)
          end
        end
      end

      alias_method :reads_from, :uses

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
          raise ArgumentError, "subnets is only valid for execution :fargate steps (this step uses :#{@turbofan_execution})"
        end
        @turbofan_subnets = Array(value)
      end

      def security_groups(value)
        if @turbofan_execution && @turbofan_execution != :fargate
          raise ArgumentError, "security_groups is only valid for execution :fargate steps (this step uses :#{@turbofan_execution})"
        end
        @turbofan_security_groups = Array(value)
      end

      def storage(value)
        if @turbofan_execution && @turbofan_execution != :fargate
          raise ArgumentError, "storage is only valid for execution :fargate steps (this step uses :#{@turbofan_execution})"
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

      # Resource keys from both uses and writes_to (symbols only, no S3)
      def turbofan_resource_keys
        @turbofan_resource_keys ||= (uses_resources + writes_to_resources).map { |d| d[:key] }.uniq.freeze
      end

      # Whether DuckDB is needed (explicit :duckdb OR any resource key OR extensions)
      def turbofan_needs_duckdb?
        turbofan_resource_keys.any? || @turbofan_duckdb_extensions.any?
      end

      # Filter helpers
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

      private

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
        raise "No schemas_path configured" unless Turbofan.schemas_path
        path = File.join(Turbofan.schemas_path, filename)
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
