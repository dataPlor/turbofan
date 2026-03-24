require "uri"
require "aws-sdk-secretsmanager"

module Turbofan
  module Runtime
    module ResourceAttacher
      def self.attach(context:)
        reads = context.uses_resources.reject { |d| d[:key] == :duckdb }
        writes = context.writes_to_resources.reject { |d| d[:key] == :duckdb }

        # Merge: same key in both reads and writes -> read_write wins
        merged = {}
        reads.each { |d| merged[d[:key]] ||= :read_only }
        writes.each { |d| merged[d[:key]] = :read_write }

        return if merged.empty?

        unless context.duckdb
          raise Turbofan::ResourceUnavailableError,
            "Resources #{merged.keys.inspect} are declared but DuckDB is not available. " \
            "Ensure the duckdb gem is installed and the step's environment includes it."
        end

        duckdb = context.duckdb
        resources = Turbofan.discover_components[:resources]

        postgres_loaded = false

        merged.each do |key, mode|
          resource = resources[key]
          raise Turbofan::ResourceUnavailableError, "Resource :#{key} not found. Discovered resources: #{resources.keys.inspect}" unless resource
          next unless resource.respond_to?(:turbofan_secret) && resource.turbofan_secret

          secret_response = context.secrets_client.get_secret_value(secret_id: resource.turbofan_secret)
          connection_string = secret_response.secret_string
          # Normalize non-standard URI schemes (e.g. postgis://) to postgresql://
          connection_string = connection_string.sub(%r{\A(postgis|postgres)://}, "postgresql://")
          # Append database name if missing from URL and resource specifies one
          if resource.respond_to?(:turbofan_database) && resource.turbofan_database
            uri = URI.parse(connection_string)
            if uri.path.nil? || uri.path == "" || uri.path == "/"
              uri.path = "/#{resource.turbofan_database}"
              connection_string = uri.to_s
            end
          end

          resource_type = resource.respond_to?(:turbofan_resource_type) ? resource.turbofan_resource_type : nil

          case resource_type
          when :postgres
            unless postgres_loaded
              duckdb.execute("LOAD postgres")
              postgres_loaded = true
            end
            safe_conn = connection_string.gsub("'", "''")
            safe_key = key.to_s.gsub('"', '""')
            raise Turbofan::ResourceUnavailableError, "Invalid resource key: #{key}" unless key.to_s.match?(/\A[a-z_][a-z0-9_]*\z/)
            attach_opts = (mode == :read_only) ? ", READ_ONLY" : ""
            duckdb.execute("ATTACH '#{safe_conn}' AS \"#{safe_key}\" (TYPE POSTGRES#{attach_opts})")
          else
            raise Turbofan::ResourceUnavailableError, "Unknown resource type: #{resource_type.inspect} for resource :#{key}. Only :postgres resources are supported."
          end
        end
      end
    end
  end
end
