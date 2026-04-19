# frozen_string_literal: true

require "aws-sdk-s3"
require "aws-sdk-secretsmanager"
require "fileutils"

module Turbofan
  module Runtime
    class Context
      attr_reader :execution_id, :attempt_number, :step_name, :stage,
        :pipeline_name, :array_index, :storage_path, :uses, :writes_to, :size, :envelope,
        :duckdb_extensions

      def initialize(execution_id:, attempt_number:, step_name:, stage:, pipeline_name:, array_index:, storage_path:, uses:, writes_to: [], size: nil, envelope: {}, duckdb_extensions: [])
        @execution_id = execution_id
        @attempt_number = attempt_number
        @step_name = step_name
        @stage = stage
        @pipeline_name = pipeline_name
        @array_index = array_index
        @storage_path = storage_path
        @uses = uses
        @writes_to = writes_to
        @size = size
        @envelope = envelope
        @duckdb_extensions = duckdb_extensions
        @interrupted = false
        @duckdb_mutex = Mutex.new
        # Guards lazy construction of per-Context singletons (logger,
        # metrics, s3 client, etc.) so two threads inside a fan_out
        # block don't race to construct two instances. The Metrics race
        # was the dangerous one: two instances would accept emit() calls
        # on separate @pending arrays and only one would get flushed,
        # silently losing half the datapoints. Flagged by Mike Perham.
        @init_mutex = Mutex.new
      end

      # Double-checked locking: the outer defined?/return is safe lock-free
      # under MRI's GIL (ivar assignment is a single bytecode op, so the
      # ivar is either fully set or undefined — no torn reads). The mutex
      # is only paid on the first call per attribute.
      def uses_resources
        return @uses_resources if defined?(@uses_resources)
        @init_mutex.synchronize do
          return @uses_resources if defined?(@uses_resources)
          @uses_resources = @uses.select { |d| d[:type] == :resource }
        end
      end

      def writes_to_resources
        return @writes_to_resources if defined?(@writes_to_resources)
        @init_mutex.synchronize do
          return @writes_to_resources if defined?(@writes_to_resources)
          @writes_to_resources = @writes_to.select { |d| d[:type] == :resource }
        end
      end

      def logger
        return @logger if defined?(@logger)
        @init_mutex.synchronize do
          return @logger if defined?(@logger)
          @logger = Logger.new(
            execution_id: @execution_id,
            step_name: @step_name,
            stage: @stage,
            pipeline_name: @pipeline_name,
            array_index: @array_index
          )
        end
      end

      def metrics
        return @metrics if defined?(@metrics)
        @init_mutex.synchronize do
          return @metrics if defined?(@metrics)
          @metrics = Metrics.new(
            pipeline_name: @pipeline_name,
            stage: @stage,
            step_name: @step_name,
            size: @size
          )
        end
      end

      # Disable SDK's built-in retry so Turbofan::Retryable owns all retry
      # decisions. Otherwise SDK's default 3-retry stacks on top of our 5,
      # yielding 15 total attempts and obscuring retry telemetry.
      #
      # `max_attempts: 1` + `retry_mode: 'standard'` is the modern idiom.
      # (`retry_limit: 0` is legacy-mode-only; ignored in standard/adaptive.)
      def s3
        return @s3 if defined?(@s3)
        @init_mutex.synchronize do
          return @s3 if defined?(@s3)
          @s3 = Aws::S3::Client.new(retry_mode: "standard", max_attempts: 1)
        end
      end

      def secrets_client
        return @secrets_client if defined?(@secrets_client)
        @init_mutex.synchronize do
          return @secrets_client if defined?(@secrets_client)
          @secrets_client = Aws::SecretsManager::Client.new(retry_mode: "standard", max_attempts: 1)
        end
      end

      def duckdb
        return @duckdb if defined?(@duckdb)
        @duckdb_mutex.synchronize do
          return @duckdb if defined?(@duckdb)
          needs = uses_resources.any? || writes_to_resources.any? || @duckdb_extensions.any?
          return @duckdb = nil unless needs

          begin
            if @storage_path
              db_path = File.join(@storage_path, "duckdb.db")
              tmp_dir = File.join(@storage_path, "tmp")
              FileUtils.mkdir_p(tmp_dir)
              raise "Invalid temp directory path" if tmp_dir.include?("'") && tmp_dir.include?("\\")
              @duckdb = ::DuckDB::Database.open(db_path).connect
              safe_tmp = tmp_dir.gsub("'", "''")
              @duckdb.execute("SET temp_directory='#{safe_tmp}'")
            else
              @duckdb = ::DuckDB::Database.open.connect
            end

            @duckdb_extensions.each do |ext|
              @duckdb.execute("LOAD #{ext}")
            rescue ::DuckDB::Error => e
              raise Turbofan::ExtensionLoadError, "Failed to load DuckDB extension '#{ext}': #{e.message}"
            end

            @duckdb
          rescue
            # Any failure during init (DB open, temp_directory set, extension
            # LOAD) must not leave a partial @duckdb visible to next callers —
            # close the partial connection to release its file handle, then
            # reset so the next `context.duckdb` call retries cleanly.
            begin
              @duckdb&.close
            rescue
              # best-effort close; don't mask the original init failure
            end
            @duckdb = nil
            raise
          end
        end
      rescue NameError, LoadError
        # DuckDB gem not installed — expected in non-DuckDB environments.
        # DuckDB::Error (init failure) intentionally propagates.
        @duckdb = nil
      end

      # `@interrupted` is written by the SIGTERM signal handler and read by
      # step code / framework infrastructure on the main thread. Ruby forbids
      # `Mutex#synchronize` in trap context (raises
      # `ThreadError: can't be called from trap context`), so synchronization
      # is impossible here. We rely on CRuby's GIL, which guarantees atomic
      # read/write of boolean values — torn reads are impossible. Non-CRuby
      # runtimes must provide equivalent guarantees for this to be safe.
      def interrupted?
        @interrupted
      end

      def interrupt!
        @interrupted = true
      end
    end
  end
end
