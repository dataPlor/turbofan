require "json"
require "fileutils"
require "json_schemer"

module Turbofan
  module Runtime
    class Wrapper
      def self.run(step_class)
        new(step_class).run
      end

      def initialize(step_class)
        @step_class = step_class
      end

      def run
        $stdout.sync = true
        Turbofan.schemas_path ||= ENV["TURBOFAN_SCHEMAS_PATH"]
        storage_path = setup_storage
        set_tmpdir(storage_path) if storage_path
        context = build_context(storage_path)
        install_sigterm_handler(context)
        attach_resources(context)
        Lineage.emit(Lineage.start_event(context: context, step_class: @step_class), context: context)

        start_time = Process.clock_gettime(Process::CLOCK_MONOTONIC)
        envelope = InputResolver.call(context)
        metadata = envelope.except("inputs")
        context.instance_variable_set(:@envelope, metadata)
        inputs = envelope["inputs"]

        # Sentinel chunk from padding (Batch minimum array size 2).
        # The chunking lambda pads with null; exit cleanly, no work to do.
        if inputs == [nil]
          context.logger.info("Sentinel chunk, no work")
          Lineage.emit(Lineage.complete_event(context: context, step_class: @step_class), context: context)
          return
        end

        SchemaValidator.validate_input!(@step_class, inputs)
        result = @step_class.new.call(inputs, context)
        SchemaValidator.validate_output!(@step_class, result)
        duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time

        output = OutputSerializer.call(result, context)
        StepMetrics.emit_success(context, @step_class, duration)
        Lineage.emit(Lineage.complete_event(context: context, step_class: @step_class), context: context)
        $stdout.puts(output)
      rescue Turbofan::Interrupted => e
        # SIGTERM-driven cooperative shutdown (e.g., Spot reclaim). Not a
        # user-code failure — log at info level and skip failure metrics /
        # Lineage fail_event. Re-raise so `ensure` runs and SystemExit
        # propagates with exit status 143.
        context&.logger&.info("Interrupted by signal", reason: e.message)
        raise
      rescue => e
        context&.logger&.error("Step failed", error_class: e.class.name, error_message: e.message)
        begin
          Lineage.emit(Lineage.fail_event(context: context, step_class: @step_class, error: e), context: context) if context
          StepMetrics.emit_failure(context) if context
        rescue => metrics_err
          warn("[Turbofan] WARNING: Failed to emit failure metrics: #{metrics_err.message}")
        end
        raise
      ensure
        cleanup_storage(storage_path)
        begin
          context&.metrics&.flush
        rescue => flush_err
          warn("[Turbofan] WARNING: Failed to flush metrics: #{flush_err.message}")
        end
      end

      private

      def set_tmpdir(storage_path)
        tmp_dir = File.join(storage_path, "tmp")
        FileUtils.mkdir_p(tmp_dir)
        ENV["TMPDIR"] = tmp_dir
      end

      def setup_storage
        mount = Turbofan::ComputeEnvironment::NVME_MOUNT_PATH
        job_id = ENV["AWS_BATCH_JOB_ID"] || "local-#{Process.pid}"
        attempt = ENV.fetch("AWS_BATCH_JOB_ATTEMPT", "1")

        if File.directory?(mount)
          # Batch with NVMe instance storage
          path = "#{mount}/#{job_id}-attempt#{attempt}"
          FileUtils.mkdir_p(path)
          ENV["TURBOFAN_STORAGE_PATH"] = path
          df = `df -h #{mount} 2>/dev/null`.lines.last&.strip
          warn("[Turbofan] Storage: NVMe at #{path} (#{df})")
          path
        elsif ENV.key?("ECS_CONTAINER_METADATA_URI_V4")
          # Fargate ephemeral storage
          path = "/tmp/turbofan-#{job_id}-attempt#{attempt}"
          FileUtils.mkdir_p(path)
          ENV["TURBOFAN_STORAGE_PATH"] = path
          warn("[Turbofan] Storage: Fargate ephemeral at #{path}")
          path
        else
          warn("[Turbofan] No local storage detected — file-backed DuckDB will use TMPDIR")
          nil
        end
      end

      def build_context(storage_path)
        Context.new(
          execution_id: ENV.fetch("TURBOFAN_EXECUTION_ID", "local-#{Process.pid}"),
          attempt_number: ENV.fetch("AWS_BATCH_JOB_ATTEMPT", "1").to_i,
          step_name: ENV.fetch("TURBOFAN_STEP_NAME") { @step_class.name ? Turbofan.snake_case(@step_class.name).to_s : "anonymous" },
          stage: ENV.fetch("TURBOFAN_STAGE", "development"),
          pipeline_name: ENV.fetch("TURBOFAN_PIPELINE", "unknown"),
          array_index: ENV.key?("AWS_BATCH_JOB_ARRAY_INDEX") ? ENV["AWS_BATCH_JOB_ARRAY_INDEX"].to_i : nil,
          storage_path: storage_path,
          uses: @step_class.turbofan_uses,
          writes_to: @step_class.turbofan_writes_to,
          size: ENV["TURBOFAN_SIZE"],
          duckdb_extensions: @step_class.turbofan_duckdb_extensions
        )
      end

      # Install a minimal SIGTERM trap. Ruby forbids almost everything useful
      # in trap context (no Mutex, no logging IO, and doing heavy work risks
      # races with in-flight S3 uploads or DuckDB queries). We do two things:
      #
      #   1. Set the interrupt flag (atomic boolean write — safe in trap).
      #   2. Inject Turbofan::Interrupted onto the main thread via
      #      Thread#raise. This is the same mechanism Ruby's default SIGINT
      #      handler uses to raise `Interrupt`. The exception unwinds through
      #      the step's call stack, reaches Wrapper#run's rescue chain, logs
      #      cleanly, runs `ensure` (storage cleanup + metrics flush), and
      #      exits with code 143 via SystemExit propagation.
      def install_sigterm_handler(context)
        main = Thread.current
        trap("TERM") do
          context.interrupt!
          main.raise(Turbofan::Interrupted.new)
        end
      end

      def attach_resources(context)
        ResourceAttacher.attach(context: context)
      end

      def cleanup_storage(path)
        FileUtils.rm_rf(path) if path && File.directory?(path)
      end
    end
  end
end
