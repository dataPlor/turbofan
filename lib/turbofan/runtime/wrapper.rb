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
        nvme_path = setup_nvme
        set_tmpdir(nvme_path) if nvme_path
        context = build_context(nvme_path)
        install_sigterm_handler(context, nvme_path: nvme_path)
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

        inputs = inputs.map { |item| item.is_a?(Hash) ? item.reject { |k, _| k.start_with?("__") } : item }
        SchemaValidator.validate_input!(@step_class, inputs)
        result = @step_class.new.call(inputs, context)
        SchemaValidator.validate_output!(@step_class, result)
        duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time

        output = OutputSerializer.call(result, context)
        StepMetrics.emit_success(context, @step_class, duration)
        Lineage.emit(Lineage.complete_event(context: context, step_class: @step_class), context: context)
        $stdout.puts(output)
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
        cleanup_nvme(nvme_path)
        begin
          context&.metrics&.flush
        rescue => flush_err
          warn("[Turbofan] WARNING: Failed to flush metrics: #{flush_err.message}")
        end
      end

      private

      def set_tmpdir(nvme_path)
        tmp_dir = File.join(nvme_path, "tmp")
        FileUtils.mkdir_p(tmp_dir)
        ENV["TMPDIR"] = tmp_dir
      end

      def setup_nvme
        mount = Turbofan::ComputeEnvironment::NVME_MOUNT_PATH
        job_id = ENV["AWS_BATCH_JOB_ID"] || "local-#{Process.pid}"
        attempt = ENV.fetch("AWS_BATCH_JOB_ATTEMPT", "1")
        path = "#{mount}/#{job_id}-attempt#{attempt}"

        if File.directory?(mount)
          FileUtils.mkdir_p(path)
          ENV["TURBOFAN_NVME_PATH"] = path
          df = `df -h #{mount} 2>/dev/null`.lines.last&.strip
          warn("[Turbofan] NVMe available: #{path} (#{df})")
          path
        else
          warn("[Turbofan] NVMe not available at #{mount} — file-backed DuckDB will use TMPDIR")
          nil
        end
      end

      def build_context(nvme_path)
        Context.new(
          execution_id: ENV.fetch("TURBOFAN_EXECUTION_ID", "local-#{Process.pid}"),
          attempt_number: ENV.fetch("AWS_BATCH_JOB_ATTEMPT", "1").to_i,
          step_name: ENV.fetch("TURBOFAN_STEP_NAME") { @step_class.name ? Turbofan.snake_case(@step_class.name).to_s : "anonymous" },
          stage: ENV.fetch("TURBOFAN_STAGE", "development"),
          pipeline_name: ENV.fetch("TURBOFAN_PIPELINE", "unknown"),
          array_index: ENV.key?("AWS_BATCH_JOB_ARRAY_INDEX") ? ENV["AWS_BATCH_JOB_ARRAY_INDEX"].to_i : nil,
          nvme_path: nvme_path,
          uses: @step_class.turbofan_uses,
          writes_to: @step_class.turbofan_writes_to,
          size: ENV["TURBOFAN_SIZE"],
          duckdb_extensions: @step_class.turbofan_duckdb_extensions
        )
      end

      def install_sigterm_handler(context, nvme_path: nil)
        trap("TERM") do
          context.interrupt!
          context.logger.info("SIGTERM received, shutting down")
          cleanup_nvme(nvme_path)
          exit(143)
        end
      end

      def attach_resources(context)
        ResourceAttacher.attach(context: context)
      end

      def cleanup_nvme(path)
        FileUtils.rm_rf(path) if path && File.directory?(path)
      end
    end
  end
end
