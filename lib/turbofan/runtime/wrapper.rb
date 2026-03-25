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
        validate_input!(inputs)
        result = @step_class.new.call(inputs, context)
        validate_output!(result)
        duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start_time

        output = serialize_output(result, context)
        emit_success_metrics(context, duration)
        Lineage.emit(Lineage.complete_event(context: context, step_class: @step_class), context: context)
        $stdout.puts(output)
      rescue => e
        context&.logger&.error("Step failed", error_class: e.class.name, error_message: e.message)
        begin
          Lineage.emit(Lineage.fail_event(context: context, step_class: @step_class, error: e), context: context) if context
          emit_failure_metrics(context) if context
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

      def validate_input!(inputs)
        schema = @step_class.turbofan_input_schema
        unless schema
          raise Turbofan::SchemaValidationError,
            "#{@step_class} has no input_schema declared"
        end

        schemer = JSONSchemer.schema(schema)
        inputs.each do |item|
          errors = schemer.validate(item).to_a
          next if errors.empty?

          raise Turbofan::SchemaValidationError,
            "Input validation failed for #{@step_class}: #{errors.map { |e| e["error"] }.join(", ")}"
        end
      end

      def validate_output!(output)
        schema = @step_class.turbofan_output_schema
        unless schema
          raise Turbofan::SchemaValidationError,
            "#{@step_class} has no output_schema declared"
        end

        schemer = JSONSchemer.schema(schema)
        errors = schemer.validate(output).to_a
        return if errors.empty?

        raise Turbofan::SchemaValidationError,
          "Output validation failed for #{@step_class}: #{errors.map { |e| e["error"] }.join(", ")}"
      end

      def set_tmpdir(nvme_path)
        tmp_dir = File.join(nvme_path, "tmp")
        FileUtils.mkdir_p(tmp_dir)
        ENV["TMPDIR"] = tmp_dir
      end

      def setup_nvme
        mount = Turbofan::ComputeEnvironment::NVME_MOUNT_PATH
        job_id = ENV["AWS_BATCH_JOB_ID"] || "local-#{Process.pid}"
        path = "#{mount}/#{job_id}"

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

      def serialize_output(result, context)
        bucket = ENV.fetch("TURBOFAN_BUCKET", "turbofan-data")
        if context.array_index
          step_name = ENV.fetch("TURBOFAN_STEP_NAME")
          size_segment = context.size ? "#{context.size}/" : ""
          key = FanOut.s3_key(context.execution_id, step_name, "output", "#{size_segment}#{context.array_index}.json")
          context.s3.put_object(bucket: bucket, key: key, body: JSON.generate(result))
          JSON.generate(result)
        else
          Payload.serialize(
            result,
            s3_client: context.s3,
            bucket: bucket,
            execution_id: context.execution_id,
            step_name: context.step_name
          )
        end
      end

      def emit_success_metrics(context, duration)
        context.metrics.emit("JobDuration", duration)
        context.metrics.emit("JobSuccess", 1)
        context.metrics.emit("PeakMemoryMB", peak_memory_mb)
        context.metrics.emit("CpuUtilization", cpu_utilization(duration))
        allocated_ram_gb = if context.size && @step_class.turbofan_sizes.any?
          @step_class.turbofan_sizes.dig(context.size.to_sym, :ram)
        else
          @step_class.turbofan_default_ram
        end
        if allocated_ram_gb
          context.metrics.emit("MemoryUtilization", memory_utilization(peak_memory_mb, allocated_ram_gb))
        end
      end

      def emit_failure_metrics(context)
        context.metrics.emit("JobFailure", 1)
      end

      def peak_memory_mb
        if File.exist?("/proc/self/status")
          status = File.read("/proc/self/status")
          if (match = status.match(/VmHWM:\s+(\d+)\s+kB/))
            return match[1].to_i / 1024.0
          end
        end
        `ps -o rss= -p #{Process.pid}`.strip.to_i / 1024.0
      rescue StandardError
        0.0
      end

      def cpu_utilization(wall_time)
        return 0.0 if wall_time <= 0

        times = Process.times
        cpu_time = times.utime + times.stime
        (cpu_time / wall_time * 100).round(1)
      rescue StandardError
        0.0
      end

      def memory_utilization(peak_mb, allocated_ram_gb)
        allocated_mb = allocated_ram_gb * 1024.0
        return 0.0 if allocated_mb <= 0
        (peak_mb / allocated_mb * 100).round(1)
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
