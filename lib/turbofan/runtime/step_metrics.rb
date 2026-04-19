# frozen_string_literal: true

module Turbofan
  module Runtime
    module StepMetrics
      def self.emit_success(context, step_class, duration)
        context.metrics.emit("JobDuration", duration)
        context.metrics.emit("JobSuccess", 1)
        context.metrics.emit("PeakMemoryMB", peak_memory_mb)
        context.metrics.emit("CpuUtilization", cpu_utilization(duration))
        allocated_ram_gb = if context.size && step_class.turbofan.sizes.any?
          step_class.turbofan.sizes.dig(context.size.to_sym, :ram)
        else
          step_class.turbofan.default_ram
        end
        if allocated_ram_gb
          context.metrics.emit("MemoryUtilization", memory_utilization(peak_memory_mb, allocated_ram_gb))
        end
      end

      def self.emit_failure(context)
        context.metrics.emit("JobFailure", 1)
      end

      def self.peak_memory_mb
        if File.exist?("/proc/self/status")
          status = File.read("/proc/self/status")
          if (match = status.match(/VmHWM:\s+(\d+)\s+kB/))
            return match[1].to_i / 1024.0
          end
        end
        stdout, _, _ = Turbofan::Subprocess.capture("ps", "-o", "rss=", "-p", Process.pid.to_s, allow_failure: true)
        stdout.strip.to_i / 1024.0
      rescue StandardError
        0.0
      end
      private_class_method :peak_memory_mb

      def self.cpu_utilization(wall_time)
        return 0.0 if wall_time <= 0

        times = Process.times
        cpu_time = times.utime + times.stime
        (cpu_time / wall_time * 100).round(1)
      rescue StandardError
        0.0
      end
      private_class_method :cpu_utilization

      def self.memory_utilization(peak_mb, allocated_ram_gb)
        allocated_mb = allocated_ram_gb * 1024.0
        return 0.0 if allocated_mb <= 0
        (peak_mb / allocated_mb * 100).round(1)
      end
      private_class_method :memory_utilization
    end
  end
end
