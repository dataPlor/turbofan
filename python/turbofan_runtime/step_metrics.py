"""Per-step success/failure metric emission.

Mirrors lib/turbofan/runtime/step_metrics.rb. Wrapper.run calls
`StepMetrics.emit_success(context, duration)` on the happy path and
`StepMetrics.emit_failure(context)` on user-code exceptions.

Metric names match Ruby exactly: `JobDuration`, `JobSuccess`,
`PeakMemoryMB`, `CpuUtilization`, optional `MemoryUtilization`,
`JobFailure`. `MemoryUtilization` is opt-in via
`TURBOFAN_ALLOCATED_RAM_MB` env var — set by the Ruby deploy code at
the JobDefinition layer (see Epic 3 / `PLAN-python-deploy-env-exports`).
"""

import os
import pathlib
import re
import sys


class StepMetrics:
    @classmethod
    def emit_success(cls, context, duration):
        peak_mb = _peak_memory_mb()
        context.metrics.emit("JobDuration", float(duration))
        context.metrics.emit("JobSuccess", 1)
        context.metrics.emit("PeakMemoryMB", peak_mb)
        context.metrics.emit("CpuUtilization", _cpu_utilization(duration))

        allocated = os.environ.get("TURBOFAN_ALLOCATED_RAM_MB")
        if not allocated:
            return
        try:
            allocated_mb = float(allocated)
            if allocated_mb <= 0:
                return
            util = round(peak_mb / allocated_mb * 100, 1)
        except (TypeError, ValueError):
            return
        context.metrics.emit("MemoryUtilization", util)

    @classmethod
    def emit_failure(cls, context):
        context.metrics.emit("JobFailure", 1)


def _peak_memory_mb():
    """Return peak resident memory in MB.

    Primary: Linux `/proc/self/status` `VmHWM` (kilobytes).
    Fallback: `resource.getrusage(RUSAGE_SELF).ru_maxrss`.
        Note: `ru_maxrss` is **bytes on Darwin**, **kilobytes on Linux**.
        We branch on platform to convert correctly. (Original Ruby
        implementation ignores macOS; we cover it as a dev convenience.)
    Final fallback: 0.0.
    """
    try:
        status = pathlib.Path("/proc/self/status").read_text()
        match = re.search(r"VmHWM:\s+(\d+)\s+kB", status)
        if match:
            return int(match.group(1)) / 1024.0
    except (OSError, ValueError):
        pass

    try:
        import resource

        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return rss / (1024.0 * 1024.0)  # bytes → MB on macOS
        return rss / 1024.0  # kilobytes → MB on Linux/BSD
    except (ImportError, AttributeError):
        return 0.0


def _cpu_utilization(wall_time):
    if wall_time <= 0:
        return 0.0
    try:
        times = os.times()
        cpu = times.user + times.system
        return round(cpu / wall_time * 100, 1)
    except OSError:
        return 0.0
