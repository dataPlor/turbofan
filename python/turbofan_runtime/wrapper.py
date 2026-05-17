"""Wrapper.run — top-level step orchestration.

Mirrors lib/turbofan/runtime/wrapper.rb. Owns the full step lifecycle:
storage setup → context construction → SIGTERM install → lineage
START → input resolution → schema validation → user `call_fn` →
output schema validation → output serialization → metrics + lineage
COMPLETE → stdout write. Error paths emit fail lineage + JobFailure
metric before re-raising.

SIGTERM divergence vs Ruby: Python signal handlers cannot interrupt
a thread blocked in a C-level boto3 syscall (unlike Ruby's
`Thread#raise`). The handler sets `context.interrupted = True` and
attempts `raise Interrupted("SIGTERM")` — the raise fires when the
interpreter next regains control. Combined with boto3
`read_timeout=30` (set in Context._build_boto3_client) this gives
~30s worst-case latency from SIGTERM delivery to graceful unwind.
For containers under AWS Batch Spot reclaim (2-min notice) this is
within budget. Step authors writing long-running compute loops should
periodically check `context.interrupted` and raise `Interrupted`
themselves at safe checkpoints.

Exit-code 143 (128 + SIGTERM=15) is NOT emitted by Wrapper.run —
it just re-raises `Interrupted`. The generated step `main.py` template
catches `Interrupted` and calls `sys.exit(143)`. See
`examples/steps/hello_python/main.py`.
"""

import os
import signal
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

from .context import Context
from .errors import Interrupted
from .input_resolver import InputResolver
from .lineage import Lineage
from .output_serializer import OutputSerializer
from .schema_validator import SchemaValidator
from .step_metrics import StepMetrics


class Wrapper:
    @classmethod
    def run(cls, call_fn, *, input_schema, output_schema):
        # Line-buffer stdout so the State Machine / CloudWatch Logs see
        # the envelope output promptly (Ruby's `$stdout.sync = true`
        # equivalent). reconfigure() is on TextIOWrapper; sys.stdout
        # might not always be one (rare), so guard.
        try:
            sys.stdout.reconfigure(line_buffering=True)
        except (AttributeError, ValueError):
            pass

        context = None
        storage_path = None
        try:
            storage_path = _setup_storage()
            if storage_path:
                tmp = Path(storage_path) / "tmp"
                tmp.mkdir(parents=True, exist_ok=True)
                tempfile.tempdir = str(tmp)

            context = Context.build(storage_path=storage_path)
            _install_sigterm_handler(context)
            # attach_resources(context) — deferred to v2

            Lineage.emit(
                Lineage.start_event(context=context, call_fn=call_fn),
                context=context,
            )
            start = time.monotonic()

            envelope = InputResolver.call(context)
            inputs = envelope["inputs"]
            context.envelope = {k: v for k, v in envelope.items() if k != "inputs"}

            if inputs == [None]:
                context.logger.info("Sentinel chunk, no work")
                Lineage.emit(
                    Lineage.complete_event(context=context, call_fn=call_fn),
                    context=context,
                )
                return

            SchemaValidator.validate_input(
                step_name=context.step_name,
                schema_file=input_schema,
                inputs=inputs,
            )
            result = call_fn(inputs, context)
            SchemaValidator.validate_output(
                step_name=context.step_name,
                schema_file=output_schema,
                output=result,
            )

            duration = time.monotonic() - start
            output = OutputSerializer.call(result, context)
            StepMetrics.emit_success(context, duration)
            Lineage.emit(
                Lineage.complete_event(context=context, call_fn=call_fn),
                context=context,
            )
            sys.stdout.write(output + "\n")
            sys.stdout.flush()

        except Interrupted as e:
            # Cooperative shutdown — NOT a user-code failure. Skip
            # failure metrics + fail lineage. Re-raise so generated
            # main.py can `sys.exit(143)`.
            if context is not None:
                context.logger.info("Interrupted by signal", reason=str(e))
            else:
                sys.stderr.write(f"[turbofan_runtime] Interrupted: {e}\n")
            raise

        except Exception as e:
            if context is not None:
                context.logger.error(
                    "Step failed",
                    error_class=type(e).__name__,
                    error_message=str(e),
                )
                try:
                    Lineage.emit(
                        Lineage.fail_event(
                            context=context, error=e, call_fn=call_fn,
                        ),
                        context=context,
                    )
                    StepMetrics.emit_failure(context)
                except Exception as metrics_err:
                    sys.stderr.write(
                        f"[turbofan_runtime] WARNING: failed to emit "
                        f"failure metrics: "
                        f"{type(metrics_err).__name__}: {metrics_err}\n"
                    )
            else:
                # Context construction itself failed — best we can do
                # is log to stderr before re-raising.
                sys.stderr.write(
                    f"[turbofan_runtime] Step failed before context built: "
                    f"{type(e).__name__}: {e}\n"
                )
            raise

        finally:
            _cleanup_storage(storage_path)
            if context is not None:
                try:
                    context.metrics.flush()
                except Exception as flush_err:
                    sys.stderr.write(
                        f"[turbofan_runtime] WARNING: failed to flush "
                        f"metrics: {type(flush_err).__name__}: {flush_err}\n"
                    )


def _setup_storage():
    """Resolve a writable scratch directory for the step.

    Resolution order:
    1. `TURBOFAN_STORAGE_PATH` already set by the launcher → use as-is.
    2. `TURBOFAN_NVME_MOUNT_PATH` env var (set by deploy code from
       `Turbofan::ComputeEnvironment::NVME_MOUNT_PATH`) → create
       `{path}/{job_id}-attempt{N}` subdir and use that.
    3. Fargate ephemeral (`ECS_CONTAINER_METADATA_URI_V4` present) →
       `/tmp/turbofan-{job_id}-attempt{N}`.
    4. None (run without a scratch dir).

    Returns the path string or None.
    """
    if "TURBOFAN_STORAGE_PATH" in os.environ:
        return os.environ["TURBOFAN_STORAGE_PATH"]

    job_id = os.environ.get("AWS_BATCH_JOB_ID", f"local-{os.getpid()}")
    attempt = os.environ.get("AWS_BATCH_JOB_ATTEMPT", "1")

    nvme = os.environ.get("TURBOFAN_NVME_MOUNT_PATH")
    if nvme and Path(nvme).is_dir():
        path = f"{nvme}/{job_id}-attempt{attempt}"
        Path(path).mkdir(parents=True, exist_ok=True)
        os.environ["TURBOFAN_STORAGE_PATH"] = path
        sys.stderr.write(f"[turbofan_runtime] Storage: NVMe at {path}\n")
        return path

    if "ECS_CONTAINER_METADATA_URI_V4" in os.environ:
        path = f"/tmp/turbofan-{job_id}-attempt{attempt}"
        Path(path).mkdir(parents=True, exist_ok=True)
        os.environ["TURBOFAN_STORAGE_PATH"] = path
        sys.stderr.write(f"[turbofan_runtime] Storage: Fargate ephemeral at {path}\n")
        return path

    sys.stderr.write("[turbofan_runtime] No local storage detected\n")
    return None


def _cleanup_storage(path):
    if not path:
        return
    if not Path(path).is_dir():
        return
    shutil.rmtree(path, ignore_errors=True)


def _install_sigterm_handler(context):
    """Install a SIGTERM handler that flags interruption + raises Interrupted.

    Caveats:
    - Python only allows `signal.signal` from the main thread; in tests
      that run inside threading harnesses this is silently skipped with
      a stderr warning (we'd rather notice the gap than hide it).
    - The handler sets `context.interrupted` (atomic under GIL) and
      `raise Interrupted("SIGTERM")`. The raise only fires when the
      interpreter regains control between bytecodes — boto3 syscalls
      complete first (bounded by their read_timeout, set to 30s on
      Context._build_boto3_client).
    """
    if threading.current_thread() is not threading.main_thread():
        sys.stderr.write(
            "[turbofan_runtime] WARNING: SIGTERM handler not installed "
            "(not on main thread); container will not unwind gracefully "
            "on SIGTERM\n"
        )
        return

    def handler(signum, frame):
        context.interrupt()
        raise Interrupted("SIGTERM received")

    try:
        signal.signal(signal.SIGTERM, handler)
    except (ValueError, OSError) as e:
        sys.stderr.write(
            f"[turbofan_runtime] WARNING: signal.signal(SIGTERM) failed: "
            f"{type(e).__name__}: {e}; container will not unwind "
            f"gracefully on SIGTERM\n"
        )
