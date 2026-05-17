"""Per-step runtime facade.

Mirrors lib/turbofan/runtime/context.rb. Exposes step author-facing
attributes (execution_id, step_name, stage, pipeline_name, array_index,
size, envelope, interrupted, logger, metrics, s3, secrets_client) and
lazily constructs AWS clients on first access.

`interrupt()` / `interrupted` are intentionally lock-free for
signal-handler safety — see the inline comment on `interrupt()`.
"""

import os
import threading

from .logger import Logger

# boto3 read/connect timeouts — chosen so a SIGTERM during a blocking
# AWS call surfaces within ~30s (Spot reclaim is 2-min notice). Without
# these, a hung socket can hold the main thread past the SIGKILL window.
_BOTO3_CONNECT_TIMEOUT = 10
_BOTO3_READ_TIMEOUT = 30


def _int_env(name, default):
    """Parse an int env var; treat empty string as the default."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _opt_int_env(name):
    """Optional int env var: None when unset OR empty string."""
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    return int(raw)


class Context:
    """Per-step runtime facade.

    The constructor takes explicit kwargs (mirrors Ruby's `Context.new`).
    Use `Context.build(storage_path=...)` to construct from environment.
    """

    def __init__(
        self,
        *,
        execution_id,
        attempt_number,
        step_name,
        stage,
        pipeline_name,
        array_index,
        storage_path,
        size=None,
        envelope=None,
    ):
        self.execution_id = execution_id
        self.attempt_number = attempt_number
        self.step_name = step_name
        self.stage = stage
        self.pipeline_name = pipeline_name
        self.array_index = array_index
        self.storage_path = storage_path
        self.size = size
        self.envelope = envelope or {}
        # Lock-free; see interrupt() docstring.
        self.interrupted = False
        # Guards lazy construction; matches Context#@init_mutex in Ruby.
        self._init_lock = threading.Lock()
        self._logger = None
        self._metrics = None
        self._s3 = None
        self._secrets = None

    @classmethod
    def build(cls, *, storage_path):
        """Construct from env vars. Mirrors Wrapper#build_context (Ruby)."""
        return cls(
            execution_id=os.environ.get(
                "TURBOFAN_EXECUTION_ID", f"local-{os.getpid()}"
            ),
            attempt_number=_int_env("AWS_BATCH_JOB_ATTEMPT", 1),
            step_name=os.environ.get("TURBOFAN_STEP_NAME", "anonymous"),
            stage=os.environ.get("TURBOFAN_STAGE", "development"),
            pipeline_name=os.environ.get("TURBOFAN_PIPELINE", "unknown"),
            array_index=_opt_int_env("AWS_BATCH_JOB_ARRAY_INDEX"),
            storage_path=storage_path,
            size=os.environ.get("TURBOFAN_SIZE") or None,
        )

    # --- Lazy singletons (double-checked locking) ---
    # Note: under CPython's GIL, attribute reads are atomic, so the
    # outside-the-lock fast path is safe. Under free-threaded Python
    # (PEP 703, opt-in in 3.13+) the unlocked read could see a torn
    # half-initialized object — if/when we adopt free-threaded builds,
    # remove the fast path and always take the lock (the cost is one
    # uncontended lock acquisition per attribute lifetime).

    @property
    def logger(self):
        if self._logger is not None:
            return self._logger
        with self._init_lock:
            if self._logger is None:
                self._logger = Logger(
                    execution_id=self.execution_id,
                    step_name=self.step_name,
                    stage=self.stage,
                    pipeline_name=self.pipeline_name,
                    array_index=self.array_index,
                )
            return self._logger

    @property
    def metrics(self):
        if self._metrics is not None:
            return self._metrics
        with self._init_lock:
            if self._metrics is None:
                # Local import: Metrics module added in ibk-7. Keep
                # context.py importable without it for early phases.
                from .metrics import Metrics

                self._metrics = Metrics(
                    pipeline_name=self.pipeline_name,
                    stage=self.stage,
                    step_name=self.step_name,
                    size=self.size,
                )
            return self._metrics

    @property
    def s3(self):
        if self._s3 is not None:
            return self._s3
        with self._init_lock:
            if self._s3 is None:
                self._s3 = _build_boto3_client("s3")
            return self._s3

    @property
    def secrets_client(self):
        if self._secrets is not None:
            return self._secrets
        with self._init_lock:
            if self._secrets is None:
                self._secrets = _build_boto3_client("secretsmanager")
            return self._secrets

    # --- Interrupt flag: NEVER guard with a lock ---

    def interrupt(self):
        """Signal-handler-safe.

        Reading and writing a Python bool is a single bytecode under
        the GIL — atomic. Locking from a signal handler would be a
        latent deadlock if the main thread holds that lock when the
        signal fires. Same model as Ruby Context#interrupt! (see the
        Jeremy Evans comment in lib/turbofan/runtime/context.rb).
        """
        self.interrupted = True


def _build_boto3_client(service_name):
    import boto3
    from botocore.config import Config

    # `mode: 'standard', total_max_attempts: 1` disables SDK's built-in
    # retries — Turbofan's own Retryable layer owns all retry decisions,
    # mirroring lib/turbofan/runtime/context.rb#s3. (Legacy mode silently
    # adds attempts; standard mode honors the value as the total count.)
    return boto3.client(
        service_name,
        config=Config(
            retries={"mode": "standard", "total_max_attempts": 1},
            connect_timeout=_BOTO3_CONNECT_TIMEOUT,
            read_timeout=_BOTO3_READ_TIMEOUT,
        ),
    )
