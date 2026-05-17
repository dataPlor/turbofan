# turbofan-runtime

Python runtime contract for [Turbofan](https://github.com/dataplor/turbofan) polyglot steps. Mirrors the Ruby `Turbofan::Runtime::Wrapper` API for Python step containers.

See `../PLAN-python-runtime-wrapper.md` for the full design.

## Install

From a checkout of the turbofan repo:

    pip install -e ./python

From a step Dockerfile:

    pip install "turbofan-runtime @ git+https://github.com/dataplor/turbofan@v0.8.0#subdirectory=python"

## Usage

A Python step's `main.py`:

```python
import sys
from turbofan_runtime import Interrupted, Wrapper


def call(inputs, context):
    return {"status": "ok"}


if __name__ == "__main__":
    try:
        Wrapper.run(
            call,
            input_schema="my_step_input.json",
            output_schema="my_step_output.json",
        )
    except Interrupted:
        sys.exit(143)
```

The companion Ruby `worker.rb` declares the step's compute environment,
resource requirements, and schemas for Turbofan's deploy-side CFN/ASL
generation:

```ruby
class MyStep
  include Turbofan::Step

  runs_on :batch
  compute_environment :compute
  cpu 1
  ram 2
  batch_size 1
  input_schema "my_step_input.json"
  output_schema "my_step_output.json"
end
```

Generate both files via:

    turbofan step new my_step --lang python

## What the runtime provides

* Envelope I/O (matches Ruby `Turbofan::Runtime::InputResolver` /
  `OutputSerializer`), including `__turbofan_s3_ref` payload hydration
  and fan_out S3-key conventions.
* JSON Schema validation against schemas resolved relative to
  `$TURBOFAN_SCHEMAS_PATH`.
* CloudWatch metrics matching the Ruby names + dimensions
  (`JobDuration`, `JobSuccess`, `JobFailure`, `PeakMemoryMB`,
  `CpuUtilization`, optional `MemoryUtilization`).
* OpenLineage 2.0.2 events to stderr.
* SIGTERM cooperative shutdown — best-effort within ~30s (Python
  signal handlers cannot interrupt threads blocked mid-`boto3`
  syscall like Ruby's `Thread#raise` can; combined with boto3
  `read_timeout=30` the interrupt is delivered at the next safe
  checkpoint).
* Retry of transient AWS errors via `Retryable` (mirrors
  `lib/turbofan/retryable.rb` — full-jitter exponential backoff,
  configurable cumulative-sleep budget).

## Known v1 limitations

* No `TURBOFAN_PREV_STEP` / `TURBOFAN_PREV_STEPS` input resolution.
  Raises `NotImplementedError`.
* No `attach_resources` — Python steps cannot `uses :postgres` /
  `uses :secret` until v2. Steps that need this must stay Ruby.
* Lineage `inputs` / `outputs` arrays are empty (no `uses:` /
  `writes_to:` declaration plumbing on the Python side yet).
* `MemoryUtilization` metric requires `TURBOFAN_ALLOCATED_RAM_MB`
  env var (set by the Ruby deploy code at the JobDefinition layer).
* Ruby Wrapper still emits raw JSON to stdout; Python wrapper emits
  the `__turbofan_s3_ref` envelope. Downstream `Payload.deserialize`
  handles either form transparently. Alignment is a coordinated
  follow-up.

## Tests

    pip install -e "./python[dev]"
    pytest python/tests
