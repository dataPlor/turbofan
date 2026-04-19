# Upgrading

## Upgrading to 0.6.0

0.6 is the biggest release since 0.1 — foundation, observability, and DSL polish all land together. Most changes are additive or deprecation-aliased, so the upgrade path is **gradual, not forced**. Flip `Turbofan.config.deprecations = true` in CI to surface every call site that still uses a to-be-removed form.

```ruby
# config/turbofan.rb (or wherever you configure)
Turbofan.configure do |c|
  c.deprecations = true if ENV["CI"]
end
```

### Breaking changes (already-required action)

These changed behavior and cannot be silenced. Search your codebase and fix.

#### 1. `FanOut.threaded_work` raises structured errors

**Before (0.5):** A single failing worker raised the raw worker exception. Multiple failures raised the first error with a `"; N others"` suffix string.

**After (0.6):** Single failure raises `Turbofan::Runtime::FanOut::WorkerError`; multiple failures raise `Turbofan::Runtime::FanOut::WorkerErrors` aggregate.

**Action:** `rescue StandardError` sites are unaffected. If you rescued a specific exception class from inside `threaded_work`:

```ruby
# Before
begin
  fan_out_step(items)
rescue Aws::S3::Errors::AccessDenied => e
  handle(e)
end

# After
begin
  fan_out_step(items)
rescue Turbofan::Runtime::FanOut::WorkerError => e
  raise unless e.cause.is_a?(Aws::S3::Errors::AccessDenied)
  handle(e.cause)
rescue Turbofan::Runtime::FanOut::WorkerErrors => e
  # multi-failure case: e.errors is [WorkerError, ...]
  aws_errors = e.errors.select { |we| we.cause.is_a?(Aws::S3::Errors::AccessDenied) }
  # handle however
end
```

Or just `rescue Turbofan::Error` — the new umbrella class (see #2) catches both.

#### 2. `DagStep` is now a `Data.define` value object

**Before (0.5):** `DagStep` was a `Struct`; instances were mutable; `DagBuilder#fan_out` mutated `fan_out=`, `tolerated_failure_rate=`, etc. in place.

**After (0.6):** `DagStep` is frozen on construction. Writer methods don't exist. The positional-name constructor moved to `DagStep.build(name, ...)` — `DagStep.new` now requires keyword args (Data's default).

**Action:** Only relevant if you constructed DagSteps directly (rare — typically done via the `pipeline do ... end` block). If so:

```ruby
# Before
step = Turbofan::DagStep.new(:process, fan_out: true)
step.fan_out_timeout = 300   # mutation

# After
step = Turbofan::DagStep.build(:process, fan_out: true)
step = step.with(fan_out_timeout: 300)   # returns a new frozen copy
```

### Deprecation-aliased (safe now, fix before 0.7 / 1.0)

These continue to work in 0.6.x. Set `Turbofan.config.deprecations = true` to surface one-time warnings per class.

#### 3. `execution :batch` → `runs_on :batch` (removed in 0.7)

Mechanical rename. Pairs grammatically with `compute_environment`.

```ruby
# Before
class MyStep
  include Turbofan::Step
  execution :lambda
  compute_environment :compute
end

# After
class MyStep
  include Turbofan::Step
  runs_on :lambda
  compute_environment :compute
end
```

`sed -i '' 's/execution :\(batch\|lambda\|fargate\)/runs_on :\1/g' turbofans/steps/**/*.rb`

#### 4. `uses :duckdb, extensions: [:foo]` → block form (removed in 0.7)

The kwarg form was idiosyncratic (`extensions:` was only valid for `:duckdb`). Block form reads as what it is: configuration of a named thing.

```ruby
# Before
class MyStep
  include Turbofan::Step
  uses :duckdb, extensions: [:json, :parquet, :spatial]
end

# After
class MyStep
  include Turbofan::Step
  uses :duckdb do
    extensions :json, :parquet, :spatial
  end
end
```

#### 5. `MyStep.turbofan_uses` → `MyStep.turbofan.uses` (removed in 1.0)

The 20+ `turbofan_*` attr_readers pollute every user Step class's public API. A single `.turbofan` façade is cleaner.

Both surfaces work through the 0.6.x line **without deprecation warnings** — migrate at your own pace. Warnings begin in 0.8; removal in 1.0.

```ruby
# Before
MyStep.turbofan_uses            # => [{type: :resource, key: :postgres}]
MyStep.turbofan_execution       # => :batch

# After
MyStep.turbofan.uses            # same return value
MyStep.turbofan.execution       # same return value
MyStep.turbofan.inspect         # new: dumps every field, handy in pry
```

Mechanical migration:

```
MyStep.turbofan_uses                 → MyStep.turbofan.uses
MyStep.turbofan_writes_to            → MyStep.turbofan.writes_to
MyStep.turbofan_execution            → MyStep.turbofan.execution
MyStep.turbofan_batch_size           → MyStep.turbofan.batch_size
MyStep.turbofan_retries              → MyStep.turbofan.retries
MyStep.turbofan_tags                 → MyStep.turbofan.tags
MyStep.turbofan_compute_environment  → MyStep.turbofan.compute_environment
MyStep.turbofan_default_cpu          → MyStep.turbofan.default_cpu
MyStep.turbofan_default_ram          → MyStep.turbofan.default_ram
MyStep.turbofan_secrets              → MyStep.turbofan.secrets
MyStep.turbofan_sizes                → MyStep.turbofan.sizes
MyStep.turbofan_docker_image         → MyStep.turbofan.docker_image
MyStep.turbofan_duckdb_extensions    → MyStep.turbofan.duckdb_extensions
MyStep.turbofan_subnets              → MyStep.turbofan.subnets
MyStep.turbofan_security_groups      → MyStep.turbofan.security_groups
MyStep.turbofan_storage              → MyStep.turbofan.storage
MyStep.turbofan_timeout              → MyStep.turbofan.timeout
MyStep.turbofan_retry_on             → MyStep.turbofan.retry_on
MyStep.turbofan_lambda?              → MyStep.turbofan.lambda?
MyStep.turbofan_fargate?             → MyStep.turbofan.fargate?
MyStep.turbofan_external?            → MyStep.turbofan.external?
MyStep.turbofan_needs_duckdb?        → MyStep.turbofan.needs_duckdb?
MyStep.turbofan_resource_keys        → MyStep.turbofan.resource_keys
MyStep.turbofan_input_schema         → MyStep.turbofan.input_schema
MyStep.turbofan_output_schema        → MyStep.turbofan.output_schema
```

### New capabilities worth adopting

#### `require "turbofan/runtime"` in container workers

Skips the 8 deploy-side AWS SDK gems the container doesn't need. Measurable cold-start + RSS savings on Lambda/Batch — ~200-400ms and 30-80MB.

```ruby
# Dockerfile's worker.rb entry
require "turbofan/runtime"   # was: require "turbofan"
```

Paired tripwire: if your worker code accidentally references `Turbofan::CLI` (typical via a stale `require` or an errant reference), turbofan now raises at load time rather than silently autoloading 8 aws-sdk gems.

#### Rescue `Turbofan::Error` for any gem-originated failure

New hierarchy unifies the gem's error surface:

```
Turbofan::Error                               (base)
├── Turbofan::ConfigError                     (config/discovery)
│   ├── ResourceUnavailableError
│   └── ExtensionLoadError
├── Turbofan::ValidationError                 (schema/check)
│   ├── SchemaIncompatibleError
│   ├── SchemaValidationError
│   └── Router::InvalidSizeError
├── Turbofan::Subprocess::Error
├── Turbofan::Runtime::Payload::HydrationError
├── Turbofan::Runtime::FanOut::WorkerError
├── Turbofan::Runtime::FanOut::WorkerErrors
└── Turbofan::RetryBudgetExhausted
```

Old rescues still work; the new base is additive.

#### Production-hardening config knobs

All default to `nil` — opt in per environment.

```ruby
Turbofan.configure do |c|
  # Cap a single Retryable.call's cumulative sleep so a throttle storm
  # can't hold a thread past the Spot reclamation horizon.
  c.max_retry_seconds = 90

  # Stop dequeuing fan-out items after N non-transient failures
  # (poison-pill protection). Transient errors don't count.
  c.fan_out_early_exit_threshold = 5

  # Warn if a fan-out worker holds an item longer than N seconds
  # without finishing (deadlock / slow-SQL / hung-HTTP).
  c.worker_stall_seconds = 300
end
```

### Upgrade checklist

- [ ] `bundle update turbofan` to 0.6.0
- [ ] Update any `rescue` blocks inside `threaded_work` (breaking change #1)
- [ ] Update direct `DagStep` constructors if any (breaking change #2)
- [ ] Run tests with `Turbofan.config.deprecations = true` to surface call sites
- [ ] Consider `require "turbofan/runtime"` in container Dockerfiles
- [ ] Consider the three new production-hardening config knobs
- [ ] Plan `execution → runs_on` + `uses-extensions-kwarg → block form` migrations before 0.7
- [ ] Optional: begin `turbofan_*` attr-reader → `.turbofan` façade migration (no rush; 1.0 removal)
