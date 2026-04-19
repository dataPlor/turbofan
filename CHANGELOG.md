# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `Turbofan::Error` base class and grouped `Turbofan::ConfigError` /
  `Turbofan::ValidationError` mid-level classes. All existing gem-raised
  errors (`SchemaIncompatibleError`, `SchemaValidationError`,
  `ResourceUnavailableError`, `ExtensionLoadError`,
  `Router::InvalidSizeError`, `Subprocess::Error`,
  `Runtime::Payload::HydrationError`, `Runtime::FanOut::WorkerError`/
  `WorkerErrors`, `RetryBudgetExhausted`) are now reparented under this
  hierarchy. Users can `rescue Turbofan::Error` for generic handling or
  the specific subclasses for targeted logic. `Turbofan::Interrupted`
  intentionally stays a `SystemExit` subclass (AWS Batch exit-code 143
  contract).
- `Turbofan::Retryable.call` accepts an optional `metrics:` kwarg
  parallel to the existing `logger:` kwarg. When present, emits two
  distinct CloudWatch metrics: `RetryAttempt` (one datapoint per
  retry — graph for throttle rate) and `RetriesExhausted` (one per
  terminal failure — page-worthy signal, distinct from retry-rate).
  Dimensions inherited from the `Metrics` instance
  (Pipeline/Stage/Step/Size only — no high-cardinality error codes or
  request IDs).
- `Turbofan::RetryBudgetExhausted` — raised by `Retryable.call` when
  the accumulated sleep time across retries would exceed
  `Turbofan.config.max_retry_seconds`. Exposes `#elapsed_seconds`,
  `#budget_seconds`, `#last_error`. Distinct signal from the existing
  max-attempts path (which re-raises the original transient error).
- `Turbofan.config.fan_out_early_exit_threshold` — when set to a
  positive Integer N, `FanOut.threaded_work` stops dequeuing remaining
  items after N non-transient worker failures. Transient errors (AWS
  throttles, networking — anything `Retryable.transient?` returns true
  for) do NOT count toward the threshold, so a throttle storm can't
  false-positive as a poison-pill burst. nil default preserves the
  existing all-workers-complete contract.
- `Turbofan.config.max_retry_seconds` — cumulative-sleep budget for a
  single `Retryable.call`. Prevents a retry loop from holding a thread
  longer than a Spot reclamation horizon (default SIGTERM notice is
  ~2 minutes; `MAX_ATTEMPTS_LIMIT * cap` could otherwise block ~10
  minutes).
- `Turbofan.config.worker_stall_seconds` — arms a coordinator thread
  in `FanOut.threaded_work` that warns when a worker holds an item
  past the threshold without finishing. Catches deadlock / slow-SQL /
  hung-HTTP bugs. nil default = no coordinator thread.
- README: new "Poison-pill semantics for fan-out (DLQ)" subsection
  explains the three knobs operators have for fan-out failure
  handling (`tolerated_failure_rate`, `fan_out_early_exit_threshold`,
  `retries`) and states Turbofan's intentional non-imposition of a
  per-record DLQ contract.
- **Block form for DuckDB extensions:** `uses :duckdb do extensions
  :json, :parquet end` replaces the kwarg form
  `uses :duckdb, extensions: [...]`. Old form still works but is
  deprecated; will be removed in 0.7. Reads more like "configure the
  named thing" than "pass a kwarg that's only valid for one target."
- **`runs_on` macro on Step**, replacing `execution`:

      class MyStep
        include Turbofan::Step
        runs_on :batch     # was: execution :batch
        compute_environment :compute
      end

  Pairs grammatically with `compute_environment` (both are nouns
  describing the step's runtime environment). The `execution` macro
  still works as a deprecated alias; will be removed in 0.7.
- **`Step.turbofan` Façade:** a single public seam replacing the 20+
  `turbofan_*` attr_readers that previously polluted each user Step
  class's public API. Example: `MyStep.turbofan.uses`,
  `MyStep.turbofan.execution`, `MyStep.turbofan.batch_size`. Includes
  an `#inspect` that dumps every field — handy in pry/irb.
- **`Turbofan::Deprecations.warn_once`** — internal helper for
  quiet-by-default deprecation warnings. Emits only when `$VERBOSE` is
  true or `Turbofan.config.deprecations` is set. Memoizes per (class,
  key) pair so a user with 100 step classes never gets 100 identical
  warnings. New `Turbofan.config.deprecations` config slot.
- `Turbofan::Discovery.class_name_of(mod)` — public helper for
  getting a class's pre-override `Module#name`. Replaces the now-removed
  `Turbofan::GET_CLASS_NAME` duplicate unbound method.
- `Turbofan::DagStep.build(name, **opts)` factory for constructing
  immutable DAG step value objects with the old positional-name API.
- `Turbofan::Subprocess.capture` — structured wrapper around `Open3.capture3`
  with consistent error handling. Always captures stderr, raises a
  `Turbofan::Subprocess::Error` (with `#command`, `#exit_code`, `#stdout`,
  `#stderr`) on non-zero exit unless `allow_failure: true` is passed.
  Replaces the mix of `Kernel#system`, `Open3.capture2`, and backticks
  previously scattered across the deploy and runtime layers.
- `Turbofan::Runtime::FanOut::WorkerError` and
  `Turbofan::Runtime::FanOut::WorkerErrors` for structured per-worker
  failure reporting in `threaded_work`. `WorkerError` wraps a single
  failing work item plus its original `#cause` (and preserves the
  original backtrace); `WorkerErrors` aggregates multiple failures and
  exposes them via `#errors`.
- `Turbofan::Deploy::PipelineContext.load(pipeline_name:, turbofans_root:)`
  — unified CLI pipeline-loading helper. `PipelineContext::DEFAULT_ROOT`
  exposes the `"turbofans"` convention.
- `Turbofan::Retryable.call` accepts an optional `logger:` kwarg for
  structured retry observability. Default `nil` keeps it silent, so
  existing callers are unaffected.
- `turbofan version` / `-v` / `--version` CLI command.
- `# frozen_string_literal: true` magic comment on every `lib/` and
  `spec/` file. `.rubocop.yml` enforces the cop going forward.
- `LICENSE` file (MIT) bundled in the gem archive.
- GitHub Actions workflow now runs Ruby 3.2/3.3/3.4 on ubuntu and
  macOS, plus a `gem build` smoke job that installs the packaged gem
  and verifies all three entry points (`turbofan`, `turbofan/runtime`,
  `turbofan/deploy`) load. Bundle-audit blocks on CVEs; rubocop is
  advisory.
- **Zeitwerk-managed autoloading.** The 45-line `require_relative`
  cascade in `lib/turbofan.rb` is replaced with a Zeitwerk loader.
  Adds `zeitwerk ~> 2.6` as a runtime dependency. `Turbofan.loader` is
  exposed (read-only) so downstream gems can call `.ignore` before
  their own setup.
- **`require "turbofan/runtime"`** — slim entry point for container
  workers. Loads only the runtime harness + the 3 AWS SDKs actually
  needed at runtime (s3, secretsmanager, cloudwatch). Skips the 8
  deploy-side SDKs (cloudformation, batch, ec2, ecr, states, sts, ecs,
  cloudwatchlogs), shaving an estimated 200–400ms of cold-start + 30–
  80MB RSS on Lambda/Batch.
- **`require "turbofan/deploy"`** — mirror entry point for deploy-only
  consumers (CI jobs that only build CloudFormation). Loads the deploy
  + generators subtrees.
- `require "turbofan"` is unchanged — still loads the full gem
  (runtime + deploy + CLI).
- `Turbofan::Discovery.subclasses_of` now returns results sorted by
  fully-qualified class name. `ObjectSpace.each_object` iteration order
  is GC-dependent; sorting produces reproducible CloudFormation diffs
  and ASL state ordering across runs/platforms.

### Changed
- **Breaking (internal API):** `Turbofan::Runtime::FanOut.threaded_work`
  now raises `WorkerError` (single failure) or `WorkerErrors` (multiple
  failures) instead of the raw underlying exception. Callers that
  rescued specific exception classes from inside `threaded_work` must
  now rescue `WorkerError` and inspect `#cause`, or iterate
  `WorkerErrors#errors`. `rescue StandardError` sites are unaffected.
- **Breaking (internal API):** `Turbofan::DagStep` is now a
  `Data.define` value object instead of a `Struct`. Instances are
  frozen on construction; writer methods (`step.fan_out = true`) are
  gone. Use `step.with(fan_out: true, ...)` to produce an updated copy.
  The positional-name constructor moved to `DagStep.build(name, ...)`
  — `DagStep.new` now requires keyword args (Data's default).
- `bin/turbofan` moved to `exe/turbofan` per current rubygems convention
  (avoids collision with `bin/rspec` dev stub). `spec.bindir = "exe"`.
- Gemspec metadata expanded: added `description`,
  `"rubygems_mfa_required" => "true"`, `bug_tracker_uri`,
  `documentation_uri`. `spec.files` now includes `README.md`,
  `CHANGELOG.md`, `LICENSE`.
- Internal constants marked `private_constant` where not part of the
  public API: `Discovery::CLASS_NAME`, `InstanceCatalog::INSTANCES`,
  `Extensions::PLATFORM/COMMUNITY/CORE_REPO/COMMUNITY_REPO`,
  `ComputeEnvironment::NVME_USERDATA`, `Step::VALID_EXECUTION_MODELS`,
  `Pipeline::RESERVED_DAG_METHODS`, `Status::PENDING_STATUSES`/
  `BATCH_STATUSES`. `Turbofan::Interrupted::EXIT_CODE` stays public
  (AWS Batch retry contract).
- Removed `Turbofan::GET_CLASS_NAME` (duplicated the existing
  `Discovery::CLASS_NAME`). Use `Turbofan::Discovery.class_name_of(c)`.

### Deprecated

Two removal milestones are staged to let users migrate incrementally.
Enable `Turbofan.config.deprecations = true` (or run with `$VERBOSE`)
to surface the quiet-by-default warnings in CI and catch every call
site.

**Scheduled for removal in 0.7:**
- `execution :batch` DSL macro — migrate to `runs_on :batch`. Pairs
  grammatically with `compute_environment :foo`.
- `uses(:duckdb, extensions: [...])` kwarg form — migrate to the block
  form: `uses(:duckdb) { extensions :json, :parquet }`.

**Scheduled for removal in 1.0:**
- The `Step#turbofan_*` attr_readers (`turbofan_uses`,
  `turbofan_execution`, etc., ~20 of them) — migrate to the new
  `.turbofan` façade: `MyStep.turbofan.uses`, `.execution`, etc. Both
  surfaces work in 0.6.x without warnings; warnings begin in 0.8 so
  the deprecation period is a full minor-version family rather than a
  single release.

### Fixed
- `Turbofan::Runtime::Context` lazy-memoized attributes (`logger`,
  `metrics`, `s3`, `secrets_client`, `uses_resources`,
  `writes_to_resources`) were constructed via `@foo ||= ...`, which
  races under concurrent `fan_out` workers — two threads could
  construct separate instances and only one would be retained. For
  `metrics`, this was a silent-data-loss bug: two racing `Metrics`
  instances each accepted `emit()` calls on their own `@pending`
  array, and only one got flushed. Now guarded by double-checked
  locking behind a per-Context `@init_mutex`.
- `Turbofan::Runtime::Metrics#emit` and `#flush` now synchronize on a
  per-instance mutex. The append in `emit` and the batch-extract +
  shift in `flush` were both racy; under concurrent `emit` during
  `flush`, a batch could be serialized to CloudWatch payload form then
  re-appended-to before being shifted, causing double-send.
- `Turbofan::Step` and `Turbofan::Pipeline` now install an
  `inherited(subclass)` hook so `class B < A` (where A includes Step)
  correctly initializes B's `@turbofan_*` ivars. Previously the
  second-level subclass would `NoMethodError` on the first DSL macro
  call. The hook calls `super` so downstream inheritance hooks
  (ActiveSupport, dry-rb, etc.) still fire. Each subclass receives
  independent `.dup`'d Array/Hash copies, so parent-class DSL state is
  never mutated by a child.
- `Turbofan::Runtime::Context#duckdb` now resets `@duckdb = nil` and
  closes the partial connection on init failure (extension load error,
  `Database.open` failure, etc.) instead of caching a half-initialized
  connection for subsequent `context.duckdb` calls.
- `Turbofan::Subprocess::Error` redacts argv beyond the first 3 tokens
  in its default message, so sensitive args (proxy URLs with embedded
  credentials, `--build-arg SECRET=...`) don't leak into exception
  messages or logs. Full argv is still available via `#command` for
  debugging.
- `Turbofan::CLI::Deploy::Preflight.git_clean?` fails loudly when git
  invocation fails (e.g. non-repo directory), instead of silently
  reporting "clean" on empty stdout with a non-zero exit.

## [0.5.0]

See git history for changes prior to this CHANGELOG.
