# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] — 2026-04-19

0.7 is a hard-break release. The install base is entirely internal, so
correctness outweighs migration grace on every breaking item below — no
deprecation cycles, no shim aliases. Three themes: remove the legacy
DSL surface (readers, `execution`, `schedule`, `uses-extensions`),
generalize pipeline triggers beyond cron-only via EventBridge, and
tighten internal polish (Discovery memoization, `step.rb` split,
`bin/release` private-gem workflow). See [UPGRADING.md](UPGRADING.md#upgrading-to-070)
for migration commands.

All pre-cut issues flagged in the 5-legend review (Jeremy Evans, Mike
Perham, Matz, Xavier Noria, Andrew Kane) were addressed. Six TODOs
were deferred to 0.7.1: Pipeline façade parity, GuardLambda
idempotency token, freeze-on-assign for tags/deps, `storage_gib`
rename, `loader.on_load` privatization of `UsesDuckdbDSL`,
`InputTransformer` for schedule triggers, and `trigger
:s3_object_created`-style sugar.

### Added
- **`trigger(type, **kwargs)` macro on Pipeline** — Rails-style
  declaration of EventBridge-backed pipeline triggers. Multiple
  triggers per pipeline; each becomes its own `AWS::Events::Rule`
  sharing a single `GuardLambda`. Two types supported:
  - `trigger :schedule, cron: "0 5 * * ? *"` — cron-backed firing.
  - `trigger :event, source: "aws.s3", detail_type: "Object Created", detail: {...}, event_bus: "..."` —
    full EventBridge pattern matching, optional custom event bus.
  Pipelines with no `trigger` declarations are manual-invocation only
  (unchanged semantics).
- **T1 input transform.** The new GuardLambda passes `event.detail`
  as the pipeline input with trigger provenance namespaced under a
  single `_turbofan.event` sub-hash at the top level —
  `_turbofan.event.source`, `.detail_type`, `.time`, `.id`,
  `.account`, `.region`. Single-namespace shape was chosen after the
  pre-cut legend review flagged that flat `__event_*` keys are
  Pythonic (dunder) and collide with publisher-supplied detail
  fields. For `trigger :schedule`, the envelope carries
  `_turbofan.event.schedule_expression` via the same path. Publishers
  can forward their own provenance under `_turbofan.*`; Turbofan
  only owns the `event` sub-key.
- **`Turbofan::Discovery.reset_cache!`** — exposes the cache
  invalidation hook that `PipelineLoader` and the root modules'
  `included` hooks call automatically. Needed for tests that create
  anonymous subclasses across examples.

### Changed
- **`Turbofan::Discovery.subclasses_of` memoized per-module.** Cache
  invalidates automatically when new classes include
  Step/Pipeline/Resource/Router/ComputeEnvironment, and when
  `PipelineLoader` re-enters `Kernel.load`. Thread-safe via Mutex
  (subclasses_of runs from fan_out workers concurrently).
- **`lib/turbofan/step.rb` split into focused files** under
  `lib/turbofan/step/` (class_methods, config_facade,
  uses_duckdb_dsl, validators). Behaviour unchanged; file is now 52
  lines instead of 457.
- **`bin/release` private-gem workflow.** `gem push` is off by
  default; opt in with `--public`. Matches how Turbofan is actually
  distributed (GitHub-tag pinning).

### Removed (breaking)
- **`Step#turbofan_*` attr_readers removed.** All ~20 legacy readers
  (`turbofan_uses`, `turbofan_execution`, `turbofan_tags`, etc.) are
  deleted — migrate to the `.turbofan` façade: `MyStep.turbofan.uses`,
  `.execution`, `.tags`. Originally staged for 1.0, pulled forward to
  0.7 because the install base is entirely internal and correctness
  outweighs graceful migration here. The façade has been available
  since 0.6.0; no new API to learn.
- **`execution :batch` macro removed** — use `runs_on :batch`.
- **`uses(:duckdb, extensions: [...])` kwarg form removed** — use the
  block form: `uses(:duckdb) { extensions :json, :parquet }`.
- **`schedule "..."` Pipeline macro removed** — use
  `trigger :schedule, cron: "..."`. Hard break, no alias cycle.

See [UPGRADING.md](UPGRADING.md#upgrading-to-070) for migration
commands.

## [0.6.1] — 2026-04-19

0.6.1 is the post-ship followup pass on the 0.6.0 legend review. Every
item flagged by Jeremy Evans, Mike Perham, Matz, Xavier Noria, and
Andrew Kane has a corresponding commit. No new migration steps for
users — this is purely additive on top of 0.6.0.

### Added
- **`WorkerStall` CloudWatch metric** — `FanOut.threaded_work` accepts
  a `metrics:` kwarg; when set, the stall coordinator emits a
  `WorkerStall` datapoint (count 1) alongside the existing stderr
  warning. Dimensions inherited from the Metrics instance
  (Pipeline/Stage/Step/Size). Operators can now alert on stalls from
  CloudWatch dashboards instead of grepping logs.
- **`RetryBudgetExhausted` CloudWatch metric** — separate from
  `RetriesExhausted`. Distinguishes "gave up on wall-clock budget"
  (often recoverable service degradation) from "gave up on attempt
  count" (persistent failure). Different alert thresholds apply.
- **`Turbofan::Retryable.call(max_retry_seconds:)` per-call override** —
  kwarg accepts a finite value (per-call budget), `nil` (bypass the
  global config even when set), or omission (falls back to
  `Turbofan.config.max_retry_seconds`). Disambiguated internally via
  a private `UNSET` sentinel. Terminal-write call sites
  (`Metrics#flush`, `OutputSerializer.call`, `Payload.serialize`) now
  pass `nil` so SIGTERM-time flushes don't self-abort from the global
  budget.
- **Polymorphic `input_schema` / `output_schema`** — the macros now
  accept any of:
  - `input_schema "hello.json"` — filename String (original behavior)
  - `input_schema({type: "object", ...})` — Hash literal
  - `input_schema HelloSchema` — Class/Module responding to `.schema`
  The macro name now agrees with what it takes.
- **Zeitwerk inflector-completeness spec** — walks
  `Turbofan.loader.all_expected_cpaths` and asserts every file→constant
  mapping resolves. Catches future contributions that add an acronym
  file (SNS, IAM, ECR, etc.) without updating the inflector rules, at
  normal spec-run time instead of only at cold boot.
- **`Step.turbofan.uses_s3` / `Step.turbofan.writes_to_s3`** — façade
  now exposes these S3-dependency filters as part of the public DSL
  surface. Previously accessible as `turbofan_uses_s3`/etc. on
  ClassMethods; now routed exclusively through the façade.

### Changed
- **Removed `Turbofan.schemas_path` / `Turbofan.schemas_path=` shim** —
  these were duplicates of `Turbofan.config.schemas_path`. 20 internal
  call sites migrated to the canonical form. Undocumented shim; no
  user migration expected.
- **Privatized 5 ClassMethods on Step** — `uses_resources`,
  `writes_to_resources`, `uses_s3`, `writes_to_s3`,
  `add_duckdb_extensions`. Previously public-by-accident (no external
  docs, no external lib/ callers except iam.rb's `uses_s3`/
  `writes_to_s3` which now uses the façade). Tightens the public
  surface per Jeremy Evans's audit.

### Fixed
- **`Metrics#flush` under retry-budget pressure** — `Retryable.call`
  inside `flush` previously shared the global `max_retry_seconds`
  budget, which meant a SIGTERM-time flush could abort itself and
  lose the telemetry of the failure it was recording. Now passes
  `max_retry_seconds: nil` explicitly. Same fix applied to
  `OutputSerializer.call` and `Payload.serialize`.

### Removed
- **`oj` runtime dependency** — the gem was declared but never
  `require`d. Codebase uses stdlib `JSON` everywhere; modern Ruby's
  perf delta is small enough that dropping the dep wins on
  maintenance.

### Docs
- **Explicit "no mutex in trap context" banner** above
  `Turbofan::Runtime::Context#interrupt!` / `#interrupted?`. Prevents
  a future contributor from "fixing" what looks like a data race by
  wrapping in `Mutex#synchronize` — which would raise `ThreadError`
  from the SIGTERM trap handler and break graceful shutdown.
- **Zeitwerk-warning header on `lib/turbofan/errors.rb`** explaining
  why a `lib/turbofan/errors/` subdirectory would break the loader
  (file defines multiple top-level error constants; Zeitwerk would
  demand a `Turbofan::Errors` parent module that doesn't exist).

## [0.6.0] — 2026-04-19

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
