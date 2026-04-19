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
  `WorkerErrors`) are now reparented under this hierarchy. Users can
  `rescue Turbofan::Error` for generic handling or the specific
  subclasses for targeted logic. `Turbofan::Interrupted` intentionally
  stays a `SystemExit` subclass (AWS Batch exit-code 143 contract).
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
  and verifies `require "turbofan"` loads. Bundle-audit blocks on
  CVEs; rubocop is advisory.

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

### Fixed
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
