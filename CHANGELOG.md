# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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

### Changed
- **Breaking (internal API):** `Turbofan::Runtime::FanOut.threaded_work`
  now raises `WorkerError` (single failure) or `WorkerErrors` (multiple
  failures) instead of the raw underlying exception. Callers that
  rescued specific exception classes from inside `threaded_work` must
  now rescue `WorkerError` and inspect `#cause`, or iterate
  `WorkerErrors#errors`. `rescue StandardError` sites are unaffected.

### Fixed
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
