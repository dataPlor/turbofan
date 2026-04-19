# frozen_string_literal: true

require "aws-sdk-core"
require "seahorse/client/networking_error"

module Turbofan
  # Retry AWS SDK calls on transient errors with full-jitter exponential backoff.
  #
  # AWS experts reviewed this design (see commit history). Key decisions:
  #
  #   * Error matching by code + HTTP status, NOT by pre-defined classes —
  #     AWS SDK v3 generates error classes dynamically from service response
  #     codes (`Aws::S3::Errors::SlowDown` does not exist at load time).
  #     Rescue the `Aws::Errors::ServiceError` umbrella + `Seahorse::Client::
  #     NetworkingError` and filter via `transient?`.
  #
  #   * Callers MUST pass `retry_mode: "standard", max_attempts: 1` when
  #     constructing AWS SDK clients that will be wrapped by Retryable.
  #     Otherwise SDK's built-in 3-retry stacks on top of ours, producing 15
  #     attempts and obscuring telemetry. (The legacy `retry_limit: 0` kwarg
  #     is ignored in standard/adaptive retry modes.) See `context.rb` and
  #     `metrics.rb` for the settings on lazily-constructed clients.
  #
  #   * Full-jitter backoff (AWS recommendation): delay = uniform(0, backoff)
  #     where backoff = min(cap, base * 2^(attempt-1)). Prevents thundering
  #     herd on retry cohorts (10,000 concurrent Batch children recovering
  #     together from a throttle burst).
  #
  #   * `NoSuchKey` must pass through unretried (sentinel-skip semantics for
  #     Batch array-job padding). Rescue it at the caller, outside the
  #     Retryable.call block.
  #
  # Usage:
  #
  #   Retryable.call do
  #     s3_client.get_object(bucket:, key:)
  #   end
  #
  #   # Caller preserves NoSuchKey sentinel-skip semantics:
  #   begin
  #     response = Retryable.call { s3_client.get_object(bucket:, key:) }
  #     process(response)
  #   rescue Aws::S3::Errors::NoSuchKey
  #     # Sentinel chunk — no output written, skip
  #   end
  module Retryable
    # Transient AWS error codes across S3, CloudWatch, SecretsManager, etc.
    # Matched case-sensitively against `error.code`.
    TRANSIENT_CODES = %w[
      Throttling
      ThrottlingException
      SlowDown
      ServiceUnavailable
      InternalError
      InternalFailure
      RequestTimeout
      RequestTimeoutException
      RequestLimitExceeded
      RequestThrottled
      RequestThrottledException
      LimitExceededException
    ].freeze

    # HTTP status codes that indicate a transient failure regardless of
    # service-specific error code: 408 (Request Timeout), 429 (Too Many
    # Requests), 500-599 (server errors).
    MAX_ATTEMPTS_LIMIT = 20

    # Optional `logger:` kwarg takes any object responding to `#info(msg, **kwargs)`
    # (including Turbofan::Runtime::Logger). When present, emits one structured
    # entry per retry attempt with: attempt, max, error_class, code, delay_ms.
    # Default `nil` keeps Retryable silent — no behavior change for existing
    # callers. Pass `logger: context.logger` at call sites that have a Context.
    #
    # Optional `metrics:` kwarg takes any object responding to
    # `#emit(name, value, unit:)` (including Turbofan::Runtime::Metrics).
    # Emits two distinct metrics:
    #
    #   RetryAttempt      — one datapoint per individual retry. Graph to
    #                       observe retry rate (e.g. throttle storms).
    #   RetriesExhausted  — one datapoint when max is hit without success.
    #                       Page-worthy alert signal; distinct from the
    #                       aggregated retry-rate series so you can alert
    #                       on "we gave up" without false positives from
    #                       "we retried and eventually succeeded."
    #
    # Dimensions follow Mike Perham's cardinality discipline — only the
    # low-cardinality Pipeline/Stage/Step/Size dimensions inherited from
    # the Metrics object are used. Error codes, request IDs, and other
    # high-cardinality values are NOT added as dimensions (CloudWatch
    # bills per unique combination).
    # Sentinel distinguishing "caller didn't pass max_retry_seconds:"
    # from "caller passed max_retry_seconds: nil to explicitly bypass."
    # Default arg uses the config value; explicit nil bypasses even when
    # the global config is set. Used by terminal-write call sites
    # (Metrics#flush, OutputSerializer, Payload.serialize) that must
    # not self-abort during SIGTERM.
    UNSET = Object.new.freeze
    private_constant :UNSET

    def self.call(max: 5, base: 0.5, cap: 30,
                  max_retry_seconds: UNSET,
                  sleeper: Kernel.method(:sleep),
                  jitter_rand: Kernel.method(:rand),
                  logger: nil,
                  metrics: nil)
      raise ArgumentError, "block required" unless block_given?
      raise ArgumentError, "max must be 1..#{MAX_ATTEMPTS_LIMIT}, got #{max}" unless max.is_a?(Integer) && max >= 1 && max <= MAX_ATTEMPTS_LIMIT
      raise ArgumentError, "base must be > 0, got #{base}" unless base.is_a?(Numeric) && base > 0
      raise ArgumentError, "cap must be > 0, got #{cap}" unless cap.is_a?(Numeric) && cap > 0

      attempt = 0
      elapsed_sleep = 0.0
      # Per-call override wins over the global config. `nil` (either
      # passed explicitly or from config) means unbounded.
      budget = max_retry_seconds.equal?(UNSET) ? Turbofan.config.max_retry_seconds : max_retry_seconds
      begin
        yield
      rescue => e
        attempt += 1
        raise unless transient?(e)
        if attempt > max
          # Final attempt exhausted. Emit RetriesExhausted before re-raising
          # so operators see the terminal failure in metrics even when the
          # caller doesn't rescue.
          metrics&.emit("RetriesExhausted", 1)
          raise
        end
        backoff = [cap, base * (2**(attempt - 1))].min.to_f
        delay = jitter_rand.call * backoff
        # Retry budget check BEFORE sleeping. Guards against the
        # "Retryable.call holds the thread for ~10 min on a Spot node
        # with 2-min SIGTERM notice" failure mode Mike flagged. When
        # Turbofan.config.max_retry_seconds is nil (default), budget
        # is skipped.
        if budget && elapsed_sleep + delay > budget
          # Distinct metric from RetriesExhausted. Operators need to
          # distinguish "gave up on attempt count" from "gave up on
          # wall-clock budget" — they're different failure modes that
          # warrant different alert thresholds. RetryBudgetExhausted
          # usually implies the service is degraded for longer than
          # we're willing to wait (Spot reclamation, SLO pressure);
          # RetriesExhausted usually implies persistent failure.
          # Mike Perham's final-review ask.
          metrics&.emit("RetryBudgetExhausted", 1)
          raise Turbofan::RetryBudgetExhausted.new(
            elapsed_seconds: elapsed_sleep,
            budget_seconds: budget,
            last_error: e
          )
        end
        logger&.info("Retryable: transient error, retrying",
          attempt: attempt,
          max: max,
          error_class: e.class.name,
          code: (e.respond_to?(:code) ? e.code : nil),
          delay_ms: (delay * 1000).round)
        metrics&.emit("RetryAttempt", 1)
        sleeper.call(delay)
        elapsed_sleep += delay
        retry
      end
    end

    # Classify an error as transient (worth retrying) or permanent. Callers
    # can reuse this predicate when their rescue needs to distinguish without
    # going through Retryable.call.
    def self.transient?(error)
      return true if error.is_a?(Seahorse::Client::NetworkingError)
      return false unless error.is_a?(Aws::Errors::ServiceError)

      code = error.code if error.respond_to?(:code)
      return true if code && TRANSIENT_CODES.include?(code)

      status = http_status_code(error)
      return true if status == 408 || status == 429
      return true if status && (500..599).cover?(status)

      false
    end

    # AWS SDK v3 exposes HTTP status via error.context.http_response.status_code.
    # Nil-safe chain — some error instances (e.g. constructed in tests with
    # nil context) may not have a full context chain.
    def self.http_status_code(error)
      return nil unless error.respond_to?(:context)
      context = error.context
      return nil if context.nil?
      response = context.respond_to?(:http_response) ? context.http_response : nil
      response&.status_code
    end
    private_class_method :http_status_code
  end
end
