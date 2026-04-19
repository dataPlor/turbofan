# frozen_string_literal: true

module Turbofan
  class Configuration
    attr_accessor :bucket, :schemas_path, :default_region,
      :log_retention_days, :notification_topic_arn, :docker_registry,
      :duckdb_version, :aws_account_id, :subnets, :security_groups,
      :cur_s3_uri, :fan_out_early_exit_threshold, :max_retry_seconds,
      :worker_stall_seconds

    def initialize
      @bucket = nil
      @schemas_path = nil
      @default_region = nil
      @log_retention_days = 30
      @notification_topic_arn = nil
      @docker_registry = nil
      @duckdb_version = "1.4.3"
      @aws_account_id = nil
      @subnets = []
      @security_groups = []
      @cur_s3_uri = nil
      # nil = preserve original all-workers-complete behavior. Operators
      # can set this to an Integer N to have FanOut.threaded_work abort
      # remaining work after N non-transient worker failures (poison-pill
      # protection). Transient errors (AWS throttling, networking) do
      # NOT count toward this threshold — they deserve backoff, not
      # early exit.
      @fan_out_early_exit_threshold = nil

      # Per-Retryable.call cumulative-sleep cap in seconds. When set,
      # Retryable.call raises Turbofan::RetryBudgetExhausted if the
      # accumulated sleep time across retries exceeds the budget. nil
      # (default) = unbounded, preserving historical behavior.
      #
      # Mike Perham's ask: a single Retryable call against a throttled
      # service could otherwise block for MAX_ATTEMPTS_LIMIT (20) * cap
      # (30s) = ~10 minutes. On a Spot node with 2-minute SIGTERM
      # notice, that's a guaranteed data-loss window. Setting this to
      # e.g. 90 lets ops cap individual retryable operations well below
      # the Spot reclamation horizon.
      @max_retry_seconds = nil

      # Number of seconds a FanOut worker can hold a single item without
      # finishing before a stall warning is emitted. When set to a
      # positive Integer, threaded_work spawns a coordinator thread that
      # periodically checks each worker's last-progress heartbeat and
      # warns for any worker exceeding this threshold. nil = disabled
      # (no coordinator, no overhead).
      #
      # Catches the deadlock / slow-SQL / hung-HTTP class of bugs that
      # Sidekiq's equivalent feature exists for — a worker that's not
      # crashing but also not making progress should be loud.
      @worker_stall_seconds = nil
    end
  end

  def self.config
    @config ||= Configuration.new
  end

  def self.configure
    yield config
  end
end
