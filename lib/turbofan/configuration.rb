# frozen_string_literal: true

module Turbofan
  class Configuration
    attr_accessor :bucket, :schemas_path, :default_region,
      :log_retention_days, :notification_topic_arn, :docker_registry,
      :duckdb_version, :aws_account_id, :subnets, :security_groups,
      :cur_s3_uri, :fan_out_early_exit_threshold

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
    end
  end

  def self.config
    @config ||= Configuration.new
  end

  def self.configure
    yield config
  end
end
