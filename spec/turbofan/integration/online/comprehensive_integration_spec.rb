# frozen_string_literal: true

require "spec_helper"
require "aws-sdk-cloudformation"
require "aws-sdk-states"
require "aws-sdk-s3"
require "aws-sdk-ecr"
require "aws-sdk-cloudwatch"
require "aws-sdk-cloudwatchlogs"
require "fileutils"
require "json"
require_relative "../support/pipeline_setup"

# ── Deploy + teardown ────────────────────────────────────────────────
#
# Prerequisites:
#   - Valid AWS credentials
#   - Docker daemon running with BuildKit support
#   - Compute environment stacks deployed:
#       turbofan ce deploy --stage staging          (TestCe)
#       turbofan ce deploy --stage staging --ce nvme_ce  (NvmeCe)
#
# Run with:
#   bundle exec rspec --tag deploy spec/turbofan/integration/online/

RSpec.describe "Comprehensive integration (online)", :deploy do # rubocop:disable RSpec/DescribeClass
  include_context "when using integration pipeline setup"

  let(:cf_client) { Aws::CloudFormation::Client.new }
  let(:sfn_client) { Aws::States::Client.new }
  let(:s3_client) { Aws::S3::Client.new(http_continue_timeout: 0) }
  let(:ecr_client) { Aws::ECR::Client.new }
  let(:cw_client) { Aws::CloudWatchLogs::Client.new }
  let(:cloudwatch_client) { Aws::CloudWatch::Client.new }
  let(:stack_name) { Turbofan::Naming.stack_name(pipeline_name, stage) }

  let(:gem_root) { File.expand_path("../../../..", __dir__) }
  let(:fixtures_root) { File.join(gem_root, "spec", "fixtures", "integration", "steps") }
  let(:schemas_dir) { File.join(gem_root, "spec", "fixtures", "schemas") }

  # For deploy, classify must be non-external so CloudFormation creates
  # an ECR repo for it. The Python Dockerfile builds the container image.
  let(:deploy_classify_class) do
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      execution :batch
      cpu 1
      ram 2
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  let(:deploy_steps_hash) do
    steps_hash.merge(classify: deploy_classify_class)
  end

  let(:step_dirs) do
    %i[retry_demo controlled_step fetch_brand read_visits classify build_items score_items aggregate].each_with_object({}) do |name, h|
      h[name] = File.join(fixtures_root, name.to_s)
    end
  end

  before do
    ENV["AWS_REGION"] ||= "us-east-1"
    # Re-stub Classify so pipeline DAG discovery finds the non-external version
    stub_const("Classify", deploy_classify_class)
  end

  after do
    # Clean up built gem files and config from step directories
    step_dirs.each_value do |dir|
      FileUtils.rm_f(File.join(dir, "turbofan-#{Turbofan::VERSION}.gem"))
      FileUtils.rm_f(File.join(dir, "integration_config.json"))
    end

    cfn_prefix = "turbofan-#{pipeline_name}-#{stage}"

    # Empty ECR repos so CloudFormation can delete them
    step_dirs.each_key do |step_name|
      repo = "#{cfn_prefix}-ecr-#{step_name}"
      images = ecr_client.list_images(repository_name: repo).image_ids
      ecr_client.batch_delete_image(repository_name: repo, image_ids: images) if images.any?
    rescue Aws::ECR::Errors::RepositoryNotFoundException
      nil # already gone
    end

    # Clean up pipeline data from shared S3 bucket
    system("aws", "s3", "rm", "s3://#{Turbofan.config.bucket}/#{Turbofan::Naming.bucket_prefix(pipeline_name, stage)}/", "--recursive", "--quiet", out: File::NULL, err: File::NULL)

    # Clean up external S3 writes from all executions
    @execution_arns&.each do |arn| # rubocop:disable RSpec/InstanceVariable
      system("aws", "s3", "rm", "s3://#{INTEGRATION_EXT_BUCKET}/turbofan-test/#{arn}/", "--recursive", "--quiet", out: File::NULL, err: File::NULL)
    end

    # Delete the pipeline stack
    delete_attempts = 0
    begin
      delete_attempts += 1
      cf_client.delete_stack(stack_name: stack_name)
      cf_client.wait_until(:stack_delete_complete, stack_name: stack_name) do |w|
        w.max_attempts = 60
        w.delay = 5
      end
    rescue Aws::CloudFormation::Errors::ValidationError
      nil # stack doesn't exist
    rescue Aws::Waiters::Errors::WaiterFailed, Aws::Waiters::Errors::FailureStateError
      if delete_attempts < 3
        # Empty ECR repos that block deletion
        step_dirs.each_key do |step_name|
          repo = "#{cfn_prefix}-ecr-#{step_name}"
          images = ecr_client.list_images(repository_name: repo).image_ids
          ecr_client.batch_delete_image(repository_name: repo, image_ids: images) if images.any?
        rescue Aws::ECR::Errors::RepositoryNotFoundException
          nil
        end
        retry
      end
    end

    # Clean up CFN template from S3
    system("aws", "s3", "rm", "s3://#{Turbofan.config.bucket}/turbofan-cfn-templates/#{stack_name}/", "--recursive", "--quiet", out: File::NULL, err: File::NULL)
  end

  it "deploys, runs 5 executions testing all features, and tears down" do # rubocop:disable RSpec/ExampleLength,RSpec/MultipleExpectations
    @execution_arns = [] # rubocop:disable RSpec/InstanceVariable

    # ═══════════════════════════════════════════════════════════════════
    # Phase 0: Prerequisites & Deploy
    # ═══════════════════════════════════════════════════════════════════
    ce_stack = TestCe.stack_name(stage)
    ce_state = Turbofan::Deploy::StackManager.detect_state(cf_client, ce_stack)
    skip "CE stack '#{ce_stack}' not deployed. Run: turbofan ce deploy --stage #{stage}" if ce_state == :does_not_exist

    nvme_stack = NvmeCe.stack_name(stage)
    nvme_state = Turbofan::Deploy::StackManager.detect_state(cf_client, nvme_stack)
    skip "CE stack '#{nvme_stack}' not deployed. Run: turbofan ce deploy --stage #{stage} --ce nvme_ce" if nvme_state == :does_not_exist

    # Build turbofan gem and copy to Ruby step directories
    gem_name = "turbofan-#{Turbofan::VERSION}.gem"
    gem_file = File.join(gem_root, gem_name)
    Dir.chdir(gem_root) do
      raise "gem build failed" unless system("gem build turbofan.gemspec -o #{gem_file} --quiet")
    end
    step_dirs.each do |name, dir|
      next if name == :classify # Python step — no gem needed
      FileUtils.cp(gem_file, File.join(dir, gem_name))
    end
    FileUtils.rm_f(gem_file)

    # Write integration config into each step dir for Docker builds
    config_json = JSON.generate({
      "secret_arn" => INTEGRATION_SECRET_ARN,
      "external_bucket" => INTEGRATION_EXT_BUCKET
    })
    step_dirs.each_value do |dir|
      File.write(File.join(dir, "integration_config.json"), config_json)
    end

    # Generate CloudFormation template with dashboard enabled
    cfn_generator = Turbofan::Generators::CloudFormation.new(
      pipeline: pipeline_class,
      steps: deploy_steps_hash,
      stage: stage,
      config: {},
      resources: {places_read: places_read_resource},
      dashboard: true
    )
    template = JSON.generate(cfn_generator.generate)

    Turbofan::Deploy::StackManager.deploy(
      cf_client,
      stack_name: stack_name,
      template_body: template,
      s3_client: s3_client,
      artifacts: cfn_generator.lambda_artifacts
    )

    state = Turbofan::Deploy::StackManager.detect_state(cf_client, stack_name)
    expect(state).to eq(:create_complete).or eq(:update_complete)

    # Authenticate ECR and build + push Docker images
    cfn_prefix = "turbofan-#{pipeline_name}-#{stage}"
    registry = Turbofan::Deploy::ImageBuilder.authenticate_ecr(ecr_client)

    configs = step_dirs.map do |step_name, step_dir|
      {
        step_dir: step_dir,
        schemas_dir: schemas_dir,
        ecr_client: ecr_client,
        repository_name: "#{cfn_prefix}-ecr-#{step_name}",
        repository_uri: "#{registry}/#{cfn_prefix}-ecr-#{step_name}",
        tag: "latest"
      }
    end
    Turbofan::Deploy::ImageBuilder.build_and_push_all(step_configs: configs)

    sm_arn = Turbofan::Deploy::StackManager.stack_output(cf_client, stack_name, "StateMachineArn")
    bucket = Turbofan.config.bucket

    # ═══════════════════════════════════════════════════════════════════
    # Phase 1: Dashboard resource check
    # ═══════════════════════════════════════════════════════════════════
    resources = cf_client.describe_stack_resources(stack_name: stack_name).stack_resources
    dashboard_resource = resources.find { |r| r.resource_type == "AWS::CloudWatch::Dashboard" }
    expect(dashboard_resource).not_to be_nil, "Expected CloudWatch Dashboard resource in stack"

    # ═══════════════════════════════════════════════════════════════════
    # Phase 2: CloudWatch Log Group naming and retention
    #   - Each step should have a log group with correct name
    #   - Retention should match Turbofan.config.log_retention_days
    # ═══════════════════════════════════════════════════════════════════
    step_dirs.each_key do |step_name|
      verify_log_group(cfn_prefix, step_name)
    end

    # ═══════════════════════════════════════════════════════════════════
    # Execution 1: Enhanced happy path
    #   - Envelope metadata (trace_id) flows via context.envelope
    #   - inject_secret env var verification
    #   - OpenLineage events in CloudWatch logs
    #   - Python container reports input format
    #   - All existing output verifications
    # ═══════════════════════════════════════════════════════════════════
    exec1_arn = start_execution(sfn_client, sm_arn,
      {inputs: [{key: "starbucks"}], trace_id: "test-trace-123"})
    @execution_arns << exec1_arn # rubocop:disable RSpec/InstanceVariable

    wait_and_assert_success(sfn_client, exec1_arn)

    # ── retry_demo: Batch retry + envelope metadata ──
    retry_out = read_s3_step_output(bucket, exec1_arn, "retry_demo")
    expect(retry_out["retried"]).to be true
    expect(retry_out["attempts"]).to eq(2)
    expect(retry_out["envelope"]).to(include("trace_id" => "test-trace-123"),
      "Expected context.envelope to contain trace_id from input envelope")

    # ── controlled_step: passthrough + inject_secret IAM access ──
    controlled_out = read_s3_step_output(bucket, exec1_arn, "controlled_step")
    expect(controlled_out["controlled_step_ran"]).to be true
    expect(controlled_out["secret_accessible"]).to(be(true),
      "Expected inject_secret to grant IAM access to SecretsManager secret")

    # ── fetch_brand: Postgres + NVMe ──
    fetch_brand_out = read_s3_step_output(bucket, exec1_arn, "fetch_brand")
    expect(fetch_brand_out["brand_name"]).to eq("Starbucks")
    expect(fetch_brand_out["source"]).to eq("postgres")
    expect(fetch_brand_out["nvme_used"]).to be true

    # ── read_visits: S3 read ──
    read_visits_out = read_s3_step_output(bucket, exec1_arn, "read_visits")
    expect(read_visits_out["row_count"]).to be > 0
    expect(read_visits_out["source"]).to eq("s3")

    # ── classify: Python container + input format ──
    classify_out = read_s3_step_output(bucket, exec1_arn, "classify")
    expect(classify_out["classification"]).to eq("food_and_beverage")
    expect(classify_out["language"]).to eq("python")
    expect(classify_out["input_keys"]).to(be_an(Array),
      "Expected Python container to report input_keys from S3 interchange format")

    # ── build_items: parallel join ──
    build_items_out = read_s3_step_output(bucket, exec1_arn, "build_items")
    expect(build_items_out["item_count"]).to eq(9)

    # ── score_items: routed fan-out per-size chunks ──
    %w[s m l].each do |size_name|
      key = "#{Turbofan::Naming.bucket_prefix(pipeline_name, stage)}/#{exec1_arn}/score_items/output/#{size_name}/0.json"
      response = s3_client.get_object(bucket: bucket, key: key)
      chunk_out = JSON.parse(response.body.read)
      expect(chunk_out["size"]).to eq(size_name)
      expect(chunk_out["scored"]).to be_an(Array)
      expect(chunk_out["scored"].size).to be > 0
    end

    # ── aggregate: fan-in + external S3 write ──
    aggregate_out = read_s3_step_output(bucket, exec1_arn, "aggregate")
    expect(aggregate_out["total_scored"]).to eq(9)
    expect(aggregate_out["chunks_received"]).to eq(6)

    external_obj = s3_client.get_object(
      bucket: INTEGRATION_EXT_BUCKET,
      key: "turbofan-test/#{exec1_arn}/summary.json"
    )
    external_data = JSON.parse(external_obj.body.read)
    expect(external_data["wrote_to_external_s3"]).to be true

    # ── OpenLineage: verify START/COMPLETE events in CloudWatch logs ──
    verify_openlineage_events(cfn_prefix, "retry_demo")

    # ── Structured logging: verify build_items log entry ──
    verify_structured_log(cfn_prefix, "build_items", "parallel_join_complete")

    # ── CLI Logs: verify turbofan logs with execution + step filter ──
    logs_output = capture_logs_output(pipeline_name, stage,
      step: "retry_demo", execution: exec1_arn)
    expect(logs_output).not_to(be_empty,
      "Expected turbofan logs --step retry_demo --execution to return output")

    # ── CLI Logs: verify custom query filter ──
    query_output = capture_logs_output(pipeline_name, stage,
      step: "build_items", query: 'message like /parallel_join_complete/')
    expect(query_output).not_to(be_empty,
      "Expected turbofan logs --query to find parallel_join_complete log entry")

    # ── Custom metrics: verify ItemsBuilt emitted to CloudWatch ──
    verify_custom_metrics(pipeline_name, stage)

    # ═══════════════════════════════════════════════════════════════════
    # Execution 2: SFN retry with error types (B2)
    #   - controlled_step has retries 2, on: ["States.TaskFailed"]
    #   - First SFN attempt: writes S3 marker, raises error
    #   - SFN retries → second attempt finds marker, succeeds
    # ═══════════════════════════════════════════════════════════════════
    exec2_arn = start_execution(sfn_client, sm_arn,
      {key: "starbucks", mode: "sfn_retry"})
    @execution_arns << exec2_arn # rubocop:disable RSpec/InstanceVariable

    wait_and_assert_success(sfn_client, exec2_arn)

    controlled_retry_out = read_s3_step_output(bucket, exec2_arn, "controlled_step")
    expect(controlled_retry_out["sfn_retry_worked"]).to(be(true),
      "Expected controlled_step to succeed after SFN retry (marker-based)")
    expect(controlled_retry_out["key"]).to eq("starbucks")

    # Verify the SFN retry marker was written
    marker = s3_client.get_object(
      bucket: bucket,
      key: "#{exec2_arn}/controlled_step/sfn_retry_marker"
    )
    expect(marker.body.read).to eq("retry-marker")

    # Verify the full pipeline completed after SFN retry
    aggregate_out_2 = read_s3_step_output(bucket, exec2_arn, "aggregate")
    expect(aggregate_out_2["total_scored"]).to eq(9)

    # ═══════════════════════════════════════════════════════════════════
    # Execution 3: Failure path
    #   - retry_demo raises on key="force_fail"
    #   - Pipeline goes through NotifyFailure catch path
    # ═══════════════════════════════════════════════════════════════════
    exec3_arn = start_execution(sfn_client, sm_arn, {key: "force_fail"})
    @execution_arns << exec3_arn # rubocop:disable RSpec/InstanceVariable

    wait_for_completion_safe(sfn_client, exec3_arn)
    statuses_3 = Turbofan::Deploy::Execution.step_statuses(sfn_client, execution_arn: exec3_arn)

    expect(statuses_3).to(have_key("NotifyFailure"),
      "Expected failure path to reach NotifyFailure state")
    expect(statuses_3.dig("NotifyFailure", :status)).to(eq("SUCCEEDED"),
      "Expected NotifyFailure SNS notification to succeed")

    # Verify OpenLineage FAIL event was emitted
    verify_openlineage_fail_event(cfn_prefix, "retry_demo")

    # ═══════════════════════════════════════════════════════════════════
    # Execution 4: Timeout path
    #   - controlled_step sleeps past its 60s timeout
    #   - Pipeline goes through NotifyFailure catch path
    # ═══════════════════════════════════════════════════════════════════
    exec4_arn = start_execution(sfn_client, sm_arn, {key: "starbucks", mode: "force_timeout"})
    @execution_arns << exec4_arn # rubocop:disable RSpec/InstanceVariable

    # Timeout test needs longer wait: Batch retry on retry_demo (~60s)
    # + controlled_step timeout (60s) + SFN processing
    wait_for_completion_safe(sfn_client, exec4_arn, timeout: 900)
    statuses_4 = Turbofan::Deploy::Execution.step_statuses(sfn_client, execution_arn: exec4_arn)

    expect(statuses_4).to(have_key("NotifyFailure"),
      "Expected timeout path to reach NotifyFailure state")
    expect(statuses_4.dig("NotifyFailure", :status)).to(eq("SUCCEEDED"),
      "Expected NotifyFailure SNS notification to succeed after timeout")

    # ═══════════════════════════════════════════════════════════════════
    # Execution 5: Schema rejection
    #   - Non-object items in input array fail validate_input!
    #   - Pipeline goes through NotifyFailure catch path
    # ═══════════════════════════════════════════════════════════════════
    exec5_arn = start_execution(sfn_client, sm_arn, [1, 2, 3])
    @execution_arns << exec5_arn # rubocop:disable RSpec/InstanceVariable

    wait_for_completion_safe(sfn_client, exec5_arn)
    statuses_5 = Turbofan::Deploy::Execution.step_statuses(sfn_client, execution_arn: exec5_arn)

    expect(statuses_5).to(have_key("NotifyFailure"),
      "Expected schema rejection to reach NotifyFailure state")

    # ═══════════════════════════════════════════════════════════════════
    # Post-execution: CLI::History verification
    # ═══════════════════════════════════════════════════════════════════
    history_output = capture_history_output(pipeline_name, stage)
    expect(history_output).to(include("SUCCEEDED"),
      "Expected CLI::History to show at least one SUCCEEDED execution")
    # Should show all 5 executions (some SUCCEEDED via NotifyFailure)
    execution_lines = history_output.lines.reject(&:empty?)
    expect(execution_lines.size).to be >= 5
  end

  private

  def start_execution(sfn_client, sm_arn, input)
    Turbofan::Deploy::Execution.start(
      sfn_client,
      state_machine_arn: sm_arn,
      input: JSON.generate({input: input})
    )
  end

  def wait_and_assert_success(sfn_client, execution_arn)
    result = Turbofan::Deploy::Execution.wait_for_completion(
      sfn_client,
      execution_arn: execution_arn,
      timeout: 900,
      poll_interval: 15
    )

    statuses = Turbofan::Deploy::Execution.step_statuses(sfn_client, execution_arn: execution_arn)

    # If NotifyFailure was reached, the pipeline's happy path failed
    if statuses.dig("NotifyFailure", :status) == "SUCCEEDED"
      diagnose_failure(sfn_client, execution_arn, statuses)
      raise "Pipeline went through NotifyFailure path — step(s) failed."
    end

    expect(result[:status]).to eq("SUCCEEDED")
  end

  def wait_for_completion_safe(sfn_client, execution_arn, timeout: 600)
    # For failure/timeout executions, the SFN may SUCCEED (via NotifyFailure)
    # or may FAIL/TIME_OUT. Handle all cases gracefully.
    Turbofan::Deploy::Execution.wait_for_completion(
      sfn_client,
      execution_arn: execution_arn,
      timeout: timeout,
      poll_interval: 15
    )
  rescue RuntimeError => e
    # Execution FAILED or TIMED_OUT — that's expected for failure tests
    {status: e.message}
  end

  def diagnose_failure(sfn_client, execution_arn, statuses)
    cfn_prefix = "turbofan-#{pipeline_name}-#{stage}"

    statuses.each do |name, info|
      next unless info[:status] == "FAILED"

      log_messages = fetch_step_logs(cfn_prefix, name)
      next unless log_messages

      warn("[Turbofan] FAILED step '#{name}' logs:")
      warn(log_messages)
    end
  end

  def read_s3_step_output(bucket, execution_id, step_name)
    prefix = Turbofan::Naming.bucket_prefix(pipeline_name, stage)
    key = "#{prefix}/#{execution_id}/#{step_name}/output.json"
    response = s3_client.get_object(bucket: bucket, key: key)
    JSON.parse(response.body.read)
  end

  def verify_openlineage_events(cfn_prefix, step_name)
    log_messages = fetch_step_logs(cfn_prefix, step_name)
    if log_messages.nil?
      warn "[Turbofan] WARNING: No CloudWatch logs found for #{step_name} — skipping OpenLineage verification"
      return
    end

    expect(log_messages).to(include("OpenLineage event"),
      "Expected OpenLineage events in #{step_name} CloudWatch logs")
    %w[START COMPLETE].each do |event_type|
      assert_log_contains_event_type(log_messages, event_type, step_name)
    end
  end

  def verify_openlineage_fail_event(cfn_prefix, step_name)
    log_messages = fetch_step_logs(cfn_prefix, step_name)
    if log_messages.nil?
      warn "[Turbofan] WARNING: No CloudWatch logs found for #{step_name} — skipping FAIL event verification"
      return
    end

    # The FAIL event should be present from the force_fail execution
    # Note: logs accumulate across executions in the same log group
    assert_log_contains_event_type(log_messages, "FAIL", step_name, suffix: " after forced failure")
  end

  def assert_log_contains_event_type(log_messages, event_type, step_name, suffix: "")
    expect(log_messages)
      .to(include("\"eventType\":\"#{event_type}\"").or(include("\"eventType\": \"#{event_type}\"")),
        "Expected OpenLineage #{event_type} event in #{step_name} logs#{suffix}")
  end

  def verify_structured_log(cfn_prefix, step_name, expected_message)
    log_messages = fetch_step_logs(cfn_prefix, step_name)
    if log_messages.nil?
      warn "[Turbofan] WARNING: No CloudWatch logs found for #{step_name} — skipping structured log verification"
      return
    end

    expect(log_messages).to(include(expected_message),
      "Expected structured log entry '#{expected_message}' in #{step_name} CloudWatch logs")
  end

  def fetch_step_logs(cfn_prefix, step_name)
    fetch_recent_logs("#{cfn_prefix}-logs-#{step_name}")
  end

  def fetch_recent_logs(log_group)
    streams = cw_client.describe_log_streams(
      log_group_name: log_group, order_by: "LastEventTime", descending: true, limit: 3
    )
    return nil unless streams.log_streams.any?

    streams.log_streams.map { |stream|
      events = cw_client.get_log_events(
        log_group_name: log_group,
        log_stream_name: stream.log_stream_name
      )
      events.events.map(&:message)
    }.flatten.join("\n")
  rescue Aws::CloudWatchLogs::Errors::ResourceNotFoundException
    nil
  end

  def capture_history_output(pipeline_name, stage)
    output = StringIO.new
    original_stdout = $stdout
    begin
      $stdout = output
      Turbofan::CLI::History.call(pipeline_name: pipeline_name, stage: stage, limit: 20)
    ensure
      $stdout = original_stdout
    end
    output.string
  end

  def capture_logs_output(pipeline_name, stage, step:, execution: nil, query: nil)
    output = StringIO.new
    original_stdout = $stdout
    begin
      $stdout = output
      Turbofan::CLI::Logs.call(
        pipeline_name: pipeline_name,
        stage: stage,
        step: step,
        execution: execution,
        query: query,
        logs_client: cw_client
      )
    ensure
      $stdout = original_stdout
    end
    output.string
  end

  def verify_log_group(cfn_prefix, step_name)
    log_group_name = "#{cfn_prefix}-logs-#{step_name}"
    log_groups = cw_client.describe_log_groups(
      log_group_name_prefix: log_group_name
    ).log_groups
    log_group = log_groups.find { |lg| lg.log_group_name == log_group_name }

    expect(log_group).not_to(be_nil, "Expected log group #{log_group_name} to exist")
    expect(log_group.retention_in_days).to(eq(Turbofan.config.log_retention_days),
      "Expected #{log_group_name} retention to be #{Turbofan.config.log_retention_days} days")
  end

  def verify_custom_metrics(pipeline_name, stage)
    # CloudWatch metrics may take 1-2 minutes to become queryable;
    # by execution 1 completion, build_items metrics should be available,
    # but retry briefly in case of propagation delay.
    metric_stats = nil
    6.times do |attempt|
      metric_stats = cloudwatch_client.get_metric_statistics(
        namespace: "Turbofan/#{pipeline_name}",
        metric_name: "ItemsBuilt",
        dimensions: [
          {name: "Pipeline", value: pipeline_name},
          {name: "Stage", value: stage},
          {name: "Step", value: "build_items"}
        ],
        start_time: Time.now - 3600,
        end_time: Time.now,
        period: 3600,
        statistics: ["Sum"]
      )
      break if metric_stats.datapoints.any?
      sleep(10) if attempt < 5
    end

    expect(metric_stats.datapoints).not_to(be_empty,
      "Expected CloudWatch to have ItemsBuilt metric in Turbofan/#{pipeline_name} namespace")
    total = metric_stats.datapoints.sum(&:sum)
    expect(total).to(be >= 9.0,
      "Expected ItemsBuilt Sum >= 9 (got #{total})")
  end
end
