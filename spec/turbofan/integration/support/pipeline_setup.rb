require "yaml"

INTEGRATION_CONFIG = begin
  path = File.join(__dir__, "..", "config.yml")
  File.exist?(path) ? YAML.safe_load_file(path) : {}
end

INTEGRATION_BUCKET       = INTEGRATION_CONFIG.fetch("bucket", "my-turbofan-bucket")
INTEGRATION_SECRET_ARN   = INTEGRATION_CONFIG.fetch("secret_arn", "arn:aws:secretsmanager:us-east-1:123456789012:secret:myapp/DATABASE_URL-AbCdEf")
INTEGRATION_EXT_BUCKET   = INTEGRATION_CONFIG.fetch("external_bucket", "my-data-bucket")

RSpec.shared_context "when using integration pipeline setup" do
  # ── Compute environments ────────────────────────────────────────────
  let(:ce_class) do
    Class.new { include Turbofan::ComputeEnvironment }
  end

  let(:nvme_ce_class) do
    Class.new { include Turbofan::ComputeEnvironment }
  end

  # ── Resource: Postgres places_read ───────────────────────────────────
  let(:places_read_resource) do
    Class.new do
      include Turbofan::Postgres

      key :places_read
      consumable 100
      secret INTEGRATION_SECRET_ARN
      database "places_read"
    end
  end

  # ── Step classes ─────────────────────────────────────────────────────

  # Step 0: Retry demo — exits 143 on first attempt, succeeds on second
  #         Supports force_fail mode via key="force_fail".
  #         Reports context.envelope metadata in output.
  let(:retry_demo_class) do
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      execution :batch
      cpu 1
      ram 2
      retries 2
      input_schema "passthrough.json"
      output_schema "passthrough.json"

      def call(inputs, context)
        key = inputs.first&.dig("key")
        mode = inputs.first&.dig("mode")

        raise "Intentional failure for integration testing" if key == "force_fail"

        if context.attempt_number == 1
          context.s3.put_object(
            bucket: ENV.fetch("TURBOFAN_BUCKET", "turbofan-data"),
            key: "#{context.execution_id}/retry_demo/attempt_1_marker",
            body: "attempted"
          )
          exit(143) # triggers Batch retry
        end

        {
          "retried" => true,
          "attempts" => context.attempt_number,
          "key" => key || "starbucks",
          "mode" => mode,
          "envelope" => context.envelope
        }
      end
    end
  end

  # Step 0.5: Controlled step — tests SFN retry, inject_secret, timeout
  #           - retries 2, on: ["States.TaskFailed"] → SFN-level retry
  #           - inject_secret :pg_url → env var injection
  #           - timeout 60 → short timeout for force_timeout testing
  #           - Supports modes: sfn_retry, force_timeout, passthrough
  let(:controlled_step_class) do
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      execution :batch
      cpu 1
      ram 2
      timeout 60
      retries 2, on: ["States.TaskFailed"]
      inject_secret :pg_url, from: INTEGRATION_SECRET_ARN
      input_schema "passthrough.json"
      output_schema "passthrough.json"

      def call(inputs, context)
        mode = inputs.first&.dig("mode")
        key = inputs.first&.dig("key")

        case mode
        when "sfn_retry"
          handle_sfn_retry(key, context)
        when "force_timeout"
          sleep(180) # exceed the 60s timeout
          {"timed_out" => false}
        else
          {
            "key" => key,
            "controlled_step_ran" => true,
            "secret_accessible" => verify_secret_access(context),
            "envelope_metadata" => context.envelope
          }
        end
      end

      private

      def verify_secret_access(context)
        secret_arn = INTEGRATION_SECRET_ARN
        context.secrets_client.get_secret_value(secret_id: secret_arn)
        true
      rescue StandardError
        false
      end

      def handle_sfn_retry(key, context)
        marker_key = "#{context.execution_id}/controlled_step/sfn_retry_marker"
        bucket = ENV.fetch("TURBOFAN_BUCKET")

        begin
          context.s3.get_object(bucket: bucket, key: marker_key)
          {
            "key" => key,
            "sfn_retry_worked" => true,
            "controlled_step_ran" => true
          }
        rescue Aws::S3::Errors::NoSuchKey
          context.s3.put_object(bucket: bucket, key: marker_key, body: "retry-marker")
          raise "Intentional SFN retry test failure"
        end
      end
    end
  end

  # Step 1: Serial + Postgres + NVMe — query brand table via DuckDB ATTACH
  let(:fetch_brand_class) do
    Class.new do
      include Turbofan::Step

      compute_environment :nvme_ce
      execution :batch
      cpu 1
      ram 2
      uses :places_read
      input_schema "passthrough.json"
      output_schema "passthrough.json"

      def call(inputs, context)
        result = context.duckdb.query(
          "SELECT name FROM places_read.public.brand WHERE key = ?",
          inputs.first["key"]
        )
        {
          "brand_name" => result.first["name"],
          "key" => inputs.first["key"],
          "source" => "postgres",
          "storage_available" => !context.storage_path.nil?
        }
      end
    end
  end

  # Step 2a: Parallel branch A + S3 — read visit percentiles CSV
  let(:read_visits_class) do
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      execution :batch
      cpu 1
      ram 2
      uses "s3://#{INTEGRATION_EXT_BUCKET}/analytics_data/test/"
      input_schema "passthrough.json"
      output_schema "passthrough.json"

      def call(inputs, context)
        require "csv"
        require "zlib"
        obj = context.s3.get_object(
          bucket: INTEGRATION_EXT_BUCKET,
          key: "analytics_data/test/sample_data.csv.gz"
        )
        csv_data = Zlib::GzipReader.new(obj.body).read
        rows = CSV.parse(csv_data, headers: true)
        {
          "brand_name" => inputs.first["brand_name"],
          "row_count" => rows.size,
          "columns" => rows.headers,
          "source" => "s3"
        }
      end
    end
  end

  # Step 2b: Parallel branch B + external Python container
  let(:classify_class) do
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      execution :batch
      cpu 1
      ram 2
      docker_image "123456789.dkr.ecr.us-east-1.amazonaws.com/classify:latest"
      input_schema "passthrough.json"
      output_schema "passthrough.json"

      def call(inputs, context)
        {
          "brand_name" => inputs.first["brand_name"],
          "classification" => "food_and_beverage",
          "language" => "python",
          "source" => "external_container"
        }
      end
    end
  end

  # Step 3: Serial after parallel join — build work items for fan-out
  #         Uses context.logger and context.metrics for observability testing.
  #         Annotates items with __turbofan_size for routed fan-out.
  let(:build_items_class) do
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      execution :batch
      cpu 1
      ram 2
      input_schema "passthrough.json"
      output_schema "passthrough.json"

      def call(inputs, context)
        brand = inputs.first["brand_name"]

        context.logger.info("parallel_join_complete", brand: brand, input_count: inputs.size)
        context.metrics.emit("ItemsBuilt", 9)

        sizes = %w[s m l]
        {
          "items" => (0..8).map { |i|
            {"id" => i, "brand_name" => brand, "__turbofan_size" => sizes[i % 3]}
          },
          "item_count" => 9
        }
      end
    end
  end

  # Step 4: Fan-out + chunking + per-size routing — score each chunk of items
  let(:score_items_class) do
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      execution :batch
      size :s, cpu: 1, ram: 2
      size :m, cpu: 2, ram: 4
      size :l, cpu: 4, ram: 8
      timeout 300
      batch_size 2
      input_schema "passthrough.json"
      output_schema "passthrough.json"

      def call(inputs, context)
        items = inputs || []
        {
          "chunk_index" => context.array_index || 0,
          "size" => context.size,
          "scored" => items.map { |item| item.merge("score" => rand(100)) },
          "scored_count" => items.size
        }
      end
    end
  end

  # Step 5: Fan-in — aggregate all chunk results + writes_to external S3
  let(:aggregate_class) do
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      execution :batch
      cpu 1
      ram 2
      writes_to "s3://#{INTEGRATION_EXT_BUCKET}/turbofan-test/"
      input_schema "passthrough.json"
      output_schema "passthrough.json"

      def call(inputs, context)
        chunks = inputs
        total = chunks.sum { |c| c["scored_count"] || 0 }

        # Write summary to external S3
        summary = {"total_scored" => total, "wrote_to_external_s3" => true}
        context.s3.put_object(
          bucket: INTEGRATION_EXT_BUCKET,
          key: "turbofan-test/#{context.execution_id}/summary.json",
          body: JSON.generate(summary)
        )

        {
          "total_scored" => total,
          "chunks_received" => chunks.size,
          "source" => "fan_in"
        }
      end
    end
  end

  # ── Pipeline class ───────────────────────────────────────────────────
  let(:pipeline_class) do
    Class.new do
      include Turbofan::Pipeline

      pipeline_name "integration-test"

      pipeline do
        retried = retry_demo(trigger_input)
        controlled = controlled_step(retried)
        brand = fetch_brand(controlled)
        read_visits(brand)                       # ─┐ parallel: same parent
        classified = classify(brand)             # ─┘
        items = build_items(classified)          # after parallel join
        scored = fan_out(score_items(items))
        aggregate(scored)
      end
    end
  end

  # ── Shared helpers ───────────────────────────────────────────────────
  let(:stage) { "staging" }
  let(:pipeline_name) { "integration-test" }
  let(:steps_hash) do
    {
      retry_demo: retry_demo_class,
      controlled_step: controlled_step_class,
      fetch_brand: fetch_brand_class,
      read_visits: read_visits_class,
      classify: classify_class,
      build_items: build_items_class,
      score_items: score_items_class,
      aggregate: aggregate_class
    }
  end

  before do
    stub_const("ComputeEnvironments::IntegrationCe", ce_class)
    stub_const("NvmeCe", nvme_ce_class)
    stub_const("ComputeEnvironments::NvmeCe", nvme_ce_class)
    stub_const("PlacesReadResource", places_read_resource)
    stub_const("RetryDemo", retry_demo_class)
    stub_const("ControlledStep", controlled_step_class)
    stub_const("FetchBrand", fetch_brand_class)
    stub_const("ReadVisits", read_visits_class)
    stub_const("Classify", classify_class)
    stub_const("BuildItems", build_items_class)
    stub_const("ScoreItems", score_items_class)
    stub_const("Aggregate", aggregate_class)
    stub_const("IntegrationTest", pipeline_class)
    Turbofan.config.bucket = INTEGRATION_BUCKET
    Turbofan.schemas_path = FIXTURE_SCHEMAS_DIR
  end

  after do
    Turbofan.schemas_path = nil
  end
end
