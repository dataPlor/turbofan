require "spec_helper"
require "json"
require_relative "../support/pipeline_setup"

RSpec.describe "Comprehensive integration (offline)", :schemas do # rubocop:disable RSpec/DescribeClass
  include_context "when using integration pipeline setup"

  let(:asl) do
    Turbofan::Generators::ASL.new(
      pipeline: pipeline_class,
      stage: stage,
      steps: steps_hash
    ).generate
  end

  let(:step_dirs) do
    {
      score_items: File.expand_path("../../../fixtures/integration/steps/score_items", __dir__)
    }
  end

  let(:cfn) do
    Turbofan::Generators::CloudFormation.new(
      pipeline: pipeline_class,
      steps: steps_hash,
      stage: stage,
      config: {},
      resources: {places_read: places_read_resource},
      step_dirs: step_dirs
    ).generate
  end

  # Builds a minimal Step class for tests that only need structural validation
  def build_passthrough_step
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      execution :batch
      cpu 1
      batch_size 1
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  # ── DAG structure tests ──────────────────────────────────────────────

  describe "DAG construction" do
    let(:dag) { pipeline_class.turbofan_dag }

    it "builds a DAG with 8 steps" do
      expect(dag.steps.map(&:name)).to contain_exactly(
        :retry_demo, :controlled_step, :fetch_brand, :read_visits, :classify, :build_items, :score_items, :aggregate
      )
    end

    it "retry_demo chains to controlled_step then fetch_brand" do
      expect(dag.edges).to include({from: :retry_demo, to: :controlled_step})
      expect(dag.edges).to include({from: :controlled_step, to: :fetch_brand})
    end

    it "detects parallel steps (read_visits and classify share the same parent)" do
      edges_from_fetch_brand = dag.edges.select { |e| e[:from] == :fetch_brand }
      expect(edges_from_fetch_brand.map { |e| e[:to] }).to contain_exactly(:read_visits, :classify)
    end

    it "marks score_items as fan_out with group 2" do
      score = dag.steps.find { |s| s.name == :score_items }
      expect(score.fan_out?).to be true
      score_class = steps_hash[:score_items]
      expect(score_class.turbofan_batch_size).to eq(2)
    end

    it "has correct serial chain after parallel join" do
      expect(dag.edges).to include({from: :classify, to: :build_items})
      expect(dag.edges).to include({from: :build_items, to: :score_items})
      expect(dag.edges).to include({from: :score_items, to: :aggregate})
    end
  end

  # ── ASL generation tests ─────────────────────────────────────────────

  describe "ASL generation" do
    let(:parallel_state) do
      asl["States"].values.find { |s| s["Type"] == "Parallel" && s.dig("ResultPath")&.include?("parallel") }
    end

    it "starts at retry_demo" do
      expect(asl["StartAt"]).to eq("retry_demo")
    end

    it "generates a Parallel state for the two branches" do
      expect(parallel_state).not_to be_nil, "Expected a Parallel state for branches in the ASL but none was found"
    end

    it "Parallel state has two branches" do
      expect(parallel_state["Branches"].size).to eq(2)
    end

    it "Parallel state branches contain read_visits and classify" do
      branch_step_names = parallel_state["Branches"].map { |b|
        b["States"].keys.first
      }
      expect(branch_step_names).to contain_exactly("read_visits", "classify")
    end

    it "includes a chunking state before score_items" do
      expect(asl["States"]).to have_key("score_items_chunk")
    end

    it "score_items chunk state has routed flag" do
      chunk = asl["States"]["score_items_chunk"]
      expect(chunk.dig("Parameters", "Payload", "routed")).to be true
    end

    it "generates a routed Parallel for score_items with 3 size branches" do
      routed = asl["States"]["score_items_routed"]
      expect(routed).not_to be_nil
      expect(routed["Type"]).to eq("Parallel")
      expect(routed["Branches"].size).to eq(3)
    end

    it "each routed branch sets TURBOFAN_SIZE" do
      routed = asl["States"]["score_items_routed"]
      sizes = routed["Branches"].map { |b|
        map_state = b["States"].values.first
        inner_task = map_state.dig("ItemProcessor", "States").values.first
        env = inner_task.dig("Parameters", "ContainerOverrides", "Environment")
        env.find { |e| e["Name"] == "TURBOFAN_SIZE" }&.dig("Value")
      }
      expect(sizes).to contain_exactly("s", "m", "l")
    end

    it "has correct state chain: ... -> build_items -> score_items_chunk -> score_items_routed -> aggregate -> NotifySuccess" do
      states = asl["States"]

      # retry_demo points to controlled_step
      expect(states["retry_demo"]["Next"]).to eq("controlled_step")

      # controlled_step points to fetch_brand
      expect(states["controlled_step"]["Next"]).to eq("fetch_brand")

      # fetch_brand points to the parallel state
      parallel_key = states["fetch_brand"]["Next"]
      expect(states[parallel_key]["Type"]).to eq("Parallel")

      # parallel state points to build_items
      expect(states[parallel_key]["Next"]).to eq("build_items")

      # linear chain after parallel: combined routing+chunking -> routed parallel -> aggregate
      expect(states["build_items"]["Next"]).to eq("score_items_chunk")
      expect(states["score_items_chunk"]["Next"]).to eq("score_items_routed")
      expect(states["score_items_routed"]["Next"]).to eq("aggregate")
      expect(states["aggregate"]["Next"]).to eq("NotifySuccess")
      expect(states).not_to have_key("score_items_route")
    end

    it "controlled_step ASL state has SFN Retry block for States.TaskFailed" do
      controlled = asl["States"]["controlled_step"]
      expect(controlled).to have_key("Retry"),
        "Expected controlled_step to have SFN-level Retry in ASL"
      retry_block = controlled["Retry"].first
      expect(retry_block["ErrorEquals"]).to eq(["States.TaskFailed"])
      expect(retry_block["MaxAttempts"]).to eq(2)
      expect(retry_block["IntervalSeconds"]).to eq(2)
      expect(retry_block["BackoffRate"]).to eq(2.0)
    end

    it "controlled_step ASL state has TimeoutSeconds 60" do
      controlled = asl["States"]["controlled_step"]
      expect(controlled["TimeoutSeconds"]).to eq(60)
    end

    it "aggregate step has TURBOFAN_PREV_FAN_OUT_SIZES for routed collection" do
      aggregate_state = asl["States"]["aggregate"]
      env = aggregate_state.dig("Parameters", "ContainerOverrides", "Environment")
      sizes_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_FAN_OUT_SIZES" }
      expect(sizes_var).not_to be_nil
      expect(sizes_var["Value"]).to eq("s,m,l")
    end

    it "aggregate step has per-size count env vars" do
      aggregate_state = asl["States"]["aggregate"]
      env = aggregate_state.dig("Parameters", "ContainerOverrides", "Environment")
      %w[S M L].each do |size|
        var = env.find { |e| e["Name"] == "TURBOFAN_PREV_FAN_OUT_SIZE_#{size}" }
        expect(var).not_to be_nil, "Expected TURBOFAN_PREV_FAN_OUT_SIZE_#{size} env var"
        expect(var["Value.$"]).to include("chunking.score_items.sizes.#{size.downcase}.parents")
      end
    end
  end

  # ── ASL + CFN coherence tests ────────────────────────────────────────

  describe "ASL and CloudFormation coherence" do
    it "ASL routed chunk state FunctionName matches per-step CFN ChunkingLambda FunctionName" do
      asl_function_name = asl.dig("States", "score_items_chunk", "Parameters", "FunctionName")
      cfn_function_name = cfn.dig("Resources", "ChunkingLambdaScoreItems", "Properties", "FunctionName")

      expect(asl_function_name).to eq(cfn_function_name)
    end

    it "TURBOFAN_BUCKET env var uses the shared bucket name" do
      # Check in a routed branch (each branch now contains a Map state with Batch Task inside)
      routed = asl["States"]["score_items_routed"]
      map_state = routed["Branches"].first["States"].values.first
      inner_task = map_state.dig("ItemProcessor", "States").values.first
      branch_env = inner_task.dig("Parameters", "ContainerOverrides", "Environment")
      bucket_var = branch_env.find { |e| e["Name"] == "TURBOFAN_BUCKET" }

      expect(bucket_var["Value"]).to eq(Turbofan.config.bucket)
    end

    it "TURBOFAN_BUCKET_PREFIX env var is set to pipeline-stage" do
      routed = asl["States"]["score_items_routed"]
      map_state = routed["Branches"].first["States"].values.first
      inner_task = map_state.dig("ItemProcessor", "States").values.first
      branch_env = inner_task.dig("Parameters", "ContainerOverrides", "Environment")
      prefix_var = branch_env.find { |e| e["Name"] == "TURBOFAN_BUCKET_PREFIX" }

      expect(prefix_var).not_to be_nil
      expect(prefix_var["Value"]).to eq("integration-test-staging")
    end
  end

  # ── CloudFormation generation tests ──────────────────────────────────

  describe "CloudFormation generation" do
    it "generates job definitions for all 8 steps" do
      resources = cfn["Resources"]
      # Non-sized steps
      %i[retry_demo controlled_step fetch_brand read_visits classify build_items aggregate].each do |step|
        key = "JobDef#{Turbofan::Naming.pascal_case(step)}"
        expect(resources).to have_key(key), "Missing job definition: #{key}"
      end
    end

    it "generates per-size job definitions for score_items" do
      resources = cfn["Resources"]
      %w[S M L].each do |size|
        key = "JobDefScoreItems#{size}"
        expect(resources).to have_key(key), "Missing sized job definition: #{key}"
      end
    end

    it "does not generate any ECR repos (ECR is managed by image builder)" do
      resources = cfn["Resources"]
      ecr_keys = resources.keys.select { |k| resources[k]["Type"] == "AWS::ECR::Repository" }
      expect(ecr_keys).to be_empty
    end

    it "uses the external docker_image for classify job definition" do
      classify_jobdef = cfn.dig("Resources", "JobDefClassify", "Properties", "ContainerProperties", "Image")
      expect(classify_jobdef).to eq("123456789.dkr.ecr.us-east-1.amazonaws.com/classify:latest")
    end

    it "includes S3 read-only policy for read_visits S3 dependency" do
      job_role_policies = cfn.dig("Resources", "JobRole", "Properties", "Policies")
      s3_policy = job_role_policies.find { |p| p["PolicyName"] == "S3Access" }
      s3_statements = s3_policy.dig("PolicyDocument", "Statement")

      read_only_statement = s3_statements.find { |s|
        s["Action"].include?("s3:GetObject") &&
          !s["Action"].include?("s3:PutObject") &&
          s["Resource"].any? { |r| r.is_a?(String) && r.include?(INTEGRATION_EXT_BUCKET) }
      }
      expect(read_only_statement).not_to be_nil, "Expected read-only S3 policy for #{INTEGRATION_EXT_BUCKET}"
    end

    it "includes S3 write policy for aggregate writes_to dependency" do
      job_role_policies = cfn.dig("Resources", "JobRole", "Properties", "Policies")
      s3_policy = job_role_policies.find { |p| p["PolicyName"] == "S3Access" }
      s3_statements = s3_policy.dig("PolicyDocument", "Statement")

      write_statement = s3_statements.find { |s|
        s["Action"].include?("s3:PutObject") &&
          s["Resource"].any? { |r| r.is_a?(String) && r.include?("#{INTEGRATION_EXT_BUCKET}/turbofan-test") }
      }
      expect(write_statement).not_to be_nil, "Expected write S3 policy for turbofan-test"
    end

    it "includes secrets policy for places_read resource" do
      job_role_policies = cfn.dig("Resources", "JobRole", "Properties", "Policies")
      secrets_policy = job_role_policies.find { |p| p["PolicyName"] == "SecretsAccess" }
      expect(secrets_policy).not_to be_nil, "Expected SecretsAccess policy for places_read"

      secret_arns = secrets_policy.dig("PolicyDocument", "Statement", 0, "Resource")
      expect(secret_arns.any? { |a| a.include?(INTEGRATION_SECRET_ARN) }).to be true
    end

    it "does not emit a shared ChunkingLambda for a routed-only pipeline" do
      # score_items is the only fan-out and it's routed, so the per-step
      # ChunkingLambdaScoreItems replaces the shared Lambda.
      expect(cfn["Resources"]).not_to have_key("ChunkingLambda")
      expect(cfn["Resources"]).not_to have_key("ChunkingLambdaRole")
    end

    it "emits a per-step ChunkingLambda for the routed score_items step" do
      expect(cfn["Resources"]).to have_key("ChunkingLambdaScoreItems")
      expect(cfn["Resources"]).to have_key("ChunkingLambdaRoleScoreItems")
    end

    it "per-step ChunkingLambda uses ruby3.3 runtime" do
      runtime = cfn.dig("Resources", "ChunkingLambdaScoreItems", "Properties", "Runtime")
      expect(runtime).to eq("ruby3.3")
    end

    it "chunking lambda handler includes routing support" do
      handler = Turbofan::Generators::CloudFormation::ChunkingLambda::HANDLER
      expect(handler).to include("routed")
      expect(handler).to include("__turbofan_size")
    end

    it "per-step ChunkingLambda references S3 for code deployment" do
      code = cfn.dig("Resources", "ChunkingLambdaScoreItems", "Properties", "Code")
      expect(code).to have_key("S3Bucket")
      expect(code).to have_key("S3Key")
      expect(code["S3Key"]).to match(%r{chunking-lambda/score_items-[0-9a-f]+\.zip})
    end

    it "includes SNS notification topic" do
      expect(cfn["Resources"]).to have_key("NotificationTopic")
    end

    it "does NOT contain any inline AWS::Batch::ConsumableResource" do
      cr_keys = cfn["Resources"].keys.select { |k|
        cfn["Resources"][k]["Type"] == "AWS::Batch::ConsumableResource"
      }
      expect(cr_keys).to be_empty
    end

    it "references consumable resources via Fn::ImportValue in job definitions" do
      # fetch_brand uses :places_read which is consumable
      jd_key = cfn["Resources"].keys.find { |k| k.start_with?("JobDefFetchBrand") }
      crp = cfn.dig("Resources", jd_key, "Properties", "ConsumableResourceProperties")
      expect(crp).not_to be_nil
      entry = crp["ConsumableResourceList"].first
      expect(entry["ConsumableResource"]).to have_key("Fn::ImportValue")
    end

    it "controlled_step has infrastructure retry budget in its job definition" do
      retry_strategy = cfn.dig("Resources", "JobDefControlledStep", "Properties", "RetryStrategy")
      expect(retry_strategy["Attempts"]).to eq(
        Turbofan::Generators::CloudFormation::JobDefinition::INFRASTRUCTURE_RETRIES
      )
    end

    it "controlled_step has timeout 60 in its job definition" do
      timeout = cfn.dig("Resources", "JobDefControlledStep", "Properties", "Timeout", "AttemptDurationSeconds")
      expect(timeout).to eq(60)
    end

    it "retry_demo has infrastructure retry budget in its job definition" do
      retry_strategy = cfn.dig("Resources", "JobDefRetryDemo", "Properties", "RetryStrategy")
      expect(retry_strategy["Attempts"]).to eq(
        Turbofan::Generators::CloudFormation::JobDefinition::INFRASTRUCTURE_RETRIES
      )
    end

    it "score_items has timeout 300 in its sized job definitions" do
      %w[S M L].each do |size|
        timeout = cfn.dig("Resources", "JobDefScoreItems#{size}", "Properties", "Timeout", "AttemptDurationSeconds")
        expect(timeout).to eq(300), "Expected score_items #{size} timeout to be 300"
      end
    end
  end

  # ── Step behavior tests ──────────────────────────────────────────────

  describe "step class configuration" do
    it "retry_demo has retries set to 2" do
      expect(retry_demo_class.turbofan_retries).to eq(2)
    end

    it "fetch_brand needs duckdb (postgres resource) and uses NvmeCe" do
      expect(fetch_brand_class.turbofan_needs_duckdb?).to be true
      expect(fetch_brand_class.turbofan_resource_keys).to include(:places_read)
      expect(fetch_brand_class.turbofan_compute_environment).to eq(:nvme_ce)
    end

    it "read_visits has S3 dependency" do
      expect(read_visits_class.uses_s3.size).to eq(1)
      expect(read_visits_class.uses_s3.first[:uri]).to start_with("s3://#{INTEGRATION_EXT_BUCKET}")
    end

    it "classify is an external step" do
      expect(classify_class.turbofan_external?).to be true
      expect(classify_class.turbofan_docker_image).to include("classify:latest")
    end

    it "score_items has 3 sizes (s, m, l)" do
      expect(score_items_class.turbofan_sizes.keys).to contain_exactly(:s, :m, :l)
    end

    it "score_items has timeout 300" do
      expect(score_items_class.turbofan_timeout).to eq(300)
    end

    it "aggregate has writes_to S3 dependency" do
      expect(aggregate_class.writes_to_s3.size).to eq(1)
      expect(aggregate_class.writes_to_s3.first[:uri]).to include("turbofan-test")
    end

    it "controlled_step has SFN retry configuration" do
      expect(controlled_step_class.turbofan_retry_on).to eq(["States.TaskFailed"])
      expect(controlled_step_class.turbofan_retries).to eq(2)
    end

    it "controlled_step has inject_secret configured" do
      expect(controlled_step_class.turbofan_secrets).to include(
        hash_including(name: :pg_url)
      )
    end

    it "controlled_step has timeout 60" do
      expect(controlled_step_class.turbofan_timeout).to eq(60)
    end
  end

  # ── Chained fan-out ASL state ordering ───────────────────────────────

  describe "chained fan-out state chain" do
    let(:step_a_class) { build_passthrough_step }
    let(:step_b_class) { build_passthrough_step }
    let(:step_c_class) { build_passthrough_step }

    let(:chained_pipeline_class) do
      stub_const("StepA", step_a_class)
      stub_const("StepB", step_b_class)
      stub_const("StepC", step_c_class)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "chained-fan-outs"
        pipeline do
          a = fan_out(step_a(trigger_input))
          b = fan_out(step_b(a))
          step_c(b)
        end
      end
    end

    let(:chained_asl) do
      Turbofan::Generators::ASL.new(pipeline: chained_pipeline_class, stage: "production").generate
    end

    it "produces states in correct order: step_a_chunk, step_a, step_b_chunk, step_b, step_c, NotifySuccess, NotifyFailure, FailExecution" do
      expected_keys = %w[step_a_chunk step_a step_b_chunk step_b step_c NotifySuccess NotifyFailure FailExecution]
      expect(chained_asl["States"].keys).to eq(expected_keys)
    end
  end

  # ── Shared S3 mock for data-flow tests ──────────────────────────────

  shared_context "with mock S3" do
    let(:bucket) { "test-bucket" }
    let(:s3_store) { {} }
    let(:mock_s3) do
      instance_double(Aws::S3::Client).tap do |client|
        allow(client).to receive(:put_object) do |args|
          s3_store["#{args[:bucket]}/#{args[:key]}"] = args[:body]
        end
        allow(client).to receive(:get_object) do |args|
          body = s3_store["#{args[:bucket]}/#{args[:key]}"]
          raise Aws::S3::Errors::NoSuchKey.new(nil, "Not found: #{args[:key]}") unless body
          double(body: StringIO.new(body))
        end
      end
    end
  end

  # ── S3 data-flow round-trip ──────────────────────────────────────────

  describe "S3 data-flow round-trip" do
    include_context "with mock S3"
    let(:exec_id) { "exec-roundtrip-123" }

    let(:original_items) do
      (1..500).map { |i| {"id" => i, "payload" => "x" * 300} }
    end
    let(:group_size) { 100 }

    it "round-trips through Payload.serialize, Lambda chunking, FanOut.read_input, output writes, and FanOut.collect_outputs" do
      # 1. Payload.serialize writes {exec_id}/extract/output.json
      Turbofan::Runtime::Payload.serialize(
        original_items,
        s3_client: mock_s3,
        bucket: bucket,
        execution_id: exec_id,
        step_name: "extract"
      )

      expect(s3_store).to have_key("#{bucket}/#{exec_id}/extract/output.json")

      # 2. Simulate what the Lambda does: read output, chunk, write single items.json
      raw = s3_store["#{bucket}/#{exec_id}/extract/output.json"]
      items = JSON.parse(raw)
      chunks = items.each_slice(group_size).to_a

      key = "#{exec_id}/process/input/items.json"
      mock_s3.put_object(bucket: bucket, key: key, body: JSON.generate(chunks))

      # 3. FanOut.read_input(array_index: 0) reads items.json[0]
      chunk_0 = Turbofan::Runtime::FanOut.read_input(
        array_index: 0,
        s3_client: mock_s3,
        bucket: bucket,
        execution_id: exec_id,
        step_name: "process"
      )
      expect(chunk_0).to eq(chunks[0])

      # 4. Simulate wrapper output: put_object to output/0.json .. output/N.json
      chunks.each_with_index do |chunk_data, idx|
        mock_s3.put_object(
          bucket: bucket,
          key: "#{exec_id}/process/output/#{idx}.json",
          body: JSON.generate(chunk_data)
        )
      end

      # 5. FanOut.collect_outputs reads all outputs
      collected = Turbofan::Runtime::FanOut.collect_outputs(
        s3_client: mock_s3,
        bucket: bucket,
        execution_id: exec_id,
        step_name: "process",
        count: chunks.size
      )

      expect(collected.flatten).to eq(original_items)
    end
  end

  # ── Routed fan-out data flow ───────────────────────────────────────

  describe "routed fan-out data flow" do
    include_context "with mock S3"
    let(:exec_id) { "exec-routed-123" }

    it "routes items by __turbofan_size, chunks per size, and collects all outputs" do
      # 1. Simulate build_items output (annotated with __turbofan_size)
      sizes = %w[s m l]
      items = (0..8).map { |i| {"id" => i, "__turbofan_size" => sizes[i % 3]} }

      # 2. Simulate routing Lambda: group by __turbofan_size, write single items.json per size
      groups = items.group_by { |item| item["__turbofan_size"] }
      size_counts = {}
      groups.each do |size_name, size_items|
        chunks = size_items.each_slice(3).to_a
        key = "#{exec_id}/score_items/input/#{size_name}/items.json"
        mock_s3.put_object(bucket: bucket, key: key, body: JSON.generate(chunks))
        size_counts[size_name] = chunks.size
      end

      # 3. FanOut.read_input with chunk: for size-aware read
      chunk_s_0 = Turbofan::Runtime::FanOut.read_input(
        array_index: 0,
        s3_client: mock_s3,
        bucket: bucket,
        execution_id: exec_id,
        step_name: "score_items",
        chunk: "s"
      )
      expect(chunk_s_0.map { |i| i["__turbofan_size"] }).to all(eq("s"))

      # 4. Simulate wrapper output per size
      groups.each do |size_name, size_items|
        size_items.each_slice(3).with_index do |chunk_data, idx|
          mock_s3.put_object(
            bucket: bucket,
            key: "#{exec_id}/score_items/output/#{size_name}/#{idx}.json",
            body: JSON.generate(chunk_data)
          )
        end
      end

      # 5. FanOut.collect_outputs with chunks: for routed collection
      collected = Turbofan::Runtime::FanOut.collect_outputs(
        s3_client: mock_s3,
        bucket: bucket,
        execution_id: exec_id,
        step_name: "score_items",
        chunks: size_counts
      )

      expect(collected.flatten.size).to eq(9)
      # All items preserved
      collected_ids = collected.flatten.map { |i| i["id"] }.sort
      expect(collected_ids).to eq((0..8).to_a)
    end
  end

  # ── Chained fan-out data flow ────────────────────────────────────────

  describe "chained fan-out data flow" do
    include_context "with mock S3"
    let(:exec_id) { "exec-chained-123" }

    it "chains fan-out steps where each appends a greeting" do
      items = [{"output" => []}, {"output" => []}, {"output" => []}]
      greetings = ["Hello from Ruby", "Hello from Python", "Hello from Node", "Hello from Rust"]
      step_names = %w[hello_ruby hello_python hello_node hello_rust]

      step_names.each_with_index do |step_name, step_idx|
        # Simulate chunking Lambda: write single items.json
        key = "#{exec_id}/#{step_name}/input/items.json"
        mock_s3.put_object(bucket: bucket, key: key, body: JSON.generate(items))

        # Simulate each fan-out child: read input, append greeting, write output
        items.each_with_index do |_item, idx|
          input = Turbofan::Runtime::FanOut.read_input(
            array_index: idx,
            s3_client: mock_s3,
            bucket: bucket,
            execution_id: exec_id,
            step_name: step_name
          )

          result = input.merge("output" => input["output"] + [greetings[step_idx]])

          mock_s3.put_object(
            bucket: bucket,
            key: "#{exec_id}/#{step_name}/output/#{idx}.json",
            body: JSON.generate(result)
          )
        end

        # Collect outputs for next step's input
        items = Turbofan::Runtime::FanOut.collect_outputs(
          s3_client: mock_s3,
          bucket: bucket,
          execution_id: exec_id,
          step_name: step_name,
          count: 3
        )
      end

      # Verify final output
      items.each do |item|
        expect(item["output"]).to eq(greetings)
      end
    end
  end
end
