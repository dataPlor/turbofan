require "spec_helper"

RSpec.describe Turbofan::Generators::CloudFormation, :schemas do
  before do
    Turbofan.config.bucket = "turbofan-shared-bucket"
  end

  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::TestCe", klass)
    klass
  end

  let(:step_class) do
    ce_class # ensure stub_const runs
    Class.new do
      include Turbofan::Step

      execution :batch
      compute_environment :test_ce
      cpu 2
      ram 4
      uses :duckdb
      uses "s3://data-bucket/input/*"
      writes_to "s3://data-bucket/output/*"
      secret :db_url, from: "turbofan/test-pipeline/db-url"
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  let(:pipeline_class) do
    step_klass = step_class
    stub_const("Process", step_klass)
    Class.new do
      include Turbofan::Pipeline

      pipeline_name "test-pipeline"

      metric "rows_processed", stat: :sum, display: :line, unit: "rows"

      pipeline do
        process(trigger_input)
      end
    end
  end

  let(:config) do
    {
      vpc_id: "vpc-123",
      subnets: ["subnet-456", "subnet-789"],
      security_groups: ["sg-abc"]
    }
  end

  let(:generator) do
    described_class.new(
      pipeline: pipeline_class,
      steps: {process: step_class},
      stage: "production",
      config: config
    )
  end

  let(:template) { generator.generate }

  describe "template structure" do
    it "has AWSTemplateFormatVersion" do
      expect(template["AWSTemplateFormatVersion"]).to eq("2010-09-09")
    end

    it "has a Resources section" do
      expect(template["Resources"]).to be_a(Hash)
    end

    it "has a non-empty Resources section" do
      expect(template["Resources"].size).to be > 0
    end

    it "has a Description" do
      expect(template["Description"]).to include("test-pipeline")
    end
  end

  describe "no inline compute environment resource" do
    it "does not create a ComputeEnvironment resource in the template" do
      ce_key = template["Resources"].keys.find { |k| k.start_with?("ComputeEnvironment") }
      expect(ce_key).to be_nil
    end

    it "does not create a LaunchTemplate resource" do
      lt_key = template["Resources"].keys.find { |k| k.start_with?("LaunchTemplate") }
      expect(lt_key).to be_nil
    end

    it "does not create a SpotFleetRole resource" do
      expect(template["Resources"]).not_to have_key("SpotFleetRole")
    end

    it "does not create an InstanceRole resource" do
      expect(template["Resources"]).not_to have_key("InstanceRole")
    end

    it "does not create an InstanceProfile resource" do
      expect(template["Resources"]).not_to have_key("InstanceProfile")
    end
  end

  describe "job definition" do
    let(:jd_key) { template["Resources"].keys.find { |k| k.start_with?("JobDef") } }
    let(:jd) { template["Resources"][jd_key] }
    let(:container) { jd["Properties"]["ContainerProperties"] }

    it "creates a job definition resource" do
      expect(jd_key).not_to be_nil
    end

    it "uses ECS platform" do
      expect(jd["Properties"]["PlatformCapabilities"]).to include("EC2")
    end

    it "specifies correct CPU resources" do
      vcpu = container["ResourceRequirements"].find { |r| r["Type"] == "VCPU" }
      expect(vcpu["Value"]).to eq("2")
    end

    it "specifies correct memory resources in MB" do
      memory = container["ResourceRequirements"].find { |r| r["Type"] == "MEMORY" }
      expect(memory["Value"]).to eq("4096") # 4 GB = 4096 MB
    end

    it "does not set Timeout when step has no timeout" do
      expect(jd["Properties"]).not_to have_key("Timeout")
    end

    it "includes a retry strategy with infrastructure retry budget" do
      retry_strategy = jd["Properties"]["RetryStrategy"]
      expect(retry_strategy).not_to be_nil
      expect(retry_strategy["Attempts"]).to eq(
        Turbofan::Generators::CloudFormation::JobDefinition::INFRASTRUCTURE_RETRIES
      )
    end

    it "includes EvaluateOnExit chain within AWS 5-condition limit" do
      evaluate = jd["Properties"]["RetryStrategy"]["EvaluateOnExit"]
      expect(evaluate).to be_an(Array)
      expect(evaluate.size).to be <= 5
    end

    it "exits on success (exit code 0)" do
      evaluate = jd["Properties"]["RetryStrategy"]["EvaluateOnExit"]
      success_entry = evaluate.find { |e| e["OnExitCode"] == "0" }
      expect(success_entry).not_to be_nil
      expect(success_entry["Action"]).to eq("EXIT")
    end

    it "retries all failures (catch-all RETRY)" do
      evaluate = jd["Properties"]["RetryStrategy"]["EvaluateOnExit"]
      catchall = evaluate.find { |e| e["OnReason"] == "*" }
      expect(catchall).not_to be_nil
      expect(catchall["Action"]).to eq("RETRY")
    end

    it "propagates tags" do
      expect(jd["Properties"]["PropagateTags"]).to be true
    end
  end

  describe "NVMe mount for duckdb steps" do
    let(:jd_key) { template["Resources"].keys.find { |k| k.start_with?("JobDef") } }
    let(:container) { template["Resources"][jd_key]["Properties"]["ContainerProperties"] }

    it "mounts /mnt/nvme volume" do
      mount = container["MountPoints"].find { |m| m["ContainerPath"] == "/mnt/nvme" }
      expect(mount).not_to be_nil
    end

    it "defines a host volume for NVMe" do
      volume = container["Volumes"].find { |v| v["Host"]["SourcePath"] == "/mnt/nvme" }
      expect(volume).not_to be_nil
    end
  end

  describe "non-duckdb step (no NVMe)" do
    let(:non_duckdb_step) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 2
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:non_duckdb_pipeline) do
      step_klass = non_duckdb_step
      stub_const("Simple", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "no-duckdb"
        pipeline do
          simple(trigger_input)
        end
      end
    end

    let(:non_duckdb_template) do
      described_class.new(
        pipeline: non_duckdb_pipeline,
        steps: {simple: non_duckdb_step},
        stage: "production",
        config: config
      ).generate
    end

    it "does not mount NVMe volume in job definition" do
      jd_key = non_duckdb_template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      container = non_duckdb_template["Resources"][jd_key]["Properties"]["ContainerProperties"]
      mounts = container["MountPoints"] || []
      nvme_mount = mounts.find { |m| m["ContainerPath"] == "/mnt/nvme" }
      expect(nvme_mount).to be_nil
    end
  end

  describe "tags" do
    it "applies stack, stack-type, stack-component tags to taggable resources" do
      template["Resources"].each do |_name, resource|
        next unless resource["Properties"]&.key?("Tags")

        tags = resource["Properties"]["Tags"]
        tag_keys = tags.is_a?(Array) ? tags.map { |t| t["Key"] } : tags.keys

        expect(tag_keys).to include("stack")
        expect(tag_keys).to include("stack-type")
        expect(tag_keys).to include("stack-component")
      end
    end

    it "sets stack-type to the stage" do
      template["Resources"].each do |_name, resource|
        next unless resource["Properties"]&.key?("Tags")

        tags = resource["Properties"]["Tags"]
        next unless tags.is_a?(Array)

        stack_type = tags.find { |t| t["Key"] == "stack-type" }
        expect(stack_type["Value"]).to eq("production") if stack_type
      end
    end

    it "sets stack-component to the pipeline name" do
      template["Resources"].each do |_name, resource|
        next unless resource["Properties"]&.key?("Tags")

        tags = resource["Properties"]["Tags"]
        next unless tags.is_a?(Array)

        component = tags.find { |t| t["Key"] == "stack-component" }
        expect(component["Value"]).to eq("test-pipeline") if component
      end
    end
  end

  describe "IAM roles" do
    it "creates a job role" do
      expect(template["Resources"]).to have_key("JobRole")
    end

    it "creates an execution role" do
      expect(template["Resources"]).to have_key("ExecutionRole")
    end

    it "creates a Step Functions role" do
      expect(template["Resources"]).to have_key("SfnRole")
    end
  end

  describe "IAM least-privilege" do
    let(:job_role) { template["Resources"]["JobRole"] }
    let(:job_policies) { job_role["Properties"]["Policies"] }
    let(:execution_role) { template["Resources"]["ExecutionRole"] }
    let(:exec_policies) { execution_role["Properties"]["Policies"] }

    describe "job role S3 policy" do
      let(:s3_policy) { job_policies.find { |p| p["PolicyName"] == "S3Access" } }
      let(:s3_statements) { s3_policy["PolicyDocument"]["Statement"] }
      let(:interchange_statement) { s3_statements.first }
      let(:all_s3_resources) { s3_statements.flat_map { |s| Array(s["Resource"]) } }

      it "includes the shared bucket ARN as a plain string" do
        expect(interchange_statement["Resource"]).to include("arn:aws:s3:::turbofan-shared-bucket")
      end

      it "includes the shared bucket objects path as a plain string" do
        expect(interchange_statement["Resource"]).to include("arn:aws:s3:::turbofan-shared-bucket/*")
      end

      it "includes read-only S3 paths from uses/reads_from" do
        read_stmt = s3_statements.find { |s|
          s["Action"] == ["s3:GetObject", "s3:ListBucket"] &&
            Array(s["Resource"]).any? { |r| r.to_s.include?("data-bucket") }
        }
        expect(read_stmt).not_to be_nil
        expect(Array(read_stmt["Resource"])).to include("arn:aws:s3:::data-bucket/input/*")
      end

      it "includes read-write S3 paths from writes_to" do
        write_stmt = s3_statements.find { |s|
          s["Action"].include?("s3:PutObject") &&
            Array(s["Resource"]).any? { |r| r.to_s.include?("data-bucket/output") }
        }
        expect(write_stmt).not_to be_nil
        expect(Array(write_stmt["Resource"])).to include("arn:aws:s3:::data-bucket/output/*")
      end

      it "does not use Resource: * on any S3 statement" do
        s3_statements.each do |stmt|
          expect(stmt["Resource"]).not_to eq("*")
        end
      end
    end

    describe "job role Secrets Manager policy" do
      let(:secrets_policy) { job_policies.find { |p| p["PolicyName"] == "SecretsAccess" } }
      let(:secret_resources) { secrets_policy["PolicyDocument"]["Statement"].first["Resource"] }

      it "scopes to specific secret ARNs" do
        expect(secret_resources).to include("arn:aws:secretsmanager:*:*:secret:turbofan/test-pipeline/db-url*")
      end

      it "does not use Resource: *" do
        expect(secret_resources).not_to eq("*")
      end
    end

    describe "job role CloudWatch Logs policy" do
      let(:logs_policy) { job_policies.find { |p| p["PolicyName"] == "CloudWatchLogs" } }
      let(:logs_resources) { logs_policy["PolicyDocument"]["Statement"].first["Resource"] }

      it "scopes to specific log group ARNs" do
        expect(logs_resources).to include("arn:aws:logs:*:*:log-group:turbofan-test-pipeline-production-logs-process:*")
      end

      it "does not use Resource: *" do
        expect(logs_resources).not_to eq("*")
      end
    end

    describe "job role CloudWatch Metrics policy" do
      let(:metrics_policy) { job_policies.find { |p| p["PolicyName"] == "CloudWatchMetrics" } }
      let(:metrics_statement) { metrics_policy["PolicyDocument"]["Statement"].first }

      it "scopes via Condition on namespace matching the pipeline name used by runtime" do
        expect(metrics_statement["Condition"]).to eq(
          {"StringEquals" => {"cloudwatch:namespace" => "Turbofan/test-pipeline"}}
        )
      end
    end

    describe "execution role ECR policy" do
      let(:ecr_policy) { exec_policies.find { |p| p["PolicyName"] == "ECRAccess" } }
      let(:ecr_statement) { ecr_policy["PolicyDocument"]["Statement"].find { |s| s["Action"].include?("ecr:GetDownloadUrlForLayer") } }

      it "scopes to specific ECR repo ARNs" do
        expect(ecr_statement["Resource"]).to include("arn:aws:ecr:*:*:repository/turbofan-test-pipeline-production-ecr-process")
      end

      it "does not use Resource: *" do
        expect(ecr_statement["Resource"]).not_to eq("*")
      end
    end

    describe "execution role Secrets Manager" do
      let(:ecr_policy) { exec_policies.find { |p| p["PolicyName"] == "ECRAccess" } }
      let(:secrets_statement) { ecr_policy["PolicyDocument"]["Statement"].find { |s| s["Action"].include?("secretsmanager:GetSecretValue") } }

      it "scopes to specific secret ARNs" do
        expect(secrets_statement["Resource"]).to include("arn:aws:secretsmanager:*:*:secret:turbofan/test-pipeline/db-url*")
      end
    end

    describe "no wildcard resources on scoped policies" do
      it "job role policies do not use Resource: * (except metrics with Condition)" do
        job_policies.each do |policy|
          policy["PolicyDocument"]["Statement"].each do |stmt|
            next if policy["PolicyName"] == "CloudWatchMetrics" && stmt.key?("Condition")
            expect(stmt["Resource"]).not_to eq("*"),
              "#{policy["PolicyName"]} uses Resource: * without Condition"
          end
        end
      end

      it "execution role policies do not use Resource: *" do
        exec_policies.each do |policy|
          policy["PolicyDocument"]["Statement"].each do |stmt|
            expect(stmt["Resource"]).not_to eq("*"),
              "#{policy["PolicyName"]} uses Resource: *"
          end
        end
      end
    end
  end

  describe "no S3 interchange bucket (shared bucket)" do
    it "does not create an S3 bucket resource" do
      bucket_key = template["Resources"].keys.find { |k| k.include?("Bucket") }
      expect(bucket_key).to be_nil
    end

    it "does not have an S3BucketName output" do
      expect(template["Outputs"]).not_to have_key("S3BucketName")
    end
  end

  describe "CloudWatch log groups" do
    let(:log_key) { "LogGroupProcess" }
    let(:log_group) { template["Resources"][log_key] }

    it "creates a log group for the step" do
      expect(log_group).not_to be_nil
    end

    it "names the log group following design convention: turbofan-{pipeline}-{stage}-logs-{step}" do
      expect(log_group["Properties"]["LogGroupName"]).to eq("turbofan-test-pipeline-production-logs-process")
    end

    it "does not use /aws/batch/ prefix" do
      expect(log_group["Properties"]["LogGroupName"]).not_to start_with("/aws/batch/")
    end

    it "does not use -logs suffix" do
      expect(log_group["Properties"]["LogGroupName"]).not_to end_with("-logs")
    end
  end

  describe "job queues live in CE stacks, not pipeline stacks" do
    it "does not generate any JobQueue resources" do
      queue_keys = template["Resources"].keys.select { |k| k.start_with?("JobQueue") }
      expect(queue_keys).to be_empty
    end

    context "with multiple steps" do
      let(:step_a) do
        ce_class # ensure stub_const runs
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:step_b) do
        ce_class # ensure stub_const runs
        Class.new do
          include Turbofan::Step

          execution :batch
          compute_environment :test_ce
          cpu 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:multi_pipeline) do
        stub_const("Extract", step_a)
        stub_const("Transform", step_b)
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "multi"
          pipeline do
            results = extract(trigger_input)
            transform(results)
          end
        end
      end

      let(:multi_template) do
        described_class.new(
          pipeline: multi_pipeline,
          steps: {extract: step_a, transform: step_b},
          stage: "production",
          config: config
        ).generate
      end

      it "does not generate any JobQueue resources" do
        queue_keys = multi_template["Resources"].keys.select { |k| k.start_with?("JobQueue") }
        expect(queue_keys).to be_empty
      end
    end
  end

  describe "pipeline compute_environment as default for steps" do
    let(:step_without_ce) do
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 2
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_with_default_ce) do
      step_klass = step_without_ce
      ce_class # ensure stub_const runs
      stub_const("DefaultCeStep", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "default-ce"
        compute_environment :test_ce
        pipeline do
          default_ce_step(trigger_input)
        end
      end
    end

    let(:default_ce_template) do
      described_class.new(
        pipeline: pipeline_with_default_ce,
        steps: {default_ce_step: step_without_ce},
        stage: "production",
        config: config
      ).generate
    end

    it "uses pipeline CE queue name in ASL when step has no CE" do
      sfn = default_ce_template["Resources"].values.find { |r| r["Type"] == "AWS::StepFunctions::StateMachine" }
      definition = JSON.parse(sfn["Properties"]["DefinitionString"]["Fn::Sub"])
      job_queue = definition["States"]["default_ce_step"].dig("Parameters", "JobQueue")
      expect(job_queue).to eq(ce_class.queue_name("production"))
    end

    it "step CE overrides pipeline CE in ASL queue reference" do
      other_ce = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::OtherCe", other_ce)

      step_with_own_ce = Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 2
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
      step_with_own_ce.compute_environment(:other_ce)

      stub_const("OwnCeStep", step_with_own_ce)
      pipeline = Class.new do
        include Turbofan::Pipeline

        pipeline_name "override-ce"
        pipeline do
          own_ce_step(trigger_input)
        end
      end
      pipeline.compute_environment(:test_ce)

      tmpl = described_class.new(
        pipeline: pipeline,
        steps: {own_ce_step: step_with_own_ce},
        stage: "production",
        config: config
      ).generate

      sfn = tmpl["Resources"].values.find { |r| r["Type"] == "AWS::StepFunctions::StateMachine" }
      definition = JSON.parse(sfn["Properties"]["DefinitionString"]["Fn::Sub"])
      job_queue = definition["States"]["own_ce_step"].dig("Parameters", "JobQueue")
      expect(job_queue).to eq(other_ce.queue_name("production"))
    end
  end

  describe "state machine" do
    let(:sfn_key) { template["Resources"].keys.find { |k| k.include?("StateMachine") } }
    let(:sfn) { template["Resources"][sfn_key] }

    it "creates a Step Functions state machine" do
      expect(sfn_key).not_to be_nil
    end

    it "names the state machine following conventions" do
      name = sfn["Properties"]["StateMachineName"]
      expect(name).to eq("turbofan-test-pipeline-production-statemachine")
    end

    it "includes a DefinitionString" do
      expect(sfn["Properties"]["DefinitionString"]).not_to be_nil
    end

    it "wraps the DefinitionString in Fn::Sub for pseudo-parameter resolution" do
      definition_string = sfn["Properties"]["DefinitionString"]
      expect(definition_string).to be_a(Hash)
      expect(definition_string).to have_key("Fn::Sub")
    end

    it "has a DefinitionString that is valid JSON with States" do
      definition = JSON.parse(sfn["Properties"]["DefinitionString"]["Fn::Sub"])
      expect(definition).to have_key("States")
      expect(definition["States"]).not_to be_empty
    end

    it "has a DefinitionString referencing the pipeline steps" do
      definition = JSON.parse(sfn["Properties"]["DefinitionString"]["Fn::Sub"])
      expect(definition["States"]).to have_key("process")
    end

    it "has a DefinitionString using CloudFormation pseudo-parameters in topic ARN" do
      json_str = sfn["Properties"]["DefinitionString"]["Fn::Sub"]
      expect(json_str).to include("${AWS::Region}")
      expect(json_str).to include("${AWS::AccountId}")
      expect(json_str).not_to include("${region}")
      expect(json_str).not_to include("${account}")
    end
  end

  describe "resource naming conventions" do
    it "all resource names start with turbofan-{pipeline}-{stage}" do
      prefix = "turbofan-test-pipeline-production"
      template["Resources"].each do |_key, resource|
        props = resource["Properties"] || {}
        # Check any name-like property
        name_fields = %w[
          StateMachineName
        ]
        name_fields.each do |field|
          next unless props.key?(field)
          expect(props[field]).to start_with(prefix),
            "Expected #{field} '#{props[field]}' to start with '#{prefix}'"
        end
      end
    end
  end

  describe "SfnRole SNS Publish permission (F-3)" do
    let(:sfn_role) { template["Resources"]["SfnRole"] }
    let(:sfn_policies) { sfn_role["Properties"]["Policies"] }

    it "includes an SNSPublish policy" do
      sns_policy = sfn_policies.find { |p| p["PolicyName"] == "SNSPublish" }
      expect(sns_policy).not_to be_nil
    end

    it "grants sns:Publish action" do
      sns_policy = sfn_policies.find { |p| p["PolicyName"] == "SNSPublish" }
      statement = sns_policy["PolicyDocument"]["Statement"].first
      expect(statement["Action"]).to include("sns:Publish")
    end

    it "scopes SNS publish to the NotificationTopic resource" do
      sns_policy = sfn_policies.find { |p| p["PolicyName"] == "SNSPublish" }
      statement = sns_policy["PolicyDocument"]["Statement"].first
      expect(statement["Resource"]).to eq({"Ref" => "NotificationTopic"})
    end
  end

  describe "no ImageTag parameter (M5: per-step image_tags)" do
    it "does not have a Parameters section with ImageTag" do
      params = template["Parameters"] || {}
      expect(params).not_to have_key("ImageTag")
    end

    it "uses 'latest' as fallback tag when no image_tags provided" do
      jd_key = template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      image = template["Resources"][jd_key]["Properties"]["ContainerProperties"]["Image"]
      image_str = image["Fn::Sub"]
      expect(image_str).to end_with(":latest")
    end
  end

  describe "CloudWatch namespace alignment with runtime (F-5)" do
    let(:job_role) { template["Resources"]["JobRole"] }
    let(:job_policies) { job_role["Properties"]["Policies"] }
    let(:metrics_policy) { job_policies.find { |p| p["PolicyName"] == "CloudWatchMetrics" } }
    let(:metrics_statement) { metrics_policy["PolicyDocument"]["Statement"].first }

    it "uses pipeline_name (not prefix) in the CloudWatch namespace condition" do
      namespace = metrics_statement["Condition"]["StringEquals"]["cloudwatch:namespace"]
      # Should be "Turbofan/test-pipeline", NOT "Turbofan/turbofan-test-pipeline-production"
      expect(namespace).to eq("Turbofan/test-pipeline")
    end

    it "matches the namespace used by Runtime::Metrics" do
      # Runtime::Metrics uses "Turbofan/#{pipeline_name}" where pipeline_name is the turbofan_name
      runtime_namespace = "Turbofan/#{pipeline_class.turbofan_name}"
      iam_namespace = metrics_statement["Condition"]["StringEquals"]["cloudwatch:namespace"]
      expect(iam_namespace).to eq(runtime_namespace)
    end
  end

  # Bug 2: JobDefinition generates VCPU "0" when a step has no cpu declared.
  # When cpu is nil, `cpu.to_s` produces "0" (since nil.to_i == 0 in Ruby),
  # which is not a valid VCPU value for AWS Batch.
  describe "job definition with no cpu declared" do
    let(:no_cpu_step) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        ram 4
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:no_cpu_pipeline) do
      step_klass = no_cpu_step
      stub_const("NoCpuStep", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "no-cpu"
        pipeline do
          no_cpu_step(trigger_input)
        end
      end
    end

    let(:no_cpu_template) do
      described_class.new(
        pipeline: no_cpu_pipeline,
        steps: {no_cpu_step: no_cpu_step},
        stage: "production",
        config: config
      ).generate
    end

    it "omits VCPU entry when cpu is nil" do
      jd_key = no_cpu_template["Resources"].keys.find { |k| k.start_with?("JobDef") }
      container = no_cpu_template["Resources"][jd_key]["Properties"]["ContainerProperties"]
      vcpu = container["ResourceRequirements"].find { |r| r["Type"] == "VCPU" }
      expect(vcpu).to be_nil
    end
  end

  # Issue 2: CloudFormation generator should raise when no compute environment is resolved.
  # When neither the step nor the pipeline declares a compute_environment,
  # ce_ref ends up nil, which produces invalid CloudFormation (nil in
  # ComputeEnvironmentOrder). The generator should raise a clear error
  # instead of silently producing an invalid template.
  describe "step with no compute_environment and no pipeline default" do
    let(:no_ce_step) do
      Class.new do
        include Turbofan::Step

        execution :batch
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:no_ce_pipeline) do
      step_klass = no_ce_step
      stub_const("NoCeStep", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "no-ce-pipeline"
        # Note: no compute_environment declared at the pipeline level
        pipeline do
          no_ce_step(trigger_input)
        end
      end
    end

    it "raises an error when no compute environment is resolved for a step" do
      generator = described_class.new(
        pipeline: no_ce_pipeline,
        steps: {no_ce_step: no_ce_step},
        stage: "production",
        config: config
      )

      expect { generator.generate }.to raise_error(/compute.?environment/i),
        "Expected the generator to raise an error about missing compute_environment " \
        "when neither step nor pipeline declares one"
    end
  end

  describe "tags_hash class method (F-13)" do
    it "is available as a class method on CloudFormation" do
      expect(described_class).to respond_to(:tags_hash)
    end

    it "converts tag array to hash format" do
      tags = [
        {"Key" => "stack", "Value" => "turbofan"},
        {"Key" => "env", "Value" => "production"}
      ]
      result = described_class.tags_hash(tags)
      expect(result).to eq({"stack" => "turbofan", "env" => "production"})
    end
  end
end
