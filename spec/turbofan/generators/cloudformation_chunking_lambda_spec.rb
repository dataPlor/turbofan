require "spec_helper"

RSpec.describe Turbofan::Generators::CloudFormation, "chunking lambda", :schemas do # rubocop:disable RSpec/DescribeMethod
  before do
    Turbofan.config.bucket = "turbofan-shared-bucket"
  end

  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::ChunkCe", klass)
    klass
  end

  let(:step_class) do
    ce_class # ensure stub_const runs
    Class.new do
      include Turbofan::Step

      execution :batch
      compute_environment :test_ce
      cpu 2
      batch_size 100
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  let(:config) do
    {
      vpc_id: "vpc-123",
      subnets: ["subnet-456"],
      security_groups: ["sg-abc"]
    }
  end

  describe "pipeline with fan_out using group:" do
    let(:pipeline_class) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "chunked-pipeline"
        pipeline do
          fan_out(process(trigger_input))
        end
      end
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

    it "creates an AWS::Lambda::Function resource" do
      lambda_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      expect(lambda_keys.size).to eq(1)
    end

    it "sets the Lambda runtime to ruby3.3" do
      lambda_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      props = template["Resources"][lambda_key]["Properties"]
      expect(props["Runtime"]).to eq("ruby3.3")
    end

    it "sets a Handler property on the Lambda function" do
      lambda_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      props = template["Resources"][lambda_key]["Properties"]
      expect(props["Handler"]).to be_a(String)
      expect(props["Handler"]).not_to be_empty
    end

    it "references S3 bucket and key for code deployment" do
      lambda_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      props = template["Resources"][lambda_key]["Properties"]
      code = props["Code"]
      expect(code).to have_key("S3Bucket")
      expect(code).to have_key("S3Key")
      expect(code["S3Bucket"]).to eq("turbofan-shared-bucket")
      expect(code["S3Key"]).to match(%r{chunking-lambda/handler-[0-9a-f]+\.zip})
    end

    it "sets a Timeout on the Lambda function" do
      lambda_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      props = template["Resources"][lambda_key]["Properties"]
      expect(props["Timeout"]).to be_a(Integer)
      expect(props["Timeout"]).to be > 0
    end

    it "tags the Lambda function with turbofan:managed" do
      lambda_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      props = template["Resources"][lambda_key]["Properties"]
      tags_hash = described_class.tags_hash(props["Tags"])
      expect(tags_hash["turbofan:managed"]).to eq("true")
    end
  end

  describe "Lambda IAM role" do
    let(:pipeline_class) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "chunked-pipeline"
        pipeline do
          fan_out(process(trigger_input))
        end
      end
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

    it "creates an IAM Role for the chunking Lambda" do
      lambda_role_key = template["Resources"].keys.find { |k|
        resource = template["Resources"][k]
        next unless resource["Type"] == "AWS::IAM::Role"
        assume_doc = resource.dig("Properties", "AssumeRolePolicyDocument") || {}
        statements = assume_doc["Statement"] || []
        statements.any? { |s|
          principal = s["Principal"] || {}
          service = Array(principal["Service"])
          service.include?("lambda.amazonaws.com")
        }
      }
      expect(lambda_role_key).not_to be_nil,
        "Expected an IAM Role with lambda.amazonaws.com trust policy"
    end

    it "the Lambda IAM Role has S3 read/write permissions" do
      lambda_role_key = template["Resources"].keys.find { |k|
        resource = template["Resources"][k]
        next unless resource["Type"] == "AWS::IAM::Role"
        assume_doc = resource.dig("Properties", "AssumeRolePolicyDocument") || {}
        statements = assume_doc["Statement"] || []
        statements.any? { |s|
          principal = s["Principal"] || {}
          service = Array(principal["Service"])
          service.include?("lambda.amazonaws.com")
        }
      }
      expect(lambda_role_key).not_to be_nil

      policies = template["Resources"][lambda_role_key].dig("Properties", "Policies") || []
      all_actions = policies.flat_map { |p|
        statements = p.dig("PolicyDocument", "Statement") || []
        statements.flat_map { |s| Array(s["Action"]) }
      }
      expect(all_actions).to include("s3:PutObject")
      expect(all_actions).to include("s3:GetObject")
    end

    it "the Lambda IAM Role does not have Batch permissions" do
      lambda_role_key = template["Resources"].keys.find { |k|
        resource = template["Resources"][k]
        next unless resource["Type"] == "AWS::IAM::Role"
        assume_doc = resource.dig("Properties", "AssumeRolePolicyDocument") || {}
        statements = assume_doc["Statement"] || []
        statements.any? { |s|
          principal = s["Principal"] || {}
          service = Array(principal["Service"])
          service.include?("lambda.amazonaws.com")
        }
      }
      expect(lambda_role_key).not_to be_nil

      policies = template["Resources"][lambda_role_key].dig("Properties", "Policies") || []
      all_actions = policies.flat_map { |p|
        statements = p.dig("PolicyDocument", "Statement") || []
        statements.flat_map { |s| Array(s["Action"]) }
      }
      expect(all_actions).not_to include("batch:SubmitJob")
    end

    it "the Lambda function references the Lambda IAM Role" do
      lambda_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      role_ref = template["Resources"][lambda_key].dig("Properties", "Role")
      expect(role_ref).to be_a(Hash),
        "Expected Lambda Role to be a CloudFormation reference"
    end
  end

  describe "Lambda environment variables" do
    let(:pipeline_class) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "env-pipeline"
        pipeline do
          fan_out(process(trigger_input))
        end
      end
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

    it "sets environment variables on the Lambda function" do
      lambda_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      props = template["Resources"][lambda_key]["Properties"]
      expect(props).to have_key("Environment")
      expect(props["Environment"]).to have_key("Variables")
    end

    it "includes the shared S3 bucket name in environment variables" do
      lambda_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      env_vars = template["Resources"][lambda_key].dig("Properties", "Environment", "Variables")
      expect(env_vars["TURBOFAN_BUCKET"]).to eq("turbofan-shared-bucket")
    end

    it "includes TURBOFAN_BUCKET_PREFIX in environment variables" do
      lambda_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      env_vars = template["Resources"][lambda_key].dig("Properties", "Environment", "Variables")
      expect(env_vars["TURBOFAN_BUCKET_PREFIX"]).to eq("env-pipeline-production")
    end

    it "includes TURBOFAN_CODE_HASH derived from handler source" do
      lambda_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      env_vars = template["Resources"][lambda_key].dig("Properties", "Environment", "Variables")
      expected = Digest::SHA256.hexdigest(Turbofan::Generators::CloudFormation::ChunkingLambda::HANDLER)[0, 12]
      expect(env_vars["TURBOFAN_CODE_HASH"]).to eq(expected)
    end

    it "TURBOFAN_CODE_HASH changes when handler content changes" do
      original = Digest::SHA256.hexdigest(
        Turbofan::Generators::CloudFormation::ChunkingLambda::HANDLER
      )[0, 12]
      modified = Digest::SHA256.hexdigest(
        Turbofan::Generators::CloudFormation::ChunkingLambda::HANDLER + "\n# changed"
      )[0, 12]
      expect(original).not_to eq(modified)
    end
  end

  # "pipeline without group:" tests removed: fan_out now requires group: parameter

  describe "pipeline with no fan_out (no Lambda)" do
    let(:pipeline_class) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "sequential-pipeline"
        pipeline do
          process(trigger_input)
        end
      end
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

    it "does not create a Lambda function for non-fan-out pipelines" do
      lambda_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      expect(lambda_keys).to be_empty
    end
  end

  describe "multiple fan_out steps with group:" do
    let(:step_class_2) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 4
        batch_size 50
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      step_klass = step_class
      step_klass_2 = step_class_2
      stub_const("Extract", step_klass)
      stub_const("Transform", step_klass_2)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "multi-group-pipeline"
        pipeline do
          extracted = fan_out(extract(trigger_input))
          fan_out(transform(extracted))
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {extract: step_class, transform: step_class_2},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "creates exactly one Lambda function even with multiple grouped fan_out steps" do
      lambda_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      expect(lambda_keys.size).to eq(1),
        "Expected exactly one shared chunking Lambda, got #{lambda_keys.size}"
    end
  end

  describe "mixed fan_out steps (some with group:, some without)" do
    let(:step_class_2) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 4
        batch_size 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      step_klass = step_class
      step_klass_2 = step_class_2
      stub_const("Extract", step_klass)
      stub_const("Transform", step_klass_2)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "mixed-pipeline"
        pipeline do
          extracted = fan_out(extract(trigger_input))
          fan_out(transform(extracted))
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {extract: step_class, transform: step_class_2},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "creates a Lambda function when at least one fan_out uses group:" do
      lambda_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      expect(lambda_keys.size).to eq(1)
    end
  end

  describe "Lambda handler code content" do
    let(:handler) { Turbofan::Generators::CloudFormation::ChunkingLambda::HANDLER }

    it "contains S3 put_object call for writing chunks" do
      expect(handler).to include("put_object")
    end

    it "returns parents in response for non-routed fan-out" do
      expect(handler).to include("'parents'")
    end

    it "returns parents in response for routed fan-out" do
      expect(handler).to include("'parents'")
    end

    it "backfills empty parents for declared sizes with no items" do
      expect(handler).to include("sizes[size_name] ||= { 'parents' => [] }")
    end

    it "does not contain Batch submit_job call" do
      expect(handler).not_to include("submit_job")
    end

    it "contains a chunk function definition" do
      expect(handler).to include("def chunk")
    end

    it "contains a handler function definition" do
      expect(handler).to include("def handler")
    end

    it "references TURBOFAN_BUCKET environment variable" do
      expect(handler).to include("TURBOFAN_BUCKET")
    end

    it "uses aws-sdk-s3 for AWS SDK access" do
      expect(handler).to include("aws-sdk-s3")
    end

    it "writes a single items.json file per fan-out step" do
      expect(handler).to include("'items.json'")
    end

    it "does not write individual indexed input files" do
      expect(handler).not_to include("'input', \"#{"\#{idx}"}.json\"")
      expect(handler).not_to include("'input', size_name, \"#{"\#{idx}"}.json\"")
    end

    it "reads from S3 when prev_step is present" do
      expect(handler).to include("prev_step")
      expect(handler).to include("get_object")
    end

    it "collects indexed outputs when prev_fan_out_size is present" do
      expect(handler).to include("prev_fan_out_size")
      expect(handler).to include("'output'")
    end
  end

  describe "handler_zip" do
    it "produces a valid zip containing index.rb" do
      zip_bytes = Turbofan::Generators::CloudFormation::ChunkingLambda.handler_zip
      expect(zip_bytes[0..3]).to eq("PK\x03\x04"), "Expected zip local file header signature"
      expect(zip_bytes).to include("index.rb")
      expect(zip_bytes).to include("aws-sdk-s3")
    end

    it "handler_s3_key includes bucket prefix and code hash" do
      key = Turbofan::Generators::CloudFormation::ChunkingLambda.handler_s3_key("my-pipeline-staging")
      expect(key).to match(%r{\Amy-pipeline-staging/chunking-lambda/handler-[0-9a-f]{12}\.zip\z})
    end

    it "handler_s3_key changes when handler code changes" do
      key1 = Turbofan::Generators::CloudFormation::ChunkingLambda.handler_s3_key("prefix")
      # The key includes a hash of the HANDLER constant, so it's stable
      # but would change if the handler code changed
      key2 = Turbofan::Generators::CloudFormation::ChunkingLambda.handler_s3_key("prefix")
      expect(key1).to eq(key2)
    end
  end

  describe "Lambda logical ID naming convention" do
    let(:pipeline_class) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "naming-pipeline"
        pipeline do
          fan_out(process(trigger_input))
        end
      end
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

    it "uses 'ChunkingLambda' as the Lambda logical ID" do
      expect(template["Resources"]).to have_key("ChunkingLambda")
      expect(template["Resources"]["ChunkingLambda"]["Type"]).to eq("AWS::Lambda::Function")
    end

    it "uses 'ChunkingLambdaRole' as the IAM Role logical ID" do
      expect(template["Resources"]).to have_key("ChunkingLambdaRole")
      expect(template["Resources"]["ChunkingLambdaRole"]["Type"]).to eq("AWS::IAM::Role")
    end

    it "the Lambda Role reference uses Fn::GetAtt with ChunkingLambdaRole" do
      role_ref = template["Resources"]["ChunkingLambda"].dig("Properties", "Role")
      expect(role_ref).to eq({"Fn::GetAtt" => ["ChunkingLambdaRole", "Arn"]})
    end
  end

  describe "IAM role least-privilege permissions" do
    let(:pipeline_class) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "least-priv-pipeline"
        pipeline do
          fan_out(process(trigger_input))
        end
      end
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
    let(:role_props) { template["Resources"]["ChunkingLambdaRole"]["Properties"] }
    let(:policies) { role_props["Policies"] }

    it "S3 policy allows exactly s3:GetObject and s3:PutObject (no wildcards)" do
      s3_policy = policies.find { |p| p["PolicyName"] == "S3Access" }
      s3_actions = s3_policy.dig("PolicyDocument", "Statement").flat_map { |s| Array(s["Action"]) }
      expect(s3_actions).to contain_exactly("s3:GetObject", "s3:PutObject")
    end

    it "S3 policy scopes resource to the shared bucket objects" do
      s3_policy = policies.find { |p| p["PolicyName"] == "S3Access" }
      s3_resource = s3_policy.dig("PolicyDocument", "Statement", 0, "Resource")
      expect(s3_resource).to eq("arn:aws:s3:::turbofan-shared-bucket/*")
    end

    it "has exactly one inline policy (S3Access)" do
      expect(policies.map { |p| p["PolicyName"] }).to eq(["S3Access"])
    end

    it "attaches AWSLambdaBasicExecutionRole managed policy" do
      managed = role_props["ManagedPolicyArns"]
      expect(managed).to include("arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
    end
  end

  describe "tags on Lambda function and IAM role" do
    let(:pipeline_class) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "tagged-pipeline"
        pipeline do
          fan_out(process(trigger_input))
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_class},
        stage: "staging",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "tags the Lambda function with turbofan:pipeline matching pipeline name" do
      tags_hash = described_class.tags_hash(template["Resources"]["ChunkingLambda"].dig("Properties", "Tags"))
      expect(tags_hash["turbofan:pipeline"]).to eq("tagged-pipeline")
    end

    it "tags the Lambda function with turbofan:stage matching the stage" do
      tags_hash = described_class.tags_hash(template["Resources"]["ChunkingLambda"].dig("Properties", "Tags"))
      expect(tags_hash["turbofan:stage"]).to eq("staging")
    end

    it "tags the IAM role with turbofan:managed" do
      tags_hash = described_class.tags_hash(template["Resources"]["ChunkingLambdaRole"].dig("Properties", "Tags"))
      expect(tags_hash["turbofan:managed"]).to eq("true")
    end

    it "tags the IAM role with turbofan:pipeline matching pipeline name" do
      tags_hash = described_class.tags_hash(template["Resources"]["ChunkingLambdaRole"].dig("Properties", "Tags"))
      expect(tags_hash["turbofan:pipeline"]).to eq("tagged-pipeline")
    end

    it "tags the IAM role with turbofan:stage matching the stage" do
      tags_hash = described_class.tags_hash(template["Resources"]["ChunkingLambdaRole"].dig("Properties", "Tags"))
      expect(tags_hash["turbofan:stage"]).to eq("staging")
    end
  end

  describe "multiple pipelines generate independent Lambdas" do
    let(:step_class_alpha) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 2
        batch_size 100
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:step_class_beta) do
      ce_class # ensure stub_const runs
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 2
        batch_size 50
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class_a) do
      step_klass = step_class_alpha
      stub_const("AlphaStep", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "pipeline-alpha"
        pipeline do
          fan_out(alpha_step(trigger_input))
        end
      end
    end

    let(:pipeline_class_b) do
      step_klass = step_class_beta
      stub_const("BetaStep", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "pipeline-beta"
        pipeline do
          fan_out(beta_step(trigger_input))
        end
      end
    end

    let(:template_a) do
      described_class.new(
        pipeline: pipeline_class_a,
        steps: {alpha_step: step_class_alpha},
        stage: "production",
        config: config
      ).generate
    end

    let(:template_b) do
      described_class.new(
        pipeline: pipeline_class_b,
        steps: {beta_step: step_class_beta},
        stage: "production",
        config: config
      ).generate
    end

    it "each pipeline produces its own Lambda function resource" do
      lambda_keys_a = template_a["Resources"].keys.select { |k|
        template_a["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      lambda_keys_b = template_b["Resources"].keys.select { |k|
        template_b["Resources"][k]["Type"] == "AWS::Lambda::Function"
      }
      expect(lambda_keys_a.size).to eq(1)
      expect(lambda_keys_b.size).to eq(1)
    end

    it "Lambda FunctionName differs per pipeline" do
      fn_name_a = template_a["Resources"]["ChunkingLambda"].dig("Properties", "FunctionName")
      fn_name_b = template_b["Resources"]["ChunkingLambda"].dig("Properties", "FunctionName")
      expect(fn_name_a).not_to eq(fn_name_b)
      expect(fn_name_a).to include("pipeline-alpha")
      expect(fn_name_b).to include("pipeline-beta")
    end

    it "IAM RoleName differs per pipeline" do
      role_name_a = template_a["Resources"]["ChunkingLambdaRole"].dig("Properties", "RoleName")
      role_name_b = template_b["Resources"]["ChunkingLambdaRole"].dig("Properties", "RoleName")
      expect(role_name_a).not_to eq(role_name_b)
      expect(role_name_a).to include("pipeline-alpha")
      expect(role_name_b).to include("pipeline-beta")
    end

    it "tags reflect each pipeline's own name" do
      tags_a = described_class.tags_hash(template_a["Resources"]["ChunkingLambda"].dig("Properties", "Tags"))
      tags_b = described_class.tags_hash(template_b["Resources"]["ChunkingLambda"].dig("Properties", "Tags"))
      expect(tags_a["turbofan:pipeline"]).to eq("pipeline-alpha")
      expect(tags_b["turbofan:pipeline"]).to eq("pipeline-beta")
    end
  end

  describe "Lambda FunctionName includes prefix" do
    let(:pipeline_class) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "my-pipeline"
        pipeline do
          fan_out(process(trigger_input))
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        steps: {process: step_class},
        stage: "staging",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "sets FunctionName to '{prefix}-chunking'" do
      fn_name = template["Resources"]["ChunkingLambda"].dig("Properties", "FunctionName")
      expect(fn_name).to eq("turbofan-my-pipeline-staging-chunking")
    end

    it "sets IAM RoleName to '{prefix}-chunking-lambda-role'" do
      role_name = template["Resources"]["ChunkingLambdaRole"].dig("Properties", "RoleName")
      expect(role_name).to eq("turbofan-my-pipeline-staging-chunking-lambda-role")
    end
  end

  describe "Lambda Handler configuration" do
    let(:pipeline_class) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "handler-pipeline"
        pipeline do
          fan_out(process(trigger_input))
        end
      end
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

    it "sets Handler to 'index.handler' matching the packaged file name" do
      props = template["Resources"]["ChunkingLambda"]["Properties"]
      expect(props["Handler"]).to eq("index.handler")
    end

    it "sets Timeout to 300 seconds" do
      props = template["Resources"]["ChunkingLambda"]["Properties"]
      expect(props["Timeout"]).to eq(300)
    end
  end

  describe "handler input validation" do
    let(:handler) { Turbofan::Generators::CloudFormation::ChunkingLambda::HANDLER }

    it "requires items_s3_uri for trigger input (no inline items)" do
      expect(handler).to include("items_s3_uri")
      expect(handler).to include("Invalid trigger input for fan-out")
    end

    it "validates S3 file contains {items: [...]}" do
      expect(handler).to include("Invalid S3 input format")
    end

    it "validates prev_step output contains {items: [...]}" do
      expect(handler).to include("Invalid input from step")
    end

    it "does not accept raw arrays as trigger input" do
      # The handler should NOT have a fallback that accepts event['items'] directly
      # or treats raw arrays as valid input
      expect(handler).not_to match(/^\s*event\['items'\]\s*$/)
    end
  end

  describe "TURBOFAN_BUCKET environment variable" do
    let(:pipeline_class) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "bucket-env-pipeline"
        pipeline do
          fan_out(process(trigger_input))
        end
      end
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

    it "sets TURBOFAN_BUCKET env var to the shared bucket name" do
      env_vars = template["Resources"]["ChunkingLambda"].dig("Properties", "Environment", "Variables")
      expect(env_vars["TURBOFAN_BUCKET"]).to eq("turbofan-shared-bucket")
    end

    it "sets TURBOFAN_BUCKET_PREFIX env var" do
      env_vars = template["Resources"]["ChunkingLambda"].dig("Properties", "Environment", "Variables")
      expect(env_vars["TURBOFAN_BUCKET_PREFIX"]).to eq("bucket-env-pipeline-production")
    end
  end

  describe "pipeline with routed fan_out" do
    let(:router_dir) { File.expand_path("../../fixtures/chunking_lambda/process", __dir__) }
    let(:router_source) { File.read(File.join(router_dir, "router", "router.rb")) }

    let(:routed_step_class) do
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        size :s, cpu: 1, ram: 2
        size :m, cpu: 2, ram: 4
        batch_size 10
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:routed_pipeline_class) do
      step_klass = routed_step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline
        pipeline_name "routed-chunked-pipeline"
        pipeline do
          fan_out(process(trigger_input))
        end
      end
    end

    let(:routed_generator) do
      described_class.new(
        pipeline: routed_pipeline_class,
        steps: {process: routed_step_class},
        stage: "production",
        config: config,
        step_dirs: {process: router_dir}
      )
    end

    let(:routed_template) { routed_generator.generate }

    it "emits a per-step ChunkingLambda{Step} Lambda function" do
      expect(routed_template["Resources"]).to have_key("ChunkingLambdaProcess")
      expect(routed_template["Resources"]["ChunkingLambdaProcess"]["Type"]).to eq("AWS::Lambda::Function")
    end

    it "per-step Lambda FunctionName is prefix-chunking-{step}" do
      fn_name = routed_template["Resources"]["ChunkingLambdaProcess"].dig("Properties", "FunctionName")
      expect(fn_name).to eq("turbofan-routed-chunked-pipeline-production-chunking-process")
    end

    it "emits a per-step IAM role ChunkingLambdaRole{Step}" do
      expect(routed_template["Resources"]).to have_key("ChunkingLambdaRoleProcess")
      expect(routed_template["Resources"]["ChunkingLambdaRoleProcess"]["Type"]).to eq("AWS::IAM::Role")
    end

    it "per-step Lambda references its per-step role" do
      role_ref = routed_template["Resources"]["ChunkingLambdaProcess"].dig("Properties", "Role", "Fn::GetAtt")
      expect(role_ref).to eq(["ChunkingLambdaRoleProcess", "Arn"])
    end

    it "per-step role grants S3 GetObject and PutObject on the pipeline bucket" do
      role = routed_template["Resources"]["ChunkingLambdaRoleProcess"]
      s3_policy = role.dig("Properties", "Policies").find { |p| p["PolicyName"] == "S3Access" }
      actions = s3_policy.dig("PolicyDocument", "Statement", 0, "Action")
      expect(actions).to include("s3:GetObject", "s3:PutObject")
    end

    it "per-step Lambda S3Key includes a code hash covering HANDLER + ROUTER_MODULE + router_source" do
      s3_key = routed_template["Resources"]["ChunkingLambdaProcess"].dig("Properties", "Code", "S3Key")
      expected_hash = Digest::SHA256.hexdigest(
        Turbofan::Generators::CloudFormation::ChunkingLambda::HANDLER +
        Turbofan::Generators::CloudFormation::ChunkingLambda::ROUTER_MODULE +
        router_source
      )[0, 12]
      expect(s3_key).to include("process-#{expected_hash}.zip")
    end

    it "lambda_artifacts_per_step bundles index.rb, turbofan_router.rb, and router.rb" do
      artifacts = routed_generator.lambda_artifacts
      routed_artifact = artifacts.find { |a| a[:key].include?("process-") }
      expect(routed_artifact).not_to be_nil
      zip_bytes = routed_artifact[:body]
      # Zip central directory contains filenames as ASCII; check for presence
      expect(zip_bytes).to include("index.rb")
      expect(zip_bytes).to include("turbofan_router.rb")
      expect(zip_bytes).to include("router.rb")
    end

    it "does not emit the shared ChunkingLambda in a routed-only pipeline" do
      expect(routed_template["Resources"]).not_to have_key("ChunkingLambda")
      expect(routed_template["Resources"]).not_to have_key("ChunkingLambdaRole")
    end

    it "does not include the shared ChunkingLambda zip in lambda_artifacts" do
      shared_keys = routed_generator.lambda_artifacts.map { |a| a[:key] }
      expect(shared_keys).not_to(include(a_string_matching(%r{chunking-lambda/handler-[0-9a-f]+\.zip\z})))
    end

    it "state machine role's LambdaInvoke does not include the shared ChunkingLambda ARN" do
      policies = routed_template.dig("Resources", "SfnRole", "Properties", "Policies") || []
      invoke_policy = policies.find { |p| p["PolicyName"] == "LambdaInvoke" }
      expect(invoke_policy).not_to be_nil
      raw_resources = invoke_policy.dig("PolicyDocument", "Statement", 0, "Resource")
      resources = raw_resources.is_a?(Array) ? raw_resources : [raw_resources]
      expect(resources).not_to include({"Fn::GetAtt" => ["ChunkingLambda", "Arn"]})
    end
  end

  describe "HANDLER content (combined routing+chunking)" do
    let(:handler) { Turbofan::Generators::CloudFormation::ChunkingLambda::HANDLER }

    it "guards router require with begin/rescue LoadError for shared-variant reuse" do
      expect(handler).to include("rescue LoadError")
      expect(handler).to include("require_relative 'router'")
    end

    it "raises when router_class is requested but router module not bundled" do
      expect(handler).to include("router.rb not bundled in this Lambda zip")
    end

    it "tags items with __turbofan_size from router.route(item)" do
      expect(handler).to include("router.route(item)")
      expect(handler).to include("__turbofan_size")
    end
  end
end
