require "spec_helper"
require "digest"

RSpec.describe Turbofan::Generators::CloudFormation::ToleranceLambda do
  let(:prefix) { "turbofan-example-pipeline-production" }
  let(:bucket_prefix) { "example-pipeline-production" }
  let(:tags) { [{"Key" => "stack", "Value" => "turbofan"}] }

  before { Turbofan.config.bucket = "turbofan-shared-bucket" }

  describe ".generate" do
    subject(:resources) { described_class.generate(prefix: prefix, bucket_prefix: bucket_prefix, tags: tags) }

    it "emits exactly two resources: function + role" do
      expect(resources.keys).to contain_exactly("ToleranceLambda", "ToleranceLambdaRole")
    end

    it "ToleranceLambda is an AWS::Lambda::Function" do
      expect(resources["ToleranceLambda"]["Type"]).to eq("AWS::Lambda::Function")
    end

    it "ToleranceLambdaRole is an AWS::IAM::Role" do
      expect(resources["ToleranceLambdaRole"]["Type"]).to eq("AWS::IAM::Role")
    end
  end

  describe ".lambda_function shape" do
    let(:props) do
      described_class.generate(prefix: prefix, bucket_prefix: bucket_prefix, tags: tags)
        .dig("ToleranceLambda", "Properties")
    end

    it "FunctionName is prefix-tolerance-check" do
      expect(props["FunctionName"]).to eq("turbofan-example-pipeline-production-tolerance-check")
    end

    it "Runtime is ruby3.3" do
      expect(props["Runtime"]).to eq("ruby3.3")
    end

    it "Handler is index.handler" do
      expect(props["Handler"]).to eq("index.handler")
    end

    it "Timeout is 300 seconds" do
      expect(props["Timeout"]).to eq(300)
    end

    it "MemorySize is 512 MB" do
      expect(props["MemorySize"]).to eq(512)
    end

    it "Role references ToleranceLambdaRole via Fn::GetAtt" do
      expect(props["Role"]).to eq({"Fn::GetAtt" => ["ToleranceLambdaRole", "Arn"]})
    end

    it "Code.S3Bucket is the turbofan shared bucket" do
      expect(props.dig("Code", "S3Bucket")).to eq("turbofan-shared-bucket")
    end

    it "Code.S3Key matches tolerance-lambda/handler-{hash}.zip format" do
      expect(props.dig("Code", "S3Key")).to match(%r{\A#{Regexp.escape(bucket_prefix)}/tolerance-lambda/handler-[0-9a-f]{12}\.zip\z})
    end

    it "Environment.Variables.TURBOFAN_BUCKET is the shared bucket" do
      expect(props.dig("Environment", "Variables", "TURBOFAN_BUCKET")).to eq("turbofan-shared-bucket")
    end

    it "Environment.Variables.TURBOFAN_BUCKET_PREFIX is the bucket_prefix arg" do
      expect(props.dig("Environment", "Variables", "TURBOFAN_BUCKET_PREFIX")).to eq(bucket_prefix)
    end

    it "Environment.Variables.TURBOFAN_CODE_HASH matches the HANDLER's SHA256 prefix" do
      expected = Digest::SHA256.hexdigest(described_class::HANDLER)[0, 12]
      expect(props.dig("Environment", "Variables", "TURBOFAN_CODE_HASH")).to eq(expected)
    end

    it "Tags are passed through unchanged" do
      expect(props["Tags"]).to eq(tags)
    end
  end

  describe ".lambda_role shape" do
    let(:role_props) do
      described_class.generate(prefix: prefix, bucket_prefix: bucket_prefix, tags: tags)
        .dig("ToleranceLambdaRole", "Properties")
    end

    it "RoleName is prefix-tolerance-lambda-role via Naming.iam_role_name" do
      expect(role_props["RoleName"]).to eq(Turbofan::Naming.iam_role_name("#{prefix}-tolerance-lambda-role"))
    end

    it "AssumeRolePolicyDocument allows lambda.amazonaws.com to AssumeRole" do
      statement = role_props.dig("AssumeRolePolicyDocument", "Statement", 0)
      expect(statement["Principal"]).to eq({"Service" => "lambda.amazonaws.com"})
      expect(statement["Action"]).to eq("sts:AssumeRole")
      expect(statement["Effect"]).to eq("Allow")
    end

    it "ManagedPolicyArns includes AWSLambdaBasicExecutionRole" do
      expect(role_props["ManagedPolicyArns"]).to include(
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
      )
    end

    it "Policies include S3Access granting GetObject/PutObject on the bucket" do
      s3_policy = role_props["Policies"].find { |p| p["PolicyName"] == "S3Access" }
      expect(s3_policy).not_to be_nil
      statement = s3_policy.dig("PolicyDocument", "Statement", 0)
      expect(statement["Action"]).to contain_exactly("s3:GetObject", "s3:PutObject")
      expect(statement["Resource"]).to eq("arn:aws:s3:::turbofan-shared-bucket/*")
    end

    it "Policies include BatchAccess granting DescribeJobs/ListJobs on all resources" do
      batch_policy = role_props["Policies"].find { |p| p["PolicyName"] == "BatchAccess" }
      expect(batch_policy).not_to be_nil
      statement = batch_policy.dig("PolicyDocument", "Statement", 0)
      expect(statement["Action"]).to contain_exactly("batch:DescribeJobs", "batch:ListJobs")
      expect(statement["Resource"]).to eq("*")
    end

    it "Tags are passed through unchanged" do
      expect(role_props["Tags"]).to eq(tags)
    end
  end

  describe ".handler_s3_key" do
    it "returns the deterministic path with 12-char code hash" do
      expected_hash = Digest::SHA256.hexdigest(described_class::HANDLER)[0, 12]
      expect(described_class.handler_s3_key(bucket_prefix))
        .to eq("#{bucket_prefix}/tolerance-lambda/handler-#{expected_hash}.zip")
    end

    it "is stable across calls with the same input" do
      expect(described_class.handler_s3_key(bucket_prefix))
        .to eq(described_class.handler_s3_key(bucket_prefix))
    end

    it "changes when bucket_prefix changes" do
      expect(described_class.handler_s3_key("a")).not_to eq(described_class.handler_s3_key("b"))
    end
  end

  describe ".handler_zip" do
    subject(:zip_bytes) { described_class.handler_zip }

    it "returns a String" do
      expect(zip_bytes).to be_a(String)
    end

    it "begins with a zip local file header (PK\\x03\\x04)" do
      expect(zip_bytes[0, 4]).to eq("PK\x03\x04".b)
    end

    it "contains index.rb as the filename" do
      expect(zip_bytes).to include("index.rb")
    end

    it "contains the HANDLER source" do
      expect(zip_bytes).to include("def handler(event:, context:)")
      expect(zip_bytes).to include("describe_jobs")
    end
  end

  describe ".lambda_artifacts" do
    subject(:artifacts) { described_class.lambda_artifacts(bucket_prefix) }

    it "returns one artifact entry" do
      expect(artifacts.size).to eq(1)
    end

    it "artifact bucket is the turbofan shared bucket" do
      expect(artifacts.first[:bucket]).to eq("turbofan-shared-bucket")
    end

    it "artifact key matches handler_s3_key" do
      expect(artifacts.first[:key]).to eq(described_class.handler_s3_key(bucket_prefix))
    end

    it "artifact body is the handler_zip bytes" do
      expect(artifacts.first[:body]).to eq(described_class.handler_zip)
    end
  end

  describe "HANDLER content (semantic markers)" do
    let(:handler) { described_class::HANDLER }

    it "includes tolerance-check logic" do
      expect(handler).to include("tolerated_failure_rate")
      expect(handler).to include("failure_rate")
    end

    it "uses aws-sdk-batch" do
      expect(handler).to include("aws-sdk-batch")
    end

    it "writes a tolerated_failures manifest to S3" do
      expect(handler).to include("tolerated_failures")
      expect(handler).to include("manifest_key")
    end
  end
end
