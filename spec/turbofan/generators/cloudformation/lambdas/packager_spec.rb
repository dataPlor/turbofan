# frozen_string_literal: true

require "spec_helper"
require "digest"
require "turbofan/generators/cloudformation/lambdas/packager"

RSpec.describe Turbofan::Generators::CloudFormation::Lambdas::Packager do
  before { Turbofan.config.bucket = "turbofan-shared-bucket" }

  let(:tags) { [{"Key" => "stack", "Value" => "turbofan"}] }

  def build_config(**overrides)
    described_class::LambdaConfig.new(
      logical_id: "MyLambda",
      role_logical_id: "MyLambdaRole",
      function_name: "turbofan-demo-production-mylambda",
      role_name: "turbofan-demo-production-mylambda-role",
      s3_key: "demo-production/my-lambda/handler-abc123def456.zip",
      memory_size: 1024,
      timeout: 300,
      code_hash: "abc123def456",
      bucket_prefix: "demo-production",
      tags: tags,
      **overrides
    )
  end

  describe ".code_hash" do
    it "returns a 12-char hex string" do
      hash = described_class.code_hash("hello")
      expect(hash).to match(/\A[0-9a-f]{12}\z/)
    end

    it "is deterministic for the same input" do
      expect(described_class.code_hash("x", "y")).to eq(described_class.code_hash("x", "y"))
    end

    it "changes when any source changes" do
      expect(described_class.code_hash("a", "b")).not_to eq(described_class.code_hash("a", "c"))
    end

    it "is order-sensitive across sources" do
      expect(described_class.code_hash("a", "b")).not_to eq(described_class.code_hash("b", "a"))
    end

    it "matches the raw SHA256 prefix of concatenated sources" do
      expected = Digest::SHA256.hexdigest("foobar")[0, 12]
      expect(described_class.code_hash("foo", "bar")).to eq(expected)
    end

    it "raises ArgumentError when given no sources" do
      expect { described_class.code_hash }.to raise_error(ArgumentError, /at least one source/)
    end
  end

  describe ".handler_s3_key" do
    it "builds the deterministic path with the default 'handler' basename" do
      key = described_class.handler_s3_key(bucket_prefix: "bp", subdir: "chunking-lambda", code_hash: "abc")
      expect(key).to eq("bp/chunking-lambda/handler-abc.zip")
    end

    it "accepts a custom basename for per-step variants" do
      key = described_class.handler_s3_key(bucket_prefix: "bp", subdir: "chunking-lambda", code_hash: "abc", basename: "process")
      expect(key).to eq("bp/chunking-lambda/process-abc.zip")
    end
  end

  describe ".build_handler_zip" do
    subject(:zip_bytes) { described_class.build_handler_zip("puts 'hello'") }

    it "returns a binary String" do
      expect(zip_bytes).to be_a(String)
    end

    it "begins with the zip local-file-header magic number (PK\\x03\\x04)" do
      expect(zip_bytes[0, 4]).to eq("PK\x03\x04".b)
    end

    it "contains index.rb as a bundled filename" do
      expect(zip_bytes).to include("index.rb")
    end

    it "contains the provided handler source" do
      expect(zip_bytes).to include("puts 'hello'")
    end
  end

  describe ".build_zip_from_files" do
    it "bundles all files with their content" do
      zip = described_class.build_zip_from_files("a.rb" => "contentA", "b.rb" => "contentB")
      expect(zip).to include("a.rb")
      expect(zip).to include("b.rb")
      expect(zip).to include("contentA")
      expect(zip).to include("contentB")
    end

    it "raises ArgumentError when given an empty hash" do
      expect { described_class.build_zip_from_files({}) }.to raise_error(ArgumentError, /at least one file/)
    end

    it "raises ArgumentError when content is not a String" do
      expect { described_class.build_zip_from_files("a.rb" => nil) }.to raise_error(ArgumentError, /must be a String/)
      expect { described_class.build_zip_from_files("a.rb" => 123) }.to raise_error(ArgumentError, /must be a String/)
    end

    it "preserves filename byte sequence so Ruby's require_relative can find the files" do
      zip = described_class.build_zip_from_files("my_helper.rb" => "x")
      expect(zip).to include("my_helper.rb")
    end
  end

  describe "LambdaConfig" do
    it "accepts all required keyword args" do
      expect { build_config }.not_to raise_error
    end

    it "defaults extra_policies to []" do
      expect(build_config.extra_policies).to eq([])
    end

    it "raises ArgumentError when a required field is nil" do
      expect { build_config(function_name: nil) }.to raise_error(ArgumentError, /function_name.*nil/)
    end

    it "raises ArgumentError when tags is not an Array" do
      expect { build_config(tags: {}) }.to raise_error(ArgumentError, /tags.*Array/)
    end
  end

  describe ".lambda_function" do
    subject(:resource) { described_class.lambda_function(build_config) }

    it "emits exactly the logical_id as the top-level key" do
      expect(resource.keys).to contain_exactly("MyLambda")
    end

    it "is an AWS::Lambda::Function type" do
      expect(resource["MyLambda"]["Type"]).to eq("AWS::Lambda::Function")
    end

    it "sets FunctionName from config" do
      expect(resource.dig("MyLambda", "Properties", "FunctionName")).to eq("turbofan-demo-production-mylambda")
    end

    it "sets Runtime to LAMBDA_RUNTIME (ruby3.3)" do
      expect(resource.dig("MyLambda", "Properties", "Runtime")).to eq("ruby3.3")
    end

    it "sets Handler to index.handler" do
      expect(resource.dig("MyLambda", "Properties", "Handler")).to eq("index.handler")
    end

    it "sets Timeout from config" do
      expect(resource.dig("MyLambda", "Properties", "Timeout")).to eq(300)
    end

    it "sets MemorySize from config" do
      expect(resource.dig("MyLambda", "Properties", "MemorySize")).to eq(1024)
    end

    it "sets Role to Fn::GetAtt of role_logical_id" do
      expect(resource.dig("MyLambda", "Properties", "Role"))
        .to eq({"Fn::GetAtt" => ["MyLambdaRole", "Arn"]})
    end

    it "sets Code.S3Bucket to the global Turbofan.config.bucket" do
      expect(resource.dig("MyLambda", "Properties", "Code", "S3Bucket")).to eq("turbofan-shared-bucket")
    end

    it "sets Code.S3Key from config.s3_key" do
      expect(resource.dig("MyLambda", "Properties", "Code", "S3Key"))
        .to eq("demo-production/my-lambda/handler-abc123def456.zip")
    end

    it "sets TURBOFAN_BUCKET env var to Turbofan.config.bucket" do
      expect(resource.dig("MyLambda", "Properties", "Environment", "Variables", "TURBOFAN_BUCKET"))
        .to eq("turbofan-shared-bucket")
    end

    it "sets TURBOFAN_BUCKET_PREFIX from config" do
      expect(resource.dig("MyLambda", "Properties", "Environment", "Variables", "TURBOFAN_BUCKET_PREFIX"))
        .to eq("demo-production")
    end

    it "sets TURBOFAN_CODE_HASH from config" do
      expect(resource.dig("MyLambda", "Properties", "Environment", "Variables", "TURBOFAN_CODE_HASH"))
        .to eq("abc123def456")
    end

    it "sets Tags from config" do
      expect(resource.dig("MyLambda", "Properties", "Tags")).to eq(tags)
    end
  end

  describe ".lambda_role" do
    subject(:resource) { described_class.lambda_role(build_config) }

    it "emits exactly the role_logical_id as the top-level key" do
      expect(resource.keys).to contain_exactly("MyLambdaRole")
    end

    it "is an AWS::IAM::Role type" do
      expect(resource["MyLambdaRole"]["Type"]).to eq("AWS::IAM::Role")
    end

    it "sets RoleName from config" do
      expect(resource.dig("MyLambdaRole", "Properties", "RoleName"))
        .to eq("turbofan-demo-production-mylambda-role")
    end

    it "allows lambda.amazonaws.com to AssumeRole" do
      statement = resource.dig("MyLambdaRole", "Properties", "AssumeRolePolicyDocument", "Statement", 0)
      expect(statement["Principal"]).to eq({"Service" => "lambda.amazonaws.com"})
      expect(statement["Action"]).to eq("sts:AssumeRole")
      expect(statement["Effect"]).to eq("Allow")
    end

    it "includes AWSLambdaBasicExecutionRole managed policy" do
      managed = resource.dig("MyLambdaRole", "Properties", "ManagedPolicyArns")
      expect(managed).to include("arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole")
    end

    it "includes S3Access as the first inline policy" do
      policies = resource.dig("MyLambdaRole", "Properties", "Policies")
      expect(policies.first["PolicyName"]).to eq("S3Access")
    end

    it "S3Access grants GetObject and PutObject on the bucket" do
      s3_policy = resource.dig("MyLambdaRole", "Properties", "Policies").find { |p| p["PolicyName"] == "S3Access" }
      statement = s3_policy.dig("PolicyDocument", "Statement", 0)
      expect(statement["Action"]).to contain_exactly("s3:GetObject", "s3:PutObject")
      expect(statement["Resource"]).to eq("arn:aws:s3:::turbofan-shared-bucket/*")
    end

    it "appends extra_policies after S3Access in order" do
      extra = {
        "PolicyName" => "CustomExtra",
        "PolicyDocument" => {"Version" => "2012-10-17", "Statement" => [{"Action" => "sns:Publish", "Resource" => "*", "Effect" => "Allow"}]}
      }
      config = build_config(extra_policies: [extra])
      policies = described_class.lambda_role(config).dig("MyLambdaRole", "Properties", "Policies")
      expect(policies.size).to eq(2)
      expect(policies.first["PolicyName"]).to eq("S3Access")
      expect(policies.last).to eq(extra)
    end

    it "sets Tags from config" do
      expect(resource.dig("MyLambdaRole", "Properties", "Tags")).to eq(tags)
    end
  end
end
