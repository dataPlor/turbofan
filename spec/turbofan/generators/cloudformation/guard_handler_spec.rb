# frozen_string_literal: true

require "spec_helper"
require "json"
require "open3"

# Executes the actual Python file embedded in the GuardLambda zip and
# asserts on the T1 input transform. build_pipeline_input is a pure
# function; the driver here stubs boto3 imports so the module loads
# without AWS creds / network.
RSpec.describe "GuardLambda build_pipeline_input (T1 transform)" do
  PY_FILE = File.expand_path(
    "../../../../../lib/turbofan/generators/cloudformation/guard_handler.py",
    __FILE__
  )

  def t1(event)
    driver = <<~PY
      import json, sys, types

      # Stub boto3 before the module imports it — the T1 function is pure
      # and doesn't touch AWS, so we only need the name to resolve.
      boto3 = types.ModuleType("boto3")
      boto3.client = lambda name: None
      sys.modules["boto3"] = boto3

      import os
      os.environ["STATE_MACHINE_ARN"] = "arn:aws:states:::stub"

      # Load the module from disk (not as an importable package).
      mod_code = open(#{PY_FILE.inspect}).read()
      mod = types.ModuleType("guard_handler")
      exec(mod_code, mod.__dict__)

      event = json.loads(sys.stdin.read())
      result = mod.build_pipeline_input(event)
      print(json.dumps(result))
    PY

    stdout, stderr, status = Open3.capture3("python3", "-c", driver, stdin_data: JSON.generate(event))
    raise "python failed: #{stderr}" unless status.success?
    JSON.parse(stdout)
  end

  describe "natural EventBridge envelope (trigger :event)" do
    let(:event) do
      {
        "version" => "0",
        "id" => "abc-123",
        "detail-type" => "Object Created",
        "source" => "aws.s3",
        "account" => "111122223333",
        "time" => "2026-04-19T10:00:00Z",
        "region" => "us-east-1",
        "resources" => ["arn:aws:s3:::my-bucket"],
        "detail" => {
          "bucket" => {"name" => "my-bucket"},
          "object" => {"key" => "foo.csv"}
        }
      }
    end

    it "promotes event.detail fields to pipeline input top-level" do
      result = t1(event)
      expect(result["bucket"]).to eq({"name" => "my-bucket"})
      expect(result["object"]).to eq({"key" => "foo.csv"})
    end

    it "injects __event_* metadata at top level" do
      result = t1(event)
      expect(result["__event_source"]).to eq("aws.s3")
      expect(result["__event_detail_type"]).to eq("Object Created")
      expect(result["__event_time"]).to eq("2026-04-19T10:00:00Z")
      expect(result["__event_id"]).to eq("abc-123")
      expect(result["__event_account"]).to eq("111122223333")
      expect(result["__event_region"]).to eq("us-east-1")
    end
  end

  describe "synthetic schedule event (trigger :schedule via Rule Input override)" do
    let(:event) do
      {
        "source" => "aws.scheduler",
        "detail-type" => "Scheduled Event",
        "detail" => {"__event_schedule_expression" => "cron(0 5 * * ? *)"}
      }
    end

    it "passes the synthetic schedule_expression through from detail" do
      result = t1(event)
      expect(result["__event_schedule_expression"]).to eq("cron(0 5 * * ? *)")
    end

    it "attaches __event_source aws.scheduler" do
      result = t1(event)
      expect(result["__event_source"]).to eq("aws.scheduler")
      expect(result["__event_detail_type"]).to eq("Scheduled Event")
    end

    it "fills __event_time with now() when missing" do
      result = t1(event)
      expect(result["__event_time"]).to match(/\d{4}-\d{2}-\d{2}T/)
    end
  end

  describe "items_s3_uri passthrough" do
    let(:event) do
      {
        "source" => "myapp.bulk",
        "detail-type" => "Batch Prepared",
        "detail" => {"items_s3_uri" => "s3://bucket/key/manifest.json"}
      }
    end

    it "preserves items_s3_uri at the pipeline-input top level" do
      result = t1(event)
      expect(result["items_s3_uri"]).to eq("s3://bucket/key/manifest.json")
    end
  end

  describe "Batch job state change" do
    let(:event) do
      {
        "source" => "aws.batch",
        "detail-type" => "Batch Job State Change",
        "detail" => {
          "jobArn" => "arn:aws:batch:...",
          "status" => "SUCCEEDED"
        }
      }
    end

    it "T1-transforms like any other event" do
      result = t1(event)
      expect(result["jobArn"]).to start_with("arn:aws:batch:")
      expect(result["status"]).to eq("SUCCEEDED")
      expect(result["__event_source"]).to eq("aws.batch")
    end
  end

  describe "empty detail" do
    it "produces pipeline input with only __event_* metadata" do
      result = t1({"source" => "aws.s3", "detail-type" => "Object Created", "detail" => {}})
      expect(result.keys).to contain_exactly(
        "__event_source", "__event_detail_type", "__event_time",
        "__event_id", "__event_account", "__event_region"
      )
    end
  end

  describe "missing detail" do
    it "treats missing detail as empty dict" do
      result = t1({"source" => "custom.source"})
      expect(result["__event_source"]).to eq("custom.source")
      expect(result.key?("__event_detail_type")).to be true
    end
  end
end
