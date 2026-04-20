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

      boto3 = types.ModuleType("boto3")
      boto3.client = lambda name: None
      sys.modules["boto3"] = boto3

      import os
      os.environ["STATE_MACHINE_ARN"] = "arn:aws:states:::stub"

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

    it "nests all provenance under _turbofan.event (no flat __event_* keys)" do
      result = t1(event)
      expect(result.keys).not_to include(a_string_matching(/\A__event_/))
      ev = result.dig("_turbofan", "event")
      expect(ev["source"]).to eq("aws.s3")
      expect(ev["detail_type"]).to eq("Object Created")
      expect(ev["time"]).to eq("2026-04-19T10:00:00Z")
      expect(ev["id"]).to eq("abc-123")
      expect(ev["account"]).to eq("111122223333")
      expect(ev["region"]).to eq("us-east-1")
    end
  end

  describe "synthetic schedule event (trigger :schedule via Rule Input override)" do
    let(:event) do
      {
        "source" => "aws.scheduler",
        "detail-type" => "Scheduled Event",
        "detail" => {
          "_turbofan" => {
            "event" => {"schedule_expression" => "cron(0 5 * * ? *)"}
          }
        }
      }
    end

    it "lifts the schedule expression into _turbofan.event namespace" do
      result = t1(event)
      expect(result.dig("_turbofan", "event", "schedule_expression")).to eq("cron(0 5 * * ? *)")
    end

    it "sets source = aws.scheduler and detail_type" do
      result = t1(event)
      expect(result.dig("_turbofan", "event", "source")).to eq("aws.scheduler")
      expect(result.dig("_turbofan", "event", "detail_type")).to eq("Scheduled Event")
    end

    it "fills time with now() when missing" do
      result = t1(event)
      expect(result.dig("_turbofan", "event", "time")).to match(/\d{4}-\d{2}-\d{2}T/)
    end

    it "does not leak the raw _turbofan sub-hash from detail into top level" do
      # Only the top-level _turbofan key — nothing else with that name.
      expect(result_for_schedule = t1(event)).not_to have_key("schedule_expression")
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
      expect(result.dig("_turbofan", "event", "source")).to eq("aws.batch")
    end
  end

  describe "empty detail" do
    it "produces pipeline input with only the _turbofan namespace" do
      result = t1({"source" => "aws.s3", "detail-type" => "Object Created", "detail" => {}})
      expect(result.keys).to eq(["_turbofan"])
    end
  end

  describe "missing detail" do
    it "treats missing detail as empty dict" do
      result = t1({"source" => "custom.source"})
      expect(result.dig("_turbofan", "event", "source")).to eq("custom.source")
      expect(result.dig("_turbofan", "event")).to have_key("detail_type")
    end
  end

  describe "collision safety" do
    it "does not clobber user detail fields that happen to be named like event fields" do
      # A publisher's detail carries `source` as business data; our T1 must
      # not blow it away. source is namespaced under _turbofan.event.
      result = t1({
        "source" => "aws.custom",
        "detail-type" => "Widget Event",
        "detail" => {"source" => "user-supplied-provenance"}
      })
      expect(result["source"]).to eq("user-supplied-provenance")
      expect(result.dig("_turbofan", "event", "source")).to eq("aws.custom")
    end

    it "preserves unrelated _turbofan sub-keys the publisher set" do
      # An upstream system can pass through their own namespaced
      # provenance in _turbofan.* — we only overwrite the `event` sub-hash.
      result = t1({
        "source" => "upstream",
        "detail-type" => "Handoff",
        "detail" => {
          "_turbofan" => {
            "upstream_trace_id" => "trace-xyz",
            "event" => {"should_be_overwritten" => true}
          }
        }
      })
      expect(result.dig("_turbofan", "upstream_trace_id")).to eq("trace-xyz")
      expect(result.dig("_turbofan", "event", "source")).to eq("upstream")
      expect(result.dig("_turbofan", "event")).not_to have_key("should_be_overwritten")
    end
  end
end
