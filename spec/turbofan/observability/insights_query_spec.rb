# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Observability::InsightsQuery do
  describe "query building" do
    let(:query_builder) do
      described_class.new(
        log_group: "turbofan-test-pipeline-production-logs-process"
      )
    end

    it "builds a base query with log group" do
      query = query_builder.build
      expect(query).to be_a(String)
      expect(query).not_to be_empty
    end

    it "includes fields selection in the base query" do
      query = query_builder.build
      expect(query).to match(/fields|@timestamp|@message/i)
    end

    it "includes sort by timestamp" do
      query = query_builder.build
      expect(query).to match(/sort.*@timestamp/i)
    end
  end

  describe "execution filter" do
    let(:query_builder) do
      described_class.new(
        log_group: "turbofan-test-pipeline-production-logs-process"
      )
    end

    it "adds execution filter" do
      query = query_builder.execution("exec-abc-123").build
      expect(query).to include("exec-abc-123")
    end

    it "filters by execution_id field" do
      query = query_builder.execution("exec-abc-123").build
      expect(query).to match(/execution.*exec-abc-123/i)
    end
  end

  describe "step filter" do
    let(:query_builder) do
      described_class.new(
        log_group: "turbofan-test-pipeline-production-logs-process"
      )
    end

    it "adds step filter" do
      query = query_builder.step("process").build
      expect(query).to include("process")
    end

    it "filters by step field" do
      query = query_builder.step("process").build
      expect(query).to match(/step.*process/i)
    end
  end

  describe "item filter (array index)" do
    let(:query_builder) do
      described_class.new(
        log_group: "turbofan-test-pipeline-production-logs-process"
      )
    end

    it "adds item filter" do
      query = query_builder.item("42").build
      expect(query).to include("42")
    end

    it "filters by array index field" do
      query = query_builder.item("42").build
      expect(query).to match(/item|array_index|index/i)
    end
  end

  describe "custom expression filter" do
    let(:query_builder) do
      described_class.new(
        log_group: "turbofan-test-pipeline-production-logs-process"
      )
    end

    it "adds custom expression filter" do
      query = query_builder.expression("level = 'ERROR'").build
      expect(query).to include("ERROR")
    end
  end

  describe "combining all filters" do
    let(:query_builder) do
      described_class.new(
        log_group: "turbofan-test-pipeline-production-logs-process"
      )
    end

    it "combines execution, step, item, and expression filters" do
      query = query_builder
        .execution("exec-abc")
        .step("process")
        .item("5")
        .expression("level = 'ERROR'")
        .build

      expect(query).to include("exec-abc")
      expect(query).to include("process")
      expect(query).to include("5")
      expect(query).to include("ERROR")
    end

    it "returns a valid CloudWatch Insights query string" do
      query = query_builder
        .execution("exec-abc")
        .step("process")
        .build

      # Should be a parseable query with fields, filter, sort
      expect(query).to match(/fields/i)
      expect(query).to match(/filter/i)
      expect(query).to match(/sort/i)
    end
  end

  describe "method chaining" do
    let(:query_builder) do
      described_class.new(
        log_group: "turbofan-test-pipeline-production-logs-process"
      )
    end

    it "supports fluent method chaining" do
      result = query_builder.execution("exec-1").step("process").item("0")
      expect(result).to respond_to(:build)
    end

    it "does not modify the original builder" do
      base_query = query_builder.build
      _filtered = query_builder.execution("exec-1").build
      expect(query_builder.build).to eq(base_query)
    end
  end

  describe "input validation" do
    let(:query_builder) do
      described_class.new(
        log_group: "turbofan-test-pipeline-production-logs-process"
      )
    end

    describe "#execution" do
      it "accepts valid execution IDs with word chars, hyphens, dots, and colons" do
        expect { query_builder.execution("arn:aws:states:us-east-1:123456:execution:my-sfn:exec-abc123") }.not_to raise_error
        expect { query_builder.execution("exec-abc-123") }.not_to raise_error
        expect { query_builder.execution("exec_123.test:run") }.not_to raise_error
      end

      it "rejects execution IDs with quotes" do
        expect { query_builder.execution('exec" or 1=1 --') }.to raise_error(ArgumentError, /Invalid execution filter value/)
      end

      it "rejects execution IDs with spaces" do
        expect { query_builder.execution("exec abc") }.to raise_error(ArgumentError, /Invalid execution filter value/)
      end

      it "rejects execution IDs with newlines" do
        expect { query_builder.execution("exec\n| limit 1") }.to raise_error(ArgumentError, /Invalid execution filter value/)
      end

      it "rejects empty execution IDs" do
        expect { query_builder.execution("") }.to raise_error(ArgumentError, /Invalid execution filter value/)
      end
    end

    describe "#step" do
      it "accepts valid step names" do
        expect { query_builder.step("process") }.not_to raise_error
        expect { query_builder.step("My.Step-Name_v2") }.not_to raise_error
      end

      it "rejects step names with quotes" do
        expect { query_builder.step('process" or 1=1') }.to raise_error(ArgumentError, /Invalid step filter value/)
      end

      it "rejects step names with special characters" do
        expect { query_builder.step("step;drop") }.to raise_error(ArgumentError, /Invalid step filter value/)
      end
    end

    describe "#item" do
      it "accepts valid numeric indices" do
        expect { query_builder.item("42") }.not_to raise_error
        expect { query_builder.item(42) }.not_to raise_error
        expect { query_builder.item("0") }.not_to raise_error
      end

      it "rejects item indices with injection attempts" do
        expect { query_builder.item("42 or 1=1") }.to raise_error(ArgumentError, /Invalid item filter value/)
      end

      it "rejects item indices with special characters" do
        expect { query_builder.item("42; drop") }.to raise_error(ArgumentError, /Invalid item filter value/)
      end
    end

    describe "#expression" do
      it "accepts raw Insights syntax (power-user feature)" do
        # expression intentionally does NOT validate - it's documented as accepting raw syntax
        expect { query_builder.expression("level = 'ERROR'") }.not_to raise_error
        expect { query_builder.expression("@message like /error/") }.not_to raise_error
      end
    end
  end
end
