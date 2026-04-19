# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Step do
  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::TestCe", klass)
    klass
  end

  describe "single-size step with compute_environment and cpu" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 2
        uses :duckdb
      end
    end

    it "stores the CPU count directly" do
      expect(step_class.turbofan_default_cpu).to eq(2)
    end

    it "does not auto-derive RAM from cpu" do
      expect(step_class.turbofan_default_ram).to be_nil
    end

    it "tracks uses declarations as structured hashes" do
      expect(step_class.turbofan_uses).to eq([{type: :resource, key: :duckdb}])
    end

    it "reports no named sizes for a single-size step" do
      expect(step_class.turbofan_sizes).to be_empty
    end

    it "stores the compute_environment" do
      expect(step_class.turbofan_compute_environment).to eq(:test_ce)
    end
  end

  describe "single-size step with compute_environment and ram" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        ram 4096
      end
    end

    it "stores the RAM value directly" do
      expect(step_class.turbofan_default_ram).to eq(4096)
    end

    it "does not auto-derive CPU from ram" do
      expect(step_class.turbofan_default_cpu).to be_nil
    end
  end

  describe "step with both cpu and ram set directly" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 4
        ram 8192
      end
    end

    it "stores cpu directly as given" do
      expect(step_class.turbofan_default_cpu).to eq(4)
    end

    it "stores ram directly as given" do
      expect(step_class.turbofan_default_ram).to eq(8192)
    end
  end

  describe "uses declarations" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 2
        uses :duckdb
      end
    end

    it "tracks :duckdb in uses" do
      expect(step_class.turbofan_uses).to eq([{type: :resource, key: :duckdb}])
    end

    it "includes :duckdb in turbofan_resource_keys" do
      expect(step_class.turbofan_resource_keys).to include(:duckdb)
    end
  end

  describe "multiple uses declarations" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 2
        uses :duckdb
        uses :gpu
      end
    end

    it "tracks all declared dependencies" do
      expect(step_class.turbofan_uses).to contain_exactly(
        {type: :resource, key: :duckdb},
        {type: :resource, key: :gpu}
      )
    end

    it "returns all resource keys" do
      expect(step_class.turbofan_resource_keys).to contain_exactly(:duckdb, :gpu)
    end
  end

  describe "uses with S3 URI" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 2
        uses "s3://data-bucket/input/*"
      end
    end

    it "stores S3 URIs as structured hashes" do
      expect(step_class.turbofan_uses).to eq([{type: :s3, uri: "s3://data-bucket/input/*"}])
    end

    it "does not include S3 URIs in turbofan_resource_keys" do
      expect(step_class.turbofan_resource_keys).to be_empty
    end

    it "returns S3 deps via the façade (uses_s3)" do
      expect(step_class.turbofan.uses_s3).to eq([{type: :s3, uri: "s3://data-bucket/input/*"}])
    end
  end

  describe "writes_to declarations" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 2
        writes_to :places_write
        writes_to "s3://output-bucket/results/"
      end
    end

    it "stores symbol writes_to as structured hashes" do
      expect(step_class.turbofan_writes_to).to include({type: :resource, key: :places_write})
    end

    it "stores S3 URI writes_to as structured hashes" do
      expect(step_class.turbofan_writes_to).to include({type: :s3, uri: "s3://output-bucket/results/"})
    end

    it "includes write resource keys in turbofan_resource_keys" do
      expect(step_class.turbofan_resource_keys).to include(:places_write)
    end

    it "returns write S3 deps via the façade (writes_to_s3)" do
      expect(step_class.turbofan.writes_to_s3).to eq([{type: :s3, uri: "s3://output-bucket/results/"}])
    end
  end

  describe "reads_from is an alias for uses" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 2
        reads_from :places_read
        reads_from "s3://data-lake/parquet/"
      end
    end

    it "stores reads_from symbols in turbofan_uses" do
      expect(step_class.turbofan_uses).to include({type: :resource, key: :places_read})
    end

    it "stores reads_from S3 URIs in turbofan_uses" do
      expect(step_class.turbofan_uses).to include({type: :s3, uri: "s3://data-lake/parquet/"})
    end
  end

  describe "turbofan_needs_duckdb?" do
    it "is true when step uses :duckdb" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        uses :duckdb
      end
      expect(step.turbofan_needs_duckdb?).to be true
    end

    it "is true when step uses a non-duckdb resource key" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        uses :places_read
      end
      expect(step.turbofan_needs_duckdb?).to be true
    end

    it "is true when step writes_to a resource key" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        writes_to :places_write
      end
      expect(step.turbofan_needs_duckdb?).to be true
    end

    it "is false when step only uses S3 URIs" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        uses "s3://bucket/path"
      end
      expect(step.turbofan_needs_duckdb?).to be false
    end

    it "is false when step has no dependencies" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
      end
      expect(step.turbofan_needs_duckdb?).to be false
    end

    it "is true when step has duckdb extensions but no resource keys" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        uses :duckdb, extensions: [:spatial]
      end
      expect(step.turbofan_needs_duckdb?).to be true
    end
  end

  describe "deduplication" do
    it "does not store duplicate uses" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        uses :duckdb
        uses :duckdb
      end
      expect(step.turbofan_uses.size).to eq(1)
    end

    it "does not store duplicate writes_to" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        writes_to :places
        writes_to :places
      end
      expect(step.turbofan_writes_to.size).to eq(1)
    end
  end

  describe "validation" do
    it "raises on invalid symbol format" do
      ce = ce_class
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          uses :"Invalid-Key"
        end
      }.to raise_error(ArgumentError, /resource key/)
    end

    it "raises on non-S3 string argument" do
      ce = ce_class
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          uses "http://example.com"
        end
      }.to raise_error(ArgumentError, /S3 URI/)
    end

    it "raises on non-symbol non-string argument" do
      ce = ce_class
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          uses 42
        end
      }.to raise_error(ArgumentError, /Symbol.*S3 URI/)
    end
  end

  describe "combined uses and writes_to resource keys" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 2
        uses :places_read
        uses :duckdb
        writes_to :places_write
        uses "s3://input-bucket/data/"
        writes_to "s3://output-bucket/results/"
      end
    end

    it "returns all resource keys from both uses and writes_to" do
      expect(step_class.turbofan_resource_keys).to contain_exactly(:places_read, :duckdb, :places_write)
    end

    it "deduplicates keys appearing in both uses and writes_to" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        uses :shared_db
        writes_to :shared_db
      end
      expect(step.turbofan_resource_keys).to eq([:shared_db])
    end
  end

  describe "timeout" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        timeout 7200
      end
    end

    it "stores the custom timeout" do
      expect(step_class.turbofan_timeout).to eq(7200)
    end
  end

  describe "retries" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        retries 5
      end
    end

    it "stores the custom retries count" do
      expect(step_class.turbofan_retries).to eq(5)
    end
  end

  describe "secret declarations" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        secret :db_url, from: "turbofan/my-pipeline/db-url"
        secret :api_key, from: "turbofan/my-pipeline/api-key"
      end
    end

    it "stores all secret declarations with name and path" do
      expect(step_class.turbofan_secrets).to eq([
        {name: :db_url, from: "turbofan/my-pipeline/db-url"},
        {name: :api_key, from: "turbofan/my-pipeline/api-key"}
      ])
    end
  end

  # ---------------------------------------------------------------------------
  # B2 — Retry with error-type filtering
  # ---------------------------------------------------------------------------
  describe "retries with on: parameter (B2)" do
    context "catch-all retries (current behavior)" do
      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          retries 3
        end
      end

      it "sets turbofan_retries to 3" do
        expect(step_class.turbofan_retries).to eq(3)
      end

      it "sets turbofan_retry_on to nil" do
        expect(step_class.turbofan_retry_on).to be_nil
      end
    end

    context "retries with single error type" do
      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          retries 3, on: ["States.TaskFailed"]
        end
      end

      it "sets turbofan_retries to 3" do
        expect(step_class.turbofan_retries).to eq(3)
      end

      it "sets turbofan_retry_on to the specified error types" do
        expect(step_class.turbofan_retry_on).to eq(["States.TaskFailed"])
      end
    end

    context "retries with multiple error types" do
      let(:step_class) do
        ce = ce_class
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          retries 2, on: ["States.Timeout", "Batch.ServerException"]
        end
      end

      it "sets turbofan_retries to 2" do
        expect(step_class.turbofan_retries).to eq(2)
      end

      it "stores multiple error types in turbofan_retry_on" do
        expect(step_class.turbofan_retry_on).to eq(["States.Timeout", "Batch.ServerException"])
      end
    end
  end

  describe "defaults" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
      end
    end

    it "defaults timeout to nil (no timeout)" do
      expect(step_class.turbofan_timeout).to be_nil
    end

    it "defaults retries to 3" do
      expect(step_class.turbofan_retries).to eq(3)
    end

    it "has no uses by default" do
      expect(step_class.turbofan_uses).to be_empty
    end

    it "has no writes_to by default" do
      expect(step_class.turbofan_writes_to).to be_empty
    end

    it "has no secrets by default" do
      expect(step_class.turbofan_secrets).to be_empty
    end

    it "has no named sizes by default" do
      expect(step_class.turbofan_sizes).to be_empty
    end
  end

  describe "class isolation" do
    let(:step_a) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 2
        uses :duckdb
        retries 5
      end
    end

    let(:step_b) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        ram 16
      end
    end

    it "does not leak state between step classes" do
      # Force both to load
      step_a
      step_b

      expect(step_a.turbofan_default_cpu).to eq(2)
      expect(step_b.turbofan_default_ram).to eq(16)

      expect(step_a.turbofan_uses).to eq([{type: :resource, key: :duckdb}])
      expect(step_b.turbofan_uses).to be_empty

      expect(step_a.turbofan_retries).to eq(5)
      expect(step_b.turbofan_retries).to eq(3)
    end
  end

  describe "family is removed" do
    it "raises NoMethodError when family is called" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          family :c
        end
      }.to raise_error(NoMethodError)
    end

    it "does not have VALID_FAMILIES constant" do
      expect(described_class).not_to be_const_defined(:VALID_FAMILIES)
    end

    it "does not expose turbofan_family reader" do
      ce = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::FamTest", ce)
      step_class = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :fam_test
        cpu 1
      end
      expect(step_class).not_to respond_to(:turbofan_family)
    end
  end

  describe "validation: cpu without compute_environment" do
    it "does not raise when cpu is called without compute_environment (A1: lazy validation)" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          cpu 2
        end
      }.not_to raise_error
    end
  end

  describe "validation: ram without compute_environment" do
    it "does not raise when ram is called without compute_environment (A1: lazy validation)" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          ram 4096
        end
      }.not_to raise_error
    end
  end

  # A1: Lazy validation of compute_environment
  describe "lazy validation: cpu before compute_environment" do
    it "does NOT raise when cpu is declared before compute_environment" do
      ce = ce_class
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          cpu 2
          compute_environment :test_ce
        end
      }.not_to raise_error
    end
  end

  # A1: Lazy validation of compute_environment
  describe "lazy validation: ram before compute_environment" do
    it "does NOT raise when ram is declared before compute_environment" do
      ce = ce_class
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          ram 4096
          compute_environment :test_ce
        end
      }.not_to raise_error
    end
  end

  # A1: Lazy validation of compute_environment
  describe "lazy validation: size before compute_environment" do
    it "does NOT raise when size is declared before compute_environment" do
      ce = ce_class
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          size :s, cpu: 1, ram: 2048
          compute_environment :test_ce
        end
      }.not_to raise_error
    end
  end

  describe "validation: non-positive cpu" do
    it "raises ArgumentError for cpu of 0" do
      expect {
        ce = Class.new { include Turbofan::ComputeEnvironment }
        stub_const("ComputeEnvironments::CpuZero", ce)
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :cpu_zero
          cpu 0
        end
      }.to raise_error(ArgumentError, /cpu must be a positive number/)
    end

    it "raises ArgumentError for negative cpu" do
      expect {
        ce = Class.new { include Turbofan::ComputeEnvironment }
        stub_const("ComputeEnvironments::CpuNeg", ce)
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :cpu_neg
          cpu(-1)
        end
      }.to raise_error(ArgumentError, /cpu must be a positive number/)
    end
  end

  describe "validation: non-positive ram" do
    it "raises ArgumentError for ram of 0" do
      expect {
        ce = Class.new { include Turbofan::ComputeEnvironment }
        stub_const("ComputeEnvironments::RamZero", ce)
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :ram_zero
          ram 0
        end
      }.to raise_error(ArgumentError, /ram must be a positive number/)
    end

    it "raises ArgumentError for negative ram" do
      expect {
        ce = Class.new { include Turbofan::ComputeEnvironment }
        stub_const("ComputeEnvironments::RamNeg", ce)
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :ram_neg
          ram(-1)
        end
      }.to raise_error(ArgumentError, /ram must be a positive number/)
    end
  end

  describe "step call interface" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1

        def call(inputs, context)
          {processed: true, count: inputs.size}
        end
      end
    end

    it "can be instantiated and called with inputs and context" do
      instance = step_class.new
      result = instance.call([1, 2, 3], double("context"))
      expect(result).to eq(processed: true, count: 3)
    end
  end

  describe "compute_environment DSL" do
    it "accepts a Symbol and stores it" do
      step_class = Class.new do
        include Turbofan::Step
        execution :batch
      end
      step_class.compute_environment(:test_ce)
      expect(step_class.turbofan_compute_environment).to eq(:test_ce)
    end

    it "raises ArgumentError if not given a Symbol" do
      bad_class = Class.new
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment bad_class
        end
      }.to raise_error(ArgumentError, /must be a Symbol/)
    end

    it "defaults to nil" do
      step_class = Class.new do
        include Turbofan::Step
        execution :batch
      end
      expect(step_class.turbofan_compute_environment).to be_nil
    end
  end

  # A8: Rename secret -> inject_secret
  describe "inject_secret declarations" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        inject_secret :db_url, from: "arn:aws:secretsmanager:us-east-1:123456789:secret:db-url"
      end
    end

    it "adds to turbofan_secrets via inject_secret" do
      expect(step_class.turbofan_secrets).to eq([
        {name: :db_url, from: "arn:aws:secretsmanager:us-east-1:123456789:secret:db-url"}
      ])
    end
  end

  # A8: Rename secret -> inject_secret (backward compat alias)
  describe "secret still works as backward-compat alias for inject_secret" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        secret :api_key, from: "arn:aws:secretsmanager:us-east-1:123456789:secret:api-key"
      end
    end

    it "adds to turbofan_secrets via the old secret method" do
      expect(step_class.turbofan_secrets).to eq([
        {name: :api_key, from: "arn:aws:secretsmanager:us-east-1:123456789:secret:api-key"}
      ])
    end
  end

  describe "uses :duckdb with extensions:" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 2
        uses :duckdb, extensions: [:spatial, :h3]
      end
    end

    it "stores declared extensions" do
      expect(step_class.turbofan_duckdb_extensions).to eq([:spatial, :h3])
    end

    it "deduplicates extensions" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        uses :duckdb, extensions: [:spatial, :spatial, :h3]
      end
      expect(step.turbofan_duckdb_extensions).to eq([:spatial, :h3])
    end

    it "converts string extensions to symbols" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        uses :duckdb, extensions: ["spatial"]
      end
      expect(step.turbofan_duckdb_extensions).to eq([:spatial])
    end

    it "raises ArgumentError for non-duckdb target with extensions" do
      ce = ce_class
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          uses :gpu, extensions: [:spatial]
        end
      }.to raise_error(ArgumentError, /only supported for :duckdb/)
    end

    it "raises ArgumentError for invalid extension names" do
      ce = ce_class
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :test_ce
          cpu 1
          uses :duckdb, extensions: [:"Invalid-Ext"]
        end
      }.to raise_error(ArgumentError, /invalid extension name/)
    end

    it "defaults to empty extensions" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
      end
      expect(step.turbofan_duckdb_extensions).to eq([])
    end

    it "accumulates extensions across multiple uses :duckdb calls" do
      ce = ce_class
      step = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        uses :duckdb, extensions: [:spatial]
        uses :duckdb, extensions: [:h3]
      end
      expect(step.turbofan_duckdb_extensions).to eq([:spatial, :h3])
    end
  end

  describe "docker_image and turbofan_external?" do
    it "returns true for a non-empty docker image URI" do
      ce = ce_class
      step_class = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        docker_image "123456789.dkr.ecr.us-east-1.amazonaws.com/my-step:latest"
      end
      expect(step_class.turbofan_external?).to be true
    end

    it "returns false when docker_image is set to an empty string" do
      ce = ce_class
      step_class = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
        docker_image ""
      end
      expect(step_class.turbofan_external?).to be false
    end

    it "returns false when docker_image is not set" do
      ce = ce_class
      step_class = Class.new do
        include Turbofan::Step
        execution :batch
        compute_environment :test_ce
        cpu 1
      end
      expect(step_class.turbofan_external?).to be false
    end
  end

  describe "execution model DSL" do
    it "sets execution to :batch" do
      klass = Class.new do
        include Turbofan::Step
        execution :batch
      end
      expect(klass.turbofan_execution).to eq(:batch)
    end

    it "sets execution to :lambda" do
      klass = Class.new do
        include Turbofan::Step
        execution :lambda
      end
      expect(klass.turbofan_execution).to eq(:lambda)
    end

    it "sets execution to :fargate" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
      end
      expect(klass.turbofan_execution).to eq(:fargate)
    end

    it "defaults to nil" do
      klass = Class.new { include Turbofan::Step }
      expect(klass.turbofan_execution).to be_nil
    end

    it "raises ArgumentError for invalid execution model" do
      expect {
        Class.new do
          include Turbofan::Step
          runs_on :kubernetes
        end
      }.to raise_error(ArgumentError, /runs_on must be one of/)
    end

    it "turbofan_lambda? returns true for :lambda" do
      klass = Class.new do
        include Turbofan::Step
        execution :lambda
      end
      expect(klass.turbofan_lambda?).to be true
      expect(klass.turbofan_fargate?).to be false
    end

    it "turbofan_fargate? returns true for :fargate" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
      end
      expect(klass.turbofan_fargate?).to be true
      expect(klass.turbofan_lambda?).to be false
    end

    it "turbofan_lambda? and turbofan_fargate? both false for :batch" do
      klass = Class.new do
        include Turbofan::Step
        execution :batch
      end
      expect(klass.turbofan_lambda?).to be false
      expect(klass.turbofan_fargate?).to be false
    end
  end

  describe "subnets" do
    it "stores subnets as array on Fargate step" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
        subnets ["subnet-abc", "subnet-def"]
      end
      expect(klass.turbofan_subnets).to eq(["subnet-abc", "subnet-def"])
    end

    it "wraps a single value in an array" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
        subnets "subnet-abc"
      end
      expect(klass.turbofan_subnets).to eq(["subnet-abc"])
    end

    it "defaults to nil" do
      klass = Class.new { include Turbofan::Step }
      expect(klass.turbofan_subnets).to be_nil
    end

    it "raises on Batch step" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          subnets ["subnet-abc"]
        end
      }.to raise_error(ArgumentError, /only valid for execution :fargate/)
    end

    it "raises on Lambda step" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :lambda
          subnets ["subnet-abc"]
        end
      }.to raise_error(ArgumentError, /only valid for execution :fargate/)
    end

    it "resolved_subnets returns step-level when set" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
        subnets ["subnet-step"]
      end
      expect(klass.resolved_subnets).to eq(["subnet-step"])
    end

    it "resolved_subnets falls back to Turbofan.config" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
      end
      allow(Turbofan.config).to receive(:subnets).and_return(["subnet-config"])
      expect(klass.resolved_subnets).to eq(["subnet-config"])
    end
  end

  describe "security_groups" do
    it "stores security_groups as array on Fargate step" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
        security_groups ["sg-abc"]
      end
      expect(klass.turbofan_security_groups).to eq(["sg-abc"])
    end

    it "defaults to nil" do
      klass = Class.new { include Turbofan::Step }
      expect(klass.turbofan_security_groups).to be_nil
    end

    it "raises on Batch step" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          security_groups ["sg-abc"]
        end
      }.to raise_error(ArgumentError, /only valid for execution :fargate/)
    end

    it "raises on Lambda step" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :lambda
          security_groups ["sg-abc"]
        end
      }.to raise_error(ArgumentError, /only valid for execution :fargate/)
    end

    it "resolved_security_groups returns step-level when set" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
        security_groups ["sg-step"]
      end
      expect(klass.resolved_security_groups).to eq(["sg-step"])
    end

    it "resolved_security_groups falls back to Turbofan.config" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
      end
      allow(Turbofan.config).to receive(:security_groups).and_return(["sg-config"])
      expect(klass.resolved_security_groups).to eq(["sg-config"])
    end
  end

  describe "storage" do
    it "stores storage value on Fargate step" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
        storage 100
      end
      expect(klass.turbofan_storage).to eq(100)
    end

    it "defaults to nil" do
      klass = Class.new { include Turbofan::Step }
      expect(klass.turbofan_storage).to be_nil
    end

    it "raises on Batch step" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          storage 50
        end
      }.to raise_error(ArgumentError, /only valid for execution :fargate/)
    end

    it "raises on Lambda step" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :lambda
          storage 50
        end
      }.to raise_error(ArgumentError, /only valid for execution :fargate/)
    end

    it "raises for value below 21" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :fargate
          storage 20
        end
      }.to raise_error(ArgumentError, /between 21 and 200/)
    end

    it "raises for value above 200" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :fargate
          storage 201
        end
      }.to raise_error(ArgumentError, /between 21 and 200/)
    end

    it "raises for non-integer" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :fargate
          storage 50.5
        end
      }.to raise_error(ArgumentError, /between 21 and 200/)
    end

    it "accepts boundary value 21" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
        storage 21
      end
      expect(klass.turbofan_storage).to eq(21)
    end

    it "accepts boundary value 200" do
      klass = Class.new do
        include Turbofan::Step
        execution :fargate
        storage 200
      end
      expect(klass.turbofan_storage).to eq(200)
    end
  end
end
