# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Turbofan::Step DSL — uses block form (8a)" do
  after { Turbofan::Deprecations.reset_seen! }

  it "accepts `uses :duckdb do extensions :json, :parquet end`" do
    step = Class.new do
      include Turbofan::Step
      uses :duckdb do
        extensions :json, :parquet
      end
    end
    stub_const("BlockUsesStep", step)

    expect(step.turbofan_duckdb_extensions).to contain_exactly(:json, :parquet)
    expect(step.turbofan_uses).to include({type: :resource, key: :duckdb})
  end

  it "raises when the block form is used on a non-:duckdb target" do
    expect {
      Class.new do
        include Turbofan::Step
        uses :postgres do
          extensions :json
        end
      end
    }.to raise_error(ArgumentError, /block form is only supported for :duckdb/)
  end

  it "raises when both block and extensions: kwarg are passed" do
    expect {
      Class.new do
        include Turbofan::Step
        uses :duckdb, extensions: [:json] do
          extensions :parquet
        end
      end
    }.to raise_error(ArgumentError, /cannot pass both/)
  end

  it "still accepts the legacy kwarg form (deprecated)" do
    step = Class.new do
      include Turbofan::Step
      uses :duckdb, extensions: [:json, :parquet]
    end
    stub_const("LegacyUsesStep", step)

    expect(step.turbofan_duckdb_extensions).to contain_exactly(:json, :parquet)
  end

  it "emits a one-time deprecation warning for the kwarg form when warnings enabled" do
    Turbofan.config.deprecations = true
    captured = StringIO.new
    orig = $stderr
    $stderr = captured
    begin
      Class.new do
        include Turbofan::Step
        uses :duckdb, extensions: [:json]
      end
    ensure
      $stderr = orig
      Turbofan.config.deprecations = nil
    end
    expect(captured.string).to include("Turbofan Deprecation")
    expect(captured.string).to include("block form")
  end

  it "does NOT emit the deprecation warning by default (Turbofan.config.deprecations = nil, $VERBOSE false)" do
    captured = StringIO.new
    orig = $stderr
    orig_verbose = $VERBOSE
    $stderr = captured
    $VERBOSE = false
    begin
      Class.new do
        include Turbofan::Step
        uses :duckdb, extensions: [:json]
      end
    ensure
      $stderr = orig
      $VERBOSE = orig_verbose
    end
    expect(captured.string).to eq("")
  end

  it "only warns once per class even across many calls" do
    Turbofan.config.deprecations = true
    captured = StringIO.new
    orig = $stderr
    $stderr = captured
    begin
      klass = Class.new do
        include Turbofan::Step
      end
      klass.uses :duckdb, extensions: [:json]
      klass.uses :duckdb, extensions: [:parquet]
      klass.uses :duckdb, extensions: [:spatial]
    ensure
      $stderr = orig
      Turbofan.config.deprecations = nil
    end
    warn_count = captured.string.scan(/Turbofan Deprecation/).size
    expect(warn_count).to eq(1)
  end
end

RSpec.describe "Turbofan::Step DSL — runs_on (8b)" do
  after { Turbofan::Deprecations.reset_seen! }

  it "accepts `runs_on :batch` and stores it in turbofan_execution" do
    step = Class.new do
      include Turbofan::Step
      runs_on :batch
    end
    stub_const("RunsOnBatchStep", step)
    expect(step.turbofan_execution).to eq(:batch)
  end

  it "accepts :lambda and :fargate" do
    [:lambda, :fargate].each do |model|
      step = Class.new do
        include Turbofan::Step
      end
      step.runs_on(model)
      expect(step.turbofan_execution).to eq(model)
    end
  end

  it "raises on invalid models" do
    expect {
      Class.new do
        include Turbofan::Step
        runs_on :ec2
      end
    }.to raise_error(ArgumentError, /runs_on must be one of/)
  end

  it "the legacy `execution` macro was removed in 0.7" do
    expect {
      Class.new do
        include Turbofan::Step
        execution :batch
      end
    }.to raise_error(NoMethodError, /undefined method ['`]execution/)
  end
end

RSpec.describe "Turbofan::Step DSL — .turbofan Façade (8c)" do
  let(:step) do
    Class.new do
      include Turbofan::Step
      runs_on :batch
      uses :postgres
      writes_to :places_db
      cpu 4
      ram 8
      batch_size 50
      retries 5
      tags(owner: "data", team: "platform")
    end
  end

  before { stub_const("FacadeTestStep", step) }

  it "exposes turbofan.uses without the turbofan_ prefix" do
    expect(step.turbofan.uses).to eq([{type: :resource, key: :postgres}])
  end

  it "exposes turbofan.execution (current attr name) identical to legacy turbofan_execution" do
    expect(step.turbofan.execution).to eq(step.turbofan_execution)
    expect(step.turbofan.execution).to eq(:batch)
  end

  it "exposes turbofan.tags, batch_size, retries, cpu, ram" do
    expect(step.turbofan.tags).to eq({"owner" => "data", "team" => "platform"})
    expect(step.turbofan.batch_size).to eq(50)
    expect(step.turbofan.retries).to eq(5)
    expect(step.turbofan.default_cpu).to eq(4)
    expect(step.turbofan.default_ram).to eq(8)
  end

  it "exposes predicate readers (lambda?, fargate?, external?)" do
    expect(step.turbofan.lambda?).to be false
    expect(step.turbofan.fargate?).to be false
    expect(step.turbofan.external?).to be false
  end

  it "memoizes the façade (same object across calls)" do
    expect(step.turbofan).to equal(step.turbofan)
  end

  it "inspect dumps all fields for debugging" do
    output = step.turbofan.inspect
    expect(output).to include("ConfigFacade")
    expect(output).to include("execution=:batch")
    expect(output).to include("batch_size=50")
  end

  it "legacy readers still work for 0.6 backward-compat" do
    expect(step.turbofan_uses).to eq(step.turbofan.uses)
    expect(step.turbofan_batch_size).to eq(step.turbofan.batch_size)
    expect(step.turbofan_tags).to eq(step.turbofan.tags)
  end

  it "subclass gets its own façade, isolated from parent state" do
    child = Class.new(step) do
      uses :redis
    end
    stub_const("FacadeChild", child)

    # Per Turbofan::Step.inherited(), subclasses start with fresh DSL
    # state (empty containers, nil scalars) — they don't inherit parent
    # uses/writes_to/etc. This is the isolation guarantee subclassing
    # specs assert separately.
    expect(child.turbofan).not_to equal(step.turbofan)
    expect(child.turbofan.uses.map { |d| d[:key] }).to eq([:redis])
    expect(step.turbofan.uses.map { |d| d[:key] }).to eq([:postgres])
  end
end

RSpec.describe "Turbofan::Step DSL — polymorphic input_schema/output_schema (0.6.1)" do
  it "accepts a Hash literal for input_schema" do
    schema_hash = {"type" => "object", "properties" => {"id" => {"type" => "string"}}, "required" => ["id"]}
    step = Class.new do
      include Turbofan::Step
      input_schema schema_hash
    end
    stub_const("HashInputStep", step)
    expect(step.turbofan_input_schema).to eq(schema_hash)
    expect(step.turbofan_input_schema_file).to be_nil
  end

  it "accepts a Hash literal for output_schema" do
    schema_hash = {"type" => "object", "properties" => {"result" => {"type" => "boolean"}}}
    step = Class.new do
      include Turbofan::Step
      output_schema schema_hash
    end
    stub_const("HashOutputStep", step)
    expect(step.turbofan_output_schema).to eq(schema_hash)
    expect(step.turbofan_output_schema_file).to be_nil
  end

  it "accepts a Class/Module responding to .schema" do
    schema_hash = {"type" => "object", "properties" => {"n" => {"type" => "integer"}}}
    schema_class = Class.new do
      define_singleton_method(:schema) { schema_hash }
    end
    stub_const("MySchemaClass", schema_class)

    step = Class.new do
      include Turbofan::Step
      input_schema MySchemaClass
    end
    stub_const("ClassSchemaStep", step)
    expect(step.turbofan_input_schema).to eq(schema_hash)
  end

  it "still accepts a filename String (backward-compat)" do
    step = Class.new do
      include Turbofan::Step
      input_schema "passthrough.json"
    end
    stub_const("FilenameInputStep", step)
    expect(step.turbofan_input_schema_file).to eq("passthrough.json")
  end

  it "raises for a Class that doesn't respond to .schema" do
    broken_class = Class.new
    stub_const("BrokenSchemaClass", broken_class)
    expect {
      Class.new do
        include Turbofan::Step
        input_schema BrokenSchemaClass
      end
    }.to raise_error(ArgumentError, /expects a filename String, a Hash, or a Class.*responding to .schema/)
  end

  it "raises when .schema returns non-Hash" do
    bad_schema = Class.new do
      define_singleton_method(:schema) { "not a hash" }
    end
    stub_const("BadReturnSchema", bad_schema)
    expect {
      Class.new do
        include Turbofan::Step
        input_schema BadReturnSchema
      end
    }.to raise_error(ArgumentError, /must return a Hash/)
  end
end
