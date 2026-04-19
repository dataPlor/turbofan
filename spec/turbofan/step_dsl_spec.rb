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
