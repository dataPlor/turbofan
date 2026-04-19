# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Runtime::Context do
  def build_context(**overrides)
    defaults = {
      execution_id: "exec-1", attempt_number: 1, step_name: "process",
      stage: "production", pipeline_name: "test", array_index: nil,
      storage_path: nil, uses: [], writes_to: []
    }
    described_class.new(**defaults.merge(overrides))
  end

  let(:context) do
    build_context(
      execution_id: "exec-abc123",
      step_name: "generate_csvs",
      pipeline_name: "test-pipeline",
      storage_path: "/mnt/nvme/job-123",
      uses: [{type: :resource, key: :duckdb}]
    )
  end

  describe "#execution_id" do
    it "exposes the execution ID" do
      expect(context.execution_id).to eq("exec-abc123")
    end
  end

  describe "#attempt_number" do
    it "exposes the attempt number" do
      expect(context.attempt_number).to eq(1)
    end
  end

  describe "#step_name" do
    it "exposes the step name" do
      expect(context.step_name).to eq("generate_csvs")
    end
  end

  describe "#stage" do
    it "exposes the stage" do
      expect(context.stage).to eq("production")
    end
  end

  describe "#pipeline_name" do
    it "exposes the pipeline name" do
      expect(context.pipeline_name).to eq("test-pipeline")
    end
  end

  describe "#array_index" do
    it "is nil for non-array jobs" do
      expect(context.array_index).to be_nil
    end

    it "exposes array index when set" do
      ctx = build_context(array_index: 42)
      expect(ctx.array_index).to eq(42)
    end
  end

  describe "#logger" do
    it "provides a structured logger" do
      expect(context.logger).to respond_to(:info)
      expect(context.logger).to respond_to(:warn)
      expect(context.logger).to respond_to(:error)
      expect(context.logger).to respond_to(:debug)
    end

    it "returns a Turbofan::Runtime::Logger instance" do
      expect(context.logger).to be_a(Turbofan::Runtime::Logger)
    end
  end

  describe "#metrics" do
    it "provides a metrics emitter" do
      expect(context.metrics).to respond_to(:emit)
    end

    it "returns a Turbofan::Runtime::Metrics instance" do
      expect(context.metrics).to be_a(Turbofan::Runtime::Metrics)
    end
  end

  describe "#interrupted?" do
    it "is false by default" do
      expect(context.interrupted?).to be false
    end

    it "is true after interrupt!" do
      context.interrupt!
      expect(context.interrupted?).to be true
    end
  end

  describe "#interrupt!" do
    it "sets the interrupted flag" do
      expect { context.interrupt! }.to change(context, :interrupted?).from(false).to(true)
    end

    it "is idempotent" do
      context.interrupt!
      context.interrupt!
      expect(context.interrupted?).to be true
    end
  end

  describe "#storage_path" do
    it "exposes the storage path" do
      expect(context.storage_path).to eq("/mnt/nvme/job-123")
    end

    it "is nil when no local storage is available" do
      ctx = build_context(storage_path: nil)
      expect(ctx.storage_path).to be_nil
    end
  end

  describe "#uses" do
    it "exposes the declared uses dependencies" do
      expect(context.uses).to eq([{type: :resource, key: :duckdb}])
    end
  end

  describe "#writes_to" do
    it "exposes the declared writes_to dependencies" do
      ctx = build_context(writes_to: [{type: :resource, key: :places_write}])
      expect(ctx.writes_to).to eq([{type: :resource, key: :places_write}])
    end

    it "defaults to empty array" do
      ctx = build_context
      expect(ctx.writes_to).to eq([])
    end
  end

  describe "#uses_resources" do
    it "filters uses to resource-type only" do
      ctx = build_context(
        uses: [{type: :resource, key: :duckdb}, {type: :s3, uri: "s3://bucket/path"}]
      )
      expect(ctx.uses_resources).to eq([{type: :resource, key: :duckdb}])
    end
  end

  describe "#writes_to_resources" do
    it "filters writes_to to resource-type only" do
      ctx = build_context(
        writes_to: [{type: :resource, key: :places}, {type: :s3, uri: "s3://out/"}]
      )
      expect(ctx.writes_to_resources).to eq([{type: :resource, key: :places}])
    end
  end

  describe "#duckdb" do
    let(:duckdb_ctx) do
      build_context(uses: [{type: :resource, key: :places_read}])
    end

    it "returns nil when DuckDB constant is not defined" do
      hide_const("DuckDB")
      expect(duckdb_ctx.duckdb).to be_nil
    end

    it "propagates DuckDB init errors" do
      duckdb_mod = Module.new
      duckdb_db = Class.new do
        define_singleton_method(:open) { raise StandardError, "DuckDB init failed" }
      end
      duckdb_mod.const_set(:Database, duckdb_db)
      stub_const("DuckDB", duckdb_mod)

      expect { duckdb_ctx.duckdb }.to raise_error(StandardError, "DuckDB init failed")
    end

    it "returns the same instance across concurrent threads" do
      skip "DuckDB gem not available" unless begin
        require "duckdb"
        true
      rescue LoadError
        false
      end

      results = Array.new(10)
      threads = Array.new(10) do |i|
        Thread.new { results[i] = duckdb_ctx.duckdb }
      end
      threads.each(&:join)

      expect(results.uniq.size).to eq(1)
      expect(results.first).not_to be_nil
    end

    context "with storage_path" do
      before do
        skip "DuckDB gem not available" unless begin
          require "duckdb"
          true
        rescue LoadError
          false
        end
      end

      let(:storage_dir) { Dir.mktmpdir("storage", SPEC_TMP_ROOT) }
      let(:ctx) do
        build_context(storage_path: storage_dir, uses: [{type: :resource, key: :places_read}])
      end

      after do
        FileUtils.remove_entry(storage_dir)
      end

      it "creates DuckDB database file on storage_path" do
        ctx.duckdb
        expect(File.exist?(File.join(storage_dir, "duckdb.db"))).to be true
      end

      it "sets DuckDB temp_directory to storage_path/tmp" do
        result = ctx.duckdb.execute("SELECT current_setting('temp_directory')")
        expect(result.first.first).to eq(File.join(storage_dir, "tmp"))
      end

      it "creates tmp directory on storage_path" do
        ctx.duckdb
        expect(Dir.exist?(File.join(storage_dir, "tmp"))).to be true
      end
    end

    context "without storage_path" do
      before do
        skip "DuckDB gem not available" unless begin
          require "duckdb"
          true
        rescue LoadError
          false
        end
      end

      let(:ctx) do
        build_context(uses: [{type: :resource, key: :places_read}])
      end

      it "creates in-memory DuckDB" do
        expect(ctx.duckdb).not_to be_nil
      end
    end

    context "without resources" do
      it "returns nil" do
        ctx = build_context
        expect(ctx.duckdb).to be_nil
      end
    end

    context "with duckdb_extensions but no resources" do
      before do
        skip "DuckDB gem not available" unless begin
          require "duckdb"
          true
        rescue LoadError
          false
        end
      end

      it "initializes DuckDB when extensions are declared" do
        ctx = build_context(duckdb_extensions: [:json])
        expect(ctx.duckdb).not_to be_nil
      end
    end

    context "with duckdb_extensions" do
      it "executes LOAD for each extension" do
        mock_conn = double("DuckDB::Connection") # rubocop:disable RSpec/VerifiedDoubles
        mock_db = double("DuckDB::Database", connect: mock_conn) # rubocop:disable RSpec/VerifiedDoubles
        duckdb_mod = Module.new
        duckdb_db = Class.new do
          define_singleton_method(:open) { |*_args| mock_db }
        end
        duckdb_mod.const_set(:Database, duckdb_db)
        duckdb_mod.const_set(:Error, Class.new(StandardError))
        stub_const("DuckDB", duckdb_mod)
        allow(mock_conn).to receive(:execute)

        ctx = build_context(
          uses: [{type: :resource, key: :some_db}],
          duckdb_extensions: [:spatial, :h3]
        )
        ctx.duckdb

        expect(mock_conn).to have_received(:execute).with("LOAD spatial").once
        expect(mock_conn).to have_received(:execute).with("LOAD h3").once
      end

      it "defaults duckdb_extensions to empty" do
        ctx = build_context
        expect(ctx.duckdb_extensions).to eq([])
      end

      it "resets @duckdb to nil when a LOAD fails, so a retry can re-init cleanly" do
        mock_conn = double("DuckDB::Connection") # rubocop:disable RSpec/VerifiedDoubles
        mock_db = double("DuckDB::Database", connect: mock_conn) # rubocop:disable RSpec/VerifiedDoubles
        duckdb_mod = Module.new
        duckdb_db = Class.new do
          define_singleton_method(:open) { |*_args| mock_db }
        end
        duckdb_err = Class.new(StandardError)
        duckdb_mod.const_set(:Database, duckdb_db)
        duckdb_mod.const_set(:Error, duckdb_err)
        stub_const("DuckDB", duckdb_mod)
        allow(mock_conn).to receive(:execute).with("LOAD missing_ext")
          .and_raise(duckdb_err.new("extension not found"))
        allow(mock_conn).to receive(:close)

        ctx = build_context(
          uses: [{type: :resource, key: :some_db}],
          duckdb_extensions: [:missing_ext]
        )

        expect { ctx.duckdb }.to raise_error(Turbofan::ExtensionLoadError, /missing_ext/)
        expect(ctx.instance_variable_get(:@duckdb)).to be_nil,
          "expected @duckdb reset to nil after extension load failure"
        expect(mock_conn).to have_received(:close),
          "expected partial DuckDB connection to be closed to release file handle"
      end

      it "resets @duckdb to nil when Database.open itself fails" do
        duckdb_mod = Module.new
        duckdb_db = Class.new do
          define_singleton_method(:open) { |*_args| raise StandardError, "database open failed" }
        end
        duckdb_mod.const_set(:Database, duckdb_db)
        duckdb_mod.const_set(:Error, Class.new(StandardError))
        stub_const("DuckDB", duckdb_mod)

        ctx = build_context(
          uses: [{type: :resource, key: :some_db}]
        )

        expect { ctx.duckdb }.to raise_error(StandardError, /database open failed/)
        expect(ctx.instance_variable_get(:@duckdb)).to be_nil
      end
    end
  end

  describe "#interrupt! cross-thread visibility" do
    it "is visible across threads after interrupt!" do
      ctx = build_context

      seen = false
      thread = Thread.new do
        loop do
          if ctx.interrupted?
            seen = true
            break
          end
          sleep 0.01
        end
      end

      sleep 0.05
      ctx.interrupt!
      thread.join(2)

      expect(seen).to be true
    end
  end

  describe "#size" do
    it "is nil by default" do
      expect(context.size).to be_nil
    end

    it "exposes the size when set" do
      ctx = build_context(size: "m")
      expect(ctx.size).to eq("m")
    end
  end

  # A2: envelope rename — context.envelope accessor
  describe "#envelope" do
    it "defaults to an empty hash" do
      ctx = build_context
      expect(ctx.envelope).to eq({})
    end

    it "exposes envelope metadata when set" do
      ctx = build_context(envelope: {"trace_id" => "abc", "request_id" => "xyz"})
      expect(ctx.envelope).to eq({"trace_id" => "abc", "request_id" => "xyz"})
    end

    it "does not include the inputs key in envelope" do
      ctx = build_context(envelope: {"trace_id" => "abc"})
      expect(ctx.envelope).not_to have_key("inputs")
    end
  end
end
