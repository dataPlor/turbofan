# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"
require "securerandom"

RSpec.describe Turbofan::Deploy::PipelineLoader do
  let(:tmpdir) { Dir.mktmpdir("turbofan-loader-test", SPEC_TMP_ROOT) }
  let(:turbofans_root) { File.join(tmpdir, "turbofans") }
  let(:pipeline_file) { File.join(turbofans_root, "pipelines", "my_pipeline.rb") }
  let(:schemas_dir) { File.join(turbofans_root, "schemas") }

  before do
    # Remove constants that may have been defined by previous test runs
    %i[MyPipeline GenerateCsvs BulkLoad].each do |name|
      Object.send(:remove_const, name) if Object.const_defined?(name) # rubocop:disable RSpec/RemoveConst
    end
    FileUtils.mkdir_p(File.join(turbofans_root, "pipelines"))
    FileUtils.mkdir_p(File.join(turbofans_root, "steps", "generate_csvs"))
    FileUtils.mkdir_p(File.join(turbofans_root, "steps", "bulk_load"))
    FileUtils.mkdir_p(schemas_dir)

    File.write(File.join(schemas_dir, "passthrough.json"), '{"type": "object"}')

    File.write(File.join(turbofans_root, "steps", "generate_csvs", "worker.rb"), <<~RUBY)
      class GenerateCsvs
        include Turbofan::Step

        compute_environment :test_ce
        runs_on :batch
        cpu 2
        uses :duckdb
        input_schema "passthrough.json"
        output_schema "passthrough.json"

        def call(inputs, context)
          {rows: 100}
        end
      end
    RUBY

    File.write(File.join(turbofans_root, "steps", "bulk_load", "worker.rb"), <<~RUBY)
      class BulkLoad
        include Turbofan::Step

        compute_environment :test_ce
        runs_on :batch
        cpu 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"

        def call(inputs, context)
          {loaded: true}
        end
      end
    RUBY

    File.write(pipeline_file, <<~RUBY)
      require_relative "../steps/generate_csvs/worker"
      require_relative "../steps/bulk_load/worker"

      class MyPipeline
        include Turbofan::Pipeline

        pipeline_name "my-pipeline"

        metric "rows_processed", stat: :sum, display: :line, unit: "rows"

        pipeline do
          results = generate_csvs(trigger_input)
          bulk_load(results)
        end
      end
    RUBY
  end

  after do
    FileUtils.rm_rf(tmpdir)
    Turbofan.config.schemas_path = nil
    %i[MyPipeline GenerateCsvs BulkLoad].each do |name|
      Object.send(:remove_const, name) if Object.const_defined?(name) # rubocop:disable RSpec/RemoveConst
    end
  end

  describe ".load" do
    let(:result) { described_class.load(pipeline_file, turbofans_root: turbofans_root) }

    it "returns a result with the pipeline class" do
      expect(result.pipeline).not_to be_nil
    end

    it "loads a class that includes Turbofan::Pipeline" do
      expect(result.pipeline.turbofan_name).to eq("my-pipeline")
    end

    it "returns a hash of step classes keyed by name" do
      expect(result.steps).to be_a(Hash)
      expect(result.steps.keys).to contain_exactly(:generate_csvs, :bulk_load)
    end

    it "loads step classes that include Turbofan::Step" do
      generate = result.steps[:generate_csvs]
      expect(generate.turbofan_compute_environment).to eq(:test_ce)
      expect(generate.turbofan.default_cpu).to eq(2)
      expect(generate.turbofan.uses).to include({type: :resource, key: :duckdb})
    end

    it "loads all step classes with correct config" do
      bulk = result.steps[:bulk_load]
      expect(bulk.turbofan_compute_environment).to eq(:test_ce)
      expect(bulk.turbofan.default_cpu).to eq(1)
      expect(bulk.turbofan.uses).to be_empty
    end

    it "returns step_dirs mapping step names to directory paths" do
      expect(result.step_dirs).to be_a(Hash)
      expect(result.step_dirs.keys).to contain_exactly(:generate_csvs, :bulk_load)
      expect(result.step_dirs[:generate_csvs]).to eq(File.join(turbofans_root, "steps", "generate_csvs"))
      expect(result.step_dirs[:bulk_load]).to eq(File.join(turbofans_root, "steps", "bulk_load"))
    end

    it "sets Turbofan.config.schemas_path" do
      result
      expect(Turbofan.config.schemas_path).to eq(schemas_dir)
    end
  end

  # Bug 5: resolve_steps raises "Step directory not found" for external steps
  # that have docker_image set. External steps don't need a local directory
  # because they use a pre-built image, but the directory check is applied
  # unconditionally to all steps.
  describe "external steps with docker_image" do
    before do
      # Remove any constants that may conflict
      %i[MyExternalPipeline LocalStep ExternalStep].each do |name|
        Object.send(:remove_const, name) if Object.const_defined?(name) # rubocop:disable RSpec/RemoveConst
      end

      FileUtils.mkdir_p(File.join(turbofans_root, "steps", "local_step"))
      FileUtils.mkdir_p(schemas_dir)
      File.write(File.join(schemas_dir, "passthrough.json"), '{"type": "object"}')

      File.write(File.join(turbofans_root, "steps", "local_step", "worker.rb"), <<~RUBY)
        class LocalStep
          include Turbofan::Step

          compute_environment :test_ce
          runs_on :batch
          cpu 2
          input_schema "passthrough.json"
          output_schema "passthrough.json"

          def call(inputs, context)
            {done: true}
          end
        end
      RUBY

      # External step has docker_image set - no local directory needed
      external_pipeline_file = File.join(turbofans_root, "pipelines", "external_pipeline.rb")
      File.write(external_pipeline_file, <<~RUBY)
        require_relative "../steps/local_step/worker"

        class ExternalStep
          include Turbofan::Step

          compute_environment :test_ce
          runs_on :batch
          cpu 1
          docker_image "123456789.dkr.ecr.us-east-1.amazonaws.com/external-repo:latest"
          input_schema "passthrough.json"
          output_schema "passthrough.json"

          def call(inputs, context)
            {done: true}
          end
        end

        class MyExternalPipeline
          include Turbofan::Pipeline

          pipeline_name "external-pipeline"

          pipeline do
            results = local_step(trigger_input)
            external_step(results)
          end
        end
      RUBY
    end

    after do
      %i[MyExternalPipeline LocalStep ExternalStep].each do |name|
        Object.send(:remove_const, name) if Object.const_defined?(name) # rubocop:disable RSpec/RemoveConst
      end
    end

    it "does not raise for external steps that have no local directory" do
      external_pipeline_file = File.join(turbofans_root, "pipelines", "external_pipeline.rb")
      expect {
        described_class.load(external_pipeline_file, turbofans_root: turbofans_root)
      }.not_to raise_error
    end
  end

  describe "error handling" do
    it "raises an error if pipeline file is missing" do
      FileUtils.rm(pipeline_file)
      expect { described_class.load(pipeline_file, turbofans_root: turbofans_root) }.to raise_error(/not found/)
    end

    it "raises a LoadError if a step file cannot be required" do
      FileUtils.rm_rf(File.join(turbofans_root, "steps", "generate_csvs"))
      expect { described_class.load(pipeline_file, turbofans_root: turbofans_root) }.to raise_error(LoadError)
    end
  end
end
