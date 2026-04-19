# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"

RSpec.describe "turbofan step new" do # rubocop:disable RSpec/DescribeClass
  let(:tmpdir) { Dir.mktmpdir("turbofan-test", SPEC_TMP_ROOT) }

  after { FileUtils.rm_rf(tmpdir) }

  before do
    Dir.chdir(tmpdir) do
      FileUtils.mkdir("turbofans")
    end
  end

  context "when adding a step with duckdb" do
    before do
      Dir.chdir(tmpdir) do
        Turbofan::CLI::Add.call("process_data", duckdb: true, compute_environment: :compute, cpu: 1)
      end
    end

    let(:step_dir) { File.join(tmpdir, "turbofans", "steps", "process_data") }
    let(:schemas_dir) { File.join(tmpdir, "turbofans", "schemas") }

    it "creates the step directory under turbofans/steps/" do
      expect(Dir.exist?(step_dir)).to be true
    end

    it "creates worker.rb" do
      expect(File.exist?(File.join(step_dir, "worker.rb"))).to be true
    end

    it "creates Gemfile" do
      expect(File.exist?(File.join(step_dir, "Gemfile"))).to be true
    end

    it "creates Dockerfile" do
      expect(File.exist?(File.join(step_dir, "Dockerfile"))).to be true
    end

    it "creates entrypoint.rb" do
      expect(File.exist?(File.join(step_dir, "entrypoint.rb"))).to be true
    end

    it "includes duckdb in Gemfile" do
      gemfile = File.read(File.join(step_dir, "Gemfile"))
      expect(gemfile).to include("duckdb")
    end

    it "includes Turbofan::Step in worker.rb" do
      content = File.read(File.join(step_dir, "worker.rb"))
      expect(content).to include("Turbofan::Step")
    end

    it "uses the step name in the class name" do
      content = File.read(File.join(step_dir, "worker.rb"))
      expect(content).to include("ProcessData")
    end

    it "declares input_schema in worker.rb" do
      content = File.read(File.join(step_dir, "worker.rb"))
      expect(content).to include('input_schema "process_data_input.json"')
    end

    it "declares output_schema in worker.rb" do
      content = File.read(File.join(step_dir, "worker.rb"))
      expect(content).to include('output_schema "process_data_output.json"')
    end

    it "creates schema files" do
      expect(File.exist?(File.join(schemas_dir, "process_data_input.json"))).to be true
      expect(File.exist?(File.join(schemas_dir, "process_data_output.json"))).to be true
    end

    it "uses compute_environment instead of family in worker.rb" do
      content = File.read(File.join(step_dir, "worker.rb"))
      expect(content).to include("compute_environment")
      expect(content).not_to include("family")
    end

    it "uses direct cpu value in worker.rb" do
      content = File.read(File.join(step_dir, "worker.rb"))
      expect(content).to include("cpu 1")
    end
  end

  context "when adding a step without duckdb" do
    before do
      Dir.chdir(tmpdir) do
        Turbofan::CLI::Add.call("simple_step", duckdb: false, compute_environment: :compute, cpu: 1)
      end
    end

    let(:step_dir) { File.join(tmpdir, "turbofans", "steps", "simple_step") }

    it "does not include duckdb in Gemfile" do
      gemfile = File.read(File.join(step_dir, "Gemfile"))
      expect(gemfile).not_to include("duckdb")
    end

    it "does not include uses :duckdb in worker.rb" do
      content = File.read(File.join(step_dir, "worker.rb"))
      expect(content).not_to include("uses :duckdb")
    end
  end

  context "with custom compute_environment and cpu" do
    it "uses compute_environment ComputeEnvironments::Compute in worker.rb" do
      Dir.chdir(tmpdir) do
        Turbofan::CLI::Add.call("ce_step", duckdb: false, compute_environment: :compute, cpu: 4)
      end
      content = File.read(File.join(tmpdir, "turbofans", "steps", "ce_step", "worker.rb"))
      expect(content).to include("compute_environment")
      expect(content).to include("cpu 4")
      expect(content).not_to include("family")
    end

    it "does not use family in generated worker.rb" do
      Dir.chdir(tmpdir) do
        Turbofan::CLI::Add.call("no_family_step", duckdb: true, compute_environment: :compute, cpu: 8)
      end
      content = File.read(File.join(tmpdir, "turbofans", "steps", "no_family_step", "worker.rb"))
      expect(content).not_to include("family :")
      expect(content).to include("compute_environment")
    end
  end

  context "when adding multiple steps" do
    before do
      Dir.chdir(tmpdir) do
        Turbofan::CLI::Add.call("step_one", duckdb: true, compute_environment: :compute, cpu: 1)
        Turbofan::CLI::Add.call("step_two", duckdb: true, compute_environment: :compute, cpu: 1)
        Turbofan::CLI::Add.call("step_three", duckdb: true, compute_environment: :compute, cpu: 1)
      end
    end

    let(:steps_dir) { File.join(tmpdir, "turbofans", "steps") }

    it "creates all step directories" do
      expect(Dir.exist?(File.join(steps_dir, "step_one"))).to be true
      expect(Dir.exist?(File.join(steps_dir, "step_two"))).to be true
      expect(Dir.exist?(File.join(steps_dir, "step_three"))).to be true
    end

    it "each step has its own worker.rb and Gemfile" do
      %w[step_one step_two step_three].each do |step_name|
        expect(File.exist?(File.join(steps_dir, step_name, "worker.rb"))).to be true
        expect(File.exist?(File.join(steps_dir, step_name, "Gemfile"))).to be true
      end
    end
  end
end
