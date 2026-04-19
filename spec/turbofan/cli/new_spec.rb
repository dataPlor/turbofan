require "spec_helper"
require "tmpdir"
require "fileutils"

RSpec.describe "turbofan new" do # rubocop:disable RSpec/DescribeClass
  let(:tmpdir) { Dir.mktmpdir("turbofan-test", SPEC_TMP_ROOT) }

  after { FileUtils.rm_rf(tmpdir) }

  context "when creating pipeline scaffold only" do
    before do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        Turbofan::CLI::New.call("my_pipeline")
      end
    end

    let(:turbofans_dir) { File.join(tmpdir, "turbofans") }
    let(:pipeline_file) { File.join(turbofans_dir, "pipelines", "my_pipeline.rb") }
    let(:schemas_dir) { File.join(turbofans_dir, "schemas") }

    describe "directory structure" do
      it "creates the pipelines directory" do
        expect(Dir.exist?(File.join(turbofans_dir, "pipelines"))).to be true
      end

      it "creates the config directory" do
        expect(Dir.exist?(File.join(turbofans_dir, "config"))).to be true
      end

      it "creates the schemas directory" do
        expect(Dir.exist?(schemas_dir)).to be true
      end

      it "does not create a step directory" do
        expect(Dir.exist?(File.join(turbofans_dir, "steps"))).to be false
      end

      it "does not create any worker.rb" do
        workers = Dir.glob(File.join(turbofans_dir, "**", "worker.rb"))
        expect(workers).to be_empty
      end

      it "does not create any schema files for a step" do
        schema_files = Dir.glob(File.join(schemas_dir, "*_input.json")) +
          Dir.glob(File.join(schemas_dir, "*_output.json"))
        expect(schema_files).to be_empty
      end
    end

    describe "pipeline file" do
      let(:content) { File.read(pipeline_file) }

      it "creates my_pipeline.rb" do
        expect(File.exist?(pipeline_file)).to be true
      end

      it "includes Turbofan::Pipeline" do
        expect(content).to include("Turbofan::Pipeline")
      end

      it "sets the pipeline name using underscores" do
        expect(content).to include('pipeline_name "my_pipeline"')
      end

      it "includes a pipeline block with input parameter" do
        expect(content).to include("pipeline do |input|")
        expect(content).to include("# Add steps with: turbofan step new STEP_NAME")
      end

      it "does not generate any step worker files" do
        # This is a pipeline-only scaffold
        workers = Dir.glob(File.join(turbofans_dir, "**", "worker.rb"))
        expect(workers).to be_empty
      end

      it "does not reference any step" do
        expect(content).not_to include("require_relative")
      end
    end

    describe "config files" do
      let(:config_dir) { File.join(turbofans_dir, "config") }

      it "creates production.yml" do
        expect(File.exist?(File.join(config_dir, "production.yml"))).to be true
      end

      it "creates staging.yml" do
        expect(File.exist?(File.join(config_dir, "staging.yml"))).to be true
      end

      it "includes network config placeholders in production.yml" do
        content = File.read(File.join(config_dir, "production.yml"))
        expect(content).to include("subnets")
        expect(content).to include("security_groups")
      end
    end
  end

  context "with pipeline name containing underscores" do
    before do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        Turbofan::CLI::New.call("data_processor")
      end
    end

    let(:pipeline_file) { File.join(tmpdir, "turbofans", "pipelines", "data_processor.rb") }

    it "preserves underscores in pipeline name" do
      content = File.read(pipeline_file)
      expect(content).to include('pipeline_name "data_processor"')
    end

    it "capitalizes class name" do
      content = File.read(pipeline_file)
      expect(content).to include("class DataProcessor")
    end
  end

  context "when invoked via CLI.start" do
    before do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir("turbofans")
        Turbofan::CLI.start(["new", "my_pipeline"])
      end
    end

    it "does not accept --duckdb flag" do
      new_command = Turbofan::CLI.commands["new"]
      expect(new_command.options.keys).not_to include("duckdb")
    end

    it "does not create any step directory" do
      steps_dir = File.join(tmpdir, "turbofans", "steps")
      expect(Dir.exist?(steps_dir)).to be false
    end
  end
end
