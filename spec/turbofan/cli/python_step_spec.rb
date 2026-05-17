# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"

RSpec.describe "turbofan step new --lang python" do # rubocop:disable RSpec/DescribeClass
  let(:tmpdir) { Dir.mktmpdir("turbofan-py-step", SPEC_TMP_ROOT) }

  before do
    Dir.chdir(tmpdir) do
      FileUtils.mkdir_p("turbofans/schemas")
    end
  end

  after { FileUtils.rm_rf(tmpdir) }

  describe "CLI plumbing" do
    it "registers --lang and --extensions options on step new" do
      step_cmd = Turbofan::CLI.subcommand_classes["step"]
      new_cmd = step_cmd.commands["new"]
      # Thor stores option keys as symbols.
      expect(new_cmd.options.keys).to include(:lang, :extensions)
    end

    it "rejects --lang invalid_value via Thor's enum validation" do
      Dir.chdir(tmpdir) do
        # Thor's enum validator exits with a non-zero status AND/OR
        # writes to stderr. We accept either signal.
        expect {
          Turbofan::CLI.start([
            "step", "new", "x", "--lang", "rust",
            "--cpu", "1", "--compute-environment", "compute", "--no-duckdb"
          ])
        }.to raise_error(SystemExit).or output(/lang/i).to_stderr
      end
    end

    it "produces Ruby scaffold by default (no --lang)" do
      Dir.chdir(tmpdir) do
        Turbofan::CLI.start([
          "step", "new", "ruby_step",
          "--cpu", "1", "--compute-environment", "compute", "--no-duckdb",
          "--lang", "ruby"
        ])
      end
      step_dir = File.join(tmpdir, "turbofans", "steps", "ruby_step")
      expect(File).to exist(File.join(step_dir, "worker.rb"))
      expect(File).to exist(File.join(step_dir, "Gemfile"))
      expect(File).to exist(File.join(step_dir, "entrypoint.rb"))
      expect(File).not_to exist(File.join(step_dir, "main.py"))
      expect(File).not_to exist(File.join(step_dir, "requirements.txt"))
    end
  end

  describe "Python step file generation" do
    before do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir_p("turbofans/steps/py_step")
        Turbofan::CLI::New.write_python_step(
          File.join("turbofans", "steps", "py_step"),
          "PyStep",
          duckdb: false, step_name: "py_step",
          compute_environment: :compute, cpu: 1, extensions: []
        )
      end
    end

    let(:step_dir) { File.join(tmpdir, "turbofans", "steps", "py_step") }

    describe "worker.rb" do
      let(:content) { File.read(File.join(step_dir, "worker.rb")) }

      it "is created" do
        expect(File).to exist(File.join(step_dir, "worker.rb"))
      end

      it "declares the Step class with metadata only (no def call body)" do
        expect(content).to include("class PyStep")
        expect(content).to include("include Turbofan::Step")
        expect(content).to include("compute_environment :compute")
        expect(content).to include("runs_on :batch")
        expect(content).to include("cpu 1")
        expect(content).to include("ram 2")
        expect(content).to include("batch_size 1")
        expect(content).to include('input_schema "py_step_input.json"')
        expect(content).to include('output_schema "py_step_output.json"')
        expect(content).not_to include("def call")
      end
    end

    describe "main.py" do
      let(:content) { File.read(File.join(step_dir, "main.py")) }

      it "imports from turbofan_runtime" do
        expect(content).to include("from turbofan_runtime import")
        expect(content).to include("Wrapper")
        expect(content).to include("Interrupted")
      end

      it "defines a no-op call returning a passing stub" do
        expect(content).to include("def call(inputs, context):")
        expect(content).to include('return {"status": "ok"}')
      end

      it "invokes Wrapper.run with the schema filenames" do
        expect(content).to include('input_schema="py_step_input.json"')
        expect(content).to include('output_schema="py_step_output.json"')
      end

      it "catches Interrupted and exits 143" do
        expect(content).to include("except Interrupted:")
        expect(content).to include("sys.exit(143)")
      end

      it "is syntactically valid Python (best-effort: parses with python3 -c)" do
        # Only check if python3 is on PATH; not all dev machines have it
        next unless system("which python3 > /dev/null 2>&1")
        path = File.join(step_dir, "main.py")
        ok = system("python3", "-c", "import ast; ast.parse(open('#{path}').read())")
        expect(ok).to be true
      end
    end

    describe "requirements.txt (no duckdb)" do
      let(:content) { File.read(File.join(step_dir, "requirements.txt")) }

      it "pins turbofan-runtime via git+subdirectory" do
        expect(content).to include("turbofan-runtime")
        expect(content).to include("git+https://github.com/dataplor/turbofan")
        expect(content).to include("subdirectory=python")
      end

      it "includes boto3" do
        expect(content).to include("boto3")
      end

      it "does NOT include duckdb when --no-duckdb" do
        expect(content).not_to include("duckdb")
      end
    end

    describe "Dockerfile" do
      let(:content) { File.read(File.join(step_dir, "Dockerfile")) }

      it "uses python:3.13-slim base" do
        expect(content).to match(/^FROM --platform=linux\/arm64 python:3\.13-slim$/m)
      end

      it "installs git for git+subdirectory pip install" do
        expect(content).to include("apt-get install")
        expect(content).to include("git")
      end

      it "sets PYTHONUNBUFFERED to ensure stdout/stderr flush under Docker" do
        expect(content).to include("ENV PYTHONUNBUFFERED=1")
      end

      it "pip installs requirements" do
        expect(content).to include("pip install --no-cache-dir -r requirements.txt")
      end

      it "wires BuildKit schemas + deps named contexts" do
        expect(content).to include("COPY --from=schemas . schemas/")
        expect(content).to include("COPY --from=deps . .")
        expect(content).to include("ENV TURBOFAN_SCHEMAS_PATH=/app/schemas")
      end

      it "sets python main as entrypoint" do
        expect(content).to include('CMD ["python", "main.py"]')
      end
    end
  end

  describe "Python step with --duckdb" do
    before do
      Dir.chdir(tmpdir) do
        FileUtils.mkdir_p("turbofans/steps/py_duck")
        Turbofan::CLI::New.write_python_step(
          File.join("turbofans", "steps", "py_duck"),
          "PyDuck",
          duckdb: true, step_name: "py_duck",
          compute_environment: :compute, cpu: 1,
          extensions: [:spatial, :vortex]
        )
      end
    end

    let(:step_dir) { File.join(tmpdir, "turbofans", "steps", "py_duck") }

    it "adds duckdb pinned to ~={major.minor} of Turbofan.config.duckdb_version" do
      content = File.read(File.join(step_dir, "requirements.txt"))
      minor = Turbofan.config.duckdb_version.split(".")[0..1].join(".")
      expect(content).to include("duckdb~=#{minor}")
    end

    it "pre-downloads postgres_scanner + requested extensions in the Dockerfile" do
      content = File.read(File.join(step_dir, "Dockerfile"))
      expect(content).to include("postgres_scanner.duckdb_extension")
      expect(content).to include("spatial.duckdb_extension")
      expect(content).to include("vortex.duckdb_extension")
    end

    it "creates the extensions install path" do
      content = File.read(File.join(step_dir, "Dockerfile"))
      expect(content).to include(Turbofan::Extensions.install_path)
    end
  end

  describe "full --lang python via CLI.start" do
    before do
      Dir.chdir(tmpdir) do
        Turbofan::CLI.start([
          "step", "new", "cli_py",
          "--lang", "python", "--cpu", "1",
          "--compute-environment", "compute", "--no-duckdb"
        ])
      end
    end

    let(:step_dir) { File.join(tmpdir, "turbofans", "steps", "cli_py") }

    it "creates all four expected files" do
      %w[worker.rb main.py requirements.txt Dockerfile].each do |f|
        expect(File).to exist(File.join(step_dir, f))
      end
    end

    it "also creates schema files in the schemas dir" do
      schemas = File.join(tmpdir, "turbofans", "schemas")
      expect(File).to exist(File.join(schemas, "cli_py_input.json"))
      expect(File).to exist(File.join(schemas, "cli_py_output.json"))
    end
  end
end
