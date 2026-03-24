require "spec_helper"
require "tmpdir"
require "fileutils"

RSpec.describe "turbofan step new Dockerfile BuildKit schemas" do # rubocop:disable RSpec/DescribeClass
  let(:tmpdir) { Dir.mktmpdir("turbofan-test") }
  let(:step_dir) { File.join(tmpdir, "turbofans", "steps", "my_step") }
  let(:dockerfile_content) { File.read(File.join(step_dir, "Dockerfile")) }

  after { FileUtils.rm_rf(tmpdir) }

  before do
    Dir.chdir(tmpdir) do
      FileUtils.mkdir("turbofans")
      Turbofan::CLI::Add.call("my_step", duckdb: true, compute_environment: :compute, cpu: 1)
    end
  end

  describe "Dockerfile schemas support" do
    it "contains COPY --from=schemas line" do
      expect(dockerfile_content).to include("COPY --from=schemas . schemas/")
    end

    it "sets TURBOFAN_SCHEMAS_PATH env var" do
      expect(dockerfile_content).to include("ENV TURBOFAN_SCHEMAS_PATH=/app/schemas")
    end

    it "contains a comment about BuildKit named context" do
      expect(dockerfile_content).to match(/BuildKit.*named context|build-context.*schemas/)
    end
  end

  describe "Dockerfile DuckDB extension pre-download" do
    it "always includes postgres_scanner when duckdb is true" do
      expect(dockerfile_content).to include("postgres_scanner.duckdb_extension")
    end

    it "uses core repo URL for postgres_scanner" do
      expect(dockerfile_content).to include("extensions.duckdb.org")
      expect(dockerfile_content).to include("postgres_scanner.duckdb_extension.gz")
    end

    it "creates the extension directory" do
      expect(dockerfile_content).to include("mkdir -p /root/.duckdb/extensions/v1.4.3/linux_arm64")
    end

    it "includes pre-download comment" do
      expect(dockerfile_content).to include("Pre-download DuckDB extensions")
    end
  end

  describe "Dockerfile with user-declared extensions" do
    let(:step_dir) { File.join(tmpdir, "turbofans", "steps", "ext_step") }
    let(:dockerfile_content) { File.read(File.join(step_dir, "Dockerfile")) }

    before do
      Dir.chdir(tmpdir) do
        Turbofan::CLI::Add.call("ext_step", duckdb: true, compute_environment: :compute, cpu: 1, extensions: [:spatial, :h3])
      end
    end

    it "includes postgres_scanner from core repo" do
      expect(dockerfile_content).to include("extensions.duckdb.org/v1.4.3/linux_arm64/postgres_scanner.duckdb_extension.gz")
    end

    it "includes spatial from core repo" do
      expect(dockerfile_content).to include("extensions.duckdb.org/v1.4.3/linux_arm64/spatial.duckdb_extension.gz")
    end

    it "includes h3 from community repo" do
      expect(dockerfile_content).to include("community-extensions.duckdb.org/v1.4.3/linux_arm64/h3.duckdb_extension.gz")
    end
  end

  describe "Dockerfile without duckdb" do
    let(:step_dir) { File.join(tmpdir, "turbofans", "steps", "no_duck_step") }
    let(:dockerfile_content) { File.read(File.join(step_dir, "Dockerfile")) }

    before do
      Dir.chdir(tmpdir) do
        Turbofan::CLI::Add.call("no_duck_step", duckdb: false, compute_environment: :compute, cpu: 1)
      end
    end

    it "does not include extension download block" do
      expect(dockerfile_content).not_to include("duckdb_extension")
      expect(dockerfile_content).not_to include("Pre-download DuckDB extensions")
    end
  end
end
