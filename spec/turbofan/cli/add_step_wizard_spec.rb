# frozen_string_literal: true

require "spec_helper"
require "tmpdir"
require "fileutils"

RSpec.describe "turbofan step new wizard" do # rubocop:disable RSpec/DescribeClass
  let(:tmpdir) { Dir.mktmpdir("turbofan-test", SPEC_TMP_ROOT) }

  after { FileUtils.rm_rf(tmpdir) }

  before do
    Dir.chdir(tmpdir) do
      FileUtils.mkdir_p("turbofans/schemas")
    end
  end

  context "when NAME is omitted in TTY mode" do
    it "prompts for the step name via Prompt.ask" do
      expect(Turbofan::CLI::Prompt).to receive(:ask) # rubocop:disable RSpec/MessageSpies, RSpec/StubbedMock
        .with("Step name (snake_case)")
        .and_return("prompted_step")

      allow(Turbofan::CLI::Prompt).to receive_messages(select: "compute", yes?: true)

      Dir.chdir(tmpdir) do
        Turbofan::CLI.start(["step", "new"])
      end

      step_dir = File.join(tmpdir, "turbofans", "steps", "prompted_step")
      expect(Dir.exist?(step_dir)).to be true
    end
  end

  context "when NAME is provided but flags are omitted" do
    it "prompts for compute_environment and cpu via Prompt.select" do
      expect(Turbofan::CLI::Prompt).to receive(:select) # rubocop:disable RSpec/MessageSpies, RSpec/StubbedMock
        .with(anything, array_including(/compute/i))
        .and_return("compute")

      expect(Turbofan::CLI::Prompt).to receive(:select) # rubocop:disable RSpec/MessageSpies, RSpec/StubbedMock
        .with(anything, %w[1 2 4 8 16])
        .and_return("2")

      expect(Turbofan::CLI::Prompt).to receive(:yes?) # rubocop:disable RSpec/MessageSpies, RSpec/StubbedMock
        .with("Include DuckDB?", default: true)
        .and_return(false)

      Dir.chdir(tmpdir) do
        Turbofan::CLI.start(["step", "new", "my_step"])
      end

      worker = File.read(File.join(tmpdir, "turbofans", "steps", "my_step", "worker.rb"))
      expect(worker).to include("compute_environment")
      expect(worker).to include("cpu 2")
      expect(worker).not_to include("family")
    end
  end

  context "when --compute-environment flag is provided" do
    it "does NOT prompt for compute_environment" do
      expect(Turbofan::CLI::Prompt).not_to receive(:select) # rubocop:disable RSpec/MessageSpies
        .with(anything, array_including(/compute/i))

      allow(Turbofan::CLI::Prompt).to receive_messages(select: "4", yes?: true)

      Dir.chdir(tmpdir) do
        Turbofan::CLI.start(["step", "new", "flagged_step", "--compute-environment", "compute"])
      end
    end
  end

  context "when --cpu flag is provided" do
    it "does NOT prompt for cpu" do
      expect(Turbofan::CLI::Prompt).not_to receive(:select) # rubocop:disable RSpec/MessageSpies
        .with(anything, %w[1 2 4 8 16])

      allow(Turbofan::CLI::Prompt).to receive_messages(select: "compute", yes?: true)

      Dir.chdir(tmpdir) do
        Turbofan::CLI.start(["step", "new", "flagged_step", "--cpu", "8"])
      end
    end
  end

  context "when all flags are provided non-interactively" do
    before do
      Dir.chdir(tmpdir) do
        Turbofan::CLI.start([
          "step", "new", "full_flags_step",
          "--compute-environment", "compute", "--cpu", "4", "--no-duckdb"
        ])
      end
    end

    let(:step_dir) { File.join(tmpdir, "turbofans", "steps", "full_flags_step") }
    let(:worker_content) { File.read(File.join(step_dir, "worker.rb")) }

    it "creates step without invoking any prompts" do
      expect(Dir.exist?(step_dir)).to be true
    end

    it "uses compute_environment from the flag" do
      expect(worker_content).to include("compute_environment")
      expect(worker_content).not_to include("family")
    end

    it "uses the cpu from the flag" do
      expect(worker_content).to include("cpu 4")
    end

    it "includes ram in the generated worker" do
      expect(worker_content).to include("ram 2048")
    end

    it "respects --no-duckdb" do
      expect(worker_content).not_to include("uses :duckdb")
    end
  end

  context "when --family flag is passed" do
    it "does not produce a worker containing 'family :' when --family is passed" do
      Dir.chdir(tmpdir) do
        allow(Turbofan::CLI::Prompt).to receive_messages(select: "4", yes?: false)

        begin
          Turbofan::CLI.start(["step", "new", "test_flag_check", "--family", "c"])
        rescue SystemExit, Thor::Error
          # expected if --family is rejected by Thor
        end
      end

      step_dir = File.join(tmpdir, "turbofans", "steps", "test_flag_check")
      worker_path = File.join(step_dir, "worker.rb")
      # If the step was created, its worker must not reference family
      # If the step was not created, the flag was rejected (also correct)
      expect(!File.exist?(worker_path) || !File.read(worker_path).include?("family :")).to be true
    end
  end

  context "when name is nil/empty after prompt" do
    it "raises an error" do
      allow(Turbofan::CLI::Prompt).to receive(:ask).and_return(nil)

      Dir.chdir(tmpdir) do
        expect {
          Turbofan::CLI.start(["step", "new"])
        }.to raise_error(Thor::Error, /name/i).or output(/name/i).to_stderr
      end
    end
  end
end
