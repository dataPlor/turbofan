# frozen_string_literal: true

require "spec_helper"

RSpec.describe "CLI positional args" do # rubocop:disable RSpec/DescribeClass
  before do
    allow(Turbofan::CLI::Check).to receive(:call)
    allow(Turbofan::CLI::Deploy).to receive(:call)
    allow(Turbofan::CLI::Destroy).to receive(:call)
    allow(Turbofan::CLI::Logs).to receive(:call)
    allow(Turbofan::CLI::Rollback).to receive(:call)
    allow(Turbofan::CLI::Run).to receive(:call)
    allow(Turbofan::CLI::Status).to receive(:call)
    allow(Turbofan::CLI::Ce).to receive(:deploy)
  end

  describe "check PIPELINE STAGE" do
    it "routes positional args to Check.call" do
      capture_stdout { Turbofan::CLI.start(["check", "my_pipeline", "production"]) }

      expect(Turbofan::CLI::Check).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "production"
      )
    end

    it "errors when pipeline and stage are missing" do
      capture_stdout do
        Turbofan::CLI.start(["check"])
      end

      expect(Turbofan::CLI::Check).not_to have_received(:call)
    end
  end

  describe "deploy PIPELINE STAGE" do
    it "routes positional args to Deploy.call" do
      capture_stdout { Turbofan::CLI.start(["deploy", "my_pipeline", "production"]) }

      expect(Turbofan::CLI::Deploy).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "production",
        dry_run: false
      )
    end

    it "passes --dry_run option along with positional args" do
      capture_stdout { Turbofan::CLI.start(["deploy", "my_pipeline", "staging", "--dry_run"]) }

      expect(Turbofan::CLI::Deploy).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "staging",
        dry_run: true
      )
    end

    it "errors when pipeline and stage are missing" do
      capture_stdout do
        Turbofan::CLI.start(["deploy"])
      end

      expect(Turbofan::CLI::Deploy).not_to have_received(:call)
    end
  end

  describe "destroy PIPELINE STAGE" do
    it "routes positional args to Destroy.call" do
      capture_stdout { Turbofan::CLI.start(["destroy", "my_pipeline", "dev"]) }

      expect(Turbofan::CLI::Destroy).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "dev",
        force: false
      )
    end

    it "passes --force option along with positional args" do
      capture_stdout { Turbofan::CLI.start(["destroy", "my_pipeline", "production", "--force"]) }

      expect(Turbofan::CLI::Destroy).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "production",
        force: true
      )
    end

    it "errors when pipeline and stage are missing" do
      capture_stdout do
        Turbofan::CLI.start(["destroy"])
      end

      expect(Turbofan::CLI::Destroy).not_to have_received(:call)
    end
  end

  describe "logs PIPELINE STAGE" do
    it "routes positional args to Logs.call with required --step" do
      capture_stdout { Turbofan::CLI.start(["logs", "my_pipeline", "production", "--step", "extract"]) }

      expect(Turbofan::CLI::Logs).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "production",
        step: "extract",
        execution: nil,
        item: nil,
        query: nil
      )
    end

    it "passes all optional flags along with positional args" do
      capture_stdout do
        Turbofan::CLI.start([
          "logs", "my_pipeline", "staging",
          "--step", "transform",
          "--execution", "exec-123",
          "--item", "item-456",
          "--query", "fields @message"
        ])
      end

      expect(Turbofan::CLI::Logs).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "staging",
        step: "transform",
        execution: "exec-123",
        item: "item-456",
        query: "fields @message"
      )
    end

    it "errors when pipeline and stage are missing" do
      capture_stdout do
        Turbofan::CLI.start(["logs"])
      end

      expect(Turbofan::CLI::Logs).not_to have_received(:call)
    end
  end

  describe "rollback PIPELINE STAGE" do
    it "routes positional args to Rollback.call" do
      capture_stdout { Turbofan::CLI.start(["rollback", "my_pipeline", "production"]) }

      expect(Turbofan::CLI::Rollback).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "production"
      )
    end

    it "errors when pipeline and stage are missing" do
      capture_stdout do
        Turbofan::CLI.start(["rollback"])
      end

      expect(Turbofan::CLI::Rollback).not_to have_received(:call)
    end
  end

  describe "start PIPELINE STAGE" do
    it "routes positional args to Run.call" do
      capture_stdout { Turbofan::CLI.start(["start", "my_pipeline", "production"]) }

      expect(Turbofan::CLI::Run).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "production",
        input: nil,
        input_file: nil,
        dry_run: false
      )
    end

    it "passes --input option along with positional args" do
      capture_stdout { Turbofan::CLI.start(["start", "my_pipeline", "staging", "--input", '{"key":"val"}']) }

      expect(Turbofan::CLI::Run).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "staging",
        input: '{"key":"val"}',
        input_file: nil,
        dry_run: false
      )
    end

    it "passes --input_file option along with positional args" do
      capture_stdout { Turbofan::CLI.start(["start", "my_pipeline", "dev", "--input_file", "data.json"]) }

      expect(Turbofan::CLI::Run).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "dev",
        input: nil,
        input_file: "data.json",
        dry_run: false
      )
    end

    it "errors when pipeline and stage are missing" do
      capture_stdout do
        Turbofan::CLI.start(["start"])
      end

      expect(Turbofan::CLI::Run).not_to have_received(:call)
    end
  end

  describe "run PIPELINE STAGE (alias)" do
    it "routes 'run' to Run.call via the start alias" do
      capture_stdout { Turbofan::CLI.start(["run", "my_pipeline", "production"]) }

      expect(Turbofan::CLI::Run).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "production",
        input: nil,
        input_file: nil,
        dry_run: false
      )
    end
  end

  describe "status PIPELINE STAGE" do
    it "routes positional args to Status.call" do
      capture_stdout { Turbofan::CLI.start(["status", "my_pipeline", "production"]) }

      expect(Turbofan::CLI::Status).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "production",
        watch: false
      )
    end

    it "passes --watch option along with positional args" do
      capture_stdout { Turbofan::CLI.start(["status", "my_pipeline", "staging", "--watch"]) }

      expect(Turbofan::CLI::Status).to have_received(:call).with(
        pipeline_name: "my_pipeline",
        stage: "staging",
        watch: true
      )
    end

    it "errors when pipeline and stage are missing" do
      capture_stdout { Turbofan::CLI.start(["status"]) }

      expect(Turbofan::CLI::Status).not_to have_received(:call)
    end
  end

  describe "ce deploy STAGE" do
    it "routes positional stage arg to Ce.deploy" do
      capture_stdout { Turbofan::CLI.start(["ce", "deploy", "production"]) }

      expect(Turbofan::CLI::Ce).to have_received(:deploy).with(
        stage: "production"
      )
    end

    it "errors when stage is missing" do
      capture_stdout do
        Turbofan::CLI.start(["ce", "deploy"])
      end

      expect(Turbofan::CLI::Ce).not_to have_received(:deploy)
    end
  end
end
