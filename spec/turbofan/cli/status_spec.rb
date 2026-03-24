require "spec_helper"

RSpec.describe Turbofan::CLI::Status do
  let(:cf_client) { instance_double(Aws::CloudFormation::Client) }
  let(:sfn_client) { instance_double(Aws::States::Client) }
  let(:pipeline_name) { "my_pipeline" }
  let(:stage) { "production" }
  let(:stack_name) { "turbofan-my-pipeline-production" }
  let(:state_machine_arn) { "arn:aws:states:us-east-1:123:stateMachine:sm" }

  let(:batch_client) { instance_double(Aws::Batch::Client) }

  before do
    allow(Aws::Batch::Client).to receive(:new).and_return(batch_client)
    allow(Aws::CloudFormation::Client).to receive(:new).and_return(cf_client)
    allow(Aws::States::Client).to receive(:new).and_return(sfn_client)
    allow(Turbofan::Deploy::StackManager).to receive(:stack_output)
      .with(cf_client, stack_name, "StateMachineArn")
      .and_return(state_machine_arn)
  end

  describe ".call" do
    it "derives stack name via Naming.stack_name" do
      allow(sfn_client).to receive(:list_executions).and_return(double(executions: []))

      expect(Turbofan::Naming).to receive(:stack_name) # rubocop:disable RSpec/MessageSpies
        .with(pipeline_name, stage)
        .and_call_original

      capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      end
    end

    it "gets StateMachineArn from StackManager.stack_output" do
      allow(sfn_client).to receive(:list_executions).and_return(double(executions: []))

      capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(Turbofan::Deploy::StackManager).to have_received(:stack_output)
        .with(cf_client, stack_name, "StateMachineArn")
    end

    it "lists executions with status_filter RUNNING" do
      allow(sfn_client).to receive(:list_executions).and_return(double(executions: []))

      capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(sfn_client).to have_received(:list_executions).with(
        state_machine_arn: state_machine_arn,
        status_filter: "RUNNING"
      )
    end

    context "without active executions" do
      before do
        allow(sfn_client).to receive(:list_executions).and_return(double(executions: []))
      end

      it "prints no active executions message" do
        output = capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(output).to include("No active executions for #{stack_name}.")
      end
    end

    context "with running executions" do
      let(:execution_arn_1) { "arn:aws:states:us-east-1:123:execution:sm:my-pipeline-run-abc123" } # rubocop:disable RSpec/IndexedLet
      let(:execution_arn_2) { "arn:aws:states:us-east-1:123:execution:sm:my-pipeline-run-def456" } # rubocop:disable RSpec/IndexedLet
      let(:start_time) { Time.now - 120 }

      before do
        allow(Turbofan::Status).to receive(:fetch).and_raise(StandardError, "not available")

        allow(sfn_client).to receive(:list_executions)
          .and_return(double(executions: [
            double(execution_arn: execution_arn_1),
            double(execution_arn: execution_arn_2)
          ]))

        allow(Turbofan::Deploy::Execution).to receive(:describe)
          .with(sfn_client, execution_arn: execution_arn_1)
          .and_return(name: "my-pipeline-run-abc123", status: "RUNNING", start_date: start_time, stop_date: nil)

        allow(Turbofan::Deploy::Execution).to receive(:describe)
          .with(sfn_client, execution_arn: execution_arn_2)
          .and_return(name: "my-pipeline-run-def456", status: "RUNNING", start_date: start_time, stop_date: nil)

        allow(Turbofan::Deploy::Execution).to receive(:step_statuses)
          .with(sfn_client, execution_arn: execution_arn_1)
          .and_return(
            "extract" => {status: "SUCCEEDED", started_at: start_time, ended_at: start_time + 30},
            "transform" => {status: "RUNNING", started_at: start_time + 30},
            "load" => {status: "PENDING"}
          )

        allow(Turbofan::Deploy::Execution).to receive(:step_statuses)
          .with(sfn_client, execution_arn: execution_arn_2)
          .and_return("extract" => {status: "RUNNING", started_at: start_time})
      end

      it "prints active executions header" do
        output = capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(output).to include("Active executions for #{stack_name}:")
      end

      it "prints each execution name and status" do
        output = capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(output).to include("my-pipeline-run-abc123")
        expect(output).to include("my-pipeline-run-def456")
        expect(output).to include("RUNNING")
      end

      it "prints step details with status indicators" do
        output = capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(output).to include("✓ extract SUCCEEDED")
        expect(output).to include("⟳ transform RUNNING")
        expect(output).to include("· load PENDING")
      end
    end

    context "with status indicators" do
      let(:execution_arn) { "arn:aws:states:us-east-1:123:execution:sm:run-1" }
      let(:start_time) { Time.now - 60 }

      before do
        allow(Turbofan::Status).to receive(:fetch).and_raise(StandardError, "not available")

        allow(sfn_client).to receive(:list_executions)
          .and_return(double(executions: [double(execution_arn: execution_arn)]))

        allow(Turbofan::Deploy::Execution).to receive(:describe)
          .and_return(name: "run-1", status: "RUNNING", start_date: start_time, stop_date: nil)
      end

      it "uses ✗ for FAILED steps" do
        allow(Turbofan::Deploy::Execution).to receive(:step_statuses)
          .and_return("step_a" => {status: "FAILED"})

        output = capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(output).to include("✗ step_a FAILED")
      end

      it "uses ? for unknown statuses" do
        allow(Turbofan::Deploy::Execution).to receive(:step_statuses)
          .and_return("step_a" => {status: "UNKNOWN"})

        output = capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(output).to include("? step_a UNKNOWN")
      end
    end

    context "with watch mode" do
      let(:execution_arn) { "arn:aws:states:us-east-1:123:execution:sm:run-watch" }
      let(:start_time) { Time.now - 60 }

      before do
        allow(Turbofan::Status).to receive(:fetch).and_raise(StandardError, "not available")
        allow(described_class).to receive(:sleep)
      end

      it "loops until no running executions remain" do
        call_count = 0
        allow(sfn_client).to receive(:list_executions) do
          call_count += 1
          if call_count <= 2
            double(executions: [double(execution_arn: execution_arn)])
          else
            double(executions: [])
          end
        end

        allow(Turbofan::Deploy::Execution).to receive_messages(describe: {name: "run-watch", status: "RUNNING", start_date: start_time, stop_date: nil}, step_statuses: {"step_a" => {status: "RUNNING"}})

        capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage, watch: true)
        end

        expect(sfn_client).to have_received(:list_executions).exactly(3).times
      end

      it "sleeps 5 seconds between iterations" do
        call_count = 0
        allow(sfn_client).to receive(:list_executions) do
          call_count += 1
          if call_count <= 2
            double(executions: [double(execution_arn: execution_arn)])
          else
            double(executions: [])
          end
        end

        allow(Turbofan::Deploy::Execution).to receive_messages(describe: {name: "run-watch", status: "RUNNING", start_date: start_time, stop_date: nil}, step_statuses: {"step_a" => {status: "RUNNING"}})

        capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage, watch: true)
        end

        expect(described_class).to have_received(:sleep).with(5).exactly(2).times
      end

      it "stops immediately when no running executions" do
        allow(sfn_client).to receive(:list_executions).and_return(double(executions: []))

        capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage, watch: true)
        end

        expect(described_class).not_to have_received(:sleep)
      end
    end

    context "without watch flag" do
      it "does not loop" do
        allow(sfn_client).to receive(:list_executions).and_return(double(executions: []))
        allow(described_class).to receive(:sleep)

        capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage)
        end

        expect(described_class).not_to have_received(:sleep)
      end
    end
  end

  describe "Status.fetch integration" do
    let(:batch_client) { instance_double(Aws::Batch::Client) }
    let(:execution_arn) { "arn:aws:states:us-east-1:123:execution:sm:run-1" }
    let(:start_time) { Time.now - 300 }

    let(:status_response) do
      {
        pipeline: "my-pipeline",
        stage: "production",
        execution_id: "run-1",
        status: "RUNNING",
        started_at: start_time.iso8601,
        steps: [
          {
            name: "extract",
            status: "SUCCEEDED",
            jobs: {pending: 0, running: 0, succeeded: 8542, failed: 0}
          },
          {
            name: "transform",
            status: "RUNNING",
            jobs: {pending: 1304, running: 100, succeeded: 8542, failed: 154}
          },
          {
            name: "load",
            status: "PENDING",
            jobs: {pending: 10000, running: 0, succeeded: 0, failed: 0}
          }
        ]
      }
    end

    before do
      allow(Aws::CloudFormation::Client).to receive(:new).and_return(cf_client)
      allow(Aws::States::Client).to receive(:new).and_return(sfn_client)
      allow(Turbofan::Deploy::StackManager).to receive(:stack_output)
        .with(cf_client, stack_name, "StateMachineArn")
        .and_return(state_machine_arn)

      allow(sfn_client).to receive(:list_executions)
        .and_return(double(executions: [double(execution_arn: execution_arn)]))

      # Stub old code path so tests reach assertion rather than erroring on unstubbed methods
      allow(Turbofan::Deploy::Execution).to receive_messages(describe: {name: "run-1", status: "RUNNING", start_date: start_time, stop_date: nil}, step_statuses: {"extract" => {status: "RUNNING"}})
    end

    it "delegates to Turbofan::Status.fetch for per-step job counts" do
      allow(Turbofan::Status).to receive(:fetch).and_return(status_response)

      capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(Turbofan::Status).to have_received(:fetch)
    end

    it "displays per-step job counts in the output" do
      allow(Turbofan::Status).to receive(:fetch).and_return(status_response)

      output = capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      end

      # The output should contain job counts like "8542/10000 succeeded"
      expect(output).to include("8542")
      expect(output).to match(/succeeded/i)
    end

    it "displays running and failed job counts" do
      allow(Turbofan::Status).to receive(:fetch).and_return(status_response)

      output = capture_stdout do
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      end

      expect(output).to match(/100.*running/i)
      expect(output).to match(/154.*failed/i)
    end

    context "with --watch mode using Status.fetch" do
      before do
        allow(described_class).to receive(:sleep)
      end

      it "polls with sleep 10 between iterations" do
        call_count = 0
        allow(sfn_client).to receive(:list_executions) do
          call_count += 1
          if call_count <= 2
            double(executions: [double(execution_arn: execution_arn)])
          else
            double(executions: [])
          end
        end

        allow(Turbofan::Status).to receive(:fetch).and_return(status_response)

        capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage, watch: true)
        end

        expect(described_class).to have_received(:sleep).with(10).at_least(:once)
      end
    end
  end
end
