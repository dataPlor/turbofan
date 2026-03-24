require "spec_helper"

RSpec.describe Turbofan::Deploy::Execution do
  let(:sfn_client) { instance_double(Aws::States::Client) }
  let(:state_machine_arn) { "arn:aws:states:us-east-1:123456789:stateMachine:turbofan-test-pipeline-production-statemachine" }
  let(:execution_arn) { "arn:aws:states:us-east-1:123456789:execution:turbofan-test-pipeline-production-statemachine:exec-abc123" }

  describe ".start" do
    before do
      allow(sfn_client).to receive(:start_execution).and_return(
        double(execution_arn: execution_arn)
      )
    end

    it "calls start_execution on the SFN client" do
      described_class.start(sfn_client, state_machine_arn: state_machine_arn, input: '{"brand_id": 123}')

      expect(sfn_client).to have_received(:start_execution).with(
        state_machine_arn: state_machine_arn,
        input: '{"brand_id": 123}'
      )
    end

    it "returns the execution ARN" do
      result = described_class.start(sfn_client, state_machine_arn: state_machine_arn, input: "{}")
      expect(result).to eq(execution_arn)
    end
  end

  describe ".describe" do
    let(:start_time) { Time.utc(2026, 2, 16, 14, 30, 0) }
    let(:stop_time) { Time.utc(2026, 2, 16, 14, 35, 0) }

    before do
      allow(sfn_client).to receive(:describe_execution).and_return(
        double(
          execution_arn: execution_arn,
          status: "SUCCEEDED",
          start_date: start_time,
          stop_date: stop_time,
          input: '{"brand_id": 123}',
          name: "exec-abc123"
        )
      )
    end

    it "calls describe_execution on the SFN client" do
      described_class.describe(sfn_client, execution_arn: execution_arn)
      expect(sfn_client).to have_received(:describe_execution).with(execution_arn: execution_arn)
    end

    it "returns a hash with execution status" do
      result = described_class.describe(sfn_client, execution_arn: execution_arn)
      expect(result[:status]).to eq("SUCCEEDED")
    end

    it "includes start time" do
      result = described_class.describe(sfn_client, execution_arn: execution_arn)
      expect(result[:start_date]).to eq(start_time)
    end

    it "includes stop time for completed executions" do
      result = described_class.describe(sfn_client, execution_arn: execution_arn)
      expect(result[:stop_date]).to eq(stop_time)
    end

    it "includes the execution name" do
      result = described_class.describe(sfn_client, execution_arn: execution_arn)
      expect(result[:name]).to eq("exec-abc123")
    end
  end

  describe ".step_statuses" do
    def history_response(events, next_token: nil)
      double(events: events, next_token: next_token)
    end

    context "when all steps succeeded" do
      before do
        allow(sfn_client).to receive(:get_execution_history).and_return(
          history_response([
            double(type: "TaskStateEntered", timestamp: Time.utc(2026, 2, 16, 14, 30, 0),
              state_entered_event_details: double(name: "brand_geocode")),
            double(type: "TaskStateExited", timestamp: Time.utc(2026, 2, 16, 14, 32, 15),
              state_exited_event_details: double(name: "brand_geocode")),
            double(type: "TaskStateEntered", timestamp: Time.utc(2026, 2, 16, 14, 32, 16),
              state_entered_event_details: double(name: "brand_validate")),
            double(type: "TaskStateExited", timestamp: Time.utc(2026, 2, 16, 14, 33, 0),
              state_exited_event_details: double(name: "brand_validate"))
          ])
        )
      end

      it "returns statuses for each step" do
        statuses = described_class.step_statuses(sfn_client, execution_arn: execution_arn)
        expect(statuses.keys).to contain_exactly("brand_geocode", "brand_validate")
      end

      it "marks completed steps as SUCCEEDED" do
        statuses = described_class.step_statuses(sfn_client, execution_arn: execution_arn)
        expect(statuses["brand_geocode"][:status]).to eq("SUCCEEDED")
        expect(statuses["brand_validate"][:status]).to eq("SUCCEEDED")
      end
    end

    context "when one step is running" do
      before do
        allow(sfn_client).to receive(:get_execution_history).and_return(
          history_response([
            double(type: "TaskStateEntered", timestamp: Time.utc(2026, 2, 16, 14, 30, 0),
              state_entered_event_details: double(name: "brand_geocode")),
            double(type: "TaskStateExited", timestamp: Time.utc(2026, 2, 16, 14, 32, 15),
              state_exited_event_details: double(name: "brand_geocode")),
            double(type: "TaskStateEntered", timestamp: Time.utc(2026, 2, 16, 14, 32, 16),
              state_entered_event_details: double(name: "brand_validate"))
          ])
        )
      end

      it "marks the currently entered step as RUNNING" do
        statuses = described_class.step_statuses(sfn_client, execution_arn: execution_arn)
        expect(statuses["brand_validate"][:status]).to eq("RUNNING")
      end

      it "marks the completed step as SUCCEEDED" do
        statuses = described_class.step_statuses(sfn_client, execution_arn: execution_arn)
        expect(statuses["brand_geocode"][:status]).to eq("SUCCEEDED")
      end
    end

    context "when one step failed" do
      before do
        allow(sfn_client).to receive(:get_execution_history).and_return(
          history_response([
            double(type: "TaskStateEntered", timestamp: Time.utc(2026, 2, 16, 14, 30, 0),
              state_entered_event_details: double(name: "brand_geocode")),
            double(type: "TaskFailed", timestamp: Time.utc(2026, 2, 16, 14, 32, 15),
              task_failed_event_details: double(resource_type: "batch", error: "Batch.JobFailed")),
            double(type: "TaskStateExited", timestamp: Time.utc(2026, 2, 16, 14, 32, 16),
              state_exited_event_details: double(name: "brand_geocode"))
          ])
        )
      end

      it "marks the failed step as FAILED" do
        statuses = described_class.step_statuses(sfn_client, execution_arn: execution_arn)
        expect(statuses["brand_geocode"][:status]).to eq("FAILED")
      end
    end

    context "when steps have not yet started" do
      before do
        allow(sfn_client).to receive(:get_execution_history).and_return(
          history_response([
            double(type: "TaskStateEntered", timestamp: Time.utc(2026, 2, 16, 14, 30, 0),
              state_entered_event_details: double(name: "brand_geocode"))
          ])
        )
      end

      it "steps not yet entered are not included (PENDING is inferred by caller)" do
        statuses = described_class.step_statuses(sfn_client, execution_arn: execution_arn)
        expect(statuses).not_to have_key("brand_validate")
      end
    end

    context "with paginated history" do
      before do
        page1_events = [
          double(type: "TaskStateEntered", timestamp: Time.utc(2026, 2, 16, 14, 30, 0),
            state_entered_event_details: double(name: "brand_geocode")),
          double(type: "TaskStateExited", timestamp: Time.utc(2026, 2, 16, 14, 32, 15),
            state_exited_event_details: double(name: "brand_geocode"))
        ]
        page2_events = [
          double(type: "TaskStateEntered", timestamp: Time.utc(2026, 2, 16, 14, 32, 16),
            state_entered_event_details: double(name: "brand_validate")),
          double(type: "TaskStateExited", timestamp: Time.utc(2026, 2, 16, 14, 33, 0),
            state_exited_event_details: double(name: "brand_validate"))
        ]

        call_count = 0
        allow(sfn_client).to receive(:get_execution_history) do |**_args|
          call_count += 1
          if call_count == 1
            history_response(page1_events, next_token: "token-page2")
          else
            history_response(page2_events)
          end
        end
      end

      it "collects events across all pages" do
        statuses = described_class.step_statuses(sfn_client, execution_arn: execution_arn)
        expect(statuses.keys).to contain_exactly("brand_geocode", "brand_validate")
        expect(statuses["brand_geocode"][:status]).to eq("SUCCEEDED")
        expect(statuses["brand_validate"][:status]).to eq("SUCCEEDED")
      end
    end
  end
end
