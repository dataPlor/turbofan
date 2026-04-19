# frozen_string_literal: true

require "spec_helper"
require "tmpdir"

RSpec.describe Turbofan::CLI::Run do
  let(:cf_client) { instance_double(Aws::CloudFormation::Client) }
  let(:sfn_client) { instance_double(Aws::States::Client) }
  let(:pipeline_name) { "test_pipeline" }
  let(:stage) { "production" }
  let(:stack_name) { "turbofan-test-pipeline-production" }
  let(:state_machine_arn) { "arn:aws:states:us-east-1:123456789:stateMachine:turbofan-test-pipeline-production-statemachine" }
  let(:execution_arn) { "arn:aws:states:us-east-1:123456789:execution:sm:exec-abc123" }

  before do
    allow(Aws::CloudFormation::Client).to receive(:new).and_return(cf_client)
    allow(Aws::States::Client).to receive(:new).and_return(sfn_client)

    allow(cf_client).to receive(:describe_stacks).and_return(
      double(stacks: [double(
        outputs: [
          double(output_key: "StateMachineArn", output_value: state_machine_arn),
          double(output_key: "S3BucketName", output_value: "turbofan-test-pipeline-production-bucket")
        ]
      )])
    )

    allow(Turbofan::Deploy::Execution).to receive(:start).and_return(execution_arn)
    allow(sfn_client).to receive(:config).and_return(double(region: "us-east-1"))
  end

  describe ".call" do
    it "reads StateMachineArn from CF stack outputs" do
      described_class.call(pipeline_name: pipeline_name, stage: stage)

      expect(cf_client).to have_received(:describe_stacks)
    end

    it "starts execution via Execution.start" do
      described_class.call(pipeline_name: pipeline_name, stage: stage)

      expect(Turbofan::Deploy::Execution).to have_received(:start).with(
        sfn_client,
        state_machine_arn: state_machine_arn,
        input: anything
      )
    end

    context "with --input JSON string" do
      it "passes the JSON string as input" do
        described_class.call(pipeline_name: pipeline_name, stage: stage, input: '{"brand_id": 123}')

        expect(Turbofan::Deploy::Execution).to have_received(:start).with(
          sfn_client,
          state_machine_arn: state_machine_arn,
          input: '{"brand_id": 123}'
        )
      end
    end

    context "with --input-file" do
      let(:tmpdir) { Dir.mktmpdir("turbofan-run-test", SPEC_TMP_ROOT) }

      after { FileUtils.rm_rf(tmpdir) }

      it "reads input from file" do
        input_file = File.join(tmpdir, "input.json")
        File.write(input_file, '{"brand_id": 456}')

        described_class.call(pipeline_name: pipeline_name, stage: stage, input_file: input_file)

        expect(Turbofan::Deploy::Execution).to have_received(:start).with(
          sfn_client,
          state_machine_arn: state_machine_arn,
          input: '{"brand_id": 456}'
        )
      end
    end

    context "with no input" do
      it "uses empty object as default input" do
        described_class.call(pipeline_name: pipeline_name, stage: stage)

        expect(Turbofan::Deploy::Execution).to have_received(:start).with(
          sfn_client,
          state_machine_arn: state_machine_arn,
          input: "{}"
        )
      end
    end

    it "prints the execution ARN" do
      expect {
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      }.to output(/#{Regexp.escape(execution_arn)}/).to_stdout
    end

    it "prints the Step Functions console URL" do
      expect {
        described_class.call(pipeline_name: pipeline_name, stage: stage)
      }.to output(%r{Console: https://us-east-1\.console\.aws\.amazon\.com/states/home\?region=us-east-1#/executions/details/#{Regexp.escape(execution_arn)}}).to_stdout
    end

    # -------------------------------------------------------------------------
    # B9 — Dry-run for start
    # -------------------------------------------------------------------------
    context "with --dry-run (B9)" do
      it "does not call Execution.start" do
        capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage, dry_run: true)
        end

        expect(Turbofan::Deploy::Execution).not_to have_received(:start)
      end

      it "outputs step execution plan" do
        output = capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage, dry_run: true)
        end

        expect(output).to match(/dry.run|plan|steps|would execute/i)
      end

      it "runs validation checks" do
        output = capture_stdout do
          described_class.call(pipeline_name: pipeline_name, stage: stage, dry_run: true)
        end

        expect(output).to match(/validat|check/i)
      end
    end
  end
end
