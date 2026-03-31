require "spec_helper"
require "json"

RSpec.describe "tolerated_failure_rate", :schemas do
  describe "DAG DSL" do
    it "accepts tolerated_failure_rate on fan_out" do
      step_class = Class.new do
        include Turbofan::Step
        compute_environment :test_ce
        cpu 1
        batch_size 10
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
      stub_const("Process", step_class)

      pipeline = Class.new do
        include Turbofan::Pipeline
        pipeline_name "tolerance-test"
        pipeline do
          fan_out(process(trigger_input), tolerated_failure_rate: 0.01)
        end
      end

      dag = pipeline.turbofan_dag
      step = dag.steps.find { |s| s.name == :process }
      expect(step.tolerated_failure_rate).to eq(0.01)
    end

    it "defaults tolerated_failure_rate to 0" do
      step_class = Class.new do
        include Turbofan::Step
        compute_environment :test_ce
        cpu 1
        batch_size 10
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
      stub_const("Process", step_class)

      pipeline = Class.new do
        include Turbofan::Pipeline
        pipeline_name "tolerance-default"
        pipeline do
          fan_out(process(trigger_input))
        end
      end

      dag = pipeline.turbofan_dag
      step = dag.steps.find { |s| s.name == :process }
      expect(step.tolerated_failure_rate).to eq(0)
    end

    it "rejects tolerated_failure_rate >= 1.0" do
      step_class = Class.new do
        include Turbofan::Step
        compute_environment :test_ce
        cpu 1
        batch_size 10
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
      stub_const("Process", step_class)

      expect {
        Class.new do
          include Turbofan::Pipeline
          pipeline_name "tolerance-invalid"
          pipeline do
            fan_out(process(trigger_input), tolerated_failure_rate: 1.0)
          end
        end.turbofan_dag
      }.to raise_error(ArgumentError, /tolerated_failure_rate/)
    end

    it "rejects negative tolerated_failure_rate" do
      step_class = Class.new do
        include Turbofan::Step
        compute_environment :test_ce
        cpu 1
        batch_size 10
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
      stub_const("Process", step_class)

      expect {
        Class.new do
          include Turbofan::Pipeline
          pipeline_name "tolerance-negative"
          pipeline do
            fan_out(process(trigger_input), tolerated_failure_rate: -0.1)
          end
        end.turbofan_dag
      }.to raise_error(ArgumentError, /tolerated_failure_rate/)
    end

    it "accepts tolerated_failure_rate of 0 explicitly" do
      step_class = Class.new do
        include Turbofan::Step
        compute_environment :test_ce
        cpu 1
        batch_size 10
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
      stub_const("Process", step_class)

      pipeline = Class.new do
        include Turbofan::Pipeline
        pipeline_name "tolerance-zero"
        pipeline do
          fan_out(process(trigger_input), tolerated_failure_rate: 0)
        end
      end

      dag = pipeline.turbofan_dag
      step = dag.steps.find { |s| s.name == :process }
      expect(step.tolerated_failure_rate).to eq(0)
    end
  end

  describe "ASL generation" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        compute_environment :test_ce
        cpu 1
        batch_size 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    context "with tolerated_failure_rate > 0" do
      let(:pipeline_class) do
        klass = step_class
        stub_const("Process", klass)
        Class.new do
          include Turbofan::Pipeline
          pipeline_name "tolerance-asl"
          pipeline do
            fan_out(process(trigger_input), tolerated_failure_rate: 0.01)
          end
        end
      end

      let(:asl) do
        Turbofan::Generators::ASL.new(
          pipeline: pipeline_class, stage: "production", steps: {process: step_class}
        ).generate
      end

      it "Map inner states include check_tolerance and tolerance_exceeded" do
        inner_states = asl["States"]["process"].dig("ItemProcessor", "States")
        expect(inner_states).to have_key("process_batch")
        expect(inner_states).to have_key("process_check_tolerance")
        expect(inner_states).to have_key("process_tolerance_exceeded")
        expect(inner_states).to have_key("process_done")
      end

      it "inner Batch task Catches Batch.JobFailed only (not States.ALL)" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        catch_clause = inner_task["Catch"].first
        expect(catch_clause["ErrorEquals"]).to eq(["Batch.JobFailed"])
        expect(catch_clause["ResultPath"]).to eq("$.error")
        expect(catch_clause["Next"]).to eq("process_check_tolerance")
      end

      it "inner Batch task chains to done state on success" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        expect(inner_task["Next"]).to eq("process_done")
        expect(inner_task).not_to have_key("End")
      end

      it "check_tolerance is a Lambda invoke" do
        check = asl["States"]["process"].dig("ItemProcessor", "States", "process_check_tolerance")
        expect(check["Type"]).to eq("Task")
        expect(check["Resource"]).to eq("arn:aws:states:::lambda:invoke")
      end

      it "check_tolerance passes tolerated_failure_rate in payload" do
        check = asl["States"]["process"].dig("ItemProcessor", "States", "process_check_tolerance")
        payload = check.dig("Parameters", "Payload")
        expect(payload["tolerated_failure_rate"]).to eq(0.01)
      end

      it "check_tolerance passes error, step_name, parent_index, execution_id" do
        check = asl["States"]["process"].dig("ItemProcessor", "States", "process_check_tolerance")
        payload = check.dig("Parameters", "Payload")
        expect(payload["error.$"]).to eq("$.error")
        expect(payload["step_name"]).to eq("process")
        expect(payload["parent_index.$"]).to eq("$.index")
        expect(payload["execution_id.$"]).to eq("$$.Execution.Id")
      end

      it "check_tolerance passes job_name and job_queue as fallback" do
        check = asl["States"]["process"].dig("ItemProcessor", "States", "process_check_tolerance")
        payload = check.dig("Parameters", "Payload")
        expect(payload["job_name.$"]).to include("process")
        expect(payload["job_queue"]).to include("process")
      end

      it "check_tolerance passes real_size for accurate rate calculation" do
        check = asl["States"]["process"].dig("ItemProcessor", "States", "process_check_tolerance")
        payload = check.dig("Parameters", "Payload")
        expect(payload["parent_real_size.$"]).to eq("$.real_size")
      end

      it "check_tolerance routes to tolerance_exceeded on failure" do
        check = asl["States"]["process"].dig("ItemProcessor", "States", "process_check_tolerance")
        catch_clause = check["Catch"].first
        expect(catch_clause["Next"]).to eq("process_tolerance_exceeded")
      end

      it "tolerance_exceeded is a Fail state" do
        fail_state = asl["States"]["process"].dig("ItemProcessor", "States", "process_tolerance_exceeded")
        expect(fail_state["Type"]).to eq("Fail")
        expect(fail_state["Error"]).to eq("ToleranceExceeded")
      end

      it "done is a Pass state with End: true" do
        done_state = asl["States"]["process"].dig("ItemProcessor", "States", "process_done")
        expect(done_state["Type"]).to eq("Pass")
        expect(done_state["End"]).to be true
      end
    end

    context "with tolerated_failure_rate = 0 (default)" do
      let(:pipeline_class) do
        klass = step_class
        stub_const("Process", klass)
        Class.new do
          include Turbofan::Pipeline
          pipeline_name "no-tolerance-asl"
          pipeline do
            fan_out(process(trigger_input))
          end
        end
      end

      let(:asl) do
        Turbofan::Generators::ASL.new(
          pipeline: pipeline_class, stage: "production", steps: {process: step_class}
        ).generate
      end

      it "Map inner states only contain the batch task" do
        inner_states = asl["States"]["process"].dig("ItemProcessor", "States")
        expect(inner_states.keys).to eq(["process_batch"])
      end

      it "inner Batch task has End: true (no tolerance check)" do
        inner_task = asl["States"]["process"].dig("ItemProcessor", "States", "process_batch")
        expect(inner_task["End"]).to be true
        expect(inner_task).not_to have_key("Catch")
      end
    end
  end

  describe "CloudFormation generation" do
    let(:step_class) do
      Class.new do
        include Turbofan::Step
        compute_environment :test_ce
        cpu 1
        batch_size 1
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    before { Turbofan.config.bucket = "test-bucket" }

    context "with tolerated_failure_rate > 0" do
      let(:pipeline_class) do
        klass = step_class
        stub_const("Process", klass)
        Class.new do
          include Turbofan::Pipeline
          pipeline_name "tolerance-cfn"
          pipeline do
            fan_out(process(trigger_input), tolerated_failure_rate: 0.05)
          end
        end
      end

      let(:template) do
        Turbofan::Generators::CloudFormation.new(
          pipeline: pipeline_class, steps: {process: step_class},
          stage: "production", config: {}
        ).generate
      end

      it "creates a ToleranceLambda resource" do
        expect(template["Resources"]).to have_key("ToleranceLambda")
        expect(template["Resources"]["ToleranceLambda"]["Type"]).to eq("AWS::Lambda::Function")
      end

      it "creates a ToleranceLambdaRole resource" do
        expect(template["Resources"]).to have_key("ToleranceLambdaRole")
        expect(template["Resources"]["ToleranceLambdaRole"]["Type"]).to eq("AWS::IAM::Role")
      end

      it "ToleranceLambdaRole has Batch DescribeJobs and ListJobs permissions" do
        policies = template["Resources"]["ToleranceLambdaRole"].dig("Properties", "Policies")
        batch_policy = policies.find { |p| p["PolicyName"] == "BatchAccess" }
        expect(batch_policy).not_to be_nil
        actions = batch_policy.dig("PolicyDocument", "Statement", 0, "Action")
        expect(actions).to include("batch:DescribeJobs")
        expect(actions).to include("batch:ListJobs")
      end

      it "SFN role grants lambda:InvokeFunction on ToleranceLambda" do
        sfn_role = template["Resources"]["SfnRole"]
        policies = sfn_role.dig("Properties", "Policies")
        lambda_policy = policies.find { |p| p["PolicyName"] == "LambdaInvoke" }
        expect(lambda_policy).not_to be_nil
        resources = lambda_policy.dig("PolicyDocument", "Statement", 0, "Resource")
        resources = [resources] unless resources.is_a?(Array)
        tolerance_ref = resources.find { |r| r.is_a?(Hash) && r.dig("Fn::GetAtt", 0) == "ToleranceLambda" }
        expect(tolerance_ref).not_to be_nil,
          "SfnRole LambdaInvoke policy must include ToleranceLambda ARN"
      end

      it "includes tolerance Lambda in artifacts" do
        cfn = Turbofan::Generators::CloudFormation.new(
          pipeline: pipeline_class, steps: {process: step_class},
          stage: "production", config: {}
        )
        artifacts = cfn.lambda_artifacts
        tolerance_artifact = artifacts.find { |a| a[:key].include?("tolerance-lambda") }
        expect(tolerance_artifact).not_to be_nil
      end
    end

    context "without tolerated_failure_rate" do
      let(:pipeline_class) do
        klass = step_class
        stub_const("Process", klass)
        Class.new do
          include Turbofan::Pipeline
          pipeline_name "no-tolerance-cfn"
          pipeline do
            fan_out(process(trigger_input))
          end
        end
      end

      let(:template) do
        Turbofan::Generators::CloudFormation.new(
          pipeline: pipeline_class, steps: {process: step_class},
          stage: "production", config: {}
        ).generate
      end

      it "does NOT create a ToleranceLambda resource" do
        expect(template["Resources"]).not_to have_key("ToleranceLambda")
      end

      it "does NOT create a ToleranceLambdaRole resource" do
        expect(template["Resources"]).not_to have_key("ToleranceLambdaRole")
      end
    end
  end

  describe "Chunking lambda real_size" do
    it "parents array includes real_size for sentinel-aware rate calculation" do
      handler = Turbofan::Generators::CloudFormation::ChunkingLambda::HANDLER
      expect(handler).to include("'real_size'")
    end
  end

  describe "Tolerance lambda handler" do
    let(:handler) { Turbofan::Generators::CloudFormation::ToleranceLambda::HANDLER }

    it "calls describeJobs to get statusSummary" do
      expect(handler).to include("describe_jobs")
      expect(handler).to include("status_summary")
    end

    it "calls listJobs with FAILED status for failed child indices" do
      expect(handler).to include("list_jobs")
      expect(handler).to include("FAILED")
    end

    it "reads input items from S3 to include in manifest" do
      expect(handler).to include("items.json")
      expect(handler).to include("input_chunks")
    end

    it "writes manifest to tolerated_failures path" do
      expect(handler).to include("tolerated_failures")
      expect(handler).to include("put_object")
    end

    it "raises when failure rate exceeds threshold" do
      expect(handler).to include("exceeds tolerance")
    end

    it "uses real_size as denominator (not padded size)" do
      expect(handler).to include("parent_real_size")
    end

    it "has a fallback for JobId via job_name + job_queue" do
      expect(handler).to include("find_job_id")
      expect(handler).to include("job_name")
      expect(handler).to include("job_queue")
    end

    it "handles JSON parse failure in Cause gracefully" do
      expect(handler).to include("JSON::ParserError")
    end
  end
end
