# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Generators::CloudFormation, "eventbridge rules", :schemas do # rubocop:disable RSpec/DescribeMethod
  before { Turbofan.config.bucket = "turbofan-shared-bucket" }

  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::EventCe", klass)
    klass
  end

  let(:step_class) do
    ce_class # ensure stub_const runs
    Class.new do
      include Turbofan::Step

      runs_on :batch
      compute_environment :test_ce
      cpu 2
      input_schema "passthrough.json"
      output_schema "passthrough.json"
    end
  end

  let(:config) do
    {
      vpc_id: "vpc-123",
      subnets: ["subnet-456"],
      security_groups: ["sg-abc"]
    }
  end

  describe "pipeline with schedule" do
    let(:scheduled_pipeline) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "scheduled-pipeline"

        trigger :schedule, cron: "0 6 * * ? *"
        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: scheduled_pipeline,
        steps: {process: step_class},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "creates an EventBridge Rule resource" do
      rule_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      expect(rule_keys.size).to eq(1)
    end

    it "sets ScheduleExpression with cron() wrapper" do
      rule_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      props = template["Resources"][rule_key]["Properties"]
      expect(props["ScheduleExpression"]).to eq("cron(0 6 * * ? *)")
    end

    it "targets the GuardLambda (not the StateMachine directly)" do
      rule_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      targets = template["Resources"][rule_key]["Properties"]["Targets"]
      expect(targets).to be_a(Array)
      expect(targets.size).to be >= 1
      target_arn = targets.first["Arn"]
      # EventBridge should target the GuardLambda, which checks for running executions
      expect(target_arn).to eq({"Fn::GetAtt" => ["GuardLambda", "Arn"]})
    end

    it "does not include a RoleArn on the Lambda target (invoked via resource-based policy)" do
      rule_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      targets = template["Resources"][rule_key]["Properties"]["Targets"]
      expect(targets.first).not_to have_key("RoleArn")
    end

    it "creates a Lambda permission for EventBridge invocation" do
      perm_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Permission"
      }
      expect(perm_key).not_to be_nil,
        "Expected a Lambda::Permission resource for EventBridge to invoke the guard Lambda"
    end

    it "sets the Rule State to ENABLED" do
      rule_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      props = template["Resources"][rule_key]["Properties"]
      expect(props["State"]).to eq("ENABLED")
    end
  end

  describe "pipeline without schedule" do
    let(:unscheduled_pipeline) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "unscheduled-pipeline"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: unscheduled_pipeline,
        steps: {process: step_class},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "does not create an EventBridge Rule" do
      rule_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      expect(rule_keys).to be_empty
    end

    it "does not create an EventBridge IAM Role" do
      eb_role_keys = template["Resources"].keys.select { |k|
        resource = template["Resources"][k]
        next false unless resource["Type"] == "AWS::IAM::Role"
        assume_doc = resource.dig("Properties", "AssumeRolePolicyDocument") || {}
        statements = assume_doc["Statement"] || []
        statements.any? { |s|
          principal = s["Principal"] || {}
          service = Array(principal["Service"])
          service.include?("events.amazonaws.com")
        }
      }
      expect(eb_role_keys).to be_empty
    end
  end

  describe "concurrent execution guard Lambda" do
    let(:scheduled_pipeline) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "guarded-pipeline"

        trigger :schedule, cron: "0 6 * * ? *"
        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: scheduled_pipeline,
        steps: {process: step_class},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "creates a GuardLambda function resource when pipeline has a schedule" do
      expect(template["Resources"]).to have_key("GuardLambda"),
        "Expected a GuardLambda resource in the template for scheduled pipelines"
      expect(template["Resources"]["GuardLambda"]["Type"]).to eq("AWS::Lambda::Function")
    end

    it "EventBridge targets the GuardLambda, not the StateMachine directly" do
      rule_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      targets = template["Resources"][rule_key]["Properties"]["Targets"]
      target_arn = targets.first["Arn"]
      # Should NOT target the StateMachine directly
      expect(target_arn).not_to eq({"Ref" => "StateMachine"}),
        "EventBridge should target the GuardLambda, not the StateMachine directly"
      expect(target_arn).not_to eq({"Fn::GetAtt" => ["StateMachine", "Arn"]}),
        "EventBridge should target the GuardLambda, not the StateMachine directly"
    end

    it "creates a GuardLambdaRole with states:ListExecutions permission" do
      guard_role_key = template["Resources"].keys.find { |k|
        resource = template["Resources"][k]
        next unless resource["Type"] == "AWS::IAM::Role"
        policies = resource.dig("Properties", "Policies") || []
        policies.any? { |p|
          statements = p.dig("PolicyDocument", "Statement") || []
          statements.any? { |s|
            actions = Array(s["Action"])
            actions.include?("states:ListExecutions")
          }
        }
      }
      expect(guard_role_key).not_to be_nil,
        "Expected a GuardLambda IAM Role with states:ListExecutions permission"
    end

    it "creates a GuardLambdaRole with states:StartExecution permission" do
      guard_role_key = template["Resources"].keys.find { |k|
        resource = template["Resources"][k]
        next unless resource["Type"] == "AWS::IAM::Role"
        policies = resource.dig("Properties", "Policies") || []
        policies.any? { |p|
          statements = p.dig("PolicyDocument", "Statement") || []
          statements.any? { |s|
            actions = Array(s["Action"])
            actions.include?("states:StartExecution") && actions.include?("states:ListExecutions")
          }
        }
      }
      expect(guard_role_key).not_to be_nil,
        "Expected a GuardLambda IAM Role with both states:StartExecution and states:ListExecutions"
    end
  end

  describe "no guard Lambda without schedule" do
    let(:unscheduled_pipeline) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "unguarded-pipeline"

        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: unscheduled_pipeline,
        steps: {process: step_class},
        stage: "production",
        config: config
      )
    end

    let(:template) { generator.generate }

    it "does not create a GuardLambda when no schedule is set" do
      expect(template["Resources"]).not_to have_key("GuardLambda")
    end
  end

  describe "schedule with different cron expressions" do
    let(:hourly_pipeline) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "hourly-pipeline"
        trigger :schedule, cron: "0 * * * ? *"
        pipeline do
          process(trigger_input)
        end
      end
    end

    let(:template) do
      described_class.new(
        pipeline: hourly_pipeline,
        steps: {process: step_class},
        stage: "staging",
        config: config
      ).generate
    end

    it "uses the pipeline schedule value in the cron expression" do
      rule_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      props = template["Resources"][rule_key]["Properties"]
      expect(props["ScheduleExpression"]).to eq("cron(0 * * * ? *)")
    end
  end

  describe "trigger :schedule emits Input override on the target" do
    let(:sched_pipeline) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline
        pipeline_name "sched-pipeline"
        trigger :schedule, cron: "0 5 * * ? *"
        pipeline { process(trigger_input) }
      end
    end

    let(:template) do
      described_class.new(
        pipeline: sched_pipeline, steps: {process: step_class},
        stage: "production", config: config
      ).generate
    end

    it "embeds a synthetic envelope with __event_schedule_expression in detail" do
      rule_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      input_json = template["Resources"][rule_key]["Properties"]["Targets"].first["Input"]
      parsed = JSON.parse(input_json)
      expect(parsed["source"]).to eq("aws.scheduler")
      expect(parsed["detail-type"]).to eq("Scheduled Event")
      expect(parsed.dig("detail", "__event_schedule_expression")).to eq("cron(0 5 * * ? *)")
    end
  end

  describe "trigger :event" do
    let(:event_pipeline) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline
        pipeline_name "event-pipeline"
        trigger :event,
          source: "aws.s3",
          detail_type: "Object Created",
          detail: {"bucket" => {"name" => ["my-bucket"]}}
        pipeline { process(trigger_input) }
      end
    end

    let(:template) do
      described_class.new(
        pipeline: event_pipeline, steps: {process: step_class},
        stage: "production", config: config
      ).generate
    end

    let(:rule_props) do
      key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      template["Resources"][key]["Properties"]
    end

    it "uses EventPattern (not ScheduleExpression)" do
      expect(rule_props).not_to have_key("ScheduleExpression")
      expect(rule_props).to have_key("EventPattern")
    end

    it "sets source, detail-type, and detail in the pattern" do
      expect(rule_props["EventPattern"]["source"]).to eq(["aws.s3"])
      expect(rule_props["EventPattern"]["detail-type"]).to eq(["Object Created"])
      expect(rule_props["EventPattern"]["detail"]).to eq({"bucket" => {"name" => ["my-bucket"]}})
    end

    it "does not set Input override (natural envelope from EventBridge)" do
      target = rule_props["Targets"].first
      expect(target).not_to have_key("Input")
    end
  end

  describe "trigger :event with custom event bus" do
    let(:bus_pipeline) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline
        pipeline_name "bus-pipeline"
        trigger :event, source: "myapp", event_bus: "ops-bus"
        pipeline { process(trigger_input) }
      end
    end

    let(:template) do
      described_class.new(
        pipeline: bus_pipeline, steps: {process: step_class},
        stage: "production", config: config
      ).generate
    end

    it "sets EventBusName on the Rule" do
      rule_key = template["Resources"].keys.find { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      props = template["Resources"][rule_key]["Properties"]
      expect(props["EventBusName"]).to eq("ops-bus")
    end
  end

  describe "multiple triggers" do
    let(:multi_pipeline) do
      step_klass = step_class
      stub_const("Process", step_klass)
      Class.new do
        include Turbofan::Pipeline
        pipeline_name "multi-pipeline"
        trigger :schedule, cron: "0 5 * * ? *"
        trigger :event, source: "aws.s3", detail_type: "Object Created"
        trigger :event, source: "myapp"
        pipeline { process(trigger_input) }
      end
    end

    let(:template) do
      described_class.new(
        pipeline: multi_pipeline, steps: {process: step_class},
        stage: "production", config: config
      ).generate
    end

    it "emits one AWS::Events::Rule per trigger declaration" do
      rule_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::Events::Rule"
      }
      expect(rule_keys.sort).to eq(%w[TriggerRule0 TriggerRule1 TriggerRule2])
    end

    it "emits one Lambda::Permission per rule" do
      perm_keys = template["Resources"].keys.select { |k|
        template["Resources"][k]["Type"] == "AWS::Lambda::Permission" && k.start_with?("TriggerRule")
      }
      expect(perm_keys.sort).to eq(%w[TriggerRule0Permission TriggerRule1Permission TriggerRule2Permission])
    end

    it "shares a single GuardLambda + Role across all triggers" do
      expect(template["Resources"].keys.count { |k| template["Resources"][k]["Type"] == "AWS::Lambda::Function" && k == "GuardLambda" }).to eq(1)
      expect(template["Resources"]).to have_key("GuardLambdaRole")
    end

    it "scopes each permission's SourceArn to its own rule" do
      arn0 = template["Resources"]["TriggerRule0Permission"]["Properties"]["SourceArn"]
      arn1 = template["Resources"]["TriggerRule1Permission"]["Properties"]["SourceArn"]
      expect(arn0).to eq({"Fn::GetAtt" => ["TriggerRule0", "Arn"]})
      expect(arn1).to eq({"Fn::GetAtt" => ["TriggerRule1", "Arn"]})
    end

    it "names each rule deterministically with its index" do
      expect(template["Resources"]["TriggerRule0"]["Properties"]["Name"]).to match(/multi-pipeline.*-trigger-0/)
      expect(template["Resources"]["TriggerRule2"]["Properties"]["Name"]).to match(/multi-pipeline.*-trigger-2/)
    end
  end
end
