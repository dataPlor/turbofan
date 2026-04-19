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

      execution :batch
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

        schedule "0 6 * * ? *"
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

        schedule "0 6 * * ? *"
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
        schedule "0 * * * ? *"
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
end
