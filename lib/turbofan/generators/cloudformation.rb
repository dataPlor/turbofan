require "json"
require_relative "asl"
require_relative "cloudformation/job_definition"
require_relative "cloudformation/job_queue"
require_relative "cloudformation/iam"
require_relative "cloudformation/ecr"
require_relative "cloudformation/logs"
require_relative "cloudformation/dashboard"
require_relative "cloudformation/sns"
require_relative "cloudformation/chunking_lambda"

module Turbofan
  module Generators
    class CloudFormation
      def initialize(pipeline:, steps:, stage:, config:, image_tags: {}, resources: {}, dashboard: true)
        @pipeline = pipeline
        @steps = steps
        @stage = stage
        @config = config
        @image_tags = image_tags
        @resources = resources
        @dashboard = dashboard
      end

      def generate
        raise "Turbofan.config.bucket must be set before generating a CloudFormation template" if Turbofan.config.bucket.nil? || Turbofan.config.bucket.empty?

        resources = {}
        pipeline_name = @pipeline.turbofan_name
        prefix = "turbofan-#{pipeline_name}-#{@stage}"

        base_tags = standard_tags(pipeline_name)
        pipeline_custom_tags = pipeline_tags
        turbofan_base_tags = turbofan_namespace_tags(pipeline_name)

        # Shared tags for all resources (legacy + turbofan namespace + pipeline custom)
        all_resource_tags = base_tags + turbofan_base_tags + pipeline_custom_tags

        # IAM roles
        resources.merge!(Iam.generate(prefix: prefix, steps: @steps, tags: all_resource_tags, pipeline_name: pipeline_name, resources: @resources, has_fan_out: any_grouped_fan_out?))

        # Per-step resources
        @steps.each do |sname, sclass|
          step_duckdb = sclass.turbofan_needs_duckdb?

          # Build per-step tags (all_resource_tags + step-specific tags)
          step_tags = all_resource_tags + step_specific_tags(sname) + custom_step_tags(sclass)

          # ECR - only for non-external steps
          unless sclass.turbofan_external?
            resources.merge!(Ecr.generate(prefix: prefix, step_name: sname, tags: step_tags))
          end

          # Log group
          resources.merge!(Logs.generate(prefix: prefix, step_name: sname, tags: step_tags))

          log_group_key = "LogGroup#{Naming.pascal_case(sname)}"

          # Resolve CE for this step
          ce_sym = sclass.turbofan_compute_environment || @pipeline.turbofan_compute_environment
          raise "No compute_environment resolved for step :#{sname}. Declare compute_environment on the step or pipeline." unless ce_sym
          ce_class = Turbofan::ComputeEnvironment.resolve(ce_sym)
          ce_ref = {"Fn::ImportValue" => ce_class.export_name(@stage)}

          # Check if this step uses consumable resources
          consumable_resource_refs = find_consumable_resource_refs(sclass, prefix)

          sizes = sclass.turbofan_sizes.any? ? sclass.turbofan_sizes : {nil => nil}
          sizes.each do |size_name, size_config|
            resources.merge!(JobDefinition.generate(
              prefix: prefix,
              step_name: sname,
              step_class: sclass,
              job_role_ref: {"Fn::GetAtt" => ["JobRole", "Arn"]},
              execution_role_ref: {"Fn::GetAtt" => ["ExecutionRole", "Arn"]},
              log_group_ref: {"Ref" => log_group_key},
              duckdb: step_duckdb,
              tags: step_tags,
              size_name: size_name,
              size_config: size_config,
              image_tag: @image_tags[sname],
              external_image: sclass.turbofan_external? ? sclass.turbofan_docker_image : nil,
              consumable_resource_refs: consumable_resource_refs
            ))
            resources.merge!(JobQueue.generate(
              prefix: prefix,
              step_name: sname,
              compute_environment_ref: ce_ref,
              tags: all_resource_tags,
              size_name: size_name
            ))
          end
        end

        # CloudWatch dashboard (optional — skipped when dashboard: false)
        if @dashboard
          resources.merge!(Dashboard.generate(prefix: prefix, pipeline: @pipeline, steps: @steps, stage: @stage, tags: all_resource_tags))
        end

        # SNS notification topic
        resources.merge!(Sns.generate(prefix: prefix, tags: all_resource_tags))

        # State machine
        resources.merge!(state_machine(prefix, all_resource_tags))

        # EventBridge schedule
        if @pipeline.turbofan_schedule
          resources.merge!(guard_lambda(prefix, all_resource_tags))
          resources.merge!(guard_lambda_role(prefix, all_resource_tags))
          resources.merge!(guard_lambda_permission)
          resources.merge!(eventbridge_rule(prefix, all_resource_tags))
        end

        # Chunking Lambda (only when at least one fan_out uses batch_size:)
        if any_grouped_fan_out?
          resources.merge!(ChunkingLambda.generate(prefix: prefix, bucket_prefix: Naming.bucket_prefix(pipeline_name, @stage), tags: all_resource_tags))
        end

        {
          "AWSTemplateFormatVersion" => "2010-09-09",
          "Description" => "Turbofan pipeline: #{pipeline_name} (#{@stage})",
          "Resources" => resources,
          "Outputs" => {
            "StateMachineArn" => {
              "Value" => {"Ref" => "StateMachine"}
            }
          }
        }
      end

      # Returns S3 artifacts that must be uploaded before deploying the stack.
      # Each entry is {bucket:, key:, body:}.
      def lambda_artifacts
        return [] unless any_grouped_fan_out?
        pipeline_name = @pipeline.turbofan_name
        bucket_prefix = Naming.bucket_prefix(pipeline_name, @stage)
        [{
          bucket: Turbofan.config.bucket,
          key: ChunkingLambda.handler_s3_key(bucket_prefix),
          body: ChunkingLambda.handler_zip
        }]
      end

      def self.tags_hash(tags)
        return tags if tags.is_a?(Hash)
        tags.each_with_object({}) { |t, h| h[t["Key"]] = t["Value"] }
      end

      private

      def standard_tags(pipeline_name)
        [
          {"Key" => "stack", "Value" => "turbofan"},
          {"Key" => "stack-type", "Value" => @stage},
          {"Key" => "stack-component", "Value" => pipeline_name}
        ]
      end

      def turbofan_namespace_tags(pipeline_name)
        [
          {"Key" => "turbofan:managed", "Value" => "true"},
          {"Key" => "turbofan:pipeline", "Value" => pipeline_name},
          {"Key" => "turbofan:stage", "Value" => @stage}
        ]
      end

      def step_specific_tags(step_name)
        [{"Key" => "turbofan:step", "Value" => step_name.to_s}]
      end

      def custom_step_tags(step_class)
        return [] unless step_class.turbofan_tags.any?
        step_class.turbofan_tags.map { |k, v| {"Key" => k.to_s, "Value" => v.to_s} }
      end

      def pipeline_tags
        return [] unless @pipeline.turbofan_tags.any?
        @pipeline.turbofan_tags.map { |k, v| {"Key" => k.to_s, "Value" => v.to_s} }
      end

      def any_grouped_fan_out?
        @pipeline.turbofan_dag.steps.any? { |s| s.fan_out? && s.batch_size }
      end

      def find_consumable_resource_refs(step_class, prefix)
        return [] if @resources.empty?
        step_class.turbofan_resource_keys.filter_map { |key|
          resource_class = @resources[key]
          next unless resource_class&.turbofan_consumable
          {"Fn::ImportValue" => resource_class.export_name(@stage)}
        }
      end

      def state_machine(prefix, tags)
        {
          "StateMachine" => {
            "Type" => "AWS::StepFunctions::StateMachine",
            "Properties" => {
              "StateMachineName" => "#{prefix}-statemachine",
              "DefinitionString" => {"Fn::Sub" => Generators::ASL.new(pipeline: @pipeline, stage: @stage, steps: @steps).to_json},
              "RoleArn" => {"Fn::GetAtt" => ["SfnRole", "Arn"]},
              "Tags" => tags
            }
          }
        }
      end

      def guard_lambda(prefix, tags)
        {
          "GuardLambda" => {
            "Type" => "AWS::Lambda::Function",
            "Properties" => {
              "FunctionName" => "#{prefix}-guard",
              "Runtime" => "python3.12",
              "Handler" => "index.handler",
              "Timeout" => 30,
              "Role" => {"Fn::GetAtt" => ["GuardLambdaRole", "Arn"]},
              "Code" => {
                "ZipFile" => <<~PYTHON
                  import json, os, boto3
                  sfn = boto3.client('stepfunctions')

                  def handler(event, context):
                      sm_arn = os.environ['STATE_MACHINE_ARN']
                      running = sfn.list_executions(stateMachineArn=sm_arn, statusFilter='RUNNING', maxResults=1)
                      if not running['executions']:
                          sfn.start_execution(stateMachineArn=sm_arn)
                      return {'guarded': True}
                PYTHON
              },
              "Environment" => {
                "Variables" => {
                  "STATE_MACHINE_ARN" => {"Ref" => "StateMachine"}
                }
              },
              "Tags" => tags
            }
          }
        }
      end

      def guard_lambda_role(prefix, tags)
        {
          "GuardLambdaRole" => {
            "Type" => "AWS::IAM::Role",
            "Properties" => {
              "RoleName" => "#{prefix}-guard-lambda-role",
              "Tags" => tags,
              "AssumeRolePolicyDocument" => {
                "Version" => "2012-10-17",
                "Statement" => [
                  {
                    "Effect" => "Allow",
                    "Principal" => {"Service" => "lambda.amazonaws.com"},
                    "Action" => "sts:AssumeRole"
                  }
                ]
              },
              "ManagedPolicyArns" => [
                "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
              ],
              "Policies" => [
                {
                  "PolicyName" => "StepFunctionsAccess",
                  "PolicyDocument" => {
                    "Version" => "2012-10-17",
                    "Statement" => [
                      {
                        "Effect" => "Allow",
                        "Action" => ["states:ListExecutions", "states:StartExecution"],
                        "Resource" => {"Ref" => "StateMachine"}
                      }
                    ]
                  }
                }
              ]
            }
          }
        }
      end

      def guard_lambda_permission
        {
          "GuardLambdaPermission" => {
            "Type" => "AWS::Lambda::Permission",
            "Properties" => {
              "FunctionName" => {"Ref" => "GuardLambda"},
              "Action" => "lambda:InvokeFunction",
              "Principal" => "events.amazonaws.com",
              "SourceArn" => {"Fn::GetAtt" => ["ScheduleRule", "Arn"]}
            }
          }
        }
      end

      def eventbridge_rule(prefix, tags)
        {
          "ScheduleRule" => {
            "Type" => "AWS::Events::Rule",
            "Properties" => {
              "Name" => "#{prefix}-schedule",
              "ScheduleExpression" => "cron(#{@pipeline.turbofan_schedule})",
              "State" => "ENABLED",
              "Targets" => [
                {
                  "Id" => "GuardLambdaTarget",
                  "Arn" => {"Fn::GetAtt" => ["GuardLambda", "Arn"]}
                }
              ],
              "Tags" => tags
            }
          }
        }
      end
    end
  end
end
