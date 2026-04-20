# frozen_string_literal: true

require "json"

module Turbofan
  module Generators
    class CloudFormation
      def initialize(pipeline:, steps:, stage:, config:, image_tags: {}, resources: {}, dashboard: true, step_dirs: {})
        @pipeline = pipeline
        @steps = steps
        @stage = stage
        @config = config
        @image_tags = image_tags
        @resources = resources
        @dashboard = dashboard
        @step_dirs = step_dirs
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
        resources.merge!(Iam.generate(
          prefix: prefix, steps: @steps, tags: all_resource_tags, pipeline_name: pipeline_name,
          resources: @resources, has_fan_out: any_non_routed_fan_out?,
          has_tolerated_fan_out: any_tolerated_fan_out?,
          routed_step_names: routed_fan_out_steps.map { |ds, _| ds.name },
          lambda_step_names: lambda_step_names,
          fargate_step_names: fargate_step_names
        ))

        # Per-step resources
        @steps.each do |sname, sclass|
          step_duckdb = sclass.turbofan.needs_duckdb?

          # Build per-step tags (all_resource_tags + step-specific tags)
          step_tags = all_resource_tags + step_specific_tags(sname) + custom_step_tags(sclass)

          # ECR repos are managed by the image builder (SAM-style), not CloudFormation.
          # See ImageBuilder::ECR_LIFECYCLE_POLICY for details.

          # Log group
          resources.merge!(Logs.generate(prefix: prefix, step_name: sname, tags: step_tags))

          log_group_key = "LogGroup#{Naming.pascal_case(sname)}"

          # Check if this step uses consumable resources
          consumable_resource_refs = find_consumable_resource_refs(sclass, prefix)

          if sclass.turbofan.fargate?
            # Fargate task definition with container image from ECR
            image_uri = sclass.turbofan.external? ? sclass.turbofan.docker_image : ecr_image_uri(prefix, sname, @image_tags[sname])
            cpu_units = (sclass.turbofan.default_cpu * 1024).to_i.to_s
            memory_mb = (sclass.turbofan.default_ram * 1024).to_i.to_s
            resources.merge!(fargate_step_resources(
              prefix: prefix, step_name: sname, step_class: sclass,
              image_uri: image_uri, cpu_units: cpu_units, memory_mb: memory_mb,
              storage_gib: sclass.turbofan.storage,
              tags: step_tags, log_group_ref: {"Ref" => log_group_key},
              pipeline_name: pipeline_name, resources: @resources
            ))
          elsif sclass.turbofan.lambda?
            # Lambda function with container image from ECR
            image_uri = sclass.turbofan.external? ? sclass.turbofan.docker_image : ecr_image_uri(prefix, sname, @image_tags[sname])
            memory_mb = ((sclass.turbofan.default_ram || 1) * 1024).to_i
            timeout_val = sclass.turbofan.timeout || 900
            resources.merge!(lambda_step_function(
              prefix: prefix, step_name: sname, step_class: sclass, image_uri: image_uri,
              memory_mb: memory_mb, timeout: timeout_val, tags: step_tags
            ))
          else
            # Batch: resolve CE (required for job queue)
            ce_sym = sclass.turbofan.compute_environment || @pipeline.turbofan_compute_environment
            raise "No compute_environment resolved for step :#{sname}. Declare compute_environment on the step or pipeline." unless ce_sym
            Turbofan::ComputeEnvironment.resolve(ce_sym)

            # Batch job definitions (one per size, or one if unsized)
            sizes = sclass.turbofan.sizes.any? ? sclass.turbofan.sizes : {nil => nil}
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
                external_image: sclass.turbofan.external? ? sclass.turbofan.docker_image : nil,
                consumable_resource_refs: consumable_resource_refs
              ))
            end
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

        # EventBridge triggers — one AWS::Events::Rule per `trigger`
        # declaration, all targeting a single shared GuardLambda. If
        # no triggers are declared the pipeline is manual-invocation
        # only and we emit none of these resources.
        if @pipeline.turbofan_triggers.any?
          resources.merge!(guard_lambda(prefix, all_resource_tags))
          resources.merge!(guard_lambda_role(prefix, all_resource_tags))
          @pipeline.turbofan_triggers.each_with_index do |trigger, idx|
            resources.merge!(trigger_rule(prefix, idx, trigger, all_resource_tags))
            resources.merge!(trigger_permission(idx, trigger))
          end
        end

        bucket_prefix = Naming.bucket_prefix(pipeline_name, @stage)

        # Shared ChunkingLambda — only needed for non-routed fan-outs.
        # Routed fan-outs use per-step ChunkingLambda{Step} (see loop below).
        if any_non_routed_fan_out?
          resources.merge!(ChunkingLambda.generate(prefix: prefix, bucket_prefix: bucket_prefix, tags: all_resource_tags))
        end

        # Tolerance Lambda (only when at least one fan_out has tolerated_failure_rate > 0)
        if any_tolerated_fan_out?
          resources.merge!(ToleranceLambda.generate(prefix: prefix, bucket_prefix: bucket_prefix, tags: all_resource_tags))
        end

        # Per-step ChunkingLambda for routed fan-out steps — bundles the user's
        # router.rb so one Lambda invocation routes and chunks in a single pass.
        routed_fan_out_steps.each do |dag_step, _step_class|
          router_source = load_router_source(dag_step.name)
          next unless router_source

          code_hash = Digest::SHA256.hexdigest(
            ChunkingLambda::HANDLER + ChunkingLambda::ROUTER_MODULE + router_source
          )[0, 12]
          resources.merge!(ChunkingLambda.generate_per_step(
            prefix: prefix, step_name: dag_step.name,
            bucket_prefix: bucket_prefix, tags: all_resource_tags,
            code_hash: code_hash
          ))
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
        pipeline_name = @pipeline.turbofan_name
        bucket_prefix = Naming.bucket_prefix(pipeline_name, @stage)
        artifacts = []
        if any_non_routed_fan_out?
          artifacts << {
            bucket: Turbofan.config.bucket,
            key: ChunkingLambda.handler_s3_key(bucket_prefix),
            body: ChunkingLambda.handler_zip
          }
        end
        if any_tolerated_fan_out?
          artifacts.concat(ToleranceLambda.lambda_artifacts(bucket_prefix))
        end
        routed_fan_out_steps.each do |dag_step, _step_class|
          router_source = load_router_source(dag_step.name)
          next unless router_source

          artifacts.concat(ChunkingLambda.lambda_artifacts_per_step(
            bucket_prefix: bucket_prefix,
            step_name: dag_step.name,
            router_source: router_source
          ))
        end
        artifacts
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
        return [] unless step_class.turbofan.tags.any?
        step_class.turbofan.tags.map { |k, v| {"Key" => k.to_s, "Value" => v.to_s} }
      end

      def pipeline_tags
        return [] unless @pipeline.turbofan_tags.any?
        @pipeline.turbofan_tags.map { |k, v| {"Key" => k.to_s, "Value" => v.to_s} }
      end

      def any_grouped_fan_out?
        @pipeline.turbofan_dag.steps.any? do |dag_step|
          if dag_step.fan_out?
            step_class = @steps[dag_step.name]
            step_class&.turbofan&.batch_size
          end
        end
      end

      # A fan-out needs the shared ChunkingLambda only when it has no `size`
      # profiles. Routed fan-outs use a per-step ChunkingLambda{Step} that
      # bundles the user's router — the shared Lambda is never invoked.
      def any_non_routed_fan_out?
        @pipeline.turbofan_dag.steps.any? do |dag_step|
          next unless dag_step.fan_out?
          step_class = @steps[dag_step.name]
          next unless step_class&.turbofan&.batch_size
          !step_class.turbofan.sizes&.any?
        end
      end

      def routed_fan_out_steps
        @pipeline.turbofan_dag.steps.filter_map do |dag_step|
          next unless dag_step.fan_out?
          step_class = @steps[dag_step.name]
          next unless step_class&.turbofan&.sizes&.any?
          [dag_step, step_class]
        end
      end

      def load_router_source(step_name)
        step_dir = @step_dirs[step_name]
        return nil unless step_dir

        router_path = File.join(step_dir, "router", "router.rb")
        return nil unless File.exist?(router_path)
        File.read(router_path)
      end

      def fargate_step_names
        @steps.filter_map { |sname, sclass| sname if sclass.turbofan.fargate? }
      end

      def fargate_step_resources(prefix:, step_name:, step_class:, image_uri:, cpu_units:, memory_mb:, storage_gib: nil, tags:, log_group_ref:, pipeline_name:, resources: {})
        task_def_name = "FargateTaskDef#{Naming.pascal_case(step_name)}"
        exec_role_name = "FargateExecRole#{Naming.pascal_case(step_name)}"
        task_role_name = "FargateTaskRole#{Naming.pascal_case(step_name)}"

        result = {}

        # Shared Fargate cluster (only created once)
        result["FargateCluster"] ||= {
          "Type" => "AWS::ECS::Cluster",
          "Properties" => {
            "ClusterName" => "#{prefix}-fargate-cluster",
            "CapacityProviders" => ["FARGATE", "FARGATE_SPOT"],
            "Tags" => tags
          }
        }

        # Execution role (pull images, send logs)
        result[exec_role_name] = {
          "Type" => "AWS::IAM::Role",
          "Properties" => {
            "RoleName" => Naming.iam_role_name("#{prefix}-fargate-exec-#{step_name}"),
            "Tags" => tags,
            "AssumeRolePolicyDocument" => {
              "Version" => "2012-10-17",
              "Statement" => [
                {
                  "Effect" => "Allow",
                  "Principal" => {"Service" => "ecs-tasks.amazonaws.com"},
                  "Action" => "sts:AssumeRole"
                }
              ]
            },
            "ManagedPolicyArns" => [
              "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
            ]
          }
        }

        # Task role — per-step policies via shared Iam.task_policies
        task_policies = Iam.task_policies(
          prefix: prefix, step_name: step_name, step_class: step_class,
          pipeline_name: pipeline_name, resources: resources
        )

        result[task_role_name] = {
          "Type" => "AWS::IAM::Role",
          "Properties" => {
            "RoleName" => Naming.iam_role_name("#{prefix}-fargate-task-#{step_name}"),
            "Tags" => tags,
            "AssumeRolePolicyDocument" => {
              "Version" => "2012-10-17",
              "Statement" => [
                {
                  "Effect" => "Allow",
                  "Principal" => {"Service" => "ecs-tasks.amazonaws.com"},
                  "Action" => "sts:AssumeRole"
                }
              ]
            },
            "Policies" => task_policies
          }
        }

        # Task definition
        task_def_props = {
          "Family" => "#{prefix}-taskdef-#{step_name}",
          "NetworkMode" => "awsvpc",
          "RequiresCompatibilities" => ["FARGATE"],
          "RuntimePlatform" => {
            "CpuArchitecture" => "ARM64",
            "OperatingSystemFamily" => "LINUX"
          },
          "Cpu" => cpu_units,
          "Memory" => memory_mb,
          "ExecutionRoleArn" => {"Fn::GetAtt" => [exec_role_name, "Arn"]},
          "TaskRoleArn" => {"Fn::GetAtt" => [task_role_name, "Arn"]},
          "ContainerDefinitions" => [
            {
              "Name" => "worker",
              "Image" => image_uri,
              "Essential" => true,
              "LogConfiguration" => {
                "LogDriver" => "awslogs",
                "Options" => {
                  "awslogs-group" => log_group_ref,
                  "awslogs-region" => {"Ref" => "AWS::Region"},
                  "awslogs-stream-prefix" => "fargate"
                }
              }
            }
          ]
        }
        task_def_props["EphemeralStorage"] = {"SizeInGiB" => storage_gib} if storage_gib

        result[task_def_name] = {
          "Type" => "AWS::ECS::TaskDefinition",
          "Properties" => task_def_props
        }

        result
      end

      def lambda_step_names
        @steps.filter_map { |sname, sclass| sname if sclass.turbofan.lambda? }
      end

      def ecr_image_uri(prefix, step_name, image_tag)
        account_id = Turbofan.config.aws_account_id
        region = Turbofan.config.default_region || "us-east-1"
        tag = image_tag || "latest"
        "#{account_id}.dkr.ecr.#{region}.amazonaws.com/#{prefix}-ecr-#{step_name}:#{tag}"
      end

      def lambda_step_function(prefix:, step_name:, step_class:, image_uri:, memory_mb:, timeout:, tags:)
        resource_name = "LambdaStep#{Naming.pascal_case(step_name)}"
        role_name = "LambdaStepRole#{Naming.pascal_case(step_name)}"
        pipeline_name = @pipeline.turbofan_name

        lambda_policies = Iam.task_policies(
          prefix: prefix, step_name: step_name, step_class: step_class,
          pipeline_name: pipeline_name, resources: @resources
        )

        {
          role_name => {
            "Type" => "AWS::IAM::Role",
            "Properties" => {
              "RoleName" => Naming.iam_role_name("#{prefix}-lambda-#{step_name}-role"),
              "Tags" => tags.is_a?(Array) ? tags : tags.map { |k, v| {"Key" => k, "Value" => v} },
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
              "Policies" => lambda_policies
            }
          },
          resource_name => {
            "Type" => "AWS::Lambda::Function",
            "Properties" => {
              "FunctionName" => "#{prefix}-lambda-#{step_name}",
              "PackageType" => "Image",
              "Code" => {"ImageUri" => image_uri},
              "Role" => {"Fn::GetAtt" => [role_name, "Arn"]},
              "Architectures" => ["arm64"],
              "Timeout" => [timeout, 900].min,
              "MemorySize" => [memory_mb, 10240].min,
              "EphemeralStorage" => {"Size" => 10240},
              "ImageConfig" => {
                "EntryPoint" => ["/usr/local/bin/aws_lambda_ric"],
                "Command" => ["turbofan/runtime/lambda_handler.Turbofan::Runtime::LambdaHandler.process"],
                "WorkingDirectory" => "/app"
              },
              "Environment" => {
                "Variables" => {
                  "TURBOFAN_BUCKET" => Turbofan.config.bucket,
                  "TURBOFAN_BUCKET_PREFIX" => Naming.bucket_prefix(@pipeline.turbofan_name, @stage),
                  "GEM_PATH" => "/usr/local/bundle/ruby/3.2.0:/usr/share/ruby3.2-gems:/usr/share/gems"
                }
              },
              "Tags" => tags.is_a?(Array) ? tags : tags.map { |k, v| {"Key" => k, "Value" => v} }
            }
          }
        }
      end

      def any_tolerated_fan_out?
        @pipeline.turbofan_dag.steps.any? { |s| s.fan_out? && (s.tolerated_failure_rate || 0) > 0 }
      end

      def find_consumable_resource_refs(step_class, prefix)
        return [] if @resources.empty?
        step_class.turbofan.resource_keys.filter_map { |key|
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

      GUARD_HANDLER_PY = File.expand_path("cloudformation/guard_handler.py", __dir__)
      private_constant :GUARD_HANDLER_PY

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
                "ZipFile" => File.read(GUARD_HANDLER_PY)
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
              "RoleName" => Naming.iam_role_name("#{prefix}-guard-lambda-role"),
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

      # One Rule per trigger declaration. Deterministic logical ID
      # (`TriggerRule0`, `TriggerRule1`, …) so CloudFormation diffs
      # line up with the source order in the pipeline file.
      def trigger_rule(prefix, idx, trigger, tags)
        target = {
          "Id" => "GuardLambdaTarget",
          "Arn" => {"Fn::GetAtt" => ["GuardLambda", "Arn"]}
        }

        props = {
          "Name" => "#{prefix}-trigger-#{idx}",
          "State" => "ENABLED",
          "Targets" => [target],
          "Tags" => tags
        }

        case trigger[:type]
        when :schedule
          # EventBridge schedule: the ScheduleExpression fires the rule
          # on cron. We override the target Input so the GuardLambda
          # sees a consistent envelope shape with __event_schedule_
          # expression in the detail — the same T1 code path as
          # :event triggers.
          props["ScheduleExpression"] = "cron(#{trigger[:cron]})"
          target["Input"] = JSON.generate(
            "source" => "aws.scheduler",
            "detail-type" => "Scheduled Event",
            "detail" => {"__event_schedule_expression" => "cron(#{trigger[:cron]})"}
          )
        when :event
          pattern = {"source" => trigger[:source]}
          pattern["detail-type"] = trigger[:detail_type] if trigger[:detail_type]
          pattern["detail"] = trigger[:detail] if trigger[:detail]
          props["EventPattern"] = pattern
          props["EventBusName"] = trigger[:event_bus] if trigger[:event_bus]
        end

        {
          "TriggerRule#{idx}" => {
            "Type" => "AWS::Events::Rule",
            "Properties" => props
          }
        }
      end

      # One Lambda::Permission per Rule → GuardLambda principal.
      # Without this the rule can't invoke the Lambda. SourceArn
      # scopes the grant to exactly that rule (tightest blast
      # radius).
      def trigger_permission(idx, _trigger)
        {
          "TriggerRule#{idx}Permission" => {
            "Type" => "AWS::Lambda::Permission",
            "Properties" => {
              "FunctionName" => {"Ref" => "GuardLambda"},
              "Action" => "lambda:InvokeFunction",
              "Principal" => "events.amazonaws.com",
              "SourceArn" => {"Fn::GetAtt" => ["TriggerRule#{idx}", "Arn"]}
            }
          }
        }
      end
    end
  end
end
