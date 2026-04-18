module Turbofan
  module Generators
    class ASL
      module StateBuilder
        CATCH_ALL_FAILURE = [{"ErrorEquals" => ["States.ALL"], "Next" => "NotifyFailure"}].freeze
        LAMBDA_RETRY = [{
          "ErrorEquals" => ["Lambda.ServiceException", "Lambda.TooManyRequestsException", "States.TaskFailed"],
          "IntervalSeconds" => 2,
          "MaxAttempts" => 3,
          "BackoffRate" => 2.0
        }].freeze

        private

        def state_name_for(step)
          if step.fan_out?
            "#{step.name}_chunk"
          else
            step.name.to_s
          end
        end

        def base_env
          [
            {"Name" => "TURBOFAN_EXECUTION_ID", "Value.$" => "$$.Execution.Id"},
            {"Name" => "TURBOFAN_STAGE", "Value" => @stage},
            {"Name" => "TURBOFAN_PIPELINE", "Value" => @pipeline_name},
            {"Name" => "TURBOFAN_BUCKET", "Value" => Turbofan.config.bucket},
            {"Name" => "TURBOFAN_BUCKET_PREFIX", "Value" => Naming.bucket_prefix(@pipeline_name, @stage)},
            {"Name" => "AWS_REGION", "Value" => "${AWS::Region}"},
            {"Name" => "AWS_DEFAULT_REGION", "Value" => "${AWS::Region}"}
          ]
        end

        def step_env(step, first:, prev_step_name:, prev_step: nil, prev_step_names: nil)
          env = base_env

          if prev_step&.fan_out? && prev_step.fan_in == false
            # fan_in: false — step receives trigger input, skips fan-out output collection
            env << {"Name" => "TURBOFAN_INPUT", "Value.$" => "States.JsonToString($.input)"}
          elsif first && !step.fan_out?
            env << {"Name" => "TURBOFAN_INPUT", "Value.$" => "States.JsonToString($.input)"}
          elsif prev_step_names
            env << {"Name" => "TURBOFAN_PREV_STEPS", "Value" => prev_step_names.map(&:to_s).join(",")}
          elsif prev_step_name
            env << {"Name" => "TURBOFAN_PREV_STEP", "Value" => prev_step_name.to_s}
          end

          env << {"Name" => "TURBOFAN_STEP_NAME", "Value" => step.name.to_s}

          if prev_step&.fan_out? && prev_step.fan_in != false
            if routed_fan_out?(prev_step)
              sizes = resolve_step_class(prev_step.name).turbofan_sizes
              env << {"Name" => "TURBOFAN_PREV_FAN_OUT_SIZES", "Value" => sizes.keys.map(&:to_s).join(",")}
              sizes.each_key do |size_name|
                env << {
                  "Name" => "TURBOFAN_PREV_FAN_OUT_SIZE_#{size_name.to_s.upcase}",
                  "Value.$" => "States.JsonToString($.chunking.#{prev_step_name}.sizes.#{size_name}.parents)"
                }
              end
            else
              env << {
                "Name" => "TURBOFAN_PREV_FAN_OUT_PARENTS",
                "Value.$" => "States.JsonToString($.chunking.#{prev_step_name}.parents)"
              }
            end
          end

          env
        end

        def resolve_job_refs(step_name)
          step_class = resolve_step_class(step_name)
          jobdef_suffix = if step_class&.turbofan_sizes&.any?
            "#{step_name}-#{step_class.turbofan_sizes.keys.first}"
          else
            step_name
          end

          {
            job_definition: "#{@prefix}-jobdef-#{jobdef_suffix}-#{config_hash_for(step_class)}",
            job_queue: resolve_queue_name(step_class)
          }
        end

        def resolve_queue_name(step_class)
          ce_sym = step_class&.turbofan_compute_environment || @pipeline.turbofan_compute_environment
          return "#{@prefix}-queue" unless ce_sym

          ce_class = Turbofan::ComputeEnvironment.resolve(ce_sym)
          ce_class.queue_name(@stage)
        end

        def config_hash_for(step_class)
          retry_cfg = Generators::CloudFormation::JobDefinition.send(:retry_strategy, step_class)
          timeout_cfg = step_class&.turbofan_timeout
          Digest::SHA256.hexdigest("#{retry_cfg}#{timeout_cfg}")[0, 6]
        end

        def routed_fan_out?(step)
          return false unless step&.fan_out?
          step_class = resolve_step_class(step.name)
          step_class&.turbofan_sizes&.any?
        end

        def execution_tags
          {"turbofan:execution.$" => "$$.Execution.Id"}
        end

        def notification_states(topic_arn)
          {
            "NotifySuccess" => {
              "Type" => "Task",
              "Resource" => SNS_RESOURCE,
              "Parameters" => {
                "TopicArn" => topic_arn,
                "Message" => "Pipeline #{@pipeline_name} completed successfully."
              },
              "End" => true
            },
            "NotifyFailure" => {
              "Type" => "Task",
              "Resource" => SNS_RESOURCE,
              "Parameters" => {
                "TopicArn" => topic_arn,
                "Message" => "Pipeline #{@pipeline_name} failed."
              },
              "Next" => "FailExecution"
            },
            "FailExecution" => {
              "Type" => "Fail",
              "Error" => "PipelineExecutionFailed",
              "Cause" => "One or more steps failed during pipeline execution."
            }
          }
        end

        def build_state(step, next_step_name, first:, last:, prev_step_name:, prev_step: nil, prev_step_names: nil)
          step_name = step.name
          step_class = resolve_step_class(step_name)

          if step_class&.turbofan_lambda?
            return build_lambda_state(step, next_step_name, first: first, last: last,
              prev_step_name: prev_step_name, prev_step: prev_step, prev_step_names: prev_step_names)
          end

          if step_class&.turbofan_fargate?
            return build_fargate_state(step, next_step_name, first: first, last: last,
              prev_step_name: prev_step_name, prev_step: prev_step, prev_step_names: prev_step_names)
          end

          env = step_env(step,
            first: first, prev_step_name: prev_step_name,
            prev_step: prev_step, prev_step_names: prev_step_names)

          refs = resolve_job_refs(step_name)

          params = {
            "JobDefinition" => refs[:job_definition],
            "JobName" => "#{@prefix}-#{step_name}",
            "JobQueue" => refs[:job_queue],
            "ContainerOverrides" => {
              "Environment" => env
            },
            "Tags" => execution_tags
          }

          state = {
            "Type" => "Task",
            "Resource" => BATCH_RESOURCE,
            "Parameters" => params
          }

          state["Catch"] = CATCH_ALL_FAILURE

          if step_class&.turbofan_timeout
            state["TimeoutSeconds"] = step_class.turbofan_timeout
          end

          if step_class&.respond_to?(:turbofan_retry_on) && step_class.turbofan_retry_on && !step.fan_out?
            state["Retry"] = [{
              "ErrorEquals" => step_class.turbofan_retry_on,
              "MaxAttempts" => step_class.turbofan_retries,
              "IntervalSeconds" => 2,
              "BackoffRate" => 2.0
            }]
          end

          if last
            state["Next"] = "NotifySuccess"
          else
            state["ResultSelector"] = {
              "JobId.$" => "$.JobId",
              "JobName.$" => "$.JobName",
              "Status.$" => "$.Status"
            }
            state["ResultPath"] = "$.steps.#{step_name}"
            state["Next"] = next_step_name.to_s
          end

          state
        end

        def build_lambda_state(step, next_step_name, first:, last:, prev_step_name:, prev_step: nil, prev_step_names: nil)
          step_name = step.name

          # Build the same env vars as Batch, but as a flat hash payload
          env = step_env(step,
            first: first, prev_step_name: prev_step_name,
            prev_step: prev_step, prev_step_names: prev_step_names)

          # Convert env array [{Name, Value}] to flat hash for Lambda payload
          payload = {}
          env.each do |e|
            if e.key?("Value.$")
              payload["#{e["Name"]}.$"] = e["Value.$"]
            else
              payload[e["Name"]] = e["Value"]
            end
          end

          state = {
            "Type" => "Task",
            "Resource" => "arn:aws:states:::lambda:invoke",
            "Parameters" => {
              "FunctionName" => "#{@prefix}-lambda-#{step_name}",
              "Payload" => payload
            },
            "Catch" => CATCH_ALL_FAILURE
          }

          step_class = resolve_step_class(step_name)
          if step_class&.turbofan_timeout
            state["TimeoutSeconds"] = step_class.turbofan_timeout
          end

          if last
            state["Next"] = "NotifySuccess"
          else
            state["ResultSelector"] = {
              "Payload.$" => "$.Payload"
            }
            state["ResultPath"] = "$.steps.#{step_name}"
            state["Next"] = next_step_name.to_s
          end

          state
        end

        def build_fargate_state(step, next_step_name, first:, last:, prev_step_name:, prev_step: nil, prev_step_names: nil)
          step_name = step.name
          step_class = resolve_step_class(step_name)

          env = step_env(step,
            first: first, prev_step_name: prev_step_name,
            prev_step: prev_step, prev_step_names: prev_step_names)

          # Networking priority: step-level > CE (backward compat) > Turbofan.config
          subnets = if step_class.turbofan_subnets
            step_class.turbofan_subnets
          elsif (ce_sym = step_class.turbofan_compute_environment || @pipeline.turbofan_compute_environment)
            Turbofan::ComputeEnvironment.resolve(ce_sym).resolved_subnets
          else
            Turbofan.config.subnets
          end

          security_groups = if step_class.turbofan_security_groups
            step_class.turbofan_security_groups
          elsif (ce_sym = step_class.turbofan_compute_environment || @pipeline.turbofan_compute_environment)
            Turbofan::ComputeEnvironment.resolve(ce_sym).resolved_security_groups
          else
            Turbofan.config.security_groups
          end

          state = {
            "Type" => "Task",
            "Resource" => "arn:aws:states:::ecs:runTask.sync",
            "Parameters" => {
              "LaunchType" => "FARGATE",
              "Cluster" => "#{@prefix}-fargate-cluster",
              "TaskDefinition" => "#{@prefix}-taskdef-#{step_name}",
              "NetworkConfiguration" => {
                "AwsvpcConfiguration" => {
                  "Subnets" => subnets,
                  "SecurityGroups" => security_groups
                }
              },
              "Overrides" => {
                "ContainerOverrides" => [
                  {
                    "Name" => "worker",
                    "Environment" => env
                  }
                ]
              },
              "Tags" => [{"Key" => "turbofan:execution", "Value.$" => "$$.Execution.Id"}]
            },
            "Catch" => CATCH_ALL_FAILURE
          }

          if step_class.turbofan_timeout
            state["TimeoutSeconds"] = step_class.turbofan_timeout
          end

          if last
            state["Next"] = "NotifySuccess"
          else
            state["ResultPath"] = "$.steps.#{step_name}"
            state["Next"] = next_step_name.to_s
          end

          state
        end

        def build_fan_out_map_state(step, next_step_name, last:)
          step_name = step.name
          refs = resolve_job_refs(step_name)
          env = base_env + [
            {"Name" => "TURBOFAN_STEP_NAME", "Value" => step_name.to_s},
            {"Name" => "TURBOFAN_PARENT_INDEX", "Value.$" => "States.Format('{}', $.index)"}
          ]

          tolerance = step.tolerated_failure_rate || 0

          inner_task = {
            "Type" => "Task",
            "Resource" => BATCH_RESOURCE,
            "Parameters" => {
              "JobDefinition" => refs[:job_definition],
              "JobName.$" => "States.Format('#{@prefix}-#{step_name}-parent{}', $.index)",
              "JobQueue" => refs[:job_queue],
              "ContainerOverrides" => {"Environment" => env},
              "ArrayProperties" => {"Size.$" => "$.size"},
              "Tags" => execution_tags
            },
            "ResultSelector" => {
              "JobId.$" => "$.JobId",
              "JobName.$" => "$.JobName",
              "Status.$" => "$.Status"
            }
          }

          step_class = resolve_step_class(step_name)
          # Don't apply step timeout to fan-out inner task — Batch
          # AttemptDurationSeconds already handles per-job timeout.
          # SFN TimeoutSeconds here would kill the entire parent array job.

          inner_states = {}

          if tolerance > 0
            # Catch Batch failures → check tolerance Lambda → succeed or fail
            inner_task["Catch"] = [{
              "ErrorEquals" => ["Batch.JobFailed"],
              "ResultPath" => "$.error",
              "Next" => "#{step_name}_check_tolerance"
            }]
            inner_task["Next"] = "#{step_name}_done"

            inner_states["#{step_name}_batch"] = inner_task
            inner_states["#{step_name}_check_tolerance"] = {
              "Type" => "Task",
              "Resource" => "arn:aws:states:::lambda:invoke",
              "Parameters" => {
                "FunctionName" => "#{@prefix}-tolerance-check",
                "Payload" => {
                  "error.$" => "$.error",
                  "step_name" => step_name.to_s,
                  "parent_index.$" => "$.index",
                  "parent_size.$" => "$.size",
                  "parent_real_size.$" => "$.real_size",
                  "tolerated_failure_rate" => tolerance,
                  "execution_id.$" => "$$.Execution.Id",
                  "job_name.$" => "States.Format('#{@prefix}-#{step_name}-parent{}', $.index)",
                  "job_queue" => refs[:job_queue]
                }
              },
              "ResultPath" => "$.tolerance_check",
              "Next" => "#{step_name}_done",
              "Catch" => [{
                "ErrorEquals" => ["States.ALL"],
                "Next" => "#{step_name}_tolerance_exceeded"
              }]
            }
            inner_states["#{step_name}_tolerance_exceeded"] = {
              "Type" => "Fail",
              "Error" => "ToleranceExceeded",
              "Cause" => "Failure rate exceeded tolerated threshold of #{(tolerance * 100).round(1)}%"
            }
            inner_states["#{step_name}_done"] = {
              "Type" => "Pass",
              "End" => true
            }
          else
            inner_task["End"] = true
            inner_states["#{step_name}_batch"] = inner_task
          end

          map_state = {
            "Type" => "Map",
            "ItemsPath" => "$.chunking.#{step_name}.parents",
            "MaxConcurrency" => 0,
            "ItemProcessor" => {
              "ProcessorConfig" => {"Mode" => "INLINE"},
              "StartAt" => "#{step_name}_batch",
              "States" => inner_states
            },
            "ResultPath" => "$.steps.#{step_name}",
            "Catch" => CATCH_ALL_FAILURE
          }

          map_state["TimeoutSeconds"] = step.fan_out_timeout if step.fan_out_timeout
          map_state["Next"] = last ? "NotifySuccess" : next_step_name.to_s
          map_state
        end

        def build_branch_state(step, prev_step_name)
          step_name = step.name

          env = base_env + [
            {"Name" => "TURBOFAN_PREV_STEP", "Value" => prev_step_name.to_s},
            {"Name" => "TURBOFAN_STEP_NAME", "Value" => step_name.to_s}
          ]

          refs = resolve_job_refs(step_name)

          params = {
            "JobDefinition" => refs[:job_definition],
            "JobName" => "#{@prefix}-#{step_name}",
            "JobQueue" => refs[:job_queue],
            "ContainerOverrides" => {
              "Environment" => env
            },
            "Tags" => execution_tags
          }

          state = {
            "Type" => "Task",
            "Resource" => BATCH_RESOURCE,
            "Parameters" => params,
            "End" => true
          }

          step_class = resolve_step_class(step_name)
          state["TimeoutSeconds"] = step_class.turbofan_timeout if step_class&.turbofan_timeout

          state
        end

        def build_branch_chain(chain, prev_step_name)
          states = {}
          chain.each_with_index do |step, idx|
            last_in_branch = (idx == chain.size - 1)
            step_prev = (idx == 0) ? prev_step_name : chain[idx - 1].name
            state = build_branch_state(step, step_prev)
            if last_in_branch
              state["End"] = true
            else
              state.delete("End")
              state["Next"] = chain[idx + 1].name.to_s
              state["ResultSelector"] = {
                "JobId.$" => "$.JobId",
                "JobName.$" => "$.JobName",
                "Status.$" => "$.Status"
              }
              state["ResultPath"] = "$.steps.#{step.name}"
            end
            states[step.name.to_s] = state
          end
          {
            "StartAt" => chain.first.name.to_s,
            "States" => states
          }
        end

        def build_chunk_state(step, prev_step_name, first:, routed: false, router_class: nil)
          step_class = resolve_step_class(step.name)
          payload = {
            "step_name" => step.name.to_s,
            "group_size" => step_class.turbofan_batch_size,
            "execution_id.$" => "$$.Execution.Id"
          }

          if routed && step_class.turbofan_sizes.any?
            batch_sizes = {}
            step_class.turbofan_sizes.each do |size_name, size_config|
              bs = step_class.turbofan_batch_size_for(size_name)
              batch_sizes[size_name.to_s] = bs if bs
            end
            payload["batch_sizes"] = batch_sizes
            payload["router_class"] = router_class if router_class
          end

          if first
            payload["trigger.$"] = "$"
          else
            payload["prev_step"] = prev_step_name.to_s
            prev_step_obj = @pipeline.turbofan_dag.sorted_steps.find { |s| s.name.to_s == prev_step_name.to_s }
            if prev_step_obj&.fan_out?
              if routed_fan_out?(prev_step_obj)
                payload["prev_fan_out_sizes.$"] = "States.JsonToString($.chunking.#{prev_step_name}.sizes)"
              else
                payload["prev_fan_out_parents.$"] = "States.JsonToString($.chunking.#{prev_step_name}.parents)"
              end
            end
          end

          payload["routed"] = true if routed

          result_selector = if routed
            {"sizes.$" => "$.Payload.sizes"}
          else
            {"parents.$" => "$.Payload.parents"}
          end

          next_state = routed ? "#{step.name}_routed" : step.name.to_s

          function_name = router_class ? "#{@prefix}-chunking-#{step.name}" : "#{@prefix}-chunking"

          {
            "Type" => "Task",
            "Resource" => "arn:aws:states:::lambda:invoke",
            "Parameters" => {
              "FunctionName" => function_name,
              "Payload" => payload
            },
            "ResultSelector" => result_selector,
            "ResultPath" => "$.chunking.#{step.name}",
            "Next" => next_state,
            "Retry" => LAMBDA_RETRY,
            "Catch" => CATCH_ALL_FAILURE
          }
        end

        def build_routed_parallel_state(step, next_step_name)
          step_name = step.name
          step_class = resolve_step_class(step_name)
          sizes = step_class.turbofan_sizes
          config_hash = config_hash_for(step_class)

          branches = sizes.map do |size_name, _size_config|
            map_state_name = "#{step_name}_#{size_name}"
            batch_state_name = "#{step_name}_#{size_name}_batch"
            env = base_env + [
              {"Name" => "TURBOFAN_STEP_NAME", "Value" => step_name.to_s},
              {"Name" => "TURBOFAN_SIZE", "Value" => size_name.to_s},
              {"Name" => "TURBOFAN_PARENT_INDEX", "Value.$" => "States.Format('{}', $.index)"}
            ]

            queue_name = resolve_queue_name(step_class)

            inner_task = {
              "Type" => "Task",
              "Resource" => BATCH_RESOURCE,
              "Parameters" => {
                "JobDefinition" => "#{@prefix}-jobdef-#{step_name}-#{size_name}-#{config_hash}",
                "JobName.$" => "States.Format('#{@prefix}-#{step_name}-#{size_name}-parent{}', $.index)",
                "JobQueue" => queue_name,
                "ContainerOverrides" => {"Environment" => env},
                "ArrayProperties" => {"Size.$" => "$.size"},
                "Tags" => execution_tags
              },
              "ResultSelector" => {
                "JobId.$" => "$.JobId",
                "JobName.$" => "$.JobName",
                "Status.$" => "$.Status"
              },
              "End" => true
            }

            {
              "StartAt" => map_state_name,
              "States" => {
                map_state_name => {
                  "Type" => "Map",
                  "ItemsPath" => "$.chunking.#{step_name}.sizes.#{size_name}.parents",
                  "MaxConcurrency" => 0,
                  "ItemProcessor" => {
                    "ProcessorConfig" => {"Mode" => "INLINE"},
                    "StartAt" => batch_state_name,
                    "States" => {
                      batch_state_name => inner_task
                    }
                  },
                  "End" => true
                }
              }
            }
          end

          parallel_next = next_step_name || "NotifySuccess"

          {
            "Type" => "Parallel",
            "Branches" => branches,
            "Next" => parallel_next,
            "ResultPath" => "$.steps.#{step_name}_routed",
            "Catch" => CATCH_ALL_FAILURE
          }
        end
      end
    end
  end
end
