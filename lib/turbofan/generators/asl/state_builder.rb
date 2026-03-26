module Turbofan
  module Generators
    class ASL
      module StateBuilder
        private

        def state_name_for(step)
          step.fan_out? ? "#{step.name}_chunk" : step.name.to_s
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

          if first && !step.fan_out?
            env << {"Name" => "TURBOFAN_INPUT", "Value.$" => "States.JsonToString($.input)"}
          elsif prev_step_names
            env << {"Name" => "TURBOFAN_PREV_STEPS", "Value" => prev_step_names.map(&:to_s).join(",")}
          elsif prev_step_name
            env << {"Name" => "TURBOFAN_PREV_STEP", "Value" => prev_step_name.to_s}
          end

          env << {"Name" => "TURBOFAN_STEP_NAME", "Value" => step.name.to_s}

          if prev_step&.fan_out?
            if routed_fan_out?(prev_step)
              sizes = @steps[prev_step.name].turbofan_sizes
              env << {"Name" => "TURBOFAN_PREV_FAN_OUT_SIZES", "Value" => sizes.keys.map(&:to_s).join(",")}
              sizes.each_key do |size_name|
                env << {
                  "Name" => "TURBOFAN_PREV_FAN_OUT_SIZE_#{size_name.to_s.upcase}",
                  "Value.$" => "States.JsonToString($.chunking.#{prev_step_name}.sizes.#{size_name}.count)"
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
          step_class = @steps[step_name]
          suffix = if step_class&.turbofan_sizes&.any?
            "#{step_name}-#{step_class.turbofan_sizes.keys.first}"
          else
            step_name
          end
          {
            job_definition: "#{@prefix}-jobdef-#{suffix}",
            job_queue: "#{@prefix}-queue-#{suffix}"
          }
        end

        def routed_fan_out?(step)
          return false unless step&.fan_out?
          step_class = @steps[step.name]
          step_class&.turbofan_sizes&.any?
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
            }
          }

          if step.fan_out?
            params["ArrayProperties"] = {"Size.$" => "$.chunking.#{step_name}.chunk_count"}
          end

          state = {
            "Type" => "Task",
            "Resource" => BATCH_RESOURCE,
            "Parameters" => params
          }

          state["Catch"] = [{"ErrorEquals" => ["States.ALL"], "Next" => "NotifyFailure"}]

          step_class = @steps[step_name]
          state["TimeoutSeconds"] = step_class.turbofan_timeout if step_class&.turbofan_timeout

          if step_class&.respond_to?(:turbofan_retry_on) && step_class.turbofan_retry_on && !step.fan_out?
            # SFN Retry only for non-fan-out steps. Fan-out steps use Batch-level
            # retries per child (retryStrategy.attempts in the job definition).
            # SFN Retry on a fan-out step would re-submit the entire array job.
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

        def build_fan_out_map_state(step, next_step_name, last:)
          step_name = step.name
          refs = resolve_job_refs(step_name)
          env = base_env + [
            {"Name" => "TURBOFAN_STEP_NAME", "Value" => step_name.to_s},
            {"Name" => "TURBOFAN_PARENT_INDEX", "Value.$" => "States.Format('{}', $.index)"}
          ]

          inner_task = {
            "Type" => "Task",
            "Resource" => BATCH_RESOURCE,
            "Parameters" => {
              "JobDefinition" => refs[:job_definition],
              "JobName.$" => "States.Format('#{@prefix}-#{step_name}-parent{}', $.index)",
              "JobQueue" => refs[:job_queue],
              "ContainerOverrides" => {"Environment" => env},
              "ArrayProperties" => {"Size.$" => "$.size"}
            },
            "ResultSelector" => {
              "JobId.$" => "$.JobId",
              "JobName.$" => "$.JobName",
              "Status.$" => "$.Status"
            },
            "End" => true
          }

          step_class = @steps[step_name]
          inner_task["TimeoutSeconds"] = step_class.turbofan_timeout if step_class&.turbofan_timeout

          map_state = {
            "Type" => "Map",
            "ItemsPath" => "$.chunking.#{step_name}.parents",
            "MaxConcurrency" => 0,
            "ItemProcessor" => {
              "ProcessorConfig" => {"Mode" => "INLINE"},
              "StartAt" => "#{step_name}_batch",
              "States" => {"#{step_name}_batch" => inner_task}
            },
            "ResultPath" => "$.steps.#{step_name}",
            "Catch" => [{"ErrorEquals" => ["States.ALL"], "Next" => "NotifyFailure"}]
          }

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
            }
          }

          state = {
            "Type" => "Task",
            "Resource" => BATCH_RESOURCE,
            "Parameters" => params,
            "End" => true
          }

          step_class = @steps[step_name]
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

        def build_chunk_state(step, prev_step_name, first:, routed: false)
          payload = {
            "step_name" => step.name.to_s,
            "group_size" => step.batch_size,
            "execution_id.$" => "$$.Execution.Id"
          }
          if first
            payload["trigger.$"] = "$"
          else
            payload["prev_step"] = prev_step_name.to_s
            prev_step_obj = @pipeline.turbofan_dag.sorted_steps.find { |s| s.name.to_s == prev_step_name.to_s }
            if prev_step_obj&.fan_out?
              if routed_fan_out?(prev_step_obj)
                payload["prev_fan_out_size.$"] = "States.JsonToString($.chunking.#{prev_step_name}.chunk_count)"
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

          {
            "Type" => "Task",
            "Resource" => "arn:aws:states:::lambda:invoke",
            "Parameters" => {
              "FunctionName" => "#{@prefix}-chunking",
              "Payload" => payload
            },
            "ResultSelector" => result_selector,
            "ResultPath" => "$.chunking.#{step.name}",
            "Next" => next_state,
            "Retry" => [{
              "ErrorEquals" => ["Lambda.ServiceException", "Lambda.TooManyRequestsException", "States.TaskFailed"],
              "IntervalSeconds" => 2,
              "MaxAttempts" => 3,
              "BackoffRate" => 2.0
            }],
            "Catch" => [{"ErrorEquals" => ["States.ALL"], "Next" => "NotifyFailure"}]
          }
        end

        def build_routed_parallel_state(step, next_step_name)
          step_name = step.name
          step_class = @steps[step_name]
          sizes = step_class.turbofan_sizes

          branches = sizes.map do |size_name, _size_config|
            branch_state_name = "#{step_name}_#{size_name}"
            env = base_env + [
              {"Name" => "TURBOFAN_STEP_NAME", "Value" => step_name.to_s},
              {"Name" => "TURBOFAN_SIZE", "Value" => size_name.to_s}
            ]

            {
              "StartAt" => branch_state_name,
              "States" => {
                branch_state_name => {
                  "Type" => "Task",
                  "Resource" => BATCH_RESOURCE,
                  "Parameters" => {
                    "JobDefinition" => "#{@prefix}-jobdef-#{step_name}-#{size_name}",
                    "JobName" => "#{@prefix}-#{step_name}-#{size_name}",
                    "JobQueue" => "#{@prefix}-queue-#{step_name}-#{size_name}",
                    "ContainerOverrides" => {"Environment" => env},
                    "ArrayProperties" => {
                      "Size.$" => "$.chunking.#{step_name}.sizes.#{size_name}.count"
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
            "Catch" => [{"ErrorEquals" => ["States.ALL"], "Next" => "NotifyFailure"}]
          }
        end
      end
    end
  end
end
