require "json"

module Turbofan
  module Generators
    class ASL
      BATCH_RESOURCE = "arn:aws:states:::batch:submitJob.sync"
      SNS_RESOURCE = "arn:aws:states:::sns:publish"
      # Maximum items per Batch ArrayProperties. Not enforced here;
      # the chunking Lambda is responsible for respecting this limit.
      MAX_ARRAY_SIZE = 10_000

      def initialize(pipeline:, stage:, steps: {})
        @pipeline = pipeline
        @stage = stage
        @steps = steps
      end

      def generate
        dag = @pipeline.turbofan_dag
        sorted = dag.sorted_steps
        pipeline_name = @pipeline.turbofan_name

        prefix = "turbofan-#{pipeline_name}-#{@stage}"
        topic_arn = "arn:aws:sns:${AWS::Region}:${AWS::AccountId}:#{prefix}-notifications"

        # Detect forks (steps with >1 children) and compute join points
        forks = {}     # fork_name => [branch_child_names]
        join_info = {} # join_name => [branch_child_names]

        sorted.each_with_index do |step, idx|
          children = dag.children_of(step.name)
          next unless children.size > 1

          forks[step.name] = children
          join_step = dag.find_join_point(children, sorted, idx)
          join_info[join_step.name] = children if join_step
        end

        visited = Set.new
        states = {}

        sorted.each_with_index do |step, index|
          next if visited.include?(step.name)

          first = (index == 0)

          if forks.key?(step.name)
            # --- Fork step: emit step then a Parallel state ---
            branch_children = forks[step.name]
            fork_join = dag.find_join_point(branch_children, sorted, index)

            # Mark all steps in all branches as visited
            branch_children.each do |child_name|
              dag.branch_steps_for(child_name, fork_join&.name, sorted).each { |s| visited << s.name }
            end

            parallel_key = "#{step.name}_parallel"

            # Emit fork step pointing to the Parallel state
            prev_step = find_prev(sorted, index, visited)
            states[step.name.to_s] = build_state(
              pipeline_name, step, parallel_key,
              first: first, last: false,
              prev_step_name: prev_step&.name, prev_step: prev_step
            )

            # Build Parallel branches
            branches = branch_children.map do |child_name|
              chain = dag.branch_steps_for(child_name, fork_join&.name, sorted)
              build_branch_chain(pipeline_name, chain, step.name)
            end

            # Determine Next for the Parallel state
            join_step = fork_join || sorted[(index + 1)..].find { |s| !visited.include?(s.name) }
            parallel_next = if join_step
              join_step.fan_out? ? "#{join_step.name}_chunk" : join_step.name.to_s
            else
              "NotifySuccess"
            end

            states[parallel_key] = {
              "Type" => "Parallel",
              "Branches" => branches,
              "Next" => parallel_next,
              "ResultPath" => "$.steps.#{step.name}_parallel",
              "Catch" => [{"ErrorEquals" => ["States.ALL"], "Next" => "NotifyFailure"}]
            }
          else
            # --- Regular step (may be a join step after a Parallel) ---
            is_join = join_info.key?(step.name)

            remaining = sorted[(index + 1)..].reject { |s| visited.include?(s.name) }
            next_step = remaining.first
            last = next_step.nil?

            actual_next = if last
              nil
            elsif next_step.fan_out?
              "#{next_step.name}_chunk"
            else
              next_step.name.to_s
            end

            if step.fan_out?
              step_class = @steps[step.name]
              routed = step_class&.turbofan_sizes&.any?
              chunk_prev = is_join ? nil : find_prev(sorted, index, visited)

              states["#{step.name}_chunk"] = build_chunk_state(
                pipeline_name, step, chunk_prev&.name, first: first, routed: routed
              )

              if routed
                routed_next = last ? "NotifySuccess" : actual_next
                states["#{step.name}_routed"] = build_routed_parallel_state(
                  pipeline_name, step, routed_next
                )
                next
              end
            end

            if is_join
              state = build_state(
                pipeline_name, step, actual_next,
                first: false, last: last,
                prev_step_name: nil, prev_step: nil,
                prev_step_names: join_info[step.name]
              )
            else
              prev_step = find_prev(sorted, index, visited)
              state = build_state(
                pipeline_name, step, actual_next,
                first: first, last: last,
                prev_step_name: prev_step&.name, prev_step: prev_step
              )
            end

            states[step.name.to_s] = state
          end
        end

        states.merge!(notification_states(topic_arn, pipeline_name))

        start_step = sorted.first
        start_at = start_step.fan_out? ? "#{start_step.name}_chunk" : start_step.name.to_s

        {
          "Comment" => "Turbofan pipeline: #{pipeline_name}",
          "StartAt" => start_at,
          "States" => states
        }
      end

      def to_json(*)
        JSON.generate(generate)
      end

      private

      def base_env(pipeline_name)
        [
          {"Name" => "TURBOFAN_EXECUTION_ID", "Value.$" => "$$.Execution.Id"},
          {"Name" => "TURBOFAN_STAGE", "Value" => @stage},
          {"Name" => "TURBOFAN_PIPELINE", "Value" => pipeline_name},
          {"Name" => "TURBOFAN_BUCKET", "Value" => Turbofan.config.bucket},
          {"Name" => "TURBOFAN_BUCKET_PREFIX", "Value" => Naming.bucket_prefix(pipeline_name, @stage)},
          {"Name" => "AWS_REGION", "Value" => "${AWS::Region}"},
          {"Name" => "AWS_DEFAULT_REGION", "Value" => "${AWS::Region}"}
        ]
      end

      def resolve_job_refs(prefix, step_name)
        step_class = @steps[step_name]
        suffix = if step_class&.turbofan_sizes&.any?
          "#{step_name}-#{step_class.turbofan_sizes.keys.first}"
        else
          step_name
        end
        {
          job_definition: "#{prefix}-jobdef-#{suffix}",
          job_queue: "#{prefix}-queue-#{suffix}"
        }
      end

      def find_prev(sorted, index, visited)
        return nil if index == 0
        (index - 1).downto(0).each do |i|
          return sorted[i] unless visited.include?(sorted[i].name)
        end
        nil
      end

      def build_branch_chain(pipeline_name, chain, fork_step_name)
        states = {}
        chain.each_with_index do |step, idx|
          last_in_branch = (idx == chain.size - 1)
          prev_step_name = (idx == 0) ? fork_step_name : chain[idx - 1].name
          state = build_branch_state(pipeline_name, step, prev_step_name)
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

      def notification_states(topic_arn, pipeline_name)
        {
          "NotifySuccess" => {
            "Type" => "Task",
            "Resource" => SNS_RESOURCE,
            "Parameters" => {
              "TopicArn" => topic_arn,
              "Message" => "Pipeline #{pipeline_name} completed successfully."
            },
            "End" => true
          },
          "NotifyFailure" => {
            "Type" => "Task",
            "Resource" => SNS_RESOURCE,
            "Parameters" => {
              "TopicArn" => topic_arn,
              "Message" => "Pipeline #{pipeline_name} failed."
            },
            "End" => true
          }
        }
      end

      def build_chunk_state(pipeline_name, step, prev_step_name, first:, routed: false)
        prefix = "turbofan-#{pipeline_name}-#{@stage}"
        payload = {
          "step_name" => step.name.to_s,
          "group_size" => step.batch_size,
          "execution_id.$" => "$$.Execution.Id"
        }
        if first
          payload["items.$"] = "$.input"
        else
          payload["prev_step"] = prev_step_name.to_s
          prev_step_obj = @pipeline.turbofan_dag.sorted_steps.find { |s| s.name.to_s == prev_step_name.to_s }
          if prev_step_obj&.fan_out?
            payload["prev_fan_out_size.$"] = "States.JsonToString($.chunking.#{prev_step_name}.chunk_count)"
          end
        end

        payload["routed"] = true if routed

        result_selector = if routed
          {"sizes.$" => "$.Payload.sizes"}
        else
          {"chunk_count.$" => "$.Payload.chunk_count"}
        end

        next_state = routed ? "#{step.name}_routed" : step.name.to_s

        {
          "Type" => "Task",
          "Resource" => "arn:aws:states:::lambda:invoke",
          "Parameters" => {
            "FunctionName" => "#{prefix}-chunking",
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

      def build_branch_state(pipeline_name, step, fork_step_name)
        prefix = "turbofan-#{pipeline_name}-#{@stage}"
        step_name = step.name

        env = base_env(pipeline_name) + [
          {"Name" => "TURBOFAN_PREV_STEP", "Value" => fork_step_name.to_s},
          {"Name" => "TURBOFAN_STEP_NAME", "Value" => step_name.to_s}
        ]

        refs = resolve_job_refs(prefix, step_name)

        params = {
          "JobDefinition" => refs[:job_definition],
          "JobName" => "#{prefix}-#{step_name}",
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

      def routed_fan_out?(step)
        return false unless step&.fan_out?
        step_class = @steps[step.name]
        step_class&.turbofan_sizes&.any?
      end

      def build_routed_parallel_state(pipeline_name, step, next_step_name)
        prefix = "turbofan-#{pipeline_name}-#{@stage}"
        step_name = step.name
        step_class = @steps[step_name]
        sizes = step_class.turbofan_sizes

        branches = sizes.map do |size_name, _size_config|
          branch_state_name = "#{step_name}_#{size_name}"
          env = base_env(pipeline_name) + [
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
                  "JobDefinition" => "#{prefix}-jobdef-#{step_name}-#{size_name}",
                  "JobName" => "#{prefix}-#{step_name}-#{size_name}",
                  "JobQueue" => "#{prefix}-queue-#{step_name}-#{size_name}",
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

      def build_state(pipeline_name, step, next_step_name, first:, last:, prev_step_name:, prev_step: nil, prev_step_names: nil)
        prefix = "turbofan-#{pipeline_name}-#{@stage}"
        step_name = step.name

        env = base_env(pipeline_name)

        if first
          env << {"Name" => "TURBOFAN_INPUT", "Value.$" => "States.JsonToString($.input)"}
        elsif prev_step_names
          env << {"Name" => "TURBOFAN_PREV_STEPS", "Value" => prev_step_names.map(&:to_s).join(",")}
        elsif prev_step_name
          env << {"Name" => "TURBOFAN_PREV_STEP", "Value" => prev_step_name.to_s}
        end

        env << {"Name" => "TURBOFAN_STEP_NAME", "Value" => step_name.to_s}

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
              "Name" => "TURBOFAN_PREV_FAN_OUT_SIZE",
              "Value.$" => "States.JsonToString($.chunking.#{prev_step_name}.chunk_count)"
            }
          end
        end

        refs = resolve_job_refs(prefix, step_name)

        params = {
          "JobDefinition" => refs[:job_definition],
          "JobName" => "#{prefix}-#{step_name}",
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

        if step_class&.respond_to?(:turbofan_retry_on) && step_class.turbofan_retry_on
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
    end
  end
end
