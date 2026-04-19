# frozen_string_literal: true

require "json"

module Turbofan
  module Generators
    class ASL
      include StateBuilder

      BATCH_RESOURCE = "arn:aws:states:::batch:submitJob.sync"
      SNS_RESOURCE = "arn:aws:states:::sns:publish"
      # Maximum items per Batch ArrayProperties. Not enforced here;
      # the chunking Lambda is responsible for respecting this limit.
      MAX_ARRAY_SIZE = 10_000

      def initialize(pipeline:, stage:, steps: {})
        @pipeline = pipeline
        @stage = stage
        @steps = steps
        @pipeline_name = pipeline.turbofan_name
        @prefix = "turbofan-#{@pipeline_name}-#{stage}"
        @discovered_steps = nil
      end

      def generate
        dag = @pipeline.turbofan_dag
        sorted = dag.sorted_steps

        topic_arn = "arn:aws:sns:${AWS::Region}:${AWS::AccountId}:#{@prefix}-notifications"

        forks, join_info = detect_forks(dag, sorted)

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
              step, parallel_key,
              first: first, last: false,
              prev_step_name: prev_step&.name, prev_step: prev_step
            )

            # Build Parallel branches
            branches = branch_children.map do |child_name|
              chain = dag.branch_steps_for(child_name, fork_join&.name, sorted)
              build_branch_chain(chain, step.name)
            end

            # Determine Next for the Parallel state
            join_step = fork_join || sorted[(index + 1)..].find { |s| !visited.include?(s.name) }
            parallel_next = join_step ? state_name_for(join_step) : "NotifySuccess"

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

            actual_next = last ? nil : state_name_for(next_step)

            if step.fan_out?
              step_class = resolve_step_class(step.name)
              routed = step_class&.turbofan&.sizes&.any?
              chunk_prev = is_join ? nil : find_prev(sorted, index, visited)
              router_class = routed ? "#{Naming.pascal_case(step.name)}Router" : nil

              states["#{step.name}_chunk"] = build_chunk_state(
                step, chunk_prev&.name, first: first, routed: routed, router_class: router_class
              )

              if routed
                routed_next = last ? "NotifySuccess" : actual_next
                states["#{step.name}_routed"] = build_routed_parallel_state(
                  step, routed_next
                )
              else
                states[step.name.to_s] = build_fan_out_map_state(
                  step, actual_next, last: last
                )
              end
              next
            end

            if is_join
              state = build_state(
                step, actual_next,
                first: false, last: last,
                prev_step_name: nil, prev_step: nil,
                prev_step_names: join_info[step.name]
              )
            else
              prev_step = find_prev(sorted, index, visited)
              state = build_state(
                step, actual_next,
                first: first, last: last,
                prev_step_name: prev_step&.name, prev_step: prev_step
              )
            end

            states[step.name.to_s] = state
          end
        end

        states.merge!(notification_states(topic_arn))

        start_step = sorted.first
        start_at = state_name_for(start_step)

        asl = {
          "Comment" => "Turbofan pipeline: #{@pipeline_name}",
          "StartAt" => start_at,
          "States" => states
        }
        asl["TimeoutSeconds"] = @pipeline.turbofan_timeout if @pipeline.turbofan_timeout
        asl
      end

      def to_json(*)
        JSON.generate(generate)
      end

      private

      def resolve_step_class(step_name)
        # First try the provided steps hash (keyed by symbol or string)
        result = @steps[step_name] || @steps[step_name.to_sym] || @steps[step_name.to_s]
        return result if result

        # Fall back to discovering components (used in tests)
        @discovered_steps ||= Turbofan.discover_components[:steps]
        @discovered_steps[step_name.to_sym] || @discovered_steps[step_name.to_s]
      end

      def detect_forks(dag, sorted)
        forks = {}     # fork_name => [branch_child_names]
        join_info = {} # join_name => [branch_child_names]

        sorted.each_with_index do |step, idx|
          children = dag.children_of(step.name)
          next unless children.size > 1

          forks[step.name] = children
          join_step = dag.find_join_point(children, sorted, idx)
          join_info[join_step.name] = children if join_step
        end

        [forks, join_info]
      end

      def find_prev(sorted, index, visited)
        return nil if index == 0
        (index - 1).downto(0).each do |i|
          return sorted[i] unless visited.include?(sorted[i].name)
        end
        nil
      end
    end
  end
end
