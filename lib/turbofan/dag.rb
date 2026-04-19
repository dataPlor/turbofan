# frozen_string_literal: true

require "tsort"

module Turbofan
  # Immutable value object describing a step's position in the DAG.
  # Data.define instances are frozen on construction — every field is
  # read-only. Use `.with(field: value)` for modifications during DAG
  # assembly (DagBuilder#fan_out replaces steps rather than mutating).
  #
  # This is intentionally a value object, not a handle to the user's
  # Step class. Look up the Step class separately via Turbofan.discover_components
  # or the pipeline's step registry.
  DagStep = Data.define(:name, :fan_out, :tolerated_failure_rate, :fan_out_timeout, :fan_in) do
    # Factory preserving the old positional-name + kw-rest sugar
    # (`DagStep.build(:process, fan_out: true)`). Validates unknown
    # keywords, fills defaults, then delegates to Data's generated
    # keyword-only constructor. Keeping this as a separate factory
    # (rather than overriding .new) leaves Data's .new and .with
    # contracts intact — which is the idiomatic Data.define pattern.
    def self.build(name, fan_out: false, tolerated_failure_rate: 0, fan_out_timeout: nil, fan_in: true, **rest)
      raise ArgumentError, "unknown keyword: group (use batch_size: instead)" if rest.key?(:group)
      raise ArgumentError, "unknown keyword: concurrency (use batch_size: instead)" if rest.key?(:concurrency)
      raise ArgumentError, "batch_size has moved to the Step class. Use `batch_size N` in your Step definition." if rest.key?(:batch_size)
      raise ArgumentError, "unknown keyword(s): #{rest.keys.join(", ")}" if rest.any?

      new(
        name: name,
        fan_out: fan_out,
        tolerated_failure_rate: tolerated_failure_rate,
        fan_out_timeout: fan_out_timeout,
        fan_in: fan_in
      )
    end

    def fan_out?
      fan_out
    end
  end

  class Dag
    include TSort

    attr_reader :steps, :edges

    def initialize
      @steps = []
      @edges = []
      @predecessors = Hash.new { |h, k| h[k] = [] }
      @nodes = Set.new
      @frozen = false
    end

    def add_step(name, **kwargs)
      raise "DAG is frozen; cannot add steps after construction" if @frozen

      step = DagStep.build(name, **kwargs)
      @steps << step
      @nodes << name
      step
    end

    # Replace a step in the DAG with an updated immutable copy.
    # DagBuilder#fan_out uses this to record fan_out/timeout/fan_in
    # attributes on an already-added step without mutating its Data instance.
    def replace_step(step_name, new_step)
      raise "DAG is frozen; cannot replace steps after construction" if @frozen

      idx = @steps.index { |s| s.name == step_name }
      raise ArgumentError, "step :#{step_name} not found in DAG" unless idx
      @steps[idx] = new_step
    end

    def add_edge(from:, to:)
      raise "DAG is frozen; cannot add edges after construction" if @frozen

      @edges << {from: from, to: to}
      @nodes << from << to
      @predecessors[to] << from
    end

    def freeze!
      @edges.freeze
      @steps.freeze
      @frozen = true
      self
    end

    def children_of(step_name)
      @edges.select { |e| e[:from] == step_name }.map { |e| e[:to] }
    end

    def parents_of(step_name)
      @predecessors[step_name]
    end

    def sorted_steps
      detect_self_cycles!
      step_map = @steps.each_with_object({}) { |s, h| h[s.name] = s }
      tsort.filter_map { |name| step_map[name] }
    end

    # Find the join point where forked branches reconverge.
    # Returns the DagStep where branches merge, or nil.
    def find_join_point(fork_children, sorted, fork_index)
      active_children = fork_children.count { |child| children_of(child).any? }
      active_children = 1 if active_children == 0

      sorted[(fork_index + 1)..].each do |step|
        next if fork_children.include?(step.name)

        ancestors = all_ancestors(step.name)
        fork_ancestor_count = fork_children.count { |child| ancestors.include?(child) }
        return step if fork_ancestor_count >= active_children
      end
      nil
    end

    # Collect all steps reachable from branch_start up to (but not including) join_point,
    # returned in topological order.
    def branch_steps_for(branch_start, join_point, sorted)
      reachable = Set.new
      queue = [branch_start]
      while (step_name = queue.shift)
        next if reachable.include?(step_name) || step_name == join_point
        reachable << step_name
        children_of(step_name).each { |c| queue << c }
      end
      sorted.select { |s| reachable.include?(s.name) }
    end

    private

    def all_ancestors(step_name)
      visited = Set.new
      queue = parents_of(step_name).dup
      while (parent = queue.shift)
        next if visited.include?(parent)
        visited << parent
        queue.concat(parents_of(parent))
      end
      visited
    end

    def detect_self_cycles!
      @edges.each do |edge|
        if edge[:from] == edge[:to]
          raise TSort::Cyclic, "topological sort failed: #{edge[:from].inspect}"
        end
      end
    end

    def tsort_each_node(&block)
      @nodes.each(&block)
    end

    def tsort_each_child(node, &block)
      @predecessors[node].each(&block)
    end
  end

  class DagProxy
    attr_reader :step_name, :schema

    def initialize(step_name, schema: nil)
      @step_name = step_name
      @schema = schema
    end
  end

  class DagBuilder
    attr_reader :dag

    def initialize
      @dag = Dag.new
      @trigger_input_override = nil
    end

    def trigger_input
      @trigger_input_override || DagProxy.new(:trigger)
    end

    def with_trigger_override(proxy)
      previous = @trigger_input_override
      @trigger_input_override = proxy
      yield
    ensure
      @trigger_input_override = previous
    end

    def fan_out(proxy, tolerated_failure_rate: 0, timeout: nil, fan_in: true, **rest)
      raise ArgumentError, "batch_size has moved to the Step class. Use `batch_size N` in your Step definition." if rest.key?(:batch_size)
      raise ArgumentError, "unknown keyword: group (use batch_size: instead)" if rest.key?(:group)
      raise ArgumentError, "unknown keyword: concurrency (use batch_size: instead)" if rest.key?(:concurrency)
      raise ArgumentError, "unknown keyword(s): #{rest.keys.join(", ")}" if rest.any?
      raise ArgumentError, "fan_out expects a DagProxy, got #{proxy.class}" unless proxy.is_a?(DagProxy)
      unless tolerated_failure_rate.is_a?(Numeric) && (0...1).cover?(tolerated_failure_rate)
        raise ArgumentError, "tolerated_failure_rate must be 0.0 to < 1.0, got #{tolerated_failure_rate}"
      end
      step = @dag.steps.find { |s| s.name == proxy.step_name }
      raise ArgumentError, "step :#{proxy.step_name} not found in DAG" unless step
      @dag.replace_step(proxy.step_name, step.with(
        fan_out: true,
        tolerated_failure_rate: tolerated_failure_rate,
        fan_out_timeout: timeout,
        fan_in: fan_in
      ))
      proxy
    end

    private

    def validate_unique_name!(name)
      return unless @dag.steps.any? { |s| s.name == name }

      raise ArgumentError, "duplicate step name #{name.inspect}"
    end

    def validate_schema_edge!(source_proxy, target_class)
      return if source_proxy.step_name == :trigger
      unless source_proxy.schema
        raise SchemaIncompatibleError,
          "Step :#{source_proxy.step_name} has no output schema"
      end
      target_label = Turbofan::Discovery.class_name_of(target_class) || target_class.inspect
      unless target_class.turbofan_input_schema
        raise SchemaIncompatibleError,
          "Step #{target_label} has no input schema"
      end
      target_schema = target_class.turbofan_input_schema
      source_schema = source_proxy.schema
      required_props = target_schema["required"] || []
      # If the source output is an {items: [...]} envelope, validate against
      # the per-item schema inside the array (the runtime unwraps items before
      # passing to the next step).
      source_props = if source_schema.dig("properties", "items", "items", "properties")
        source_schema["properties"]["items"]["items"]["properties"]
      else
        source_schema["properties"] || {}
      end
      missing = required_props - source_props.keys
      return if missing.empty?
      raise SchemaIncompatibleError,
        "Step #{target_label} requires properties #{missing.inspect} " \
        "not present in output of :#{source_proxy.step_name}"
    end
  end
end
