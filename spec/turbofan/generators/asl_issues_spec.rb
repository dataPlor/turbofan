require "spec_helper"
require "json"

RSpec.describe Turbofan::Generators::ASL, :schemas do
  # ---------------------------------------------------------------------------
  # Branch error handling: branches rely on Parallel state Catch
  # ---------------------------------------------------------------------------
  describe "branch states rely on Parallel state Catch for error handling" do
    let(:pipeline_class) { build_pipeline_for_dag(build_diamond_dag, pipeline_name: "diamond-catch") }
    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
    let(:asl) { generator.generate }

    it "branch Task states do NOT have Catch clauses" do
      parallel = asl["States"]["step_a_parallel"]
      expect(parallel).not_to be_nil, "expected step_a_parallel state to exist"

      parallel["Branches"].each do |branch|
        branch["States"].each do |state_name, state|
          next unless state["Type"] == "Task"

          expect(state).not_to have_key("Catch"),
            "branch state '#{state_name}' should NOT have a Catch clause. " \
            "The Parallel state's own Catch handles errors from any branch."
        end
      end
    end

    it "the Parallel state itself has a Catch clause routing to NotifyFailure" do
      parallel = asl["States"]["step_a_parallel"]
      expect(parallel).to have_key("Catch")

      catch_clause = parallel["Catch"]
      expect(catch_clause).to be_an(Array)
      expect(catch_clause.first["ErrorEquals"]).to eq(["States.ALL"])
      expect(catch_clause.first["Next"]).to eq("NotifyFailure")
    end
  end

  # ---------------------------------------------------------------------------
  # Branch ResultPath/ResultSelector on intermediate states
  # ---------------------------------------------------------------------------
  describe "intermediate branch states have ResultPath and ResultSelector" do
    let(:pipeline_class) { build_pipeline_for_dag(build_multi_step_branch_dag, pipeline_name: "branch-result-path") }
    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
    let(:asl) { generator.generate }

    it "intermediate branch states have ResultSelector with JobId, JobName, Status" do
      parallel = asl["States"]["step_a_parallel"]
      expect(parallel).not_to be_nil

      parallel["Branches"].each do |branch|
        states = branch["States"]
        start_name = branch["StartAt"]
        first_state = states[start_name]

        next unless first_state.key?("Next")

        expect(first_state).to have_key("ResultSelector"),
          "expected intermediate branch state '#{start_name}' to have ResultSelector"

        selector = first_state["ResultSelector"]
        expect(selector).to include("JobId.$" => "$.JobId")
        expect(selector).to include("JobName.$" => "$.JobName")
        expect(selector).to include("Status.$" => "$.Status")
      end
    end

    it "intermediate branch states have ResultPath" do
      parallel = asl["States"]["step_a_parallel"]

      parallel["Branches"].each do |branch|
        states = branch["States"]
        start_name = branch["StartAt"]
        first_state = states[start_name]

        next unless first_state.key?("Next")

        expect(first_state).to have_key("ResultPath"),
          "expected intermediate branch state '#{start_name}' to have ResultPath"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # No literal "nil" in TURBOFAN_PREV_STEP
  # ---------------------------------------------------------------------------
  describe "TURBOFAN_PREV_STEP never contains the literal string 'nil'" do
    context "with a linear pipeline" do
      let(:pipeline_class) do
        stub_const("StepA", Class.new {
          include Turbofan::Step
          compute_environment TestCe
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("StepB", Class.new {
          include Turbofan::Step
          compute_environment TestCe
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline
          pipeline_name "nil-prev-step"
          pipeline do
            results = step_a(trigger_input)
            step_b(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "no batch state has TURBOFAN_PREV_STEP set to 'nil' or empty string" do
        asl["States"].each do |state_name, state|
          env = state.dig("Parameters", "ContainerOverrides", "Environment")
          next unless env

          prev_step_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEP" }
          next unless prev_step_var

          expect(prev_step_var["Value"]).not_to eq("nil"),
            "state '#{state_name}' has TURBOFAN_PREV_STEP set to literal 'nil' string"
          expect(prev_step_var["Value"]).not_to eq(""),
            "state '#{state_name}' has TURBOFAN_PREV_STEP set to empty string"
        end
      end
    end

    context "with a diamond DAG (join step)" do
      let(:pipeline_class) { build_pipeline_for_dag(build_diamond_dag, pipeline_name: "join-nil-check") }
      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "join step uses TURBOFAN_PREV_STEPS, not TURBOFAN_PREV_STEP with 'nil'" do
        step_d = asl["States"]["step_d"]
        env = step_d.dig("Parameters", "ContainerOverrides", "Environment")

        prev_step_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEP" }
        prev_steps_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEPS" }

        if prev_step_var
          expect(prev_step_var["Value"]).not_to eq("nil"),
            "join step_d has TURBOFAN_PREV_STEP = 'nil'"
        end

        expect(prev_steps_var).not_to be_nil,
          "join step_d should have TURBOFAN_PREV_STEPS"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # MAX_ARRAY_SIZE constant
  # ---------------------------------------------------------------------------
  describe "MAX_ARRAY_SIZE constant" do
    it "is defined with value 10_000" do
      expect(described_class::MAX_ARRAY_SIZE).to eq(10_000)
    end
  end

  # ---------------------------------------------------------------------------
  # ASL structural self-consistency
  # ---------------------------------------------------------------------------
  describe "ASL structural self-consistency" do
    context "single-step pipeline" do
      let(:pipeline_class) do
        stub_const("Process", Class.new {
          include Turbofan::Step
          compute_environment TestCe
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline
          pipeline_name "validate-single"
          pipeline { process(trigger_input) }
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      include_examples "valid ASL structure"
    end

    context "two-step linear pipeline" do
      let(:pipeline_class) do
        stub_const("Extract", Class.new {
          include Turbofan::Step
          compute_environment TestCe
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Load", Class.new {
          include Turbofan::Step
          compute_environment TestCe
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline
          pipeline_name "validate-linear"
          pipeline do
            results = extract(trigger_input)
            load(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      include_examples "valid ASL structure"
    end

    context "diamond DAG (A -> {B, C} -> D)" do
      let(:pipeline_class) { build_pipeline_for_dag(build_diamond_dag, pipeline_name: "validate-diamond") }
      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      include_examples "valid ASL structure"
    end

    context "fan-out pipeline" do
      let(:pipeline_class) do
        stub_const("Discover", Class.new {
          include Turbofan::Step
          compute_environment TestCe
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Process", Class.new {
          include Turbofan::Step
          compute_environment TestCe
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        stub_const("Aggregate", Class.new {
          include Turbofan::Step
          compute_environment TestCe
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        })
        Class.new do
          include Turbofan::Pipeline
          pipeline_name "validate-fanout"
          pipeline do
            files = discover(trigger_input)
            results = fan_out(process(files), batch_size: 100)
            aggregate(results)
          end
        end
      end

      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      include_examples "valid ASL structure"
    end

    context "multi-step branch diamond (A -> {B->B2, C->C2} -> D)" do
      let(:pipeline_class) { build_pipeline_for_dag(build_multi_step_branch_dag, pipeline_name: "validate-multi-branch") }
      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      include_examples "valid ASL structure"
    end

    context "sequential forks (A -> {B,C} -> D -> {E,F} -> G)" do
      let(:pipeline_class) { build_pipeline_for_dag(build_sequential_forks_dag, pipeline_name: "validate-seq-forks") }
      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      include_examples "valid ASL structure"
    end
  end

  # ---------------------------------------------------------------------------
  # Complex fork patterns
  # ---------------------------------------------------------------------------
  describe "complex fork patterns" do
    context "sequential forks: A -> {B, C} -> D -> {E, F} -> G" do
      let(:pipeline_class) { build_pipeline_for_dag(build_sequential_forks_dag, pipeline_name: "sequential-forks") }
      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "starts at step_a" do
        expect(asl["StartAt"]).to eq("step_a")
      end

      it "creates the first Parallel state after step_a with 2 branches" do
        expect(asl["States"]).to have_key("step_a_parallel")
        parallel = asl["States"]["step_a_parallel"]
        expect(parallel["Type"]).to eq("Parallel")
        expect(parallel["Branches"].size).to eq(2)
      end

      it "first Parallel state joins at step_d" do
        parallel = asl["States"]["step_a_parallel"]
        expect(parallel["Next"]).to eq("step_d")
      end

      it "step_d exists as a regular task state pointing to second Parallel" do
        expect(asl["States"]).to have_key("step_d")
        expect(asl["States"]["step_d"]["Type"]).to eq("Task")
        expect(asl["States"]["step_d"]["Next"]).to eq("step_d_parallel")
      end

      it "creates the second Parallel state after step_d with 2 branches" do
        expect(asl["States"]).to have_key("step_d_parallel")
        parallel = asl["States"]["step_d_parallel"]
        expect(parallel["Type"]).to eq("Parallel")
        expect(parallel["Branches"].size).to eq(2)
      end

      it "second Parallel state joins at step_g" do
        parallel = asl["States"]["step_d_parallel"]
        expect(parallel["Next"]).to eq("step_g")
      end

      it "step_g chains to success notification" do
        expect(asl["States"]).to have_key("step_g")
        expect(asl["States"]["step_g"]["Next"]).to match(/success/i)
      end

      include_examples "valid ASL structure"
    end

    context "3-way fork: A -> {B, C, D} -> E" do
      let(:pipeline_class) { build_pipeline_for_dag(build_three_way_fork_dag, pipeline_name: "three-way-fork") }
      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "creates a Parallel state with 3 branches" do
        expect(asl["States"]).to have_key("step_a_parallel")
        parallel = asl["States"]["step_a_parallel"]
        expect(parallel["Type"]).to eq("Parallel")
        expect(parallel["Branches"].size).to eq(3)
      end

      it "each branch contains exactly one state" do
        parallel = asl["States"]["step_a_parallel"]
        parallel["Branches"].each_with_index do |branch, idx|
          expect(branch["States"].size).to eq(1),
            "expected branch #{idx} to have 1 state, got #{branch["States"].size}"
        end
      end

      it "Parallel state joins at step_e" do
        parallel = asl["States"]["step_a_parallel"]
        expect(parallel["Next"]).to eq("step_e")
      end

      it "step_e is a join step with TURBOFAN_PREV_STEPS" do
        step_e = asl["States"]["step_e"]
        env = step_e.dig("Parameters", "ContainerOverrides", "Environment")
        prev_steps_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEPS" }
        expect(prev_steps_var).not_to be_nil,
          "expected step_e to have TURBOFAN_PREV_STEPS since it joins 3 branches"
      end

      it "step_e chains to success notification" do
        expect(asl["States"]["step_e"]["Next"]).to match(/success/i)
      end

      include_examples "valid ASL structure"
    end
  end

  # ---------------------------------------------------------------------------
  # TimeoutSeconds from step timeout config
  # ---------------------------------------------------------------------------
  describe "TimeoutSeconds from step timeout config" do
    let(:step_with_timeout) do
      Class.new do
        include Turbofan::Step
        compute_environment TestCe
        cpu 1
        timeout 7200
        input_schema "passthrough.json"
        output_schema "passthrough.json"
      end
    end

    let(:pipeline_class) do
      stub_const("Compute", step_with_timeout)
      Class.new do
        include Turbofan::Pipeline
        pipeline_name "timeout-pipeline"
        pipeline { compute(trigger_input) }
      end
    end

    let(:generator) do
      described_class.new(
        pipeline: pipeline_class,
        stage: "production",
        steps: {compute: step_with_timeout}
      )
    end
    let(:asl) { generator.generate }

    it "the Step DSL supports timeout configuration" do
      expect(step_with_timeout.turbofan_timeout).to eq(7200)
    end

    it "generated Task state includes TimeoutSeconds matching the custom value" do
      compute_state = asl["States"]["compute"]
      expect(compute_state).to have_key("TimeoutSeconds")
      expect(compute_state["TimeoutSeconds"]).to eq(7200)
    end

    context "step with default timeout (3600)" do
      let(:default_timeout_step) do
        Class.new do
          include Turbofan::Step
          compute_environment TestCe
          cpu 1
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:pipeline_class) do
        stub_const("DefaultStep", default_timeout_step)
        Class.new do
          include Turbofan::Pipeline
          pipeline_name "default-timeout"
          pipeline { default_step(trigger_input) }
        end
      end

      let(:generator) do
        described_class.new(
          pipeline: pipeline_class,
          stage: "production",
          steps: {default_step: default_timeout_step}
        )
      end
      let(:asl) { generator.generate }

      it "emits TimeoutSeconds with the default value of 3600" do
        expect(default_timeout_step.turbofan_timeout).to eq(3600)
        state = asl["States"]["default_step"]
        expect(state).to have_key("TimeoutSeconds")
        expect(state["TimeoutSeconds"]).to eq(3600)
      end
    end

    context "non-Task states" do
      let(:pipeline_class) { build_pipeline_for_dag(build_diamond_dag, pipeline_name: "timeout-non-task") }
      let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
      let(:asl) { generator.generate }

      it "Parallel states do not have TimeoutSeconds" do
        asl["States"].each do |state_name, state|
          next unless state["Type"] == "Parallel"

          expect(state).not_to have_key("TimeoutSeconds"),
            "Parallel state '#{state_name}' should not have TimeoutSeconds"
        end
      end

      it "notification states (SNS) do not have TimeoutSeconds" do
        %w[NotifySuccess NotifyFailure].each do |name|
          state = asl["States"][name]
          next unless state

          expect(state).not_to have_key("TimeoutSeconds"),
            "notification state '#{name}' should not have TimeoutSeconds"
        end
      end
    end

    context "branch states" do
      let(:branch_step) do
        Class.new do
          include Turbofan::Step
          compute_environment TestCe
          cpu 1
          timeout 5400
          input_schema "passthrough.json"
          output_schema "passthrough.json"
        end
      end

      let(:pipeline_class) { build_pipeline_for_dag(build_diamond_dag, pipeline_name: "branch-timeout") }
      let(:generator) do
        described_class.new(
          pipeline: pipeline_class,
          stage: "production",
          steps: {step_b: branch_step, step_c: branch_step}
        )
      end
      let(:asl) { generator.generate }

      it "branch Task states include TimeoutSeconds when step class is provided" do
        parallel = asl["States"]["step_a_parallel"]
        expect(parallel).not_to be_nil

        parallel["Branches"].each do |branch|
          branch["States"].each do |state_name, state|
            next unless state["Type"] == "Task"

            expect(state).to have_key("TimeoutSeconds"),
              "expected branch state '#{state_name}' to have TimeoutSeconds"
            expect(state["TimeoutSeconds"]).to eq(5400)
          end
        end
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Branch Catch validation (resolved: branches have no Catch)
  # ---------------------------------------------------------------------------
  describe "branch states do not have invalid Catch targets" do
    let(:pipeline_class) { build_pipeline_for_dag(build_diamond_dag, pipeline_name: "branch-catch-validation") }
    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
    let(:asl) { generator.generate }

    it "branch states do NOT have Catch clauses" do
      parallel = asl["States"]["step_a_parallel"]
      parallel["Branches"].each do |branch|
        branch["States"].each do |state_name, state|
          next unless state["Type"] == "Task"
          expect(state).not_to have_key("Catch"),
            "branch state '#{state_name}' should not have a Catch clause"
        end
      end
    end

    it "Parallel state has Catch routing to NotifyFailure" do
      parallel = asl["States"]["step_a_parallel"]
      expect(parallel).to have_key("Catch")
      expect(parallel["Catch"].first["Next"]).to eq("NotifyFailure")
    end
  end

  # ---------------------------------------------------------------------------
  # Multi-step branch predecessor chain
  # ---------------------------------------------------------------------------
  describe "multi-step branch states reference their immediate predecessor" do
    let(:pipeline_class) { build_pipeline_for_dag(build_multi_step_branch_dag, pipeline_name: "branch-prev-step") }
    let(:generator) { described_class.new(pipeline: pipeline_class, stage: "production") }
    let(:asl) { generator.generate }

    it "first branch state gets TURBOFAN_PREV_STEP set to the fork step" do
      parallel = asl["States"]["step_a_parallel"]
      branch_b = parallel["Branches"].find { |b| b["StartAt"] == "step_b" }
      step_b_state = branch_b["States"]["step_b"]
      env = step_b_state.dig("Parameters", "ContainerOverrides", "Environment")
      prev_step_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEP" }

      expect(prev_step_var).not_to be_nil
      expect(prev_step_var["Value"]).to eq("step_a")
    end

    it "second branch state gets TURBOFAN_PREV_STEP set to its immediate predecessor" do
      parallel = asl["States"]["step_a_parallel"]
      branch_b = parallel["Branches"].find { |b| b["StartAt"] == "step_b" }
      step_b2_state = branch_b["States"]["step_b2"]
      env = step_b2_state.dig("Parameters", "ContainerOverrides", "Environment")
      prev_step_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEP" }

      expect(prev_step_var).not_to be_nil
      expect(prev_step_var["Value"]).to eq("step_b")
    end

    it "both branches have correct predecessor chains" do
      parallel = asl["States"]["step_a_parallel"]

      parallel["Branches"].each do |branch|
        state_names = branch["States"].keys
        state_names.each_with_index do |state_name, idx|
          state = branch["States"][state_name]
          env = state.dig("Parameters", "ContainerOverrides", "Environment")
          prev_step_var = env.find { |e| e["Name"] == "TURBOFAN_PREV_STEP" }

          expected_prev = (idx == 0) ? "step_a" : state_names[idx - 1]

          expect(prev_step_var).not_to be_nil,
            "branch state '#{state_name}' should have TURBOFAN_PREV_STEP"
          expect(prev_step_var["Value"]).to eq(expected_prev),
            "branch state '#{state_name}' should have TURBOFAN_PREV_STEP=#{expected_prev}, " \
            "got #{prev_step_var["Value"]}"
        end
      end
    end
  end
end
