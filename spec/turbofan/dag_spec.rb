# frozen_string_literal: true

require "spec_helper"

RSpec.describe Turbofan::Dag do # rubocop:disable RSpec/MultipleDescribes
  subject(:dag) { described_class.new }

  describe "#add_step" do
    it "adds a step to the DAG" do
      dag.add_step(:process)
      expect(dag.steps.size).to eq(1)
      expect(dag.steps.first.name).to eq(:process)
    end

    it "defaults fan_out to false" do
      dag.add_step(:process)
      expect(dag.steps.first.fan_out?).to be false
    end

    it "accepts fan_out flag" do
      dag.add_step(:process, fan_out: true)
      step = dag.steps.first
      expect(step.fan_out?).to be true
    end

    it "returns the created DagStep" do
      step = dag.add_step(:process)
      expect(step).to be_a(Turbofan::DagStep)
      expect(step.name).to eq(:process)
    end
  end

  describe "#add_edge" do
    it "records an edge between two nodes" do
      dag.add_step(:a)
      dag.add_step(:b)
      dag.add_edge(from: :a, to: :b)

      expect(dag.edges).to include(from: :a, to: :b)
    end

    it "records multiple edges" do
      dag.add_step(:a)
      dag.add_step(:b)
      dag.add_step(:c)
      dag.add_edge(from: :trigger, to: :a)
      dag.add_edge(from: :a, to: :b)
      dag.add_edge(from: :b, to: :c)

      expect(dag.edges.size).to eq(3)
    end
  end

  describe "#sorted_steps (topological sort)" do
    it "returns steps in topological order for a linear DAG" do
      dag.add_step(:extract)
      dag.add_step(:transform)
      dag.add_step(:load)
      dag.add_edge(from: :trigger, to: :extract)
      dag.add_edge(from: :extract, to: :transform)
      dag.add_edge(from: :transform, to: :load)

      sorted = dag.sorted_steps
      expect(sorted.map(&:name)).to eq(%i[extract transform load])
    end

    it "excludes the :trigger pseudo-node from sorted output" do
      dag.add_step(:process)
      dag.add_edge(from: :trigger, to: :process)

      sorted = dag.sorted_steps
      names = sorted.map(&:name)
      expect(names).not_to include(:trigger)
      expect(names).to eq([:process])
    end

    it "maintains correct order for fan-out steps" do
      dag.add_step(:discover)
      dag.add_step(:process, fan_out: true)
      dag.add_step(:aggregate)
      dag.add_edge(from: :trigger, to: :discover)
      dag.add_edge(from: :discover, to: :process)
      dag.add_edge(from: :process, to: :aggregate)

      sorted = dag.sorted_steps
      expect(sorted.map(&:name)).to eq(%i[discover process aggregate])
    end

    it "preserves DagStep attributes in sorted output" do
      dag.add_step(:process, fan_out: true)
      dag.add_edge(from: :trigger, to: :process)

      sorted = dag.sorted_steps
      step = sorted.first
      expect(step.fan_out?).to be true
    end
  end

  describe "cycle detection" do
    it "raises TSort::Cyclic for a cycle" do
      dag.add_step(:a)
      dag.add_step(:b)
      dag.add_edge(from: :a, to: :b)
      dag.add_edge(from: :b, to: :a)

      expect { dag.sorted_steps }.to raise_error(TSort::Cyclic)
    end

    it "raises TSort::Cyclic for a self-referencing node" do
      dag.add_step(:a)
      dag.add_edge(from: :a, to: :a)

      expect { dag.sorted_steps }.to raise_error(TSort::Cyclic)
    end

    it "raises TSort::Cyclic for a three-node cycle" do
      dag.add_step(:a)
      dag.add_step(:b)
      dag.add_step(:c)
      dag.add_edge(from: :a, to: :b)
      dag.add_edge(from: :b, to: :c)
      dag.add_edge(from: :c, to: :a)

      expect { dag.sorted_steps }.to raise_error(TSort::Cyclic)
    end
  end

  describe "#freeze!" do
    it "freezes the edges array" do
      dag.add_step(:a)
      dag.add_edge(from: :trigger, to: :a)
      dag.freeze!

      expect(dag.edges).to be_frozen
    end

    it "freezes the steps array" do
      dag.add_step(:a)
      dag.freeze!

      expect(dag.steps).to be_frozen
    end

    it "prevents adding new steps after freezing" do
      dag.freeze!

      expect { dag.add_step(:a) }.to raise_error(RuntimeError, /frozen/)
    end

    it "prevents adding new edges after freezing" do
      dag.freeze!

      expect { dag.add_edge(from: :a, to: :b) }.to raise_error(RuntimeError, /frozen/)
    end
  end

  describe "#children_of" do
    it "returns direct children of a step" do
      dag.add_step(:a)
      dag.add_step(:b)
      dag.add_step(:c)
      dag.add_edge(from: :a, to: :b)
      dag.add_edge(from: :a, to: :c)

      expect(dag.children_of(:a)).to contain_exactly(:b, :c)
    end

    it "returns empty array for a leaf step" do
      dag.add_step(:a)
      dag.add_step(:b)
      dag.add_edge(from: :a, to: :b)

      expect(dag.children_of(:b)).to be_empty
    end
  end

  describe "#parents_of" do
    it "returns direct parents of a step" do
      dag.add_step(:a)
      dag.add_step(:b)
      dag.add_step(:c)
      dag.add_edge(from: :a, to: :c)
      dag.add_edge(from: :b, to: :c)

      expect(dag.parents_of(:c)).to contain_exactly(:a, :b)
    end

    it "returns empty array for a root step" do
      dag.add_step(:a)
      dag.add_edge(from: :trigger, to: :a)

      expect(dag.parents_of(:a)).to contain_exactly(:trigger)
    end
  end

  describe "empty DAG" do
    it "has no steps" do
      expect(dag.steps).to be_empty
    end

    it "has no edges" do
      expect(dag.edges).to be_empty
    end

    it "returns empty sorted_steps" do
      expect(dag.sorted_steps).to be_empty
    end
  end

  # A4: Rename group -> batch_size in DagBuilder#fan_out
  describe "fan_out with batch_size", :schemas do
    it "reads batch_size from the Step class" do
      stub_const("Process", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1
        batch_size 100

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      pipeline_class = Class.new do
        include Turbofan::Pipeline

        pipeline_name "batch-size-test"

        pipeline do
          fan_out(process(trigger_input))
        end
      end

      dag = pipeline_class.turbofan_dag
      process_step = dag.steps.find { |s| s.name == :process }
      expect(process_step.fan_out?).to be true
    end

    it "raises when using the old group: keyword in fan_out" do
      stub_const("Process", Class.new {
        include Turbofan::Step

        execution :batch
        compute_environment :test_ce
        cpu 1
        batch_size 100

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      pipeline_class = Class.new do
        include Turbofan::Pipeline

        pipeline_name "group-error-test"

        pipeline do
          fan_out(process(trigger_input), group: 100)
        end
      end

      expect { pipeline_class.turbofan_dag }.to raise_error(ArgumentError, /use batch_size: instead/)
    end
  end

  describe "single-step DAG" do
    before do
      dag.add_step(:only)
      dag.add_edge(from: :trigger, to: :only)
    end

    it "sorts to just the one step" do
      expect(dag.sorted_steps.map(&:name)).to eq([:only])
    end
  end
end

RSpec.describe Turbofan::DagStep do
  describe "#name" do
    it "returns the step name" do
      step = described_class.new(:process)
      expect(step.name).to eq(:process)
    end
  end

  describe "#fan_out?" do
    it "is false by default" do
      step = described_class.new(:process)
      expect(step.fan_out?).to be false
    end

    it "is true when created with fan_out: true" do
      step = described_class.new(:process, fan_out: true)
      expect(step.fan_out?).to be true
    end
  end

  describe "#batch_size (moved to Step class)" do
    it "raises when passing batch_size: to DagStep" do
      expect { described_class.new(:process, batch_size: 100) }
        .to raise_error(ArgumentError, /batch_size has moved to the Step class/)
    end

    it "raises when using the old group: keyword" do
      expect { described_class.new(:foo, group: 5) }
        .to raise_error(ArgumentError, /use batch_size: instead/)
    end
  end

  describe "#fan_in" do
    it "defaults to true" do
      step = described_class.new(:process)
      expect(step.fan_in).to be true
    end

    it "can be set to false" do
      step = described_class.new(:process, fan_in: false)
      expect(step.fan_in).to be false
    end
  end
end
