require "spec_helper"

RSpec.describe Turbofan::Pipeline, :schemas do
  describe "basic two-step pipeline" do
    let(:pipeline_class) do
      stub_const("Process", Class.new {
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

        pipeline_name "test-pipeline"
        pipeline do
          results = process(trigger_input)
          load(results)
        end
      end
    end

    it "stores the pipeline name" do
      expect(pipeline_class.turbofan_name).to eq("test-pipeline")
    end

    it "builds a DAG with two steps" do
      dag = pipeline_class.turbofan_dag
      expect(dag.steps.map(&:name)).to eq(%i[process load])
    end

    it "records edges from trigger to first step" do
      dag = pipeline_class.turbofan_dag
      expect(dag.edges).to include(from: :trigger, to: :process)
    end

    it "records edges between sequential steps" do
      dag = pipeline_class.turbofan_dag
      expect(dag.edges).to include(from: :process, to: :load)
    end

    it "marks both steps as non-fan-out" do
      dag = pipeline_class.turbofan_dag
      dag.steps.each do |step|
        expect(step.fan_out?).to be false
      end
    end
  end

  describe "pipeline do |input| block parameter syntax" do
    let(:pipeline_class) do
      stub_const("Extract", Class.new {
        include Turbofan::Step

        compute_environment TestCe
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      stub_const("Transform", Class.new {
        include Turbofan::Step

        compute_environment TestCe
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "input-param-pipeline"

        pipeline do |input|
          results = extract(input)
          transform(results)
        end
      end
    end

    it "builds a DAG with the same structure as trigger_input style" do
      dag = pipeline_class.turbofan_dag
      expect(dag.steps.map(&:name)).to eq(%i[extract transform])
    end

    it "wires trigger to first step" do
      dag = pipeline_class.turbofan_dag
      expect(dag.edges).to include(from: :trigger, to: :extract)
    end

    it "wires sequential steps" do
      dag = pipeline_class.turbofan_dag
      expect(dag.edges).to include(from: :extract, to: :transform)
    end
  end

  describe "three-step linear pipeline" do
    let(:pipeline_class) do
      stub_const("Extract", Class.new {
        include Turbofan::Step

        compute_environment TestCe
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      stub_const("Transform", Class.new {
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

        pipeline_name "three-step"

        pipeline do
          a = extract(trigger_input)
          b = transform(a)
          load(b)
        end
      end
    end

    it "builds a DAG with three steps in order" do
      dag = pipeline_class.turbofan_dag
      expect(dag.steps.map(&:name)).to eq(%i[extract transform load])
    end

    it "records edges for the full chain" do
      dag = pipeline_class.turbofan_dag
      expect(dag.edges).to include(from: :trigger, to: :extract)
      expect(dag.edges).to include(from: :extract, to: :transform)
      expect(dag.edges).to include(from: :transform, to: :load)
    end
  end

  describe "pipeline with fan_out" do
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

        pipeline_name "fan-pipeline"

        pipeline do
          files = discover(trigger_input)
          results = fan_out(process(files), batch_size: 100)
          aggregate(results)
        end
      end
    end

    it "builds a DAG with three steps" do
      dag = pipeline_class.turbofan_dag
      expect(dag.steps.map(&:name)).to eq(%i[discover process aggregate])
    end

    it "marks the fan_out step" do
      dag = pipeline_class.turbofan_dag
      process_step = dag.steps.find { |s| s.name == :process }
      expect(process_step.fan_out?).to be true
    end

    it "stores group on the fan_out step" do
      dag = pipeline_class.turbofan_dag
      process_step = dag.steps.find { |s| s.name == :process }
      expect(process_step.batch_size).to eq(100)
    end

    it "marks non-fan-out steps as regular steps" do
      dag = pipeline_class.turbofan_dag
      discover = dag.steps.find { |s| s.name == :discover }
      aggregate = dag.steps.find { |s| s.name == :aggregate }
      expect(discover.fan_out?).to be false
      expect(aggregate.fan_out?).to be false
    end

    it "records correct edges including fan_out" do
      dag = pipeline_class.turbofan_dag
      expect(dag.edges).to include(from: :trigger, to: :discover)
      expect(dag.edges).to include(from: :discover, to: :process)
      expect(dag.edges).to include(from: :process, to: :aggregate)
    end
  end

  describe "fan_out without explicit group" do
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

        pipeline_name "no-group"

        pipeline do
          fan_out(process(trigger_input))
        end
      end
    end

    it "raises ArgumentError when batch_size is not provided" do
      expect { pipeline_class.turbofan_dag }.to raise_error(ArgumentError, /fan_out requires batch_size: parameter/)
    end
  end

  describe "metric declarations" do
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

        pipeline_name "metrics-pipeline"

        metric "rows_processed", stat: :sum, display: :line, unit: "rows"
        metric "processing_speed", stat: :average, display: :line, unit: "rows/sec"
        metric "files_generated", stat: :sum, display: :number
        metric "error_rate", stat: :average, display: :line, unit: "%", step: :generate_csvs

        pipeline do
          process(trigger_input)
        end
      end
    end

    it "stores all metric declarations" do
      expect(pipeline_class.turbofan_metrics.size).to eq(4)
    end

    it "stores metric with all options" do
      metric = pipeline_class.turbofan_metrics.first
      expect(metric).to eq(
        name: "rows_processed",
        stat: :sum,
        display: :line,
        unit: "rows",
        step: nil
      )
    end

    it "stores metric scoped to a specific step" do
      metric = pipeline_class.turbofan_metrics.last
      expect(metric[:step]).to eq(:generate_csvs)
    end

    it "stores metric without unit" do
      metric = pipeline_class.turbofan_metrics.find { |m| m[:name] == "files_generated" }
      expect(metric[:unit]).to be_nil
      expect(metric[:display]).to eq(:number)
    end
  end

  describe "metric defaults" do
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

        pipeline_name "default-metrics"

        metric "simple_metric"

        pipeline do
          process(trigger_input)
        end
      end
    end

    it "defaults stat to :sum" do
      expect(pipeline_class.turbofan_metrics.first[:stat]).to eq(:sum)
    end

    it "defaults display to :line" do
      expect(pipeline_class.turbofan_metrics.first[:display]).to eq(:line)
    end

    it "defaults unit to nil" do
      expect(pipeline_class.turbofan_metrics.first[:unit]).to be_nil
    end

    it "defaults step to nil (all steps)" do
      expect(pipeline_class.turbofan_metrics.first[:step]).to be_nil
    end
  end

  describe "pipeline defaults" do
    let(:pipeline_class) do
      stub_const("OnlyStep", Class.new {
        include Turbofan::Step

        compute_environment TestCe
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "defaults-pipeline"

        pipeline do
          only_step(trigger_input)
        end
      end
    end

    it "defaults metrics to an empty array" do
      expect(pipeline_class.turbofan_metrics).to eq([])
    end
  end

  describe "class isolation" do
    let(:pipeline_a) do
      stub_const("StepA", Class.new {
        include Turbofan::Step

        compute_environment TestCe
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "pipeline-a"
        metric "custom_metric"

        pipeline do
          step_a(trigger_input)
        end
      end
    end

    let(:pipeline_b) do
      stub_const("StepB", Class.new {
        include Turbofan::Step

        compute_environment TestCe
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "pipeline-b"

        pipeline do
          step_b(trigger_input)
        end
      end
    end

    it "does not leak state between pipeline classes" do
      # Force both to load
      pipeline_a
      pipeline_b

      expect(pipeline_a.turbofan_name).to eq("pipeline-a")
      expect(pipeline_b.turbofan_name).to eq("pipeline-b")

      expect(pipeline_a.turbofan_metrics.size).to eq(1)
      expect(pipeline_b.turbofan_metrics).to be_empty
    end
  end

  describe "validation: missing name" do
    let(:pipeline_class) do
      stub_const("Only", Class.new {
        include Turbofan::Step

        compute_environment TestCe
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline do
          only(trigger_input)
        end
      end
    end

    it "returns nil for turbofan_name when name is not declared" do
      expect(pipeline_class.turbofan_name).to be_nil
    end
  end

  describe "validation: missing pipeline block" do
    let(:pipeline_class) do
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "no-block"
      end
    end

    it "raises ArgumentError when accessing turbofan_dag without a pipeline block" do
      expect { pipeline_class.turbofan_dag }.to raise_error(ArgumentError, /no pipeline block defined/)
    end
  end

  describe "validation: duplicate step names" do
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

        pipeline_name "dup-steps"

        pipeline do
          process(trigger_input)
          process(trigger_input)
        end
      end
    end

    it "raises ArgumentError for duplicate step names in DAG" do
      expect { pipeline_class.turbofan_dag }.to raise_error(ArgumentError, /duplicate step name :process/)
    end
  end

  describe "single-step pipeline" do
    let(:pipeline_class) do
      stub_const("Only", Class.new {
        include Turbofan::Step

        compute_environment TestCe
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "single"

        pipeline do
          only(trigger_input)
        end
      end
    end

    it "builds a DAG with one step" do
      dag = pipeline_class.turbofan_dag
      expect(dag.steps.size).to eq(1)
      expect(dag.steps.first.name).to eq(:only)
    end

    it "has one edge from trigger to the step" do
      dag = pipeline_class.turbofan_dag
      expect(dag.edges).to eq([{from: :trigger, to: :only}])
    end
  end

  # A10: Rename Pipeline name -> pipeline_name
  describe "pipeline_name DSL" do
    let(:pipeline_class) do
      stub_const("OnlyStep", Class.new {
        include Turbofan::Step

        compute_environment TestCe
        cpu 1

        input_schema "passthrough.json"
        output_schema "passthrough.json"
      })
      Class.new do
        include Turbofan::Pipeline

        pipeline_name "my-pipeline"

        pipeline do
          only_step(trigger_input)
        end
      end
    end

    it "sets turbofan_name via pipeline_name" do
      expect(pipeline_class.turbofan_name).to eq("my-pipeline")
    end

    it "does not override Module#name — .name returns the Ruby class name" do
      stub_const("MyPipeline", pipeline_class)
      expect(MyPipeline.name).to eq("MyPipeline")
    end

    it "returns the pipeline name via turbofan_name, not .name" do
      stub_const("MyPipeline", pipeline_class)
      expect(MyPipeline.turbofan_name).to eq("my-pipeline")
    end
  end

  describe "compute_environment DSL" do
    let(:ce_class) do
      klass = Class.new { include Turbofan::ComputeEnvironment }
      stub_const("ComputeEnvironments::TestCe", klass)
      klass
    end

    it "accepts a class that includes ComputeEnvironment and stores it" do
      ce_klass = ce_class
      pipeline = Class.new do
        include Turbofan::Pipeline

        pipeline_name "ce-pipeline"
        compute_environment ce_klass
      end
      expect(pipeline.turbofan_compute_environment).to eq(ce_class)
    end

    it "raises ArgumentError if class does not include ComputeEnvironment" do
      bad_class = Class.new
      expect {
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "bad-ce"
          compute_environment bad_class
        end
      }.to raise_error(ArgumentError, /must include Turbofan::ComputeEnvironment/)
    end

    it "defaults to nil" do
      pipeline = Class.new do
        include Turbofan::Pipeline

        pipeline_name "no-ce"
      end
      expect(pipeline.turbofan_compute_environment).to be_nil
    end
  end
end
