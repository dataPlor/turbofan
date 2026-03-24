require "spec_helper"

RSpec.describe Turbofan::Check::DagCheck, :schemas do
  describe ".run" do
    context "with a valid DAG" do
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

          pipeline_name "valid-pipeline"

          pipeline do
            results = extract(trigger_input)
            load(results)
          end
        end
      end

      it "passes for a valid linear DAG" do
        result = described_class.run(pipeline: pipeline_class)
        expect(result.passed?).to be true
      end

      it "has no errors" do
        result = described_class.run(pipeline: pipeline_class)
        expect(result.errors).to be_empty
      end

      it "has no warnings" do
        result = described_class.run(pipeline: pipeline_class)
        expect(result.warnings).to be_empty
      end
    end

    context "with a single-step pipeline" do
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

          pipeline_name "single-step"

          pipeline do
            only(trigger_input)
          end
        end
      end

      it "passes for a single-step pipeline" do
        result = described_class.run(pipeline: pipeline_class)
        expect(result.passed?).to be true
      end
    end

    context "with a fan-out pipeline" do
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

          pipeline_name "fan-out-check"

          pipeline do
            files = discover(trigger_input)
            results = fan_out(process(files), batch_size: 100)
            aggregate(results)
          end
        end
      end

      it "passes for a valid fan-out pipeline" do
        result = described_class.run(pipeline: pipeline_class)
        expect(result.passed?).to be true
      end
    end

    context "with a cyclic dependency" do
      let(:pipeline_class) do
        dag = Turbofan::Dag.new
        dag.add_step(:step_a)
        dag.add_step(:step_b)
        dag.add_step(:step_c)
        dag.add_edge(from: :trigger, to: :step_a)
        dag.add_edge(from: :step_a, to: :step_b)
        dag.add_edge(from: :step_b, to: :step_c)
        dag.add_edge(from: :step_c, to: :step_a)

        klass = Class.new do
          include Turbofan::Pipeline

          pipeline_name "cyclic-pipeline"
          pipeline {}
        end
        allow(klass).to receive(:turbofan_dag).and_return(dag)
        klass
      end

      it "fails when the DAG has a cycle" do
        result = described_class.run(pipeline: pipeline_class)
        expect(result.passed?).to be false
      end

      it "reports a cyclic dependency error" do
        result = described_class.run(pipeline: pipeline_class)
        expect(result.errors.any? { |e| e.match?(/cycl/i) }).to be true
      end
    end

    context "with an empty pipeline" do
      let(:pipeline_class) do
        Class.new do
          include Turbofan::Pipeline

          pipeline_name "empty-pipeline"
        end
      end

      it "fails when the pipeline has no steps" do
        result = described_class.run(pipeline: pipeline_class)
        expect(result.passed?).to be false
      end

      it "reports an error about empty pipeline" do
        result = described_class.run(pipeline: pipeline_class)
        expect(result.errors.any? { |e| e.match?(/empty|no steps|no pipeline/i) }).to be true
      end
    end
  end
end
