require "spec_helper"

RSpec.describe "fan_out batch_size: parameter", :schemas do # rubocop:disable RSpec/DescribeClass
  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::GroupCe", klass)
    klass
  end

  describe "DagStep.batch_size accessor" do
    it "has a .batch_size accessor on DagStep" do
      step = Turbofan::DagStep.new(:process, fan_out: true, batch_size: 100)
      expect(step.batch_size).to eq(100)
    end

    it "defaults batch_size to nil" do
      step = Turbofan::DagStep.new(:process, fan_out: true)
      expect(step.batch_size).to be_nil
    end

    it "allows batch_size to be set via writer" do
      step = Turbofan::DagStep.new(:process, fan_out: true)
      step.batch_size = 50
      expect(step.batch_size).to eq(50)
    end
  end

  context "with pipeline step classes" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step

        input_schema "geocode_input.json"
        output_schema "geocode_output.json"
        compute_environment ce
        cpu 1
      end
    end

    let(:export_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step

        input_schema "geocode_output.json"
        output_schema "geocode_output.json"
        compute_environment ce
        cpu 1
      end
    end

    before do
      stub_const("BrandProcess", step_class)
    end

    describe "fan_out with batch_size: N" do
      it "stores batch_size=100 on the DagStep" do
        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_group"
          pipeline do
            fan_out(brand_process(trigger_input), batch_size: 100)
          end
        end

        dag = pipeline_class.turbofan_dag
        step = dag.steps.find { |s| s.name == :brand_process }
        expect(step.batch_size).to eq(100)
      end

      it "preserves fan_out? = true" do
        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_group"
          pipeline do
            fan_out(brand_process(trigger_input), batch_size: 50)
          end
        end

        dag = pipeline_class.turbofan_dag
        step = dag.steps.find { |s| s.name == :brand_process }
        expect(step.fan_out?).to be true
      end

      it "stores arbitrary positive integer batch_size values" do
        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_group"
          pipeline do
            fan_out(brand_process(trigger_input), batch_size: 1)
          end
        end

        dag = pipeline_class.turbofan_dag
        step = dag.steps.find { |s| s.name == :brand_process }
        expect(step.batch_size).to eq(1)
      end
    end

    describe "fan_out without batch_size: raises ArgumentError" do
      it "raises ArgumentError when batch_size: is not provided" do
        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_no_group"
          pipeline do
            fan_out(brand_process(trigger_input))
          end
        end

        expect { pipeline_class.turbofan_dag }.to raise_error(ArgumentError, /fan_out requires batch_size: parameter/)
      end
    end

    describe "concurrency: keyword is removed" do
      it "raises ArgumentError when concurrency: is passed to fan_out" do
        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_removed"
          pipeline do
            fan_out(brand_process(trigger_input), concurrency: 50)
          end
        end

        expect { pipeline_class.turbofan_dag }.to raise_error(ArgumentError, /concurrency.*use batch_size: instead/)
      end

      it "does not accept concurrency: as a DagStep keyword" do
        expect {
          Turbofan::DagStep.new(:process, fan_out: true, concurrency: 50)
        }.to raise_error(ArgumentError, /concurrency.*use batch_size: instead/)
      end

      it "does not accept concurrency: on Dag#add_step" do
        dag = Turbofan::Dag.new
        expect {
          dag.add_step(:process, fan_out: true, concurrency: 50)
        }.to raise_error(ArgumentError, /concurrency.*use batch_size: instead/)
      end
    end

    describe "batch_size value validation" do
      it "raises ArgumentError when batch_size is zero" do
        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_validation"
          pipeline do
            fan_out(brand_process(trigger_input), batch_size: 0)
          end
        end

        expect { pipeline_class.turbofan_dag }.to raise_error(ArgumentError, /batch_size must be a positive integer/)
      end

      it "raises ArgumentError when batch_size is negative" do
        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_validation"
          pipeline do
            fan_out(brand_process(trigger_input), batch_size: -5)
          end
        end

        expect { pipeline_class.turbofan_dag }.to raise_error(ArgumentError, /batch_size must be a positive integer/)
      end

      it "raises ArgumentError when batch_size is not an integer" do
        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_validation"
          pipeline do
            fan_out(brand_process(trigger_input), batch_size: 10.5)
          end
        end

        expect { pipeline_class.turbofan_dag }.to raise_error(ArgumentError, /batch_size must be a positive integer/)
      end
    end

    describe "chaining with batch_size:" do
      it "returns the original proxy for chaining" do
        stub_const("S3Export", export_class)

        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_chain"
          pipeline do
            result = fan_out(brand_process(trigger_input), batch_size: 100)
            s3_export(result)
          end
        end

        dag = pipeline_class.turbofan_dag
        expect(dag.steps.map(&:name)).to eq([:brand_process, :s3_export])
      end
    end
  end

  describe "Dag#add_step with batch_size:" do
    it "accepts batch_size: keyword" do
      dag = Turbofan::Dag.new
      step = dag.add_step(:process, fan_out: true, batch_size: 100)
      expect(step.batch_size).to eq(100)
      expect(step.fan_out?).to be true
    end

    it "defaults batch_size to nil when not provided" do
      dag = Turbofan::Dag.new
      step = dag.add_step(:process, fan_out: true)
      expect(step.batch_size).to be_nil
    end

    it "preserves batch_size through sorted_steps" do
      dag = Turbofan::Dag.new
      dag.add_step(:process, fan_out: true, batch_size: 75)
      dag.add_edge(from: :trigger, to: :process)

      sorted = dag.sorted_steps
      step = sorted.first
      expect(step.batch_size).to eq(75)
      expect(step.fan_out?).to be true
    end
  end
end
