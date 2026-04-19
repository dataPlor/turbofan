# frozen_string_literal: true

require "spec_helper"

RSpec.describe "batch_size on Step class", :schemas do # rubocop:disable RSpec/DescribeClass
  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::GroupCe", klass)
    klass
  end

  describe "Step.batch_size DSL" do
    it "sets turbofan_batch_size" do
      klass = Class.new do
        include Turbofan::Step
        execution :batch
        batch_size 100
      end
      expect(klass.turbofan_batch_size).to eq(100)
    end

    it "defaults to 1" do
      klass = Class.new { include Turbofan::Step; execution :batch }
      expect(klass.turbofan_batch_size).to eq(1)
    end

    it "raises ArgumentError when batch_size is zero" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          batch_size 0
        end
      }.to raise_error(ArgumentError, /batch_size must be a positive integer/)
    end

    it "raises ArgumentError when batch_size is negative" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          batch_size(-5)
        end
      }.to raise_error(ArgumentError, /batch_size must be a positive integer/)
    end

    it "raises ArgumentError when batch_size is not an integer" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          batch_size 10.5
        end
      }.to raise_error(ArgumentError, /batch_size must be a positive integer/)
    end
  end

  describe "Step.size with batch_size:" do
    it "stores per-size batch_size" do
      klass = Class.new do
        include Turbofan::Step
        execution :batch
        size :s, cpu: 1, ram: 2, batch_size: 100
      end
      expect(klass.turbofan_sizes[:s][:batch_size]).to eq(100)
    end

    it "defaults per-size batch_size to nil" do
      klass = Class.new do
        include Turbofan::Step
        execution :batch
        size :s, cpu: 1, ram: 2
      end
      expect(klass.turbofan_sizes[:s][:batch_size]).to be_nil
    end

    it "raises ArgumentError when per-size batch_size is zero" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          size :s, cpu: 1, ram: 2, batch_size: 0
        end
      }.to raise_error(ArgumentError, /batch_size must be a positive integer/)
    end

    it "raises ArgumentError when per-size batch_size is negative" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          size :s, cpu: 1, ram: 2, batch_size: -1
        end
      }.to raise_error(ArgumentError, /batch_size must be a positive integer/)
    end

    it "raises ArgumentError when per-size batch_size is not an integer" do
      expect {
        Class.new do
          include Turbofan::Step
          execution :batch
          size :s, cpu: 1, ram: 2, batch_size: 10.5
        end
      }.to raise_error(ArgumentError, /batch_size must be a positive integer/)
    end
  end

  describe "Step.turbofan_batch_size_for" do
    it "returns per-size batch_size when set" do
      klass = Class.new do
        include Turbofan::Step
        execution :batch
        batch_size 10
        size :s, cpu: 1, ram: 2, batch_size: 100
      end
      expect(klass.turbofan_batch_size_for(:s)).to eq(100)
    end

    it "falls back to step default when per-size not set" do
      klass = Class.new do
        include Turbofan::Step
        execution :batch
        batch_size 10
        size :s, cpu: 1, ram: 2
      end
      expect(klass.turbofan_batch_size_for(:s)).to eq(10)
    end

    it "falls back to default of 1 when neither per-size nor explicit default set" do
      klass = Class.new do
        include Turbofan::Step
        execution :batch
        size :s, cpu: 1, ram: 2
      end
      expect(klass.turbofan_batch_size_for(:s)).to eq(1)
    end
  end

  context "with pipeline step classes" do
    let(:step_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :group_ce
        cpu 1
        batch_size 100
        input_schema "geocode_input.json"
        output_schema "geocode_output.json"
      end
    end

    let(:export_class) do
      ce = ce_class
      Class.new do
        include Turbofan::Step

        execution :batch
        compute_environment :group_ce
        cpu 1
        input_schema "geocode_output.json"
        output_schema "geocode_output.json"
      end
    end

    before do
      stub_const("BrandProcess", step_class)
    end

    describe "fan_out with default batch_size (no explicit declaration)" do
      it "uses the default batch_size of 1" do
        ce = ce_class
        no_bs_step = Class.new do
          include Turbofan::Step
          execution :batch
          compute_environment :group_ce
          cpu 1
          input_schema "geocode_input.json"
          output_schema "geocode_output.json"
        end
        stub_const("NoBsProcess", no_bs_step)

        pipeline_class = Class.new do
          include Turbofan::Pipeline
          pipeline_name "default_batch_size"
          pipeline do
            fan_out(no_bs_process(trigger_input))
          end
        end

        dag = pipeline_class.turbofan_dag
        step = dag.steps.find { |s| s.name == :no_bs_process }
        expect(step.fan_out?).to be true
        expect(no_bs_step.turbofan_batch_size).to eq(1)
      end
    end

    describe "fan_out without batch_size: kwarg" do
      it "marks step as fan_out" do
        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_group"
          pipeline do
            fan_out(brand_process(trigger_input))
          end
        end

        dag = pipeline_class.turbofan_dag
        step = dag.steps.find { |s| s.name == :brand_process }
        expect(step.fan_out?).to be true
      end

      it "reads batch_size from the step class" do
        expect(step_class.turbofan_batch_size).to eq(100)
      end
    end

    describe "fan_out with batch_size: raises migration error" do
      it "raises ArgumentError with migration message" do
        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_migration"
          pipeline do
            fan_out(brand_process(trigger_input), batch_size: 50)
          end
        end

        expect { pipeline_class.turbofan_dag }.to raise_error(ArgumentError, /batch_size has moved to the Step class/)
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

    describe "chaining with fan_out" do
      it "returns the original proxy for chaining" do
        stub_const("S3Export", export_class)

        pipeline_class = Class.new do
          include Turbofan::Pipeline

          pipeline_name "test_chain"
          pipeline do
            result = fan_out(brand_process(trigger_input))
            s3_export(result)
          end
        end

        dag = pipeline_class.turbofan_dag
        expect(dag.steps.map(&:name)).to eq([:brand_process, :s3_export])
      end
    end
  end

  describe "DagStep rejects batch_size:" do
    it "raises ArgumentError when batch_size: is passed" do
      expect {
        Turbofan::DagStep.new(:process, fan_out: true, batch_size: 100)
      }.to raise_error(ArgumentError, /batch_size has moved to the Step class/)
    end

    it "raises ArgumentError on Dag#add_step with batch_size:" do
      dag = Turbofan::Dag.new
      expect {
        dag.add_step(:process, fan_out: true, batch_size: 100)
      }.to raise_error(ArgumentError, /batch_size has moved to the Step class/)
    end
  end
end
