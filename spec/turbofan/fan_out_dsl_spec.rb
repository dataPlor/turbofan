require "spec_helper"

RSpec.describe "fan_out DSL", :schemas do # rubocop:disable RSpec/DescribeClass
  let(:ce_class) do
    klass = Class.new { include Turbofan::ComputeEnvironment }
    stub_const("ComputeEnvironments::FanOutCe", klass)
    klass
  end

  it "marks a step as fan_out" do
    ce = ce_class
    step_class = Class.new do
      include Turbofan::Step

      input_schema "geocode_input.json"
      output_schema "geocode_output.json"
      compute_environment :fan_out_ce
      cpu 1
    end
    stub_const("BrandProcess", step_class)

    pipeline_class = Class.new do
      include Turbofan::Pipeline

      pipeline_name "test"
      pipeline do
        fan_out(brand_process(trigger_input), batch_size: 50)
      end
    end

    dag = pipeline_class.turbofan_dag
    step = dag.steps.find { |s| s.name == :brand_process }
    expect(step.fan_out?).to be true
    expect(step.batch_size).to eq(50)
  end

  it "returns the original proxy for chaining" do
    ce = ce_class
    step_class = Class.new do
      include Turbofan::Step

      input_schema "geocode_input.json"
      output_schema "geocode_output.json"
      compute_environment :fan_out_ce
      cpu 1
    end
    stub_const("BrandProcess", step_class)

    export_class = Class.new do
      include Turbofan::Step

      input_schema "geocode_output.json"
      output_schema "geocode_output.json"
      compute_environment :fan_out_ce
      cpu 1
    end
    stub_const("S3Export", export_class)

    pipeline_class = Class.new do
      include Turbofan::Pipeline

      pipeline_name "test"
      pipeline do
        result = fan_out(brand_process(trigger_input), batch_size: 50)
        s3_export(result)
      end
    end

    dag = pipeline_class.turbofan_dag
    expect(dag.steps.map(&:name)).to eq([:brand_process, :s3_export])
  end

  it "raises ArgumentError when fan_out is called without batch_size:" do
    ce = ce_class
    step_class = Class.new do
      include Turbofan::Step

      input_schema "geocode_input.json"
      output_schema "geocode_output.json"
      compute_environment :fan_out_ce
      cpu 1
    end
    stub_const("BrandProcess", step_class)

    pipeline_class = Class.new do
      include Turbofan::Pipeline

      pipeline_name "test-no-group"
      pipeline do
        fan_out(brand_process(trigger_input))
      end
    end

    expect { pipeline_class.turbofan_dag }.to raise_error(ArgumentError, /fan_out requires batch_size: parameter/)
  end
end
