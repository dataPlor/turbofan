# frozen_string_literal: true

require "spec_helper"

RSpec.describe "Pipeline composition", :schemas do # rubocop:disable RSpec/DescribeClass
  it "inlines a sub-pipeline's steps into the parent DAG" do
    geocode = Class.new do
      include Turbofan::Step
      runs_on :batch
      input_schema "geocode_input.json"
      output_schema "geocode_output.json"
      compute_environment :test_ce
      cpu 1
    end
    stub_const("BrandGeocode", geocode)

    validate = Class.new do
      include Turbofan::Step
      runs_on :batch
      input_schema "geocode_output.json"
      output_schema "geocode_output.json"
      compute_environment :test_ce
      cpu 1
    end
    stub_const("BrandValidate", validate)

    sub_pipeline = Class.new do
      include Turbofan::Pipeline

      pipeline_name "sub"
      pipeline do
        brand_validate(brand_geocode(trigger_input))
      end
    end
    stub_const("BrandEnrichment", sub_pipeline)

    export = Class.new do
      include Turbofan::Step
      runs_on :batch
      input_schema "geocode_output.json"
      output_schema "geocode_output.json"
      compute_environment :test_ce
      cpu 1
    end
    stub_const("S3Export", export)

    parent = Class.new do
      include Turbofan::Pipeline

      pipeline_name "parent"
      pipeline do
        result = brand_enrichment(trigger_input)
        s3_export(result)
      end
    end

    dag = parent.turbofan_dag
    expect(dag.steps.map(&:name)).to eq([:brand_geocode, :brand_validate, :s3_export])
  end

  it "supports 3-level nested composition" do
    step_a = Class.new do
      include Turbofan::Step
      runs_on :batch
      input_schema "passthrough.json"
      output_schema "passthrough.json"
      compute_environment :test_ce
      cpu 1
    end
    stub_const("StepA", step_a)

    step_b = Class.new do
      include Turbofan::Step
      runs_on :batch
      input_schema "passthrough.json"
      output_schema "passthrough.json"
      compute_environment :test_ce
      cpu 1
    end
    stub_const("StepB", step_b)

    step_c = Class.new do
      include Turbofan::Step
      runs_on :batch
      input_schema "passthrough.json"
      output_schema "passthrough.json"
      compute_environment :test_ce
      cpu 1
    end
    stub_const("StepC", step_c)

    inner = Class.new do
      include Turbofan::Pipeline

      pipeline_name "inner"
      pipeline { step_b(step_a(trigger_input)) }
    end
    stub_const("InnerPipeline", inner)

    middle = Class.new do
      include Turbofan::Pipeline

      pipeline_name "middle"
      pipeline { step_c(inner_pipeline(trigger_input)) }
    end
    stub_const("MiddlePipeline", middle)

    outer = Class.new do
      include Turbofan::Pipeline

      pipeline_name "outer"
      pipeline { middle_pipeline(trigger_input) }
    end

    dag = outer.turbofan_dag
    expect(dag.steps.map(&:name)).to eq([:step_a, :step_b, :step_c])
    expect(dag.edges).to include(
      {from: :trigger, to: :step_a},
      {from: :step_a, to: :step_b},
      {from: :step_b, to: :step_c}
    )
  end

  it "raises on step name collision" do
    geocode = Class.new do
      include Turbofan::Step
      runs_on :batch
      input_schema "geocode_input.json"
      output_schema "geocode_output.json"
      compute_environment :test_ce
      cpu 1
    end
    stub_const("BrandGeocode", geocode)

    sub = Class.new do
      include Turbofan::Pipeline

      pipeline_name "sub"
      pipeline do
        brand_geocode(trigger_input)
      end
    end
    stub_const("SubPipeline", sub)

    parent = Class.new do
      include Turbofan::Pipeline

      pipeline_name "parent"
      pipeline do
        brand_geocode(trigger_input)
        sub_pipeline(trigger_input)  # collision: brand_geocode already added
      end
    end

    expect { parent.turbofan_dag }.to raise_error(ArgumentError, /duplicate/)
  end
end
