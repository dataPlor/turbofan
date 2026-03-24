require "spec_helper"

RSpec.describe "TaskFlow DSL", :schemas do # rubocop:disable RSpec/DescribeClass
  it "allows calling a step as a method in the pipeline block" do
    step_class = Class.new do
      include Turbofan::Step

      input_schema "geocode_input.json"
      output_schema "geocode_output.json"
      compute_environment :test_ce
      cpu 1
    end
    stub_const("BrandGeocode", step_class)

    pipeline_class = Class.new do
      include Turbofan::Pipeline

      pipeline_name "test"
      pipeline do
        brand_geocode(trigger_input)
      end
    end

    dag = pipeline_class.turbofan_dag
    expect(dag.steps.map(&:name)).to eq([:brand_geocode])
  end

  it "returns a DagProxy with the step's output schema" do
    step_class = Class.new do
      include Turbofan::Step

      input_schema "geocode_input.json"
      output_schema "geocode_output.json"
      compute_environment :test_ce
      cpu 1
    end
    stub_const("BrandGeocode", step_class)

    captured_proxy = nil
    pipeline_class = Class.new do
      include Turbofan::Pipeline

      pipeline_name "test"
      pipeline do
        captured_proxy = brand_geocode(trigger_input)
      end
    end

    pipeline_class.turbofan_dag
    expect(captured_proxy.schema).to be_a(Hash)
    expect(captured_proxy.schema["properties"]).to have_key("lat")
  end

  it "validates schema compatibility at DAG edges" do
    geocode = Class.new do
      include Turbofan::Step

      input_schema "geocode_input.json"
      output_schema "geocode_output.json"
      compute_environment :test_ce
      cpu 1
    end
    stub_const("BrandGeocode", geocode)

    # Step that expects a property not in geocode's output
    validate = Class.new do
      include Turbofan::Step

      input_schema "incompatible_input.json"
      output_schema "geocode_output.json"
      compute_environment :test_ce
      cpu 1
    end
    stub_const("BrandValidate", validate)

    pipeline_class = Class.new do
      include Turbofan::Pipeline

      pipeline_name "test"
      pipeline do
        result = brand_geocode(trigger_input)
        brand_validate(result)
      end
    end

    expect { pipeline_class.turbofan_dag }.to raise_error(
      Turbofan::SchemaIncompatibleError
    )
  end

  it "raises NoMethodError for unregistered steps" do
    pipeline_class = Class.new do
      include Turbofan::Pipeline

      pipeline_name "test"
      pipeline do
        nonexistent_step(trigger_input)
      end
    end

    expect { pipeline_class.turbofan_dag }.to raise_error(NoMethodError)
  end
end
