require "spec_helper"

RSpec.describe "Step schema DSL", :schemas do # rubocop:disable RSpec/DescribeClass
  it "stores input_schema filename" do
    klass = Class.new do
      include Turbofan::Step

      input_schema "geocode_input.json"
      output_schema "geocode_output.json"
    end
    expect(klass.turbofan_input_schema_file).to eq("geocode_input.json")
  end

  it "stores output_schema filename" do
    klass = Class.new do
      include Turbofan::Step

      input_schema "geocode_input.json"
      output_schema "geocode_output.json"
    end
    expect(klass.turbofan_output_schema_file).to eq("geocode_output.json")
  end

  it "loads and caches parsed input schema" do
    klass = Class.new do
      include Turbofan::Step

      input_schema "geocode_input.json"
      output_schema "geocode_output.json"
    end
    schema = klass.turbofan_input_schema
    expect(schema).to be_a(Hash)
    expect(schema["properties"]).to have_key("query")
  end

  it "raises if schema file does not exist" do
    klass = Class.new do
      include Turbofan::Step

      input_schema "nonexistent.json"
      output_schema "geocode_output.json"
    end
    expect { klass.turbofan_input_schema }.to raise_error(Errno::ENOENT)
  end
end
