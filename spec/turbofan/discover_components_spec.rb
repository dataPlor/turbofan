require "spec_helper"

RSpec.describe "Turbofan.discover_components" do # rubocop:disable RSpec/DescribeClass, RSpec/MultipleDescribes
  it "discovers loaded Step classes by snake_case name" do
    step_class = Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      cpu 1
    end
    stub_const("BrandGeocode", step_class)

    components = Turbofan.discover_components
    expect(components[:steps][:brand_geocode]).to eq(step_class)
  end

  it "discovers loaded Pipeline classes by snake_case name" do
    pipeline_class = Class.new do
      include Turbofan::Pipeline

      pipeline_name "test"
    end
    stub_const("BrandEnrichment", pipeline_class)

    components = Turbofan.discover_components
    expect(components[:pipelines][:brand_enrichment]).to eq(pipeline_class)
  end

  it "skips anonymous classes (no name)" do
    Class.new { include Turbofan::Step }
    components = Turbofan.discover_components
    expect(components[:steps].values).not_to include(nil)
  end

  it "only returns the live constant, not stale ObjectSpace references (H-1)" do
    old_class = Class.new {
      include Turbofan::Step

      compute_environment :test_ce
      cpu 1
    }

    stub_const("Ephemeral", old_class)
    # stub_const will remove the constant after this example,
    # but the class object remains in ObjectSpace.
    # discover_components must use const_get to verify liveness.
    components = Turbofan.discover_components
    expect(components[:steps][:ephemeral]).to eq(old_class)
  end
end

RSpec.describe "Turbofan.snake_case" do # rubocop:disable RSpec/DescribeClass
  it "converts CamelCase to snake_case symbol" do
    expect(Turbofan.snake_case("BrandGeocode")).to eq(:brand_geocode)
    expect(Turbofan.snake_case("S3Export")).to eq(:s3_export)
    expect(Turbofan.snake_case("POINormalize")).to eq(:poi_normalize)
  end

  it "includes all namespace segments joined with underscores" do
    expect(Turbofan.snake_case("Turbofan::Step")).to eq(:turbofan_step)
    expect(Turbofan.snake_case("My::BrandGeocode")).to eq(:my_brand_geocode)
  end
end
