class DetectLocations
  include Turbofan::Step

  family :c
  cpu 2
  uses :duckdb

  input_schema "detect_locations_input.json"
  output_schema "detect_locations_output.json"

  def call(inputs, context)
    # TODO: implement
  end
end
