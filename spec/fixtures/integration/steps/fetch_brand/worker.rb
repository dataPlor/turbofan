# frozen_string_literal: true

require "json"

INTEGRATION_CONFIG = JSON.parse(File.read(File.join(__dir__, "integration_config.json")))

class PlacesRead
  include Turbofan::Postgres

  key :places_read
  secret INTEGRATION_CONFIG["secret_arn"]
  database "places_read"
end

class FetchBrand
  include Turbofan::Step

  execution :batch
  uses :places_read
  input_schema "passthrough.json"
  output_schema "passthrough.json"

  def call(inputs, context)
    result = context.duckdb.query(
      "SELECT name FROM places_read.public.brands WHERE key = ?",
      inputs.first["key"]
    )
    {
      "brand_name" => result.first[0],
      "key" => inputs.first["key"],
      "source" => "postgres",
      "storage_available" => !context.storage_path.nil?
    }
  end
end
