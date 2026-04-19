# frozen_string_literal: true

require "json"

INTEGRATION_CONFIG = JSON.parse(File.read(File.join(__dir__, "integration_config.json")))

class ReadVisits
  include Turbofan::Step

  execution :batch
  input_schema "passthrough.json"
  output_schema "passthrough.json"

  def call(inputs, context)
    require "csv"
    require "zlib"
    obj = context.s3.get_object(
      bucket: INTEGRATION_CONFIG["external_bucket"],
      key: "analytics_data/test/sample_data.csv.gz"
    )
    csv_data = Zlib::GzipReader.new(obj.body).read
    rows = CSV.parse(csv_data, headers: true)
    {
      "brand_name" => inputs.first["brand_name"],
      "row_count" => rows.size,
      "columns" => rows.headers,
      "source" => "s3"
    }
  end
end
