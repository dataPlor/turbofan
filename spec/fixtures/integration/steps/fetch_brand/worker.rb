class PlacesRead
  include Turbofan::Postgres

  key :places_read
  secret "arn:aws:secretsmanager:us-east-1:123456789012:secret:myapp/DATABASE_URL-AbCdEf"
  database "places_read"
end

class FetchBrand
  include Turbofan::Step

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
      "nvme_used" => !context.nvme_path.nil?
    }
  end
end
