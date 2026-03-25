require "json"

INTEGRATION_CONFIG = JSON.parse(File.read(File.join(__dir__, "integration_config.json")))

class Aggregate
  include Turbofan::Step

  input_schema "passthrough.json"
  output_schema "passthrough.json"

  def call(inputs, context)
    chunks = inputs
    total = chunks.sum { |c| c["scored_count"] || 0 }

    # Write summary to external S3
    summary = {"total_scored" => total, "wrote_to_external_s3" => true}
    context.s3.put_object(
      bucket: INTEGRATION_CONFIG["external_bucket"],
      key: "turbofan-test/#{context.execution_id}/summary.json",
      body: JSON.generate(summary)
    )

    {
      "total_scored" => total,
      "chunks_received" => chunks.size,
      "source" => "fan_in"
    }
  end
end
