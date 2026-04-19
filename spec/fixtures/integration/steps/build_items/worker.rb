# frozen_string_literal: true

class BuildItems
  include Turbofan::Step

  runs_on :batch
  input_schema "passthrough.json"
  output_schema "passthrough.json"

  def call(inputs, context)
    brand = inputs.first["brand_name"]

    context.logger.info("parallel_join_complete", brand: brand, input_count: inputs.size)
    context.metrics.emit("ItemsBuilt", 9)

    sizes = %w[s m l]
    {
      "items" => (0..8).map { |i|
        {"id" => i, "brand_name" => brand, "__turbofan_size" => sizes[i % 3]}
      },
      "item_count" => 9
    }
  end
end
