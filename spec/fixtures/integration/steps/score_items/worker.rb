class ScoreItems
  include Turbofan::Step

  execution :batch
  input_schema "passthrough.json"
  output_schema "passthrough.json"

  def call(inputs, context)
    items = inputs || []
    {
      "chunk_index" => context.array_index || 0,
      "size" => context.size,
      "scored" => items.map { |item| item.merge("score" => rand(100)) },
      "scored_count" => items.size
    }
  end
end
