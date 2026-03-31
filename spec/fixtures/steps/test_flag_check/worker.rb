class TestFlagCheck
  include Turbofan::Step

  compute_environment :test_ce
  execution :batch
  cpu 1
  ram 2
  uses :duckdb

  input_schema "test_flag_check_input.json"
  output_schema "test_flag_check_output.json"

  def call(inputs, context)
    # TODO: implement
  end
end
