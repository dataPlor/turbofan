class HelloNode
  include Turbofan::Step

  compute_environment ComputeEnvironments::TurbofanTempTest
  runs_on :batch
  cpu 1
  ram 2
  batch_size 1

  input_schema "hello_polyglot.json"
  output_schema "hello_polyglot.json"
end
