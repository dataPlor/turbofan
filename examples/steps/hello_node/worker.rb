class HelloNode
  include Turbofan::Step

  compute_environment ComputeEnvironments::TurbofanTempTest
  cpu 1
  ram 2

  input_schema "hello_polyglot.json"
  output_schema "hello_polyglot.json"
end
