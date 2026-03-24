class HelloRuby
  include Turbofan::Step

  if defined?(ComputeEnvironments::TurbofanTempTest)
    compute_environment ComputeEnvironments::TurbofanTempTest
    cpu 1
    ram 2
  end

  input_schema "hello_polyglot.json"
  output_schema "hello_polyglot.json"

  def call(inputs, context)
    output = inputs.first["output"] + ["Hello from Ruby"]
    {"output" => output}
  end
end
