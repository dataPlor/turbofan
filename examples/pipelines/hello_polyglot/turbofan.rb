require_relative "../../compute_environments/turbofan_temp_test/definition"
require_relative "../../steps/hello_ruby/worker"
require_relative "../../steps/hello_python/worker"
require_relative "../../steps/hello_node/worker"
require_relative "../../steps/hello_rust/worker"

class HelloPolyglot
  include Turbofan::Pipeline

  pipeline_name "hello_polyglot"
  compute_environment ComputeEnvironments::TurbofanTempTest

  pipeline do
    r = fan_out(hello_ruby(trigger_input))
    p = fan_out(hello_python(r))
    n = fan_out(hello_node(p))
    fan_out(hello_rust(n))
  end
end
