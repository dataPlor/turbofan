require "bundler/setup"
require "turbofan"

Dir[File.join(__dir__, "support", "**", "*.rb")].each { |f| require f }

module ComputeEnvironments; end unless defined?(ComputeEnvironments)
TestCe = Class.new { include Turbofan::ComputeEnvironment } unless defined?(TestCe)
ComputeEnvironments::TestCe = TestCe unless ComputeEnvironments.const_defined?(:TestCe)

FIXTURE_SCHEMAS_DIR = File.join(__dir__, "fixtures", "schemas")

RSpec.configure do |config|
  config.expect_with :rspec do |expectations|
    expectations.include_chain_clauses_in_custom_matcher_descriptions = true
  end

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.shared_context_metadata_behavior = :apply_to_host_groups
  config.filter_run_when_matching :focus
  config.filter_run_excluding deploy: true
  config.disable_monkey_patching!
  config.order = :random
  Kernel.srand config.seed

  config.before(:example, :schemas) do
    Turbofan.schemas_path = FIXTURE_SCHEMAS_DIR
  end

  config.after do
    Turbofan.instance_variable_set(:@config, nil) if Turbofan.instance_variable_defined?(:@config)
    Turbofan::CLI::Prompt.reset!
  end

  def capture_stdout
    original = $stdout
    $stdout = StringIO.new
    yield
    $stdout.string
  ensure
    $stdout = original
  end
end
