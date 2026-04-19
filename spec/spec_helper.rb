require "bundler/setup"
require "fileutils"
require "turbofan"

Dir[File.join(__dir__, "support", "**", "*.rb")].each { |f| require f }

module ComputeEnvironments; end unless defined?(ComputeEnvironments)
TestCe = Class.new { include Turbofan::ComputeEnvironment } unless defined?(TestCe)
ComputeEnvironments::TestCe = TestCe unless ComputeEnvironments.const_defined?(:TestCe, false)

FIXTURE_SCHEMAS_DIR = File.join(__dir__, "fixtures", "schemas")

# Repo-local tmp root for specs. Dir.mktmpdir's default parent
# (`/var/folders/...` on macOS) fails under restricted sandboxes with
# Errno::EPERM; routing through a path the test process definitely has
# write access to (the repo itself) keeps specs portable. `tmp/` is
# gitignored, so accumulated test dirs don't pollute the working tree.
SPEC_TMP_ROOT = File.expand_path("../tmp", __dir__).tap { |p| FileUtils.mkdir_p(p) }

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

  # Wrapper#set_tmpdir mutates ENV["TMPDIR"] as production behavior. Without
  # this guard, specs that call wrapper.run leak a TMPDIR pointing at a
  # subsequently-cleaned tmp dir, causing later Dir.mktmpdir calls (both in
  # specs and in production code they exercise) to fail intermittently.
  config.around do |example|
    saved_tmpdir = ENV["TMPDIR"]
    example.run
  ensure
    if saved_tmpdir.nil?
      ENV.delete("TMPDIR")
    else
      ENV["TMPDIR"] = saved_tmpdir
    end
  end

  def capture_stdout
    original = $stdout
    $stdout = StringIO.new
    yield
    $stdout.string
  ensure
    $stdout = original
  end

  def capture_stderr
    original = $stderr
    $stderr = StringIO.new
    yield
    $stderr.string
  ensure
    $stderr = original
  end
end
