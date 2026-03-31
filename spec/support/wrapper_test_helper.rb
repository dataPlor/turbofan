require "stringio"

module WrapperTestHelper
  # Creates a minimal step class that captures input and returns a result.
  # The block receives (input, ctx) and should return the step output.
  # Without a block, the step captures input and returns {}.
  #
  # Options:
  #   uses:      - array of resource keys the step reads from
  #   writes_to: - array of resource keys the step writes to
  def make_step(name: "SpyStep", uses: [], writes_to: [], &block)
    callback = block || proc { |_inputs, _ctx| {} }
    uses_list = Array(uses)
    writes_list = Array(writes_to)
    Class.new do
      include Turbofan::Step

      compute_environment :test_ce
      execution :batch
      cpu 1
      input_schema "passthrough.json"
      output_schema "passthrough.json"
      uses_list.each { |key| uses key }
      writes_list.each { |key| writes_to key }
      define_singleton_method(:name) { name }
      define_method(:call, &callback)
    end
  end

  # Creates a resource class with the given key, secret ARN, and optional Postgres mixin.
  def make_resource(key:, secret_arn: nil, postgres: true)
    Class.new do
      include Turbofan::Resource
      include Turbofan::Postgres if postgres

      key key
      secret secret_arn if secret_arn
    end
  end

  # Stubs secrets_client to return the given connection string for a secret ARN.
  def stub_secret(arn, value)
    allow(secrets_client).to receive(:get_secret_value).with(
      secret_id: arn
    ).and_return(
      instance_double("Aws::SecretsManager::Types::GetSecretValueResponse", # rubocop:disable RSpec/VerifiedDoubleReference
        secret_string: value)
    )
  end

  # Requires `let(:cloudwatch_client)` and `let(:s3_client)` in the calling spec.
  # Optionally responds to `duckdb_conn` and `secrets_client` for resource attachment tests.
  def run_wrapper(step_klass, env: {}, nvme_base: nil)
    saved_env = {}
    env.each do |k, v|
      saved_env[k] = ENV[k]
      ENV[k] = v
    end

    wrapper = Turbofan::Runtime::Wrapper.new(step_klass)

    context = Turbofan::Runtime::Context.new(
      execution_id: env["TURBOFAN_EXECUTION_ID"] || "test-exec",
      attempt_number: (env["AWS_BATCH_JOB_ATTEMPT"] || "1").to_i,
      step_name: step_klass.name || "anonymous",
      stage: env["TURBOFAN_STAGE"] || "development",
      pipeline_name: env["TURBOFAN_PIPELINE"] || "test-pipeline",
      array_index: env.key?("AWS_BATCH_JOB_ARRAY_INDEX") ? env["AWS_BATCH_JOB_ARRAY_INDEX"].to_i : nil,
      nvme_path: nvme_base,
      uses: step_klass.turbofan_uses,
      writes_to: step_klass.turbofan_writes_to,
      size: env["TURBOFAN_SIZE"]
    )

    if respond_to?(:secrets_client)
      allow(context).to receive(:secrets_client).and_return(secrets_client)
    end
    if respond_to?(:duckdb_conn)
      allow(context).to receive(:duckdb).and_return(duckdb_conn)
    end

    metrics = Turbofan::Runtime::Metrics.new(
      cloudwatch_client: cloudwatch_client,
      pipeline_name: env["TURBOFAN_PIPELINE"] || "test-pipeline",
      stage: env["TURBOFAN_STAGE"] || "development",
      step_name: step_klass.name || "anonymous"
    )
    allow(context).to receive_messages(s3: s3_client, metrics: metrics)

    allow(wrapper).to receive_messages(setup_nvme: nvme_base, build_context: context)

    original_stdout = $stdout
    captured = StringIO.new
    $stdout = captured

    wrapper.run

    $stdout = original_stdout
    {output: captured.string.strip, context: context, metrics: metrics}
  ensure
    $stdout = original_stdout if original_stdout
    saved_env&.each { |k, v| v.nil? ? ENV.delete(k) : ENV[k] = v }
  end
end
