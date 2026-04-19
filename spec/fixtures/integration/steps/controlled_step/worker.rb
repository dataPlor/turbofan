# frozen_string_literal: true

require "json"

INTEGRATION_CONFIG = JSON.parse(File.read(File.join(__dir__, "integration_config.json")))

class ControlledStep
  include Turbofan::Step

  runs_on :batch
  input_schema "passthrough.json"
  output_schema "passthrough.json"

  def call(inputs, context)
    mode = begin
      inputs.first["mode"]
    rescue
      nil
    end
    key = begin
      inputs.first["key"]
    rescue
      nil
    end

    case mode
    when "sfn_retry"
      handle_sfn_retry(key, context)
    when "force_timeout"
      sleep(180) # exceed the 60s timeout
      {"timed_out" => false}
    else
      # Verify inject_secret IAM access by reading the secret
      secret_accessible = verify_secret_access(context)

      {
        "key" => key,
        "controlled_step_ran" => true,
        "secret_accessible" => secret_accessible,
        "envelope_metadata" => context.envelope
      }
    end
  end

  private

  def handle_sfn_retry(key, context)
    marker_key = "#{context.execution_id}/controlled_step/sfn_retry_marker"
    bucket = ENV.fetch("TURBOFAN_BUCKET")

    begin
      context.s3.get_object(bucket: bucket, key: marker_key)
      # Marker exists — this is the retry, succeed
      {
        "key" => key,
        "sfn_retry_worked" => true,
        "controlled_step_ran" => true
      }
    rescue Aws::S3::Errors::NoSuchKey
      # First attempt — write marker and fail
      context.s3.put_object(bucket: bucket, key: marker_key, body: "retry-marker")
      raise "Intentional SFN retry test failure"
    end
  end

  def verify_secret_access(context)
    # inject_secret grants IAM access — verify by reading the secret
    secret_arn = INTEGRATION_CONFIG["secret_arn"]
    context.secrets_client.get_secret_value(secret_id: secret_arn)
    true
  rescue StandardError
    false
  end
end
