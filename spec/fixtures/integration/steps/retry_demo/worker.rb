# frozen_string_literal: true

class RetryDemo
  include Turbofan::Step

  runs_on :batch
  input_schema "passthrough.json"
  output_schema "passthrough.json"

  def call(inputs, context)
    key = begin
      inputs.first["key"]
    rescue
      nil
    end
    mode = begin
      inputs.first["mode"]
    rescue
      nil
    end

    # Force fail mode — raise immediately (Batch won't retry exit code 1)
    raise "Intentional failure for integration testing" if key == "force_fail"

    if context.attempt_number == 1
      context.s3.put_object(
        bucket: ENV.fetch("TURBOFAN_BUCKET", "turbofan-data"),
        key: "#{context.execution_id}/retry_demo/attempt_1_marker",
        body: "attempted"
      )
      exit(143)
    end

    {
      "retried" => true,
      "attempts" => context.attempt_number,
      "key" => key || "starbucks",
      "mode" => mode,
      "envelope" => context.envelope
    }
  end
end
