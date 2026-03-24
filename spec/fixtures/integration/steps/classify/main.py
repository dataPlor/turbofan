import json
import os
import boto3

s3 = boto3.client("s3")

execution_id = os.environ["TURBOFAN_EXECUTION_ID"]
step_name = os.environ["TURBOFAN_STEP_NAME"]
prev_step = os.environ["TURBOFAN_PREV_STEP"]
bucket = os.environ.get("TURBOFAN_BUCKET", "turbofan-data")
bucket_prefix = os.environ.get("TURBOFAN_BUCKET_PREFIX", "")

def s3_key(*parts):
    key = "/".join(parts)
    return f"{bucket_prefix}/{key}" if bucket_prefix else key

# Read previous step output
input_key = s3_key(execution_id, prev_step, "output.json")
response = s3.get_object(Bucket=bucket, Key=input_key)
input_data = json.loads(response["Body"].read().decode("utf-8"))

# Classify
result = {
    "brand_name": input_data.get("brand_name", "unknown"),
    "classification": "food_and_beverage",
    "language": "python",
    "source": "external_container",
    "input_keys": sorted(input_data.keys()),
}

# Write output
output_key = s3_key(execution_id, step_name, "output.json")
s3.put_object(Bucket=bucket, Key=output_key, Body=json.dumps(result))

print(json.dumps(result))
