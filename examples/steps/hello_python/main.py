import json
import os

import boto3

s3 = boto3.client("s3")


def main():
    bucket = os.environ["TURBOFAN_BUCKET"]
    execution_id = os.environ["TURBOFAN_EXECUTION_ID"]
    step_name = os.environ["TURBOFAN_STEP_NAME"]
    array_index = os.environ["AWS_BATCH_JOB_ARRAY_INDEX"]

    # Read input
    input_key = f"{execution_id}/{step_name}/input/{array_index}.json"
    response = s3.get_object(Bucket=bucket, Key=input_key)
    data = json.loads(response["Body"].read())

    # Append greeting
    data["output"].append("Hello from Python")

    # Write output
    output_key = f"{execution_id}/{step_name}/output/{array_index}.json"
    s3.put_object(Bucket=bucket, Key=output_key, Body=json.dumps(data))

    # Print result to stdout
    print(json.dumps(data))


if __name__ == "__main__":
    main()
