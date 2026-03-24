import { S3Client, GetObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";

const s3 = new S3Client();

async function main() {
  const bucket = process.env.TURBOFAN_BUCKET;
  const executionId = process.env.TURBOFAN_EXECUTION_ID;
  const stepName = process.env.TURBOFAN_STEP_NAME;
  const arrayIndex = process.env.AWS_BATCH_JOB_ARRAY_INDEX;

  // Read input
  const inputKey = `${executionId}/${stepName}/input/${arrayIndex}.json`;
  const getResponse = await s3.send(
    new GetObjectCommand({ Bucket: bucket, Key: inputKey })
  );
  const body = await getResponse.Body.transformToString();
  const data = JSON.parse(body);

  // Append greeting
  data.output.push("Hello from Node");

  // Write output
  const outputKey = `${executionId}/${stepName}/output/${arrayIndex}.json`;
  await s3.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: outputKey,
      Body: JSON.stringify(data),
    })
  );

  // Print result to stdout
  console.log(JSON.stringify(data));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
