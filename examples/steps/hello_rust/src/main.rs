use aws_sdk_s3::Client;
use serde_json::Value;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let s3 = Client::new(&config);

    let bucket = env::var("TURBOFAN_BUCKET")?;
    let execution_id = env::var("TURBOFAN_EXECUTION_ID")?;
    let step_name = env::var("TURBOFAN_STEP_NAME")?;
    let array_index = env::var("AWS_BATCH_JOB_ARRAY_INDEX")?;

    // Read input
    let input_key = format!("{execution_id}/{step_name}/input/{array_index}.json");
    let get_result = s3
        .get_object()
        .bucket(&bucket)
        .key(&input_key)
        .send()
        .await?;
    let body = get_result.body.collect().await?.into_bytes();
    let mut data: Value = serde_json::from_slice(&body)?;

    // Append greeting
    if let Some(output) = data.get_mut("output").and_then(|v| v.as_array_mut()) {
        output.push(Value::String("Hello from Rust".to_string()));
    }

    // Write output
    let output_key = format!("{execution_id}/{step_name}/output/{array_index}.json");
    let output_body = serde_json::to_vec(&data)?;
    s3.put_object()
        .bucket(&bucket)
        .key(&output_key)
        .body(output_body.into())
        .send()
        .await?;

    // Print result to stdout
    println!("{}", serde_json::to_string(&data)?);

    Ok(())
}
