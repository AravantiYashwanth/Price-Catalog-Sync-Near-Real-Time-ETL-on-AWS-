# lambda_function.py
import boto3
import os
import urllib.parse
import json

# Initialize clients
s3 = boto3.client('s3')
glue_client = boto3.client('glue')

# Get Glue job name from environment variables
GLUE_JOB_NAME = os.environ.get('GLUE_JOB_NAME')


def lambda_handler(event, context):
    if not GLUE_JOB_NAME:
        raise ValueError("Glue job name not configured (GLUE_JOB_NAME missing).")

    all_records = []

    # Process each record in the event
    for record in event.get('Records', []):
        bucket = record['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')

        # Read JSON from S3
        json_obj = s3.get_object(Bucket=bucket, Key=key)
        json_data = json_obj['Body'].read().decode('utf-8')

        # ==================== MODIFIED CODE BLOCK START ====================
        # Process the file line by line (JSON Lines format)
        for line in json_data.splitlines():
            if not line.strip():  # Skip empty lines
                continue
            try:
                # Parse each individual line as a JSON object
                record_obj = json.loads(line)
                all_records.append(record_obj)
            except json.JSONDecodeError:
                print(
                    f"⚠️ Failed to parse a line in file s3://{bucket}/{key}. "
                    f"Skipping line:\n{line}"
                )
        # ===================== MODIFIED CODE BLOCK END =====================

    if not all_records:
        print("No valid JSON data found in provided files.")
        return {'statusCode': 200, 'body': 'No data to process'}

    # Create a single JSON array file
    output_bucket = bucket  # Reuse same bucket (can be changed)
    output_key = f"processed/combined_data_{context.aws_request_id}.json"  # Unique file name

    s3.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=json.dumps(all_records, indent=2).encode('utf-8')
    )

    print(f"✅ Combined JSON written to s3://{output_bucket}/{output_key}")

    # Start Glue job once for all data
    response = glue_client.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            '--s3_file_paths_list': f"s3://{output_bucket}/{output_key}"
        }
    )

    job_run_id = response['JobRunId']
    print(f"✅ Glue job triggered successfully: {job_run_id}")

    return {
        'statusCode': 200,
        'body': json.dumps(f"Started Glue job {job_run_id}")
    }
