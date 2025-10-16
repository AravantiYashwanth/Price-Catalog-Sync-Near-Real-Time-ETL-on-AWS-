import pandas as pd
import boto3
import json
import time

# --- Configuration ---
# It's best practice to configure your region and credentials outside the script.
# Boto3 will automatically find them from environment variables or ~/.aws/credentials.
region_name = 'ap-south-1'
stream_name = 'demo'

# --- Initialize Kinesis Client ---
# No need to pass credentials here if they are configured correctly.
kinesis_client = boto3.client('kinesis', region_name=region_name)

# --- Read and Prepare Data ---
try:
    df = pd.read_csv("new.csv")
except FileNotFoundError:
    print("Error: dataset.csv not found. Make sure the file is in the same directory.")
    exit()

# Convert DataFrame to a list of dictionaries
records = df.to_dict(orient='records')
print(f"Found {len(records)} records to send to Kinesis.\n")

# --- Send Data to Kinesis in a Loop ---
# This loop sends one record at a time.
for record in records:
    # Convert the Python dictionary to a JSON string
    payload = json.dumps(record)
    
    # Use a dynamic partition key to distribute data across shards
    partition_key = str(record['product_id'])
    
    print(f"Sending record with product_id: {partition_key}...")
    
    # Send the single record to the Kinesis stream
    kinesis_client.put_record(
        StreamName=stream_name,
        Data=payload.encode('utf-8'),
        PartitionKey=partition_key
    )
    
    # Pause for a short time to simulate a real-time stream
    time.sleep(0.1) 

print("\n✅ All records sent successfully to the Kinesis stream.")
