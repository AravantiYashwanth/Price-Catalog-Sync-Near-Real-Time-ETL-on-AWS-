# 🧩 Price & Catalog Sync (Near-Real-Time ETL on AWS)

## 📖 Overview
This project automates the synchronization of **product prices and catalog information** across all company systems **in near real time** using **Amazon Web Services (AWS)**.  

It demonstrates a robust, scalable, and fault-tolerant **dual-pipeline architecture** designed for both **live analytics** and **historical data analysis**.  

---

## 🚀 Architecture Summary

The system is composed of two parallel data pipelines:

| Pipeline | Purpose | Technologies | Target |
|-----------|----------|---------------|---------|
| **Hot Path (Pipeline 1)** | Real-time ingestion and analytics | Kinesis Streams, Lambda, Firehose, Redshift | Power live dashboards |
| **Cold Path (Pipeline 2)** | Historical archiving & analytics | Kinesis Firehose, S3, Lambda, Glue (PySpark), Athena | Build SCD-Type 2 historical data lake |

---

## 🏗️ Solution Architecture Diagram

```
CSV (Product Data)
   │
   ▼
Kinesis Data Stream
   ├──▶ Lambda → Kinesis Firehose → Amazon Redshift  (Live Prices)
   └──▶ Kinesis Firehose → Amazon S3 (Raw)
                              │
                              ▼
                         S3 Trigger → Lambda → AWS Glue (PySpark)
                                               │
                                               ▼
                ┌─────────────┬────────────┬────────────┐
                │  Bronze     │  Silver    │  Gold (SCD 2) │
                └─────────────┴────────────┴────────────┘
                              │
                              ▼
                         Amazon Athena (SQL queries)
```

---

## 🧰 Technologies Used

- **Data Ingestion:** AWS Kinesis Data Streams  
- **Data Processing:** AWS Lambda, AWS Glue (PySpark)  
- **Data Storage:** Amazon S3, Amazon Redshift  
- **Data Querying:** Amazon Athena  
- **Data Format:** Delta Lake  
- **Languages:** Python, SQL, Boto3, Pandas, PySpark  

---

## 🧩 Components

### 1️⃣ Producer Script (`producer.py`)
Simulates streaming data by reading `dataset.csv` and sending JSON records to **Kinesis Data Stream**.

```python
import pandas as pd, boto3, json, time

# AWS configuration
stream_name = 'demo'
region = 'ap-south-1'
kinesis = boto3.client('kinesis', region_name=region)

df = pd.read_csv("dataset.csv")
df['price'] = df['price'].astype(float)

records = df.to_dict(orient='records')
payload = json.dumps(records)

kinesis.put_record(
    StreamName=stream_name,
    Data=payload.encode('utf-8'),
    PartitionKey="partition-1"
)
print("✅ Sent payload to Kinesis")
```

---

### 2️⃣ AWS Lambda (Firehose Trigger)
Transforms the data stream and forwards it to **Kinesis Data Firehose**, which loads into **Amazon Redshift**.

Redshift staging table:
```sql
CREATE TABLE live_prices (
  product_id VARCHAR(255),
  price DECIMAL(10,2),
  event_timestamp TIMESTAMP
);
```

Merge logic (to maintain latest prices):
```sql
BEGIN;
DELETE FROM product_prices_live USING product_prices
WHERE product_prices_live.product_id = product_prices.product_id;

INSERT INTO product_prices_live
SELECT product_id, product_name, price, category, last_updated_timestamp
FROM (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY product_id ORDER BY last_updated_timestamp DESC) rn
  FROM product_prices
) WHERE rn = 1;
TRUNCATE product_prices;
END;
```

---

### 3️⃣ Lambda → Glue Trigger (S3 Event)
Once Firehose writes raw data to **S3**, an S3 event triggers a **Lambda function** that starts an AWS Glue job.

```python
import boto3, os, json, urllib.parse
s3 = boto3.client('s3')
glue = boto3.client('glue')
GLUE_JOB_NAME = os.environ['GLUE_JOB_NAME']

def lambda_handler(event, context):
    records = []
    for rec in event['Records']:
        bucket = rec['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(rec['s3']['object']['key'])
        content = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
        for line in content.splitlines():
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    output_key = f"processed/combined_{context.aws_request_id}.json"
    s3.put_object(Bucket=bucket, Key=output_key, Body=json.dumps(records).encode('utf-8'))
    glue.start_job_run(JobName=GLUE_JOB_NAME, Arguments={'--s3_file_paths_list': f"s3://{bucket}/{output_key}"})
```

---

### 4️⃣ AWS Glue Job (`glue_scd_job.py`)
Implements **Medallion Architecture** + **SCD Type 2** logic with Delta Lake.

- **Bronze:** raw ingestion (schema-enforced JSON)  
- **Silver:** cleaned, deduplicated, validated data  
- **Gold:** historical price tracking using SCD Type 2

---

### 5️⃣ Querying with Athena
Register the **Gold Delta table** in the Glue Catalog and query via Athena:

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS prices_history_gold (
  product_id BIGINT,
  product_name STRING,
  price DOUBLE,
  category STRING,
  update_timestamp TIMESTAMP,
  is_current BOOLEAN,
  start_date TIMESTAMP,
  end_date TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3://project-medallion-ncpl/gold/prices_history/';
```

---

## ⚙️ Deployment Steps

1. Create **Kinesis Stream** (`demo`)
2. Create **Redshift cluster** and run initial SQL
3. Configure **Kinesis Firehose** (one → Redshift, one → S3)
4. Deploy both **Lambda functions**
5. Upload **Glue script** and create **Glue job** with Delta Lake config:
   ```
   --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
   --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
   --datalake-formats delta
   ```
6. Run `producer.py` locally to simulate streaming data
7. Verify tables in Redshift and Athena

---

## 🧠 Key Features

✅ Dual-path real-time + historical design  
✅ Fully serverless components  
✅ Delta Lake schema evolution & SCD Type 2 tracking  
✅ Automated pipeline orchestration with S3 triggers  
✅ Queryable through Athena and Redshift  

---

## 🩺 Common Issues & Fixes

**Error:** `DELTA_FAILED_TO_MERGE_FIELDS`  
**Cause:** schema mismatch on column `price` (e.g., LONG vs DOUBLE)  
**Fix:**  
- Enforced explicit schema with `DoubleType`  
- Enabled auto schema merge (`spark.databricks.delta.schema.autoMerge.enabled=true`)  
- Implemented robust recovery: read-cast-union-overwrite logic in Glue job  

---

## 📊 Future Enhancements

- Implement end-to-end CI/CD using AWS CodePipeline  
- Add monitoring via CloudWatch Metrics  
- Extend to handle multi-tenant product catalogs  

---

## 🧑‍💻 Author

**A. Yashwanth**  
🎓 B.Tech in ECE (AIDS) | 💡 Passionate about Cloud & Data Engineering  
📧 your.email@example.com | 🌐 [LinkedIn Profile](https://www.linkedin.com)

---

## 🪪 License

This project is released under the [MIT License](LICENSE).
