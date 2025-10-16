

# 🧩 Price & Catalog Sync (Near-Real-Time ETL)

### 🚀 Overview
The **Price & Catalog Sync** project demonstrates a **near-real-time, serverless data pipeline** built entirely on **AWS**, designed to automatically synchronize **product prices** and **catalog information** across all company systems.

It caters to two business needs:
1. **Real-Time Analytics (Hot Path)** – For live dashboards and instant updates.
2. **Historical Data Lake (Cold Path)** – For deep analysis, machine learning, and business intelligence.

This architecture ensures that every product price change is reflected across analytics, reporting, and historical storage layers with minimal latency.

---

## 🏗️ Solution Architecture

```

CSV (Product Data)
│
▼
AWS Kinesis Data Stream
├──▶ Lambda → Kinesis Firehose → Redshift (Live Prices)
└──▶ Kinesis Firehose → S3 (Raw Zone) → Lambda Trigger → AWS Glue (Medallion ETL)
│
▼
S3 (Bronze → Silver → Gold)
│
▼
Amazon Athena

````

---

## ⚙️ Technologies Used

| Category | Technologies |
|-----------|---------------|
| **Data Ingestion** | AWS Kinesis Data Streams |
| **Data Processing** | AWS Lambda, AWS Glue, PySpark |
| **Data Storage** | Amazon S3, Amazon Redshift |
| **Query & Analytics** | Amazon Athena, Redshift |
| **Data Format** | Delta Lake |
| **Languages / Libraries** | Python, Boto3, Pandas, PySpark, SQL |

---

## 🔄 Pipeline Breakdown

### 🔹 Pipeline 1: Real-Time Analytics (Hot Path)
Provides **live dashboards** with the latest product prices.

**Flow:**
1. **Producer (Python Script)** – Simulates a data source reading a CSV and sending JSON to **Kinesis Data Stream**.
2. **AWS Lambda** – Performs lightweight transformations and validation.
3. **Kinesis Firehose → Redshift** – Loads transformed data into Redshift.
4. **Redshift Merge Logic** – Keeps only the latest price per product using periodic MERGE jobs.

```sql
-- Create live prices table
CREATE TABLE IF NOT EXISTS public.product_prices_live (
    product_id INTEGER,
    product_name VARCHAR(255),
    price DECIMAL(10, 2),
    category VARCHAR(100),
    last_updated_timestamp TIMESTAMPTZ
);
````

---

### 🔹 Pipeline 2: Historical Data Lake (Cold Path)

Builds a **Medallion Architecture (Bronze–Silver–Gold)** on Amazon S3 using **Delta Lake** and **SCD Type 2**.

**Flow:**

1. **Kinesis Firehose → S3 (Raw Zone)** – Archives all incoming raw data.
2. **Lambda Trigger** – Detects new data in S3 and starts the Glue ETL job.
3. **AWS Glue Job (PySpark)** – Performs heavy transformations and maintains historical data layers.

#### 🥉 Bronze Layer

* Raw JSON data with schema enforcement.
* Stored in Delta format.

#### 🥈 Silver Layer

* Cleaned, validated, and de-duplicated records.
* Upsert logic ensures the current snapshot of all products.

#### 🥇 Gold Layer

* Implements **Slowly Changing Dimension (SCD Type 2)**.
* Maintains full history of price changes with `is_current`, `start_date`, and `end_date` columns.

**Sample Athena Table:**

```sql
CREATE DATABASE IF NOT EXISTS medallion_db;

CREATE EXTERNAL TABLE IF NOT EXISTS medallion_db.prices_history_gold (
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
LOCATION 's3://project-medallion-ncpl/gold/prices_history/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
```

---

## 🧠 Key Features

✅ Dual-pipeline architecture (Hot & Cold)
✅ Delta Lake integration with schema evolution
✅ Real-time Redshift updates
✅ Historical SCD Type 2 tracking
✅ Serverless querying via Athena
✅ Automated ETL triggered by S3 events

---

## ⚔️ Challenges & Solutions

**Challenge:**
Frequent `DELTA_FAILED_TO_MERGE_FIELDS` errors during Glue job runs due to schema mismatch (e.g., `price` field type inconsistencies).

**Solution Implemented:**

* Defined an explicit **PySpark schema** (`DoubleType` for price).
* Enabled **schema evolution** (`spark.databricks.delta.schema.autoMerge.enabled`).
* Added **error recovery** logic to cast existing data and overwrite with the correct schema when needed.

Result: **A robust and self-healing Delta pipeline**.

---

## 📂 Project Structure

```
Price-Catalog-Sync/
│
├── producer.py              # Sends sample product data to Kinesis
├── lambda_function.py       # Triggered by S3 → starts Glue job
├── glue_scd_job.py          # PySpark ETL implementing Medallion layers
├── dataset.csv              # Sample input data
├── architecture_diagram.png # (Optional visualization)
└── README.md
```

---

## 📊 Example Query (Athena)

```sql
SELECT 
  product_id, 
  product_name, 
  price, 
  start_date, 
  end_date 
FROM medallion_db.prices_history_gold
WHERE is_current = TRUE
ORDER BY start_date DESC;
```

---

## 💡 Learnings

* Designing **real-time + batch hybrid** architectures on AWS.
* Implementing **SCD Type 2** using Delta Lake.
* Handling schema evolution gracefully in Glue ETL.
* Automating serverless orchestration using **Kinesis + Lambda + Glue**.

---

## 🧰 Tools & Services

AWS Kinesis • AWS Lambda • AWS Glue • Amazon S3 • Amazon Redshift • Amazon Athena • Delta Lake • PySpark • Boto3 • Pandas

---

## 🏁 Conclusion

This project showcases how to design a **modern, serverless, near-real-time ETL pipeline** capable of powering both **live analytics** and **historical insights** with **fault tolerance** and **schema flexibility**.

---

### 👨‍💻 Author

**A. Yashwanth**
Aspiring Data Engineer | Python & AWS Enthusiast
📧 [www.linkedin.com/in/yashwantharavanti]
