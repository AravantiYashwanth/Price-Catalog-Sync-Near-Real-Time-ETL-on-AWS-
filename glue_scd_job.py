# glue_scd_job.py
# UPDATED SCRIPT (v4.6) - Fixed DELTA_FAILED_TO_MERGE_FIELDS for 'price'
# Key Changes:
# - Added schema overwrite mode for Bronze table initialization
# - Enhanced error handling with schema mismatch detection
# - Added option to evolve schema or overwrite based on compatibility

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, trim, current_timestamp, coalesce
from pyspark.sql.types import (
    StringType, DoubleType, LongType, TimestampType, StructType, StructField
)
from pyspark.sql.utils import AnalysisException

# --- Spark Session and Glue Context Initialization ---
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Enable Delta configurations
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.sql.legacy.decimal.retainFractionDigitsOnTruncate", "true")
spark.conf.set("spark.databricks.delta.schema.typeEvolution.enabled", "true")

job = Job(glueContext)

# --- Argument Parsing ---
args = getResolvedOptions(sys.argv, ['JOB_NAME', 's3_file_paths_list'])
job.init(args['JOB_NAME'], args)

# --- Parameters ---
all_new_paths_str = args['s3_file_paths_list']
s3_paths_to_process = all_new_paths_str.split(',') if all_new_paths_str else []

processed_data_bucket = "s3://project-medallion-ncpl/"
bronze_path = f"{processed_data_bucket}bronze/prices/"
silver_path = f"{processed_data_bucket}silver/prices/"
gold_path = f"{processed_data_bucket}gold/prices_history/"

# --- Graceful Exit if No Files ---
if not s3_paths_to_process:
    print("No new S3 file paths provided. Job is finishing.")
    job.commit()
    sys.exit(0)

print(f"Received {len(s3_paths_to_process)} file(s) to process.")
print(f"Bronze path: {bronze_path}")
print(f"Silver path: {silver_path}")
print(f"Gold path: {gold_path}")

# --- BRONZE LAYER (RAW INGESTION) ---
# Define consistent schema with DoubleType for price
bronze_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", DoubleType(), True),  # Consistent DoubleType
    StructField("category", StringType(), True),
    StructField("supplier_timestamp", StringType(), True)
])

try:
    raw_updates_df = (
        spark.read
        .schema(bronze_schema)
        .option("multiLine", "true")
        .json(s3_paths_to_process)
    )

    if raw_updates_df.rdd.isEmpty():
        print("Input files were empty or contained no valid JSON. Job finished.")
        job.commit()
        sys.exit(0)

    print(f"Read {raw_updates_df.count()} new records from raw source.")
    print("Schema of incoming data:")
    raw_updates_df.printSchema()
    raw_updates_df.show(5, truncate=False)

except AnalysisException as e:
    print(f"Error reading JSON data. Exiting. Error: {e}")
    job.commit()
    sys.exit(0)

# Write to Bronze with enhanced error handling
print("Writing new records to Bronze Delta table.")

try:
    bronze_exists = DeltaTable.isDeltaTable(spark, bronze_path)

    if bronze_exists:
        existing_bronze = DeltaTable.forPath(spark, bronze_path)
        existing_schema = existing_bronze.toDF().schema
        print("Existing Bronze table schema:")
        print(existing_schema)

        # Detect price field type
        price_field_existing = next(
            (f for f in existing_schema.fields if f.name == "price"), None
        )
        if price_field_existing:
            print(f"Existing 'price' field type: {price_field_existing.dataType}")

        # Try schema merge append
        try:
            raw_updates_df.write.format("delta") \
                .option("mergeSchema", "true") \
                .mode("append") \
                .save(bronze_path)
            print("Bronze layer append successful with schema merge.")

        except Exception as merge_error:
            print(f"Schema merge failed: {merge_error}")
            print("Attempting to overwrite Bronze table with correct schema...")

            existing_data = existing_bronze.toDF()

            corrected_existing = existing_data.select(
                col("product_id").cast(StringType()),
                col("product_name").cast(StringType()),
                col("price").cast(DoubleType()),
                col("category").cast(StringType()),
                col("supplier_timestamp").cast(StringType())
            )

            combined_data = corrected_existing.union(raw_updates_df)

            combined_data.write.format("delta") \
                .option("overwriteSchema", "true") \
                .mode("overwrite") \
                .save(bronze_path)

            print("Bronze table overwritten with corrected schema.")

    else:
        print("Bronze table does not exist. Creating with correct schema.")
        raw_updates_df.write.format("delta") \
            .option("overwriteSchema", "true") \
            .mode("overwrite") \
            .save(bronze_path)
        print("Bronze layer created successfully.")

except Exception as e:
    print(f"Error writing to Bronze: {e}")
    import traceback
    traceback.print_exc()
    job.commit()
    sys.exit(1)

# --- SILVER LAYER (CLEANED & CURRENT STATE) ---
print("\n--- Starting Silver layer processing ---")

input_timestamp = coalesce(
    col("supplier_timestamp").cast(TimestampType()), current_timestamp()
).alias("update_timestamp")

cleaned_updates_df = (
    raw_updates_df
    .withColumn("product_id", trim(col("product_id")).cast(LongType()))
    .withColumn("price", col("price").cast(DoubleType()))
    .withColumn("product_name", trim(col("product_name")).cast(StringType()))
    .withColumn("category", trim(col("category")).cast(StringType()))
    .withColumn("update_timestamp", input_timestamp)
    .filter((col("product_id").isNotNull()) & (col("price") > 0))
    .dropDuplicates(["product_id"])
)

print(f"After cleaning, {cleaned_updates_df.count()} unique product updates are ready for Silver.")
cleaned_updates_df.show(5, truncate=False)

print("Upserting data into Silver Delta table...")

try:
    if DeltaTable.isDeltaTable(spark, silver_path):
        print("Silver table exists. Merging data.")
        silver_delta_table = DeltaTable.forPath(spark, silver_path)

        silver_delta_table.alias("target") \
            .merge(cleaned_updates_df.alias("source"), "target.product_id = source.product_id") \
            .whenMatchedUpdate(set={
                "price": "source.price",
                "product_name": "source.product_name",
                "category": "source.category",
                "update_timestamp": "source.update_timestamp"
            }) \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        print("Silver table does not exist. Creating it with the first batch.")
        cleaned_updates_df.write.format("delta") \
            .option("overwriteSchema", "true") \
            .save(silver_path)

    print("Silver layer operation complete.")

except Exception as e:
    print(f"Error in Silver merge: {e}")
    import traceback
    traceback.print_exc()
    job.commit()
    sys.exit(1)

# --- GOLD LAYER (SCD TYPE 2 PRICE HISTORY) ---
print("\n--- Starting Gold layer processing (SCD Type 2) ---")

source_df_for_gold = cleaned_updates_df
ALWAYS_INSERT_NEW_VERSION = False

try:
    if not DeltaTable.isDeltaTable(spark, gold_path):
        print("Gold table does not exist. Creating it with initial data.")
        initial_gold_df = (
            source_df_for_gold
            .withColumn("is_current", lit(True))
            .withColumn("start_date", col("update_timestamp"))
            .withColumn("end_date", lit(None).cast(TimestampType()))
        )

        initial_gold_df.write.format("delta") \
            .option("overwriteSchema", "true") \
            .save(gold_path)

    else:
        print("Gold table exists. Performing SCD Type 2 merge logic.")
        gold_delta_table = DeltaTable.forPath(spark, gold_path)

        current_history_df = gold_delta_table.toDF().where("is_current = true")

        joined_df = (
            source_df_for_gold.alias("updates")
            .join(current_history_df.alias("history"), "product_id", "left_outer")
            .select(
                col("updates.*"),
                col("history.price").alias("history_price"),
                col("history.product_name").alias("history_product_name"),
                col("history.category").alias("history_category"),
                col("history.product_id").alias("history_product_id")
            )
        )

        change_condition = (
            (col("price") != col("history_price")) |
            (col("product_name") != col("history_product_name")) |
            (col("category") != col("history_category"))
        )

        records_to_insert = joined_df.where(
            col("history_product_id").isNull() | change_condition | lit(ALWAYS_INSERT_NEW_VERSION)
        )

        keys_to_expire = records_to_insert.where(
            col("history_product_id").isNotNull() & ~lit(ALWAYS_INSERT_NEW_VERSION)
        ).select("product_id", "update_timestamp")

        if keys_to_expire.count() > 0:
            print(f"Found {keys_to_expire.count()} existing records to expire.")
            gold_delta_table.alias("history") \
                .merge(
                    keys_to_expire.alias("updates"),
                    "history.product_id = updates.product_id AND history.is_current = true"
                ) \
                .whenMatchedUpdate(set={
                    "is_current": lit(False),
                    "end_date": col("updates.update_timestamp")
                }) \
                .execute()

        if records_to_insert.count() > 0:
            print(f"Found {records_to_insert.count()} new, changed, or forced records to append as current.")

            new_current_records = (
                records_to_insert
                .select("product_id", "product_name", "price", "category", "update_timestamp")
                .withColumn("is_current", lit(True))
                .withColumn("start_date", col("update_timestamp"))
                .withColumn("end_date", lit(None).cast(TimestampType()))
            )

            print("Appending new current records to Gold table.")
            new_current_records.write.format("delta") \
                .option("mergeSchema", "true") \
                .mode("append") \
                .save(gold_path)

        else:
            print("No product changes found. No updates to the Gold table.")

except Exception as e:
    print(f"Error in Gold SCD processing: {e}")
    import traceback
    traceback.print_exc()
    job.commit()
    sys.exit(1)

print("Glue job finished successfully.")
job.commit()
