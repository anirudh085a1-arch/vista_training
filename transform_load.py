

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, concat, upper, when, to_date

# Define file paths
RAW_PATH = "/mnt/raw/martech/campaign_data"
REFINED_PATH = "/mnt/refined/martech/edw_unified_campaigns"
LOAD_DATE = "2025-07-31" # Example load date

# Read raw data that has already passed profiling
raw_campaign_df = spark.read.format("delta").load(RAW_PATH)

print("Starting data transformation to EDW structure...")

# --- 1. Apply Transformation Logic ---
edw_df = raw_campaign_df.select(
    # A. Create a Unified Customer Key (Mapping Logic)
    concat(lit("MDW_"), upper(col("source_customer_id"))).alias("customer_key"),
    
    # B. Map Campaign Data and Rename/Cast Columns
    col("campaign_id").cast("string"),
    col("campaign_name").alias("campaign_title"),
    col("campaign_start_dt").alias("start_date").cast("date"),
    
    # C. Standardize Status (e.g., map different source status codes to standard EDW codes)
    when(col("status_code") == "A", lit("ACTIVE"))
        .when(col("status_code") == "P", lit("PENDING"))
        .when(col("status_code") == "D", lit("COMPLETE"))
        .otherwise(lit("UNKNOWN"))
        .alias("campaign_status"),
        
    # D. Simple Metric Calculation/Selection
    col("response_count"),
    col("conversion_value").cast("decimal(10, 2)"),
    
    # E. Add Load Metadata
    lit(LOAD_DATE).cast("date").alias("edw_load_date")
)

# --- 2. Write Transformed Data to Refined Zone ---
print(f"Schema check for EDW structure:")
edw_df.printSchema()

print(f"Writing {edw_df.count()} transformed records to Delta table at: {REFINED_PATH}")

# Write to a refined zone Delta table, partitioned by load date
edw_df.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("edw_load_date") \
    .save(REFINED_PATH)

print("Data transformation and load to EDW refined zone complete.")