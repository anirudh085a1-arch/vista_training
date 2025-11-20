

from pyspark.sql.functions import count, col, sum as spark_sum, isnull

TARGET_PATH = "/mnt/raw/martech/campaign_data"
raw_df = spark.read.format("delta").load(TARGET_PATH)

print("--- Starting Data Profiling ---")

# A. Null Value Check
null_counts = raw_df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in raw_df.columns
])
print("Null Counts per Column:")
null_counts.show()

# B. Duplicate Record Identification (Key: customer_id, campaign_id)
key_columns = ["customer_id", "campaign_id"]

duplicate_summary = raw_df.groupBy(*key_columns).agg(
    count("*").alias("record_count")
).filter(col("record_count") > 1)

print(f"Found {duplicate_summary.count()} unique keys with duplicate records.")
print("Summary of Duplicate Keys:")
duplicate_summary.show(5)

# C. Basic Statistics
print("Summary Statistics for Numeric Columns:")
raw_df.describe("engagement_score").show()

# Write profiling results to a separate Delta table for auditing
# duplicate_summary.write.format("delta").mode("overwrite").save("/mnt/audit/duplicate_report")