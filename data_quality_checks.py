# data_quality_checks.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when

# 1. Initialize Spark Session 
# spark = SparkSession.builder.appName("DataQualityChecks").getOrCreate()

def run_data_quality_checks(df):
    """Performs standard null/invalid value checks on a DataFrame."""
    
    # 2. Null Value Check
    print("--- Null Value Checks ---")
    null_counts = df.select([
        sum(when(col(c).isNull(), 1).otherwise(0)).alias(f"{c}_null_count")
        for c in df.columns
    ])
    null_counts.show()

    # 3. Invalid Date Check (Example: Check for dates that are far in the future)
    print("--- Invalid Date Checks ---")
    invalid_date_count = df.filter(col("campaign_start_date") > "2030-01-01").count()
    print(f"Number of rows with invalid future dates: {invalid_date_count}")

    # 4. Negative Metric Check (Example: Campaign revenue cannot be negative)
    print("--- Negative/Invalid Metric Checks ---")
    negative_revenue_count = df.filter(col("campaign_revenue") < 0).count()
    print(f"Number of rows with negative revenue: {negative_revenue_count}")
    
    # Simple Pass/Fail based on checks
    if null_counts.collect()[0]['key_column_null_count'] > 0 or negative_revenue_count > 0:
        print("\nDATA QUALITY CHECK: FAILED (Review output above)")
        return False
    else:
        print("\nDATA QUALITY CHECK: PASSED")
        return True