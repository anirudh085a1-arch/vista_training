# delta_optimization.py

from pyspark.sql import SparkSession

# spark = SparkSession.builder.appName("DeltaOptimization").getOrCreate()

TABLE_PATH = "/mnt/raw/martech/campaign_data"
OPTIMIZATION_COLUMNS = ["campaign_id", "country_code"] # Z-ORDER columns

def optimize_delta_table(table_path, columns):
    """Runs OPTIMIZE ZORDER and VACUUM on the specified Delta table."""
    
    print(f"--- Optimizing Delta Table: {table_path} ---")
    
    # 1. OPTIMIZE with ZORDER Clustering
    zorder_cols = ", ".join(columns)
    print(f"Running OPTIMIZE ... ZORDER BY ({zorder_cols})")
    
    # Note: Spark SQL is often easier for Delta commands
    spark.sql(f"""
        OPTIMIZE delta.`{table_path}`
        ZORDER BY ({zorder_cols})
    """)
    print("OPTIMIZE ZORDER Complete.")

    # 2. VACUUM (cleanup old files)
    print("Running VACUUM with RETAIN 7 HOURS...")
    spark.sql(f"VACUUM delta.`{table_path}` RETAIN 7 HOURS")
    print("VACUUM Complete.")