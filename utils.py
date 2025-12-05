# utils.py

from pyspark.sql.functions import col, regexp_replace, date_format, to_timestamp

# --- 1. Date and Time Standardization Function ---
def standardize_timestamp(df, column_name, target_format="yyyy-MM-dd HH:mm:ss"):
    """
    Standardizes a timestamp column to a consistent format.
    (Addresses timestamp conversions mentioned in reports)
    """
    print(f"Standardizing column '{column_name}' to {target_format}")
    return df.withColumn(
        column_name,
        date_format(col(column_name).cast("timestamp"), target_format)
    )

# --- 2. Data Cleansing Function ---
def clean_string_column(df, column_name):
    """
    Performs basic string cleansing by removing common leading/trailing
    whitespace and special characters from a column.
    (Addresses data cleansing mentioned in reports)
    """
    print(f"Cleaning string column: {column_name}")
    # Trim spaces
    df = df.withColumn(column_name, trim(col(column_name)))
    # Replace common non-alphanumeric characters with a space
    df = df.withColumn(
        column_name,
        regexp_replace(col(column_name), "[^a-zA-Z0-9 ]", " ")
    )
    return df

# --- 3. Simple Column Rename Function ---
def rename_column(df, old_name, new_name):
    """Renames a column within a DataFrame."""
    print(f"Renaming column '{old_name}' to '{new_name}'")
    return df.withColumnRenamed(old_name, new_name)

# --- Example of function usage (for testing) ---
# df = spark.createDataFrame([
#     (" 2023-01-01 12:30:00 ", " Email-Campaign! "),
# ], ["event_timestamp", "campaign_name"])

# df_standardized = standardize_timestamp(df, "event_timestamp", "MM/dd/yyyy")
# df_cleaned = clean_string_column(df_standardized, "campaign_name")
# df_cleaned.show(truncate=False)