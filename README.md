# 🚀 Vista Martech Data Consolidation ETL

This repository contains the PySpark and SQL scripts developed for the **Vista Martech Data Consolidation** project, aiming to unify marketing data from various systems (MDW) into the Enterprise Data Warehouse (EDW) structure using **Azure Databricks** and **Delta Lake**.

## 🎯 Project Goals

The primary objectives of this project are to:
1.  **Extract** raw data from SQL Server staging tables (MDW) into the Databricks Raw Zone (Delta Lake).
2.  **Implement** data quality checks, including profiling and duplicate identification.
3.  **Transform** and map core campaign, lead, and engagement data into the unified EDW schema.
4.  **Automate** and orchestrate the end-to-end pipeline using Azure Data Factory (ADF).

## 🛠️ Technology Stack

* **Platform:** Azure Databricks (for compute)
* **Language:** PySpark, SQL, Python
* **Data Lake:** Azure Data Lake Storage (ADLS Gen2)
* **Data Format:** Delta Lake
* **Orchestration:** Azure Data Factory (ADF)
* **Security:** Azure Key Vault (for credential management)

## 📁 Key Directories and Files

| File/Directory | Description |
| :--- | :--- |
| `src/etl_jobs/` | Contains the core PySpark scripts (`.py` notebooks) executed in Databricks. |
| `src/sql_validation/` | Contains SQL scripts used for source-to-target data validation and cross-checking. |
| `conf/job_config.json` | Configuration file holding parameters like connection details, schema names, and load dates. |
| `requirements.txt` | Lists Python dependencies (e.g., Azure SDKs) needed for non-Spark tasks. |

## 📦 Core PySpark Scripts

### 1. `src/etl_jobs/01_raw_ingestion.py`

**Purpose:** Extracts data from the source **SQL Server MDW staging tables** and writes it as raw Delta tables in the Databricks Raw Zone.

```python
# 01_raw_ingestion.py

from pyspark.sql import SparkSession

# Initialize Spark Session (already done in Databricks notebook environment)
# spark = SparkSession.builder.appName("RawIngestion").getOrCreate()

# --- Configuration (Pulled from job_config.json or Key Vault) ---
JDBC_URL = "jdbc:sqlserver://<server_name>;database=<db_name>" # Retrieved from Key Vault
USER = "svc_user" # Retrieved from Key Vault
PASSWORD = "secret_password" # Retrieved from Key Vault
SOURCE_TABLE = "MDW.dbo.stg_campaign_data"
TARGET_PATH = "/mnt/raw/martech/campaign_data"

print(f"Reading data from source: {SOURCE_TABLE}")

# 1. Read raw data from SQL Server using JDBC
raw_df = spark.read \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", SOURCE_TABLE) \
    .option("user", USER) \
    .option("password", PASSWORD) \
    .load()

# 2. Write the raw data to Delta Lake (Raw Zone)
print(f"Writing {raw_df.count()} records to Delta table at: {TARGET_PATH}")
raw_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(TARGET_PATH)

print("Raw data ingestion complete.")





1.# PySpark Utility Functions (utils.py)
This file contains reusable functions designed to enforce data standardization and cleansing across various ETL jobs.

Purpose: Promotes code reuse and consistency for repetitive data manipulation tasks.

Key Functions Included:

Timestamp Standardization: Converts timestamp columns to a consistent target format.

String Cleansing: Removes common special characters and excess whitespace from text fields.

Dynamic Column Renaming: Provides a centralized function for schema mapping.

2.# Data Quality and Validation (data_quality_checks.py)
This PySpark script addresses the requirements for data validation and data quality checks on campaign performance data.

Purpose: Ensures data integrity by identifying null values, invalid metrics (e.g., negative revenue), and date anomalies.

Methodology: Utilizes PySpark aggregation functions to count specific issues (e.g., sum(when(col(c).isNull(), 1))) across all input columns.

Related Activities: Supports end-to-end data validation and data reconciliation across MDW and EDW layers.

3.# Delta Lake Optimization (delta_optimization.py)
This script implements performance tuning measures for the high-volume Delta Lake tables, aligning with the project's storage and query performance goals.

Purpose: Improves query efficiency and reduces storage overhead for frequently accessed Delta tables.

Key Operations:

OPTIMIZE ... ZORDER BY: Applies Z-Order clustering on key columns (e.g., campaign_id, country_code) to co-locate related data, significantly speeding up query filters.

VACUUM: Removes old, unreferenced data files to manage storage costs and improve file listing performance.

4.# Transformation and Load (transform_load.py)
This script handles the initial ingestion of raw data.

Source: Reads raw campaign data from a SQL Server table using JDBC.

Target: Writes the raw data to the Delta Lake Raw Zone path (/mnt/raw/martech/campaign_data).

Mode: Uses overwrite mode for loading raw data.