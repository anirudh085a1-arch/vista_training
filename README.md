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