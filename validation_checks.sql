
-- Run this script in both the Source SQL Server environment and the Databricks environment
-- (using SQL endpoints) to validate row counts and key metrics.

-- NOTE: In Databricks, the table is accessed via its path or a registered Delta table name.

-- 1. COUNT CHECK: Compare total records loaded on the last run date.
SELECT 'MDW_Source' AS data_source, COUNT(1) AS total_records
FROM MDW.dbo.stg_campaign_data
WHERE load_date = '{{ dag_run.conf.load_date }}' -- Dynamic Date Variable

UNION ALL

SELECT 'Databricks_Target' AS data_source, COUNT(1) AS total_records
FROM delta.`/mnt/raw/martech/campaign_data`
WHERE load_date = '{{ dag_run.conf.load_date }}';


-- 2. METRIC CHECK: Ensure the sum of a key numeric field matches.
SELECT 'MDW_Source' AS data_source, SUM(conversion_value) AS sum_conversion_value
FROM MDW.dbo.stg_campaign_data
WHERE load_date = '{{ dag_run.conf.load_date }}'

UNION ALL

SELECT 'Databricks_Target' AS data_source, SUM(conversion_value) AS sum_conversion_value
FROM delta.`/mnt/raw/martech/campaign_data`
WHERE load_date = '{{ dag_run.conf.load_date }}';