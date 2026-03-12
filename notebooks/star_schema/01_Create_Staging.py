# Databricks notebook source
# COMMAND ----------
spark = SparkSession.builder.getOrCreate()

spark.sql("USE CATALOG students_data")
spark.sql("USE SCHEMA team3-chicago")

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE TABLE stg_location AS
SELECT
    location_id,
    dba_name,
    aka_name,
    license_number,
    facility_type,
    risk,
    address,
    city,
    state,
    zip,
    latitude,
    longitude,
    location
FROM 
    students_data.team3-chicago.food_inspections_bronze
""")

# COMMAND ----------
spark.sql("""
CREATE OR REPLACE TABLE stg_inspection AS
SELECT
    inspection_id,
    location_id,
    inspection_date,
    inspection_type,
    results,
    violations
FROM 
    students_data.team3-chicago.food_inspections_bronze
""")
# COMMAND ----------