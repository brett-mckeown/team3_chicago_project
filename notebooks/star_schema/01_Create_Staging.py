# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG students_data;
# MAGIC USE SCHEMA `team3-chicago`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE stg_location AS
# MAGIC SELECT
# MAGIC     dba_name,
# MAGIC     aka_name,
# MAGIC     license_number,
# MAGIC     facility_type,
# MAGIC     risk,
# MAGIC     address,
# MAGIC     city,
# MAGIC     state,
# MAGIC     zip
# MAGIC FROM 
# MAGIC     students_data.`team3-chicago`.food_inspections_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE stg_inspection AS
# MAGIC SELECT
# MAGIC     inspection_id,
# MAGIC     to_date(trim(inspection_date), 'MM/dd/yyyy') AS inspection_date,
# MAGIC     inspection_type,
# MAGIC     results,
# MAGIC     violations,
# MAGIC     latitude,
# MAGIC     longitude,
# MAGIC     location
# MAGIC FROM 
# MAGIC     students_data.`team3-chicago`.food_inspections_bronze

# COMMAND ----------




