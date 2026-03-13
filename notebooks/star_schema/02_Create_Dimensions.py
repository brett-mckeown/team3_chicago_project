# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG students_data;
# MAGIC USE SCHEMA `team3-chicago`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE DimLocation (
# MAGIC   d_location_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   zip STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DimLocation (address, city, state, zip)
# MAGIC SELECT
# MAGIC   address,
# MAGIC   city,
# MAGIC   state,
# MAGIC   zip
# MAGIC FROM
# MAGIC   students_data.`team3-chicago`.stg_location;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE DimFacilityType (
# MAGIC   d_facility_type_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   facility_type STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DimFacilityType (facility_type)
# MAGIC SELECT
# MAGIC   facility_type
# MAGIC FROM
# MAGIC   students_data.`team3-chicago`.stg_location;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cleaned AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN risk IS NULL OR trim(risk) = '' THEN 'Unknown'
# MAGIC       WHEN lower(risk) LIKE '%1%' OR lower(risk) LIKE '%high%' THEN 'Risk 1'
# MAGIC       WHEN lower(risk) LIKE '%2%' OR lower(risk) LIKE '%medium%' THEN 'Risk 2'
# MAGIC       WHEN lower(risk) LIKE '%3%' OR lower(risk) LIKE '%low%' THEN 'Risk 3'
# MAGIC       ELSE 'Unknown'
# MAGIC     END AS risk_category
# MAGIC   FROM students_data.`team3-chicago`.stg_location
# MAGIC )
# MAGIC SELECT DISTINCT risk_category
# MAGIC FROM cleaned;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE DimRisk (
# MAGIC   d_risk_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   risk_category STRING NOT NULL
# MAGIC );
# MAGIC
# MAGIC ALTER TABLE DimRisk ADD CONSTRAINT risk_category_check CHECK (risk_category IN ('Risk 1', 'Risk 2', 'Risk 3', 'Unknown'));
# MAGIC
# MAGIC INSERT INTO DimRisk (risk_category)
# MAGIC WITH cleaned AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN risk IS NULL OR trim(risk) = '' THEN 'Unknown'
# MAGIC       WHEN lower(risk) LIKE '%1%' OR lower(risk) LIKE '%high%' THEN 'Risk 1'
# MAGIC       WHEN lower(risk) LIKE '%2%' OR lower(risk) LIKE '%medium%' THEN 'Risk 2'
# MAGIC       WHEN lower(risk) LIKE '%3%' OR lower(risk) LIKE '%low%' THEN 'Risk 3'
# MAGIC       ELSE 'Unknown'
# MAGIC     END AS risk_category
# MAGIC   FROM students_data.`team3-chicago`.stg_location
# MAGIC )
# MAGIC SELECT DISTINCT risk_category FROM cleaned;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE DimBusiness (
# MAGIC   d_business_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   dba_name STRING,
# MAGIC   aka_name STRING,
# MAGIC   license_number STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DimBusiness (dba_name, aka_name, license_number)
# MAGIC SELECT
# MAGIC   dba_name,
# MAGIC   aka_name,
# MAGIC   license_number
# MAGIC FROM
# MAGIC   students_data.`team3-chicago`.stg_location;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE DimDate (
# MAGIC   d_date_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   date DATE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DimDate (date)
# MAGIC SELECT
# MAGIC   inspection_date
# MAGIC FROM
# MAGIC   students_data.`team3-chicago`.stg_inspection;
