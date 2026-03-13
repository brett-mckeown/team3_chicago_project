# Databricks notebook source
%sql
USE CATALOG students_data;
USE SCHEMA `team3-chicago`;

# COMMAND ----------

%sql
CREATE OR REPLACE TABLE stg_location AS
SELECT
    dba_name,
    aka_name,
    license_number,
    facility_type,
    risk,
    address,
    city,
    state,
    zip
FROM 
    students_data.`team3-chicago`.food_inspections_bronze

# COMMAND ----------

%sql
CREATE OR REPLACE TABLE stg_inspection AS
SELECT
    inspection_id,
    to_date(trim(inspection_date), 'MM/dd/yyyy') AS inspection_date,
    inspection_type,
    results,
    violations,
    latitude,
    longitude,
    location
FROM 
    students_data.`team3-chicago`.food_inspections_bronze

# COMMAND ----------




