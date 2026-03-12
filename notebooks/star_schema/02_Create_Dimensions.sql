-- Databricks notebook source
USE CATALOG students_data;
USE SCHEMA `team3-chicago`;

-- COMMAND ----------

CREATE OR REPLACE TABLE DimLocation (
  d_location_id BIGINT GENERATED ALWAYS AS IDENTITY,
  address STRING,
  city STRING,
  state STRING,
  zip STRING
);

INSERT INTO DimLocation (address, city, state, zip)
SELECT
  address,
  city,
  state,
  zip
FROM
  students_data.`team3-chicago`.stg_location;

-- COMMAND ----------

CREATE OR REPLACE TABLE DimFacilityType (
  d_facility_type_id BIGINT GENERATED ALWAYS AS IDENTITY,
  facility_type STRING
);

INSERT INTO DimFacilityType (facility_type)
SELECT
  facility_type
FROM
  students_data.`team3-chicago`.stg_location;

-- COMMAND ----------

CREATE OR REPLACE TABLE DimRisk (
  d_risk_id BIGINT GENERATED ALWAYS AS IDENTITY,
  risk_category STRING
);

INSERT INTO DimRisk (risk_category)
SELECT
  risk
FROM
  students_data.`team3-chicago`.stg_location;

-- COMMAND ----------

CREATE OR REPLACE TABLE DimBusiness (
  d_business_id BIGINT GENERATED ALWAYS AS IDENTITY,
  dba_name STRING,
  aka_name STRING,
  license_number STRING
);

INSERT INTO DimBusiness (dba_name, aka_name, license_number)
SELECT
  dba_name,
  aka_name,
  license_number
FROM
  students_data.`team3-chicago`.stg_location;

-- COMMAND ----------

CREATE OR REPLACE TABLE DimDate (
  d_date_id BIGINT GENERATED ALWAYS AS IDENTITY,
  date DATE
);

INSERT INTO DimDate (date)
SELECT
  inspection_date
FROM
  students_data.`team3-chicago`.stg_inspection;
