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
WITH cleaned AS (
  SELECT
    CASE
      WHEN risk IS NULL OR trim(risk) = '' THEN 'Unknown'
      WHEN lower(risk) LIKE '%1%' OR lower(risk) LIKE '%high%' THEN 'Risk 1'
      WHEN lower(risk) LIKE '%2%' OR lower(risk) LIKE '%medium%' THEN 'Risk 2'
      WHEN lower(risk) LIKE '%3%' OR lower(risk) LIKE '%low%' THEN 'Risk 3'
      ELSE 'Unknown'
    END AS risk_category
  FROM students_data.`team3-chicago`.stg_location
)
SELECT DISTINCT risk_category
FROM cleaned;


CREATE OR REPLACE TABLE DimRisk (
  d_risk_id BIGINT GENERATED ALWAYS AS IDENTITY,
  risk_category STRING NOT NULL
  CONSTRAINT risk_category_check CHECK (risk_category IN ('Risk 1', 'Risk 2', 'Risk 3', 'Unknown'))
  CONSTRAINT risk_category_unique UNIQUE (risk_category)
);

INSERT INTO DimRisk (risk_category)
WITH cleaned AS (
  SELECT
    CASE
      WHEN risk IS NULL OR trim(risk) = '' THEN 'Unknown'
      WHEN lower(risk) LIKE '%1%' OR lower(risk) LIKE '%high%' THEN 'Risk 1'
      WHEN lower(risk) LIKE '%2%' OR lower(risk) LIKE '%medium%' THEN 'Risk 2'
      WHEN lower(risk) LIKE '%3%' OR lower(risk) LIKE '%low%' THEN 'Risk 3'
      ELSE 'Unknown'
    END AS risk_category
  FROM students_data.`team3-chicago`.stg_location
)
SELECT DISTINCT risk_category FROM cleaned;


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
