# Databricks notebook source
# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- USE CATALOG & SCHEMA
# MAGIC -- =====================================================
# MAGIC USE CATALOG students_data;
# MAGIC USE SCHEMA `team3-chicago`;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- FACT INSPECTION
# MAGIC -- Grain: ONE ROW PER INSPECTION
# MAGIC -- =====================================================
# MAGIC CREATE OR REPLACE TABLE FactInspection (
# MAGIC   f_inspection_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
# MAGIC   d_business_id BIGINT NOT NULL,
# MAGIC   d_location_id BIGINT NOT NULL,
# MAGIC   d_facility_type_id BIGINT NOT NULL,
# MAGIC   d_risk_id BIGINT NOT NULL,
# MAGIC   d_date_id BIGINT NOT NULL,
# MAGIC   inspection_id STRING,
# MAGIC   inspection_type STRING,
# MAGIC   results STRING,
# MAGIC   inspection_score INT,
# MAGIC   CONSTRAINT pk_factinspection PRIMARY KEY (f_inspection_id)
# MAGIC );
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- LOAD FACT INSPECTION
# MAGIC -- =====================================================
# MAGIC INSERT INTO FactInspection (
# MAGIC   d_business_id,
# MAGIC   d_location_id,
# MAGIC   d_facility_type_id,
# MAGIC   d_risk_id,
# MAGIC   d_date_id,
# MAGIC   inspection_id,
# MAGIC   inspection_type,
# MAGIC   results,
# MAGIC   inspection_score
# MAGIC )
# MAGIC SELECT
# MAGIC   b.d_business_id,
# MAGIC   l.d_location_id,
# MAGIC   ft.d_facility_type_id,
# MAGIC   r.d_risk_id,
# MAGIC   d.d_date_id,
# MAGIC   i.inspection_id,
# MAGIC   i.inspection_type,
# MAGIC   i.results,
# MAGIC   CAST(NULL AS INT)
# MAGIC FROM students_data.`team3-chicago`.stg_inspection i
# MAGIC JOIN students_data.`team3-chicago`.stg_location sl
# MAGIC   ON sl.inspection_id = i.inspection_id
# MAGIC JOIN DimBusiness b
# MAGIC   ON b.license_number = trim(sl.license_number)
# MAGIC   AND b.dba_name <=> initcap(trim(sl.dba_name))
# MAGIC   AND b.aka_name <=> initcap(trim(sl.aka_name))
# MAGIC JOIN DimLocation l
# MAGIC   ON l.address = REGEXP_REPLACE(initcap(trim(sl.address)), '\\bSt\\.?\\b', 'Street')
# MAGIC   AND l.zip = regexp_extract(sl.zip, '([0-9]{5})', 1)
# MAGIC JOIN DimFacilityType ft
# MAGIC   ON ft.facility_type_raw = sl.facility_type
# MAGIC JOIN DimRisk r
# MAGIC   ON r.risk_raw <=> sl.risk
# MAGIC JOIN DimDate d
# MAGIC   ON d.date = i.inspection_date;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- FACT INSPECTION VIOLATION (BRIDGE TABLE)
# MAGIC -- Grain: ONE ROW PER (INSPECTION, VIOLATION)
# MAGIC -- Stores the business-specific comment per violation
# MAGIC -- =====================================================
# MAGIC CREATE OR REPLACE TABLE FactInspectionViolation (
# MAGIC   f_inspection_id BIGINT NOT NULL,
# MAGIC   d_violation_id BIGINT NOT NULL,
# MAGIC   comment STRING,
# MAGIC   CONSTRAINT pk_factinspectionviolation PRIMARY KEY (f_inspection_id, d_violation_id)
# MAGIC );
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- LOAD FACT INSPECTION VIOLATION
# MAGIC -- Parses each violation: code, description, and comment
# MAGIC -- Joins DimViolation on (code, description)
# MAGIC -- =====================================================
# MAGIC INSERT INTO FactInspectionViolation (f_inspection_id, d_violation_id, comment)
# MAGIC SELECT DISTINCT
# MAGIC   expanded.f_inspection_id,
# MAGIC   v.d_violation_id,
# MAGIC   expanded.comment
# MAGIC FROM (
# MAGIC   SELECT
# MAGIC     f_inspection_id,
# MAGIC     trim(regexp_extract(cleaned, '^(\\d+)\\.', 1)) AS violation_code,
# MAGIC     trim(regexp_extract(cleaned, '^\\d+\\.\\s*(.+?)\\s*-\\s*Comments:', 1)) AS violation_description,
# MAGIC     trim(regexp_extract(cleaned, '-\\s*Comments:\\s*(.*)', 1)) AS comment
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       f.f_inspection_id,
# MAGIC       trim(BOTH '"' FROM trim(single_violation)) AS cleaned
# MAGIC     FROM students_data.`team3-chicago`.stg_inspection i
# MAGIC     JOIN FactInspection f
# MAGIC       ON f.inspection_id = i.inspection_id
# MAGIC     LATERAL VIEW explode(split(i.violations, '[|]')) viol AS single_violation
# MAGIC     WHERE i.violations IS NOT NULL
# MAGIC       AND length(trim(single_violation)) > 5
# MAGIC   )
# MAGIC ) expanded
# MAGIC JOIN DimViolation v
# MAGIC   ON v.violation_code = expanded.violation_code
# MAGIC   AND v.violation_description = expanded.violation_description
# MAGIC WHERE expanded.violation_code != '';
