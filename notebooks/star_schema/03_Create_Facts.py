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
# MAGIC -- LOAD FACT INSPECTION (DIM LOOKUPS ONLY)
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
# MAGIC   i.score
# MAGIC FROM students_data.`team3-chicago`.stg_inspection i
# MAGIC JOIN students_data.`team3-chicago`.stg_location sl
# MAGIC   ON i.license_number = sl.license_number
# MAGIC JOIN DimBusiness b
# MAGIC   ON b.license_number = sl.license_number
# MAGIC JOIN DimLocation l
# MAGIC   ON l.license_number = sl.license_number
# MAGIC JOIN DimFacilityType ft
# MAGIC   ON ft.facility_type_raw = sl.facility_type
# MAGIC JOIN DimRisk r
# MAGIC   ON r.risk_raw = sl.risk
# MAGIC JOIN DimDate d
# MAGIC   ON d.date = TRY_CAST(i.inspection_date AS DATE);
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- FACT INSPECTION VIOLATION (BRIDGE TABLE)
# MAGIC -- Grain: ONE ROW PER (INSPECTION, VIOLATION)
# MAGIC -- =====================================================
# MAGIC CREATE OR REPLACE TABLE FactInspectionViolation (
# MAGIC   f_inspection_id BIGINT NOT NULL,
# MAGIC   d_violation_id BIGINT NOT NULL,
# MAGIC   CONSTRAINT pk_factinspectionviolation PRIMARY KEY (f_inspection_id, d_violation_id)
# MAGIC );
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- LOAD FACT INSPECTION VIOLATION
# MAGIC -- =====================================================
# MAGIC INSERT INTO FactInspectionViolation (f_inspection_id, d_violation_id)
# MAGIC SELECT DISTINCT
# MAGIC   f.f_inspection_id,
# MAGIC   v.d_violation_id
# MAGIC FROM students_data.`team3-chicago`.stg_inspection i
# MAGIC JOIN FactInspection f
# MAGIC   ON f.inspection_id = i.inspection_id
# MAGIC LATERAL VIEW explode(split(i.violations, '\\|')) viol AS single_violation
# MAGIC JOIN DimViolation v
# MAGIC   ON v.violation_code = trim(regexp_extract(single_violation, '^\\s*(\\d+)\\.', 1))
# MAGIC WHERE i.violations IS NOT NULL
# MAGIC   AND trim(regexp_extract(single_violation, '^\\s*(\\d+)\\.', 1)) != '';