%sql
-- =====================================================
-- USE CATALOG & SCHEMA
-- =====================================================
USE CATALOG students_data;
USE SCHEMA `team3-chicago`;

-- =====================================================
-- FACT INSPECTION
-- Grain: ONE ROW PER INSPECTION
-- =====================================================
CREATE OR REPLACE TABLE FactInspection (
  f_inspection_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  d_business_id BIGINT NOT NULL,
  d_location_id BIGINT NOT NULL,
  d_facility_type_id BIGINT NOT NULL,
  d_risk_id BIGINT NOT NULL,
  d_date_id BIGINT NOT NULL,
  inspection_id STRING,
  inspection_type STRING,
  results STRING,
  inspection_score INT,
  CONSTRAINT pk_factinspection PRIMARY KEY (f_inspection_id)
);

-- =====================================================
-- LOAD FACT INSPECTION (DIM LOOKUPS ONLY)
-- =====================================================
INSERT INTO FactInspection (
  d_business_id,
  d_location_id,
  d_facility_type_id,
  d_risk_id,
  d_date_id,
  inspection_id,
  inspection_type,
  results,
  inspection_score
)
SELECT
  b.d_business_id,
  l.d_location_id,
  ft.d_facility_type_id,
  r.d_risk_id,
  d.d_date_id,
  i.inspection_id,
  i.inspection_type,
  i.results,
  i.score
FROM students_data.`team3-chicago`.stg_inspection i
JOIN students_data.`team3-chicago`.stg_location sl
  ON i.license_number = sl.license_number
JOIN DimBusiness b
  ON b.license_number = sl.license_number
JOIN DimLocation l
  ON l.license_number = sl.license_number
JOIN DimFacilityType ft
  ON ft.facility_type_raw = sl.facility_type
JOIN DimRisk r
  ON r.risk_raw = sl.risk
JOIN DimDate d
  ON d.date = TRY_CAST(i.inspection_date AS DATE);

-- =====================================================
-- FACT INSPECTION VIOLATION (BRIDGE TABLE)
-- Grain: ONE ROW PER (INSPECTION, VIOLATION)
-- =====================================================
CREATE OR REPLACE TABLE FactInspectionViolation (
  f_inspection_id BIGINT NOT NULL,
  d_violation_id BIGINT NOT NULL,
  CONSTRAINT pk_factinspectionviolation PRIMARY KEY (f_inspection_id, d_violation_id)
);

-- =====================================================
-- LOAD FACT INSPECTION VIOLATION
-- =====================================================
INSERT INTO FactInspectionViolation (f_inspection_id, d_violation_id)
SELECT DISTINCT
  f.f_inspection_id,
  v.d_violation_id
FROM students_data.`team3-chicago`.stg_inspection i
JOIN FactInspection f
  ON f.inspection_id = i.inspection_id
LATERAL VIEW explode(split(i.violations, '\\|')) viol AS single_violation
JOIN DimViolation v
  ON v.violation_code = trim(regexp_extract(single_violation, '^\\s*(\\d+)\\.', 1))
WHERE i.violations IS NOT NULL
  AND trim(regexp_extract(single_violation, '^\\s*(\\d+)\\.', 1)) != '';