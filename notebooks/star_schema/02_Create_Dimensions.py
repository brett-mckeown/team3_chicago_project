%sql
-- =====================================================
-- USE CATALOG & SCHEMA
-- =====================================================
USE CATALOG students_data;
USE SCHEMA `team3-chicago`;

-- =====================================================
-- DIM LOCATION (FIXED: add license_number as natural key)
-- =====================================================
CREATE OR REPLACE TABLE DimLocation (
  d_location_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  license_number STRING NOT NULL,
  address STRING,
  city STRING,
  state STRING,
  zip STRING,
  latitude DOUBLE,  
  longitude DOUBLE,
  CONSTRAINT pk_location PRIMARY KEY (d_location_id)
);

INSERT INTO DimLocation (license_number, address, city, state, zip)
SELECT DISTINCT
  trim(license_number) AS license_number,
  REGEXP_REPLACE(initcap(trim(address)), '\\bSt\\.?\\b', 'Street') AS address,
  'Chicago' AS city,
  'IL' AS state,
  regexp_extract(zip, '([0-9]{5})', 1) AS zip
FROM students_data.`team3-chicago`.stg_location
WHERE license_number IS NOT NULL
  AND regexp_extract(zip, '([0-9]{5})', 1) IS NOT NULL;
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;

-- =====================================================
-- DIM FACILITY TYPE (FIXED: preserve raw + cleaned value)
-- =====================================================
CREATE OR REPLACE TABLE DimFacilityType (
  d_facility_type_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  facility_type_raw STRING NOT NULL,
  facility_type STRING NOT NULL,
  CONSTRAINT pk_facility_type PRIMARY KEY (d_facility_type_id)
);

INSERT INTO DimFacilityType (facility_type_raw, facility_type)
SELECT DISTINCT
  facility_type AS facility_type_raw,
  CASE
    WHEN lower(facility_type) RLIKE 'restaurant|rest/|cafe|coffee|bakery|banquet|catering|deli|juice|smoothie|donut|sushi|protein|tea|food booth|food hall|kitchen|dining|hot dog|ice cream|gelato|popcorn|snack|tasting' THEN 'Food Service'
    WHEN lower(facility_type) RLIKE 'grocery|convenience|dollar|drug store|butcher|meat market|produce|health food|candy|general store|retail food|packaged|market' THEN 'Retail Food'
    WHEN lower(facility_type) RLIKE 'tavern|bar|liquor|tap room|wine' THEN 'Alcohol Service'
    WHEN lower(facility_type) RLIKE 'mobile|truck|pushcart|cart|vending|frozen dessert' THEN 'Mobile Food Vendor'
    WHEN lower(facility_type) RLIKE 'school|college|university|classroom|teaching|culinary school' THEN 'School / Educational Facility'
    WHEN lower(facility_type) RLIKE 'day care|daycare|after school|children|boys and girls club|kids' THEN 'Childcare Facility'
    WHEN lower(facility_type) RLIKE 'nursing home|long term care|assisted living|hospital|rehab|adult day' THEN 'Healthcare / Long-Term Care'
    WHEN lower(facility_type) RLIKE 'church|pantry|soup kitchen|shelter|community|non-profit|charity' THEN 'Religious / Community Facility'
    WHEN lower(facility_type) RLIKE 'clothing|cell phone|book store|furniture|gift shop|video store|hair salon|nail shop|spa|tobacco|store$' THEN 'Retail (Non-Food)'
    WHEN lower(facility_type) RLIKE 'gym|fitness|studio|golf|bowling|stadium|youth housing' THEN 'Fitness / Recreation'
    WHEN lower(facility_type) RLIKE 'commissary|warehouse|storage|distribution|cold storage' THEN 'Commissary / Warehouse'
    WHEN lower(facility_type) RLIKE 'poultry|slaughter|meat packing|butcher' THEN 'Meat / Poultry Processing'
    WHEN lower(facility_type) RLIKE 'theater|theatre|music venue|event|concert|wrigley|museum|gallery' THEN 'Entertainment Venue'
    WHEN lower(facility_type) RLIKE 'hotel|hostel|room service|lounge' THEN 'Hotel / Lodging'
    ELSE 'Unknown / Other'
  END AS facility_type
FROM students_data.`team3-chicago`.stg_location
WHERE facility_type IS NOT NULL;

-- =====================================================
-- DIM RISK (FIXED: preserve raw + cleaned value)
-- =====================================================
CREATE OR REPLACE TABLE DimRisk (
  d_risk_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  risk_raw STRING NOT NULL,
  risk_category STRING NOT NULL,
  CONSTRAINT pk_risk PRIMARY KEY (d_risk_id),
  CONSTRAINT risk_category_check CHECK (risk_category IN ('Risk 1','Risk 2','Risk 3','Unknown'))
);

INSERT INTO DimRisk (risk_raw, risk_category)
SELECT DISTINCT
  risk AS risk_raw,
  CASE
    WHEN risk IS NULL OR trim(risk) = '' THEN 'Unknown'
    WHEN lower(risk) LIKE '%1%' OR lower(risk) LIKE '%high%' THEN 'Risk 1'
    WHEN lower(risk) LIKE '%2%' OR lower(risk) LIKE '%medium%' THEN 'Risk 2'
    WHEN lower(risk) LIKE '%3%' OR lower(risk) LIKE '%low%' THEN 'Risk 3'
    ELSE 'Unknown'
  END AS risk_category
FROM students_data.`team3-chicago`.stg_location;

-- =====================================================
-- DIM BUSINESS (MINOR HARDENING)
-- =====================================================
CREATE OR REPLACE TABLE DimBusiness (
  d_business_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  dba_name STRING,
  aka_name STRING,
  license_number STRING NOT NULL,
  CONSTRAINT pk_business PRIMARY KEY (d_business_id)
);

INSERT INTO DimBusiness (dba_name, aka_name, license_number)
SELECT DISTINCT
  initcap(trim(dba_name)) AS dba_name,
  initcap(trim(aka_name)) AS aka_name,
  trim(license_number) AS license_number
FROM students_data.`team3-chicago`.stg_location
WHERE license_number IS NOT NULL;

-- =====================================================
-- DIM DATE (FIXED: real date dimension)
-- =====================================================
CREATE OR REPLACE TABLE DimDate (
  d_date_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  date DATE NOT NULL,
  year INT,
  month INT,
  day INT,
  day_of_week STRING,
  CONSTRAINT pk_date PRIMARY KEY (d_date_id)
);

INSERT INTO DimDate (date, year, month, day, day_of_week)
SELECT DISTINCT
  d,
  year(d),
  month(d),
  day(d),
  date_format(d, 'EEEE')
FROM (
  SELECT TRY_CAST(inspection_date AS DATE) AS d
  FROM students_data.`team3-chicago`.stg_inspection
)
WHERE d IS NOT NULL
  AND d >= '2000-01-01'
  AND d <= current_date();

-- =====================================================
-- DIM VIOLATION (FIXED: one row per violation code)
-- =====================================================
CREATE OR REPLACE TABLE DimViolation (
  d_violation_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
  violation_code STRING NOT NULL,
  CONSTRAINT pk_violation PRIMARY KEY (d_violation_id)
);

INSERT INTO DimViolation (violation_code)
SELECT DISTINCT
  trim(regexp_extract(single_violation, '^\\s*(\\d+)\\.', 1)) AS violation_code
FROM students_data.`team3-chicago`.stg_inspection
LATERAL VIEW explode(split(violations, '\\|')) viol AS single_violation
WHERE violations IS NOT NULL
  AND trim(regexp_extract(single_violation, '^\\s*(\\d+)\\.', 1)) != '';