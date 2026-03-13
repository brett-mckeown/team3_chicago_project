# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- USE CATALOG & SCHEMA
# MAGIC -- =====================================================
# MAGIC USE CATALOG students_data;
# MAGIC USE SCHEMA `team3-chicago`;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- DIM LOCATION (FIXED: add license_number as natural key)
# MAGIC -- =====================================================
# MAGIC CREATE OR REPLACE TABLE DimLocation (
# MAGIC   d_location_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
# MAGIC   license_number STRING NOT NULL,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   zip STRING,
# MAGIC   CONSTRAINT pk_location PRIMARY KEY (d_location_id)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DimLocation (license_number, address, city, state, zip)
# MAGIC SELECT DISTINCT
# MAGIC   trim(license_number) AS license_number,
# MAGIC   REGEXP_REPLACE(initcap(trim(address)), '\\bSt\\.?\\b', 'Street') AS address,
# MAGIC   'Chicago' AS city,
# MAGIC   'IL' AS state,
# MAGIC   regexp_extract(zip, '([0-9]{5})', 1) AS zip
# MAGIC FROM students_data.`team3-chicago`.stg_location
# MAGIC WHERE license_number IS NOT NULL
# MAGIC   AND regexp_extract(zip, '([0-9]{5})', 1) IS NOT NULL;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- DIM FACILITY TYPE (FIXED: preserve raw + cleaned value)
# MAGIC -- =====================================================
# MAGIC CREATE OR REPLACE TABLE DimFacilityType (
# MAGIC   d_facility_type_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
# MAGIC   facility_type_raw STRING NOT NULL,
# MAGIC   facility_type STRING NOT NULL,
# MAGIC   CONSTRAINT pk_facility_type PRIMARY KEY (d_facility_type_id)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DimFacilityType (facility_type_raw, facility_type)
# MAGIC SELECT DISTINCT
# MAGIC   facility_type AS facility_type_raw,
# MAGIC   CASE
# MAGIC     WHEN lower(facility_type) RLIKE 'restaurant|rest/|cafe|coffee|bakery|banquet|catering|deli|juice|smoothie|donut|sushi|protein|tea|food booth|food hall|kitchen|dining|hot dog|ice cream|gelato|popcorn|snack|tasting' THEN 'Food Service'
# MAGIC     WHEN lower(facility_type) RLIKE 'grocery|convenience|dollar|drug store|butcher|meat market|produce|health food|candy|general store|retail food|packaged|market' THEN 'Retail Food'
# MAGIC     WHEN lower(facility_type) RLIKE 'tavern|bar|liquor|tap room|wine' THEN 'Alcohol Service'
# MAGIC     WHEN lower(facility_type) RLIKE 'mobile|truck|pushcart|cart|vending|frozen dessert' THEN 'Mobile Food Vendor'
# MAGIC     WHEN lower(facility_type) RLIKE 'school|college|university|classroom|teaching|culinary school' THEN 'School / Educational Facility'
# MAGIC     WHEN lower(facility_type) RLIKE 'day care|daycare|after school|children|boys and girls club|kids' THEN 'Childcare Facility'
# MAGIC     WHEN lower(facility_type) RLIKE 'nursing home|long term care|assisted living|hospital|rehab|adult day' THEN 'Healthcare / Long-Term Care'
# MAGIC     WHEN lower(facility_type) RLIKE 'church|pantry|soup kitchen|shelter|community|non-profit|charity' THEN 'Religious / Community Facility'
# MAGIC     WHEN lower(facility_type) RLIKE 'clothing|cell phone|book store|furniture|gift shop|video store|hair salon|nail shop|spa|tobacco|store$' THEN 'Retail (Non-Food)'
# MAGIC     WHEN lower(facility_type) RLIKE 'gym|fitness|studio|golf|bowling|stadium|youth housing' THEN 'Fitness / Recreation'
# MAGIC     WHEN lower(facility_type) RLIKE 'commissary|warehouse|storage|distribution|cold storage' THEN 'Commissary / Warehouse'
# MAGIC     WHEN lower(facility_type) RLIKE 'poultry|slaughter|meat packing|butcher' THEN 'Meat / Poultry Processing'
# MAGIC     WHEN lower(facility_type) RLIKE 'theater|theatre|music venue|event|concert|wrigley|museum|gallery' THEN 'Entertainment Venue'
# MAGIC     WHEN lower(facility_type) RLIKE 'hotel|hostel|room service|lounge' THEN 'Hotel / Lodging'
# MAGIC     ELSE 'Unknown / Other'
# MAGIC   END AS facility_type
# MAGIC FROM students_data.`team3-chicago`.stg_location
# MAGIC WHERE facility_type IS NOT NULL;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- DIM RISK (FIXED: preserve raw + cleaned value)
# MAGIC -- =====================================================
# MAGIC CREATE OR REPLACE TABLE DimRisk (
# MAGIC   d_risk_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
# MAGIC   risk_raw STRING NOT NULL,
# MAGIC   risk_category STRING NOT NULL,
# MAGIC   CONSTRAINT pk_risk PRIMARY KEY (d_risk_id),
# MAGIC   CONSTRAINT risk_category_check CHECK (risk_category IN ('Risk 1','Risk 2','Risk 3','Unknown'))
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DimRisk (risk_raw, risk_category)
# MAGIC SELECT DISTINCT
# MAGIC   risk AS risk_raw,
# MAGIC   CASE
# MAGIC     WHEN risk IS NULL OR trim(risk) = '' THEN 'Unknown'
# MAGIC     WHEN lower(risk) LIKE '%1%' OR lower(risk) LIKE '%high%' THEN 'Risk 1'
# MAGIC     WHEN lower(risk) LIKE '%2%' OR lower(risk) LIKE '%medium%' THEN 'Risk 2'
# MAGIC     WHEN lower(risk) LIKE '%3%' OR lower(risk) LIKE '%low%' THEN 'Risk 3'
# MAGIC     ELSE 'Unknown'
# MAGIC   END AS risk_category
# MAGIC FROM students_data.`team3-chicago`.stg_location;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- DIM BUSINESS (MINOR HARDENING)
# MAGIC -- =====================================================
# MAGIC CREATE OR REPLACE TABLE DimBusiness (
# MAGIC   d_business_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
# MAGIC   dba_name STRING,
# MAGIC   aka_name STRING,
# MAGIC   license_number STRING NOT NULL,
# MAGIC   CONSTRAINT pk_business PRIMARY KEY (d_business_id)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DimBusiness (dba_name, aka_name, license_number)
# MAGIC SELECT DISTINCT
# MAGIC   initcap(trim(dba_name)) AS dba_name,
# MAGIC   initcap(trim(aka_name)) AS aka_name,
# MAGIC   trim(license_number) AS license_number
# MAGIC FROM students_data.`team3-chicago`.stg_location
# MAGIC WHERE license_number IS NOT NULL;
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- DIM DATE (FIXED: real date dimension)
# MAGIC -- =====================================================
# MAGIC CREATE OR REPLACE TABLE DimDate (
# MAGIC   d_date_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
# MAGIC   date DATE NOT NULL,
# MAGIC   year INT,
# MAGIC   month INT,
# MAGIC   day INT,
# MAGIC   day_of_week STRING,
# MAGIC   CONSTRAINT pk_date PRIMARY KEY (d_date_id)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DimDate (date, year, month, day, day_of_week)
# MAGIC SELECT DISTINCT
# MAGIC   d,
# MAGIC   year(d),
# MAGIC   month(d),
# MAGIC   day(d),
# MAGIC   date_format(d, 'EEEE')
# MAGIC FROM (
# MAGIC   SELECT TRY_CAST(inspection_date AS DATE) AS d
# MAGIC   FROM students_data.`team3-chicago`.stg_inspection
# MAGIC )
# MAGIC WHERE d IS NOT NULL
# MAGIC   AND d >= '2000-01-01'
# MAGIC   AND d <= current_date();
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- DIM VIOLATION (FIXED: one row per violation code)
# MAGIC -- =====================================================
# MAGIC CREATE OR REPLACE TABLE DimViolation (
# MAGIC   d_violation_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY,
# MAGIC   violation_code STRING NOT NULL,
# MAGIC   CONSTRAINT pk_violation PRIMARY KEY (d_violation_id)
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DimViolation (violation_code)
# MAGIC SELECT DISTINCT
# MAGIC   trim(regexp_extract(single_violation, '^\\s*(\\d+)\\.', 1)) AS violation_code
# MAGIC FROM students_data.`team3-chicago`.stg_inspection
# MAGIC LATERAL VIEW explode(split(violations, '\\|')) viol AS single_violation
# MAGIC WHERE violations IS NOT NULL
# MAGIC   AND trim(regexp_extract(single_violation, '^\\s*(\\d+)\\.', 1)) != '';