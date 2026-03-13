# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG students_data;
# MAGIC USE SCHEMA `team3-chicago`;

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH cleaned AS (
# MAGIC   SELECT
# MAGIC     /* Clean address */
# MAGIC     initcap(trim(address)) AS address_clean,
# MAGIC
# MAGIC     /* Clean city */
# MAGIC     initcap(trim(city)) AS city_clean,
# MAGIC
# MAGIC     /* Clean state (force IL, drop invalid) */
# MAGIC     CASE
# MAGIC       WHEN upper(trim(state)) = 'IL' THEN 'IL'
# MAGIC       ELSE NULL
# MAGIC     END AS state_clean,
# MAGIC
# MAGIC     /* Clean ZIP: extract first 5 digits only */
# MAGIC     regexp_extract(zip, '([0-9]{5})', 1) AS zip_clean
# MAGIC   FROM students_data.`team3-chicago`.stg_location
# MAGIC ),
# MAGIC validated AS (
# MAGIC   SELECT
# MAGIC     address_clean,
# MAGIC     city_clean,
# MAGIC     state_clean,
# MAGIC     zip_clean
# MAGIC   FROM cleaned
# MAGIC   WHERE address_clean IS NOT NULL
# MAGIC     AND city_clean IS NOT NULL
# MAGIC     AND state_clean IS NOT NULL
# MAGIC     AND zip_clean IS NOT NULL
# MAGIC     AND length(zip_clean) = 5
# MAGIC )
# MAGIC SELECT DISTINCT *
# MAGIC FROM validated;

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE DimLocation (
# MAGIC   d_location_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   address STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   zip STRING
# MAGIC );

# MAGIC %sql
# MAGIC INSERT INTO DimLocation (address, city, state, zip)
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     initcap(trim(address)) AS address_raw,
# MAGIC     initcap(trim(city)) AS city_raw,
# MAGIC     upper(trim(state)) AS state_raw,
# MAGIC     regexp_extract(zip, '([0-9]{5})', 1) AS zip_raw
# MAGIC   FROM students_data.`team3-chicago`.stg_location
# MAGIC ),
# MAGIC
# MAGIC normalized AS (
# MAGIC   SELECT
# MAGIC     /* Standardize common street suffixes */
# MAGIC     REGEXP_REPLACE(address_raw, '\\bSt\\.?\\b', 'Street') AS address_step1,
# MAGIC     REGEXP_REPLACE(address_raw, '\\bAve\\.?\\b', 'Avenue') AS address_step2,
# MAGIC     REGEXP_REPLACE(address_raw, '\\bRd\\.?\\b', 'Road') AS address_step3,
# MAGIC     REGEXP_REPLACE(address_raw, '\\bBlvd\\.?\\b', 'Boulevard') AS address_step4,
# MAGIC
# MAGIC     REGEXP_REPLACE(city_raw, '\\s+', ' ') AS city_step1,
# MAGIC     state_raw AS state_step1,
# MAGIC     zip_raw
# MAGIC   FROM base
# MAGIC ),
# MAGIC
# MAGIC corrected AS (
# MAGIC   SELECT
# MAGIC     /* Apply spelling corrections if lookup table exists */
# MAGIC     COALESCE(
# MAGIC       REPLACE(address_step1, sc.wrong, sc.correct),
# MAGIC       address_step1
# MAGIC     ) AS address_clean,
# MAGIC
# MAGIC     CASE WHEN lower(city_step1) LIKE '%chicago%' THEN 'Chicago' ELSE NULL END AS city_clean,
# MAGIC     CASE WHEN state_step1 = 'IL' THEN 'IL' ELSE NULL END AS state_clean,
# MAGIC     zip_raw AS zip_clean
# MAGIC   FROM normalized n
# MAGIC   LEFT JOIN street_corrections sc
# MAGIC     ON n.address_step1 LIKE CONCAT('%', sc.wrong, '%')
# MAGIC ),
# MAGIC
# MAGIC validated AS (
# MAGIC   SELECT *
# MAGIC   FROM corrected
# MAGIC   WHERE address_clean IS NOT NULL
# MAGIC     AND city_clean IS NOT NULL
# MAGIC     AND state_clean IS NOT NULL
# MAGIC     AND zip_clean IS NOT NULL
# MAGIC     AND length(zip_clean) = 5
# MAGIC )
# MAGIC
# MAGIC SELECT DISTINCT address_clean, city_clean, state_clean, zip_clean
# MAGIC FROM validated;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO DimFacilityType (facility_type)
# MAGIC WITH cleaned AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN lower(facility_type) RLIKE 'restaurant|rest/|cafe|coffee|bakery|banquet|catering|deli|juice|smoothie|donut|sushi|protein|tea|food booth|food hall|kitchen|dining|hot dog|ice cream|gelato|popcorn|snack|tasting' THEN 'Food Service'
# MAGIC       WHEN lower(facility_type) RLIKE 'grocery|convenience|dollar|drug store|butcher|meat market|produce|health food|candy|general store|retail food|packaged|market' THEN 'Retail Food'
# MAGIC       WHEN lower(facility_type) RLIKE 'tavern|bar|liquor|tap room|wine' THEN 'Alcohol Service'
# MAGIC       WHEN lower(facility_type) RLIKE 'mobile|truck|pushcart|cart|vending|frozen dessert' THEN 'Mobile Food Vendor'
# MAGIC       WHEN lower(facility_type) RLIKE 'school|college|university|classroom|teaching|culinary school' THEN 'School / Educational Facility'
# MAGIC       WHEN lower(facility_type) RLIKE 'day care|daycare|after school|children|boys and girls club|kids' THEN 'Childcare Facility'
# MAGIC       WHEN lower(facility_type) RLIKE 'nursing home|long term care|assisted living|hospital|rehab|adult day' THEN 'Healthcare / Long-Term Care'
# MAGIC       WHEN lower(facility_type) RLIKE 'church|pantry|soup kitchen|shelter|community|non-profit|charity' THEN 'Religious / Community Facility'
# MAGIC       WHEN lower(facility_type) RLIKE 'clothing|cell phone|book store|furniture|gift shop|video store|hair salon|nail shop|spa|tobacco|store$' THEN 'Retail (Non-Food)'
# MAGIC       WHEN lower(facility_type) RLIKE 'gym|fitness|studio|golf|bowling|stadium|youth housing' THEN 'Fitness / Recreation'
# MAGIC       WHEN lower(facility_type) RLIKE 'commissary|warehouse|storage|distribution|cold storage' THEN 'Commissary / Warehouse'
# MAGIC       WHEN lower(facility_type) RLIKE 'poultry|slaughter|meat packing|butcher' THEN 'Meat / Poultry Processing'
# MAGIC       WHEN lower(facility_type) RLIKE 'theater|theatre|music venue|event|concert|wrigley|museum|gallery' THEN 'Entertainment Venue'
# MAGIC       WHEN lower(facility_type) RLIKE 'hotel|hostel|room service|lounge' THEN 'Hotel / Lodging'
# MAGIC       WHEN facility_type IS NULL OR trim(facility_type) = '' THEN 'Unknown / Other'
# MAGIC       ELSE 'Unknown / Other'
# MAGIC     END AS facility_type_clean
# MAGIC   FROM students_data.`team3-chicago`.stg_location
# MAGIC )
# MAGIC SELECT DISTINCT facility_type_clean
# MAGIC FROM cleaned;

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE DimFacilityType (
# MAGIC   d_facility_type_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   facility_type STRING
# MAGIC );
# MAGIC
# MAGIC %sql
# MAGIC INSERT INTO DimFacilityType (facility_type)
# MAGIC WITH cleaned AS (
# MAGIC   SELECT
# MAGIC     CASE
# MAGIC       WHEN lower(facility_type) RLIKE 'restaurant|rest/|cafe|coffee|bakery|banquet|catering|deli|juice|smoothie|donut|sushi|protein|tea|food booth|food hall|kitchen|dining|hot dog|ice cream|gelato|popcorn|snack|tasting' THEN 'Food Service'
# MAGIC       WHEN lower(facility_type) RLIKE 'grocery|convenience|dollar|drug store|butcher|meat market|produce|health food|candy|general store|retail food|packaged|market' THEN 'Retail Food'
# MAGIC       WHEN lower(facility_type) RLIKE 'tavern|bar|liquor|tap room|wine' THEN 'Alcohol Service'
# MAGIC       WHEN lower(facility_type) RLIKE 'mobile|truck|pushcart|cart|vending|frozen dessert' THEN 'Mobile Food Vendor'
# MAGIC       WHEN lower(facility_type) RLIKE 'school|college|university|classroom|teaching|culinary school' THEN 'School / Educational Facility'
# MAGIC       WHEN lower(facility_type) RLIKE 'day care|daycare|after school|children|boys and girls club|kids' THEN 'Childcare Facility'
# MAGIC       WHEN lower(facility_type) RLIKE 'nursing home|long term care|assisted living|hospital|rehab|adult day' THEN 'Healthcare / Long-Term Care'
# MAGIC       WHEN lower(facility_type) RLIKE 'church|pantry|soup kitchen|shelter|community|non-profit|charity' THEN 'Religious / Community Facility'
# MAGIC       WHEN lower(facility_type) RLIKE 'clothing|cell phone|book store|furniture|gift shop|video store|hair salon|nail shop|spa|tobacco|store$' THEN 'Retail (Non-Food)'
# MAGIC       WHEN lower(facility_type) RLIKE 'gym|fitness|studio|golf|bowling|stadium|youth housing' THEN 'Fitness / Recreation'
# MAGIC       WHEN lower(facility_type) RLIKE 'commissary|warehouse|storage|distribution|cold storage' THEN 'Commissary / Warehouse'
# MAGIC       WHEN lower(facility_type) RLIKE 'poultry|slaughter|meat packing|butcher' THEN 'Meat / Poultry Processing'
# MAGIC       WHEN lower(facility_type) RLIKE 'theater|theatre|music venue|event|concert|wrigley|museum|gallery' THEN 'Entertainment Venue'
# MAGIC       WHEN lower(facility_type) RLIKE 'hotel|hostel|room service|lounge' THEN 'Hotel / Lodging'
# MAGIC       WHEN facility_type IS NULL OR trim(facility_type) = '' THEN 'Unknown / Other'
# MAGIC       ELSE 'Unknown / Other'
# MAGIC     END AS facility_type_clean
# MAGIC   FROM students_data.`team3-chicago`.stg_location
# MAGIC )
# MAGIC SELECT DISTINCT facility_type_clean
# MAGIC FROM cleaned;

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
# MAGIC WITH cleaned AS (
# MAGIC   SELECT
# MAGIC     TRY_CAST(inspection_date AS DATE) AS inspection_date_clean
# MAGIC   FROM students_data.`team3-chicago`.stg_inspection
# MAGIC ),
# MAGIC validated AS (
# MAGIC   SELECT
# MAGIC     inspection_date_clean
# MAGIC   FROM cleaned
# MAGIC   WHERE inspection_date_clean IS NOT NULL
# MAGIC     AND inspection_date_clean >= '2000-01-01'
# MAGIC     AND inspection_date_clean <= current_date()
# MAGIC )
# MAGIC SELECT DISTINCT inspection_date_clean
# MAGIC FROM validated;

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE DimDate (
# MAGIC   d_date_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   date DATE NOT NULL
# MAGIC );

# MAGIC %sql
# MAGIC INSERT INTO DimDate (date)
# MAGIC WITH cleaned AS (
# MAGIC   SELECT
# MAGIC     TRY_CAST(inspection_date AS DATE) AS inspection_date_clean
# MAGIC   FROM students_data.`team3-chicago`.stg_inspection
# MAGIC ),
# MAGIC validated AS (
# MAGIC   SELECT
# MAGIC     inspection_date_clean
# MAGIC   FROM cleaned
# MAGIC   WHERE inspection_date_clean IS NOT NULL
# MAGIC     AND inspection_date_clean >= '2000-01-01'
# MAGIC     AND inspection_date_clean <= current_date()
# MAGIC )
# MAGIC SELECT DISTINCT inspection_date_clean
# MAGIC FROM validated;
