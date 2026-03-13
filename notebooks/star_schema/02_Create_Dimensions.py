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
# MAGIC
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

# COMMAND ----------

# DBTITLE 1,Create DimViolation
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE DimViolation (
# MAGIC   d_violation_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   violation_code STRING,
# MAGIC   comment STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO DimViolation (violation_code, comment)
# MAGIC WITH exploded AS (
# MAGIC   SELECT
# MAGIC     explode(split(violations, '\\|')) AS single_violation
# MAGIC   FROM students_data.`team3-chicago`.stg_inspection
# MAGIC   WHERE violations IS NOT NULL AND trim(violations) != ''
# MAGIC ),
# MAGIC parsed AS (
# MAGIC   SELECT
# MAGIC     trim(regexp_extract(single_violation, '^\\s*(\\d+)\\.', 1)) AS violation_code,
# MAGIC     trim(regexp_extract(single_violation, '- Comments:\\s*(.*)', 1)) AS comment
# MAGIC   FROM exploded
# MAGIC )
# MAGIC SELECT DISTINCT violation_code, comment
# MAGIC FROM parsed
# MAGIC WHERE violation_code != '';
