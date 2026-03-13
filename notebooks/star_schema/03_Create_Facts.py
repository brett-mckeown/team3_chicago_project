# Databricks notebook source
# COMMAND ----------
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE FactInspection (
# MAGIC   f_inspection_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   d_business_id BIGINT,
# MAGIC   d_location_id BIGINT,
# MAGIC   d_risk_id BIGINT,
# MAGIC   d_facility_type_id BIGINT,
# MAGIC   d_date_id BIGINT,
# MAGIC   inspection_id STRING,
# MAGIC   inspection_type STRING,
# MAGIC   result STRING,
# MAGIC   num_violations INT,
# MAGIC   violation_codes STRING
# MAGIC );
# MAGIC 
# MAGIC INSERT INTO FactInspection (
# MAGIC   d_business_id,
# MAGIC   d_location_id,
# MAGIC   d_risk_id,
# MAGIC   d_facility_type_id,
# MAGIC   d_date_id,
# MAGIC   inspection_id,
# MAGIC   inspection_type,
# MAGIC   result,
# MAGIC   num_violations,
# MAGIC   violation_codes
# MAGIC )
# MAGIC SELECT
# MAGIC   b.d_business_id,
# MAGIC   l.d_location_id,
# MAGIC   r.d_risk_id,
# MAGIC   f.d_facility_type_id,
# MAGIC   d.d_date_id,
# MAGIC   i.inspection_id,
# MAGIC   i.inspection_type,
# MAGIC   i.results,
# MAGIC   CASE
# MAGIC     WHEN i.violation_codes IS NULL OR trim(i.violation_codes) = '' THEN 0
# MAGIC     ELSE size(split(i.violation_codes, ','))
# MAGIC   END AS num_violations,
# MAGIC   i.violation_codes
# MAGIC FROM students_data.`team3-chicago`.stg_inspection i
# MAGIC LEFT JOIN DimBusiness b
# MAGIC   ON i.license_number = b.license_number
# MAGIC LEFT JOIN DimLocation l
# MAGIC   ON initcap(trim(i.address)) = l.address
# MAGIC  AND initcap(trim(i.city)) = l.city
# MAGIC  AND upper(trim(i.state)) = l.state
# MAGIC  AND regexp_extract(i.zip, '([0-9]{5})', 1) = l.zip
# MAGIC LEFT JOIN DimRisk r
# MAGIC   ON r.risk_category = CASE
# MAGIC     WHEN lower(i.risk) LIKE '%1%' THEN 'Risk 1'
# MAGIC     WHEN lower(i.risk) LIKE '%2%' THEN 'Risk 2'
# MAGIC     WHEN lower(i.risk) LIKE '%3%' THEN 'Risk 3'
# MAGIC     ELSE 'Unknown'
# MAGIC   END
# MAGIC LEFT JOIN DimFacilityType f
# MAGIC   ON f.facility_type = CASE
# MAGIC     WHEN lower(i.facility_type) RLIKE 'restaurant|rest/|cafe|coffee|bakery|banquet|catering|deli|juice|smoothie|donut|sushi|protein|tea|food booth|food hall|kitchen|dining|hot dog|ice cream|gelato|popcorn|snack|tasting' THEN 'Food Service'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'grocery|convenience|dollar|drug store|butcher|meat market|produce|health food|candy|general store|retail food|packaged|market' THEN 'Retail Food'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'tavern|bar|liquor|tap room|wine' THEN 'Alcohol Service'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'mobile|truck|pushcart|cart|vending|frozen dessert' THEN 'Mobile Food Vendor'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'school|college|university|classroom|teaching|culinary school' THEN 'School / Educational Facility'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'day care|daycare|after school|children|boys and girls club|kids' THEN 'Childcare Facility'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'nursing home|long term care|assisted living|hospital|rehab|adult day' THEN 'Healthcare / Long-Term Care'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'church|pantry|soup kitchen|shelter|community|non-profit|charity' THEN 'Religious / Community Facility'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'clothing|cell phone|book store|furniture|gift shop|video store|hair salon|nail shop|spa|tobacco|store$' THEN 'Retail (Non-Food)'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'gym|fitness|studio|golf|bowling|stadium|youth housing' THEN 'Fitness / Recreation'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'commissary|warehouse|storage|distribution|cold storage' THEN 'Commissary / Warehouse'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'poultry|slaughter|meat packing|butcher' THEN 'Meat / Poultry Processing'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'theater|theatre|music venue|event|concert|wrigley|museum|gallery' THEN 'Entertainment Venue'
# MAGIC     WHEN lower(i.facility_type) RLIKE 'hotel|hostel|room service|lounge' THEN 'Hotel / Lodging'
# MAGIC     ELSE 'Unknown / Other'
# MAGIC   END
# MAGIC LEFT JOIN DimDate d
# MAGIC   ON d.date = TRY_CAST(i.inspection_date AS DATE);