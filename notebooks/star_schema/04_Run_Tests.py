# Databricks notebook source
# DBTITLE 1,Star Schema Overview — Row Counts
# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- STAR SCHEMA OVERVIEW: Verify all tables loaded
# MAGIC -- =====================================================
# MAGIC SELECT 'FactInspection'          AS table_name, COUNT(*) AS row_count FROM students_data.`team3-chicago`.factinspection
# MAGIC UNION ALL
# MAGIC SELECT 'FactInspectionViolation', COUNT(*) FROM students_data.`team3-chicago`.factinspectionviolation
# MAGIC UNION ALL
# MAGIC SELECT 'DimBusiness',             COUNT(*) FROM students_data.`team3-chicago`.dimbusiness
# MAGIC UNION ALL
# MAGIC SELECT 'DimLocation',             COUNT(*) FROM students_data.`team3-chicago`.dimlocation
# MAGIC UNION ALL
# MAGIC SELECT 'DimFacilityType',         COUNT(*) FROM students_data.`team3-chicago`.dimfacilitytype
# MAGIC UNION ALL
# MAGIC SELECT 'DimRisk',                 COUNT(*) FROM students_data.`team3-chicago`.dimrisk
# MAGIC UNION ALL
# MAGIC SELECT 'DimDate',                 COUNT(*) FROM students_data.`team3-chicago`.dimdate
# MAGIC UNION ALL
# MAGIC SELECT 'DimViolation',            COUNT(*) FROM students_data.`team3-chicago`.dimviolation
# MAGIC ORDER BY row_count DESC

# COMMAND ----------

# DBTITLE 1,Inspections by Year — Data Completeness
# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- INSPECTIONS BY YEAR: Check temporal coverage
# MAGIC -- =====================================================
# MAGIC SELECT
# MAGIC   d.year,
# MAGIC   COUNT(*)                                                      AS total_inspections,
# MAGIC   SUM(CASE WHEN f.results = 'Pass' THEN 1 ELSE 0 END)          AS passed,
# MAGIC   SUM(CASE WHEN f.results = 'Fail' THEN 1 ELSE 0 END)          AS failed,
# MAGIC   ROUND(SUM(CASE WHEN f.results = 'Fail' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS fail_rate_pct
# MAGIC FROM students_data.`team3-chicago`.factinspection f
# MAGIC JOIN students_data.`team3-chicago`.dimdate d ON f.d_date_id = d.d_date_id
# MAGIC GROUP BY d.year
# MAGIC ORDER BY d.year

# COMMAND ----------

# DBTITLE 1,Inspection Results Breakdown
# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- INSPECTION RESULTS: Overall outcome distribution
# MAGIC -- =====================================================
# MAGIC SELECT
# MAGIC   results,
# MAGIC   COUNT(*)                                             AS inspection_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1)  AS pct_of_total
# MAGIC FROM students_data.`team3-chicago`.factinspection
# MAGIC GROUP BY results
# MAGIC ORDER BY inspection_count DESC

# COMMAND ----------

# DBTITLE 1,Top 10 Riskiest Zip Codes (High-Risk Failures)
# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- RISKIEST ZIP CODES: Where high-risk inspections fail most
# MAGIC -- =====================================================
# MAGIC SELECT
# MAGIC   l.zip,
# MAGIC   COUNT(*)                                                                          AS high_risk_inspections,
# MAGIC   SUM(CASE WHEN f.results = 'Fail' THEN 1 ELSE 0 END)                              AS failures,
# MAGIC   ROUND(SUM(CASE WHEN f.results = 'Fail' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS fail_rate_pct
# MAGIC FROM students_data.`team3-chicago`.factinspection f
# MAGIC JOIN students_data.`team3-chicago`.dimlocation l  ON f.d_location_id = l.d_location_id
# MAGIC JOIN students_data.`team3-chicago`.dimrisk r      ON f.d_risk_id     = r.d_risk_id
# MAGIC WHERE r.risk_category = 'Risk 1'
# MAGIC GROUP BY l.zip
# MAGIC HAVING COUNT(*) >= 50
# MAGIC ORDER BY fail_rate_pct DESC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Top 10 Most Inspected Businesses
# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- MOST INSPECTED BUSINESSES: Who gets inspected the most?
# MAGIC -- =====================================================
# MAGIC SELECT
# MAGIC   b.dba_name,
# MAGIC   ft.facility_type,
# MAGIC   COUNT(*)                                                      AS total_inspections,
# MAGIC   SUM(CASE WHEN f.results = 'Pass' THEN 1 ELSE 0 END)          AS passed,
# MAGIC   SUM(CASE WHEN f.results = 'Fail' THEN 1 ELSE 0 END)          AS failed
# MAGIC FROM students_data.`team3-chicago`.factinspection f
# MAGIC JOIN students_data.`team3-chicago`.dimbusiness b       ON f.d_business_id     = b.d_business_id
# MAGIC JOIN students_data.`team3-chicago`.dimfacilitytype ft  ON f.d_facility_type_id = ft.d_facility_type_id
# MAGIC GROUP BY b.dba_name, ft.facility_type
# MAGIC ORDER BY total_inspections DESC
# MAGIC LIMIT 10

# COMMAND ----------

# DBTITLE 1,Top 15 Most Common Violations
# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- MOST COMMON VIOLATIONS: What goes wrong most often?
# MAGIC -- =====================================================
# MAGIC SELECT
# MAGIC   v.violation_code,
# MAGIC   v.violation_description,
# MAGIC   COUNT(*)                              AS occurrence_count,
# MAGIC   COUNT(DISTINCT fiv.f_inspection_id)   AS inspections_affected
# MAGIC FROM students_data.`team3-chicago`.factinspectionviolation fiv
# MAGIC JOIN students_data.`team3-chicago`.dimviolation v ON fiv.d_violation_id = v.d_violation_id
# MAGIC GROUP BY v.violation_code, v.violation_description
# MAGIC ORDER BY occurrence_count DESC
# MAGIC LIMIT 15

# COMMAND ----------

# DBTITLE 1,Busiest Inspection Days of the Week
# MAGIC %sql
# MAGIC -- =====================================================
# MAGIC -- BUSIEST DAYS: When do inspectors hit the streets?
# MAGIC -- =====================================================
# MAGIC SELECT
# MAGIC   d.day_of_week,
# MAGIC   COUNT(*)                                                                          AS total_inspections,
# MAGIC   ROUND(AVG(CASE WHEN f.results = 'Fail' THEN 1.0 ELSE 0.0 END) * 100, 1)         AS avg_fail_rate_pct
# MAGIC FROM students_data.`team3-chicago`.factinspection f
# MAGIC JOIN students_data.`team3-chicago`.dimdate d ON f.d_date_id = d.d_date_id
# MAGIC GROUP BY d.day_of_week
# MAGIC ORDER BY total_inspections DESC

# COMMAND ----------

# DBTITLE 1,Key Findings & Interesting Stats
# MAGIC %md
# MAGIC ## Key Findings & Interesting Stats
# MAGIC
# MAGIC ### Schema Scale
# MAGIC The star schema contains **301,904 inspections** across **50,233 businesses** at **20,187 locations**, generating **951,002 violation records** — roughly **3.1 violations per inspection** on average.
# MAGIC
# MAGIC ### Temporal Trends
# MAGIC * Data spans **2010–2026** (16+ years of Chicago food inspections)
# MAGIC * Peak inspection year was **2016** with **22,702 inspections**
# MAGIC * Fail rates have **improved over time**, dropping from **24.7%** in 2010 to ~**17–19%** in recent years
# MAGIC * **2020** saw a notable dip to **14,988 inspections**, likely due to COVID-19
# MAGIC
# MAGIC ### Outcome Distribution
# MAGIC | Result | Count | % of Total |
# MAGIC | --- | --- | --- |
# MAGIC | Pass | 158,512 | 52.5% |
# MAGIC | Fail | 58,864 | 19.5% |
# MAGIC | Pass w/ Conditions | 45,720 | 15.1% |
# MAGIC | Out of Business | 21,234 | 7.0% |
# MAGIC
# MAGIC ### Geographic Risk
# MAGIC * **Zip 60827** has the highest Risk 1 failure rate at **27.6%**
# MAGIC * South-side zip codes (60619, 60620, 60628) consistently dominate the top of the failure list, all above **24%**
# MAGIC
# MAGIC ### Business Insights
# MAGIC * **Subway** is the most inspected chain with **4,157 inspections** (13.1% fail rate)
# MAGIC * **Dunkin Donuts** has a notably higher fail rate at **15.7%** across 2,159 inspections
# MAGIC * **Whole Foods Market** stands out with the lowest fail rate among top chains at **11.0%**
# MAGIC
# MAGIC ### Most Common Violations
# MAGIC * \#1: *"Physical facilities installed, maintained & clean"* — flagged **71,616 times** across 49K inspections
# MAGIC * Facility maintenance and cleanliness violations (floors, walls, equipment) dominate the top 5
# MAGIC
# MAGIC ### Day-of-Week Patterns
# MAGIC * **Tuesday** is the busiest inspection day (**68,759 inspections**), followed by Thursday
# MAGIC * Weekend inspections are virtually non-existent — only **121 on Saturdays** and **13 on Sundays**
# MAGIC * **Sunday** has the highest fail rate at **23.1%**, though the tiny sample size (n=13) makes this unreliable
