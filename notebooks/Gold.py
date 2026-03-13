spark.sql("""
CREATE OR REPLACE VIEW students_data.`team3-chicago`.gold_inspections AS
SELECT
  fi.f_inspection_id,
  fi.inspection_id,
  fi.inspection_type,
  fi.results,
  -- Business
  db.dba_name,
  db.aka_name,
  db.license_number,
  -- Location
  dl.address,
  dl.city,
  dl.state,
  dl.zip,
  -- Facility Type
  dft.facility_type,
  dft.facility_type_raw,
  -- Risk
  dr.risk_category,
  dr.risk_raw,
  -- Date
  dd.date       AS inspection_date,
  dd.year       AS inspection_year,
  dd.month      AS inspection_month,
  dd.day        AS inspection_day,
  dd.day_of_week
FROM students_data.`team3-chicago`.factinspection fi
LEFT JOIN students_data.`team3-chicago`.dimbusiness     db  ON fi.d_business_id       = db.d_business_id
LEFT JOIN students_data.`team3-chicago`.dimlocation     dl  ON fi.d_location_id       = dl.d_location_id
LEFT JOIN students_data.`team3-chicago`.dimfacilitytype dft ON fi.d_facility_type_id  = dft.d_facility_type_id
LEFT JOIN students_data.`team3-chicago`.dimrisk         dr  ON fi.d_risk_id           = dr.d_risk_id
LEFT JOIN students_data.`team3-chicago`.dimdate         dd  ON fi.d_date_id           = dd.d_date_id
""")
print("gold_inspections view created successfully")