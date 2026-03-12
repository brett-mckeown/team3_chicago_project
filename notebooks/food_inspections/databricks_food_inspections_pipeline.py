# Databricks notebook source
# Food Inspections pipeline template
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("my-app").getOrCreate()
spark.sql("SELECT 1").show()

# COMMAND ----------

# Config: update these for your workspace/catalog naming standards.
SOURCE_PATH = "/Volumes/students_data/team3-chicago/files/Food_Inspections.csv"
CATALOG = "students_data"
SCHEMA = "team3-chicago"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.food_inspections_bronze"

# COMMAND ----------


from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Example schema — adjust field types and order to match your CSV
schema = StructType(
    [
        StructField("inspection_id", StringType(), True),
        StructField("dba_name", StringType(), True),
        StructField("aka_name", StringType(), True),
        StructField("license_number", StringType(), True),
        StructField("facility_type", StringType(), True),
        StructField("risk", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("inspection_date", StringType(), True),
        StructField("inspection_type", StringType(), True),
        StructField("results", StringType(), True),
        StructField("violations", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("location", StringType(), True),
    ]
)

# Read source CSV from Databricks Volume.
raw_df = spark.read.option("header", "true").schema(schema).csv(SOURCE_PATH)

# Basic ingestion metadata for lineage.
bronze_df = raw_df.withColumn("_ingested_at", F.current_timestamp()).withColumn("_source_path", F.lit(SOURCE_PATH))

# Write bronze table.
(
    bronze_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("delta.columnMapping.mode", "name")
    .saveAsTable(f"`{CATALOG}`.`{SCHEMA}`.food_inspections_bronze")
)

print(f"Bronze table written: {BRONZE_TABLE}")
