# Databricks notebook source
# COMMAND ----------

dbutils.notebook.run("./01_Create_Staging", 600)

# COMMAND ----------

dbutils.notebook.run("./02_Create_Dimensions", 600)

# COMMAND ----------

dbutils.notebook.run("./03_Create_Facts", 600)

# COMMAND ----------

dbutils.notebook.run("./04_Run_Tests", 600)
