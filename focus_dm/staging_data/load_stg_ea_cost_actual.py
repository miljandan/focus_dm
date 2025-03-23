# Databricks notebook source
# Path to the uploaded CSV file in DBFS (adjust the path to your file location)
file_path = 'dbfs:/FileStore/tables/EA_Cost_Actual.csv'

# COMMAND ----------

# Loading data from .csv into dataframe

df = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(schemaEaCostActual)
    .option("delimiter", ",")
    .option("quote", '"')
    .option("dateFormat", "MM/dd/yyyy")  
    .option("escape", '"')
    .load(file_path)
)

df = df.withColumn("CurrTimestamp", current_timestamp())

# COMMAND ----------

# Saving dataframe as table in staging database

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("stg.ea_cost_actual_stg")