# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA stg

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA dm

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM stg.ea_cost_actual_stg
# MAGIC WHERE (ChargeType == "Purchase" OR ChargeType = "Refund") 
# MAGIC   AND (PricingModel = "Reservation" OR PricingModel = "SavingsPlan")
# MAGIC
# MAGIC --Keep all rows from the amortized cost data.
# MAGIC --Filter the actual cost data to only include rows where ChargeType == "Purchase" or "Refund" and PricingModel == --"Reservation" or "SavingsPlan"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.stg.ea_cost_actual_stg WHERE Tags like '%application%'