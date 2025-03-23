# Databricks notebook source
# MAGIC %md
# MAGIC Populate Focus DM

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Loading stage data

# COMMAND ----------

# MAGIC %run /focus_dm/staging_data/load_stg_main

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Loading transformed data and preparing it for analysis

# COMMAND ----------

# MAGIC %run /focus_dm/transforming_data/load_dm_discounts_applied

# COMMAND ----------

# MAGIC %run /focus_dm/transforming_data/load_dm_cost_by_service