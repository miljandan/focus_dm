# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

#dbutils.widgets.text('EnvironmentShortName', 'DatabricksUndefined', label='EnvironmentShortName')
#dbutils.widgets.text('url', 'DatabricksUndefined', label = 'URL')
#dbutils.widgets.text('loadIncremental', 'DatabricksUndefined', label = 'loadIncremental')
#dbutils.widgets.help()
dbutils.widgets.help("get")
