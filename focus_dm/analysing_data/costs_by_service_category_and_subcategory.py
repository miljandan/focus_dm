# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC   ProviderName,
# MAGIC   BillingCurrency,
# MAGIC   BillingPeriodStart,
# MAGIC   ServiceCategory,
# MAGIC   ServiceSubcategory,
# MAGIC   SUM(BilledCost) AS TotalBilledCost
# MAGIC FROM dm.cost_by_service_dt
# MAGIC WHERE BillingPeriodStart >= '06/01/2024' and BillingPeriodEnd  < '07/02/2024'
# MAGIC GROUP BY
# MAGIC   ProviderName,
# MAGIC   BillingCurrency,
# MAGIC   BillingPeriodStart,
# MAGIC   ServiceCategory,
# MAGIC   ServiceSubcategory
# MAGIC ORDER BY TotalBilledCost DESC