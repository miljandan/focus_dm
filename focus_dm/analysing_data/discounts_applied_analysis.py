# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC   ProviderName,
# MAGIC   BillingAccountId,
# MAGIC   BillingAccountName,
# MAGIC   BillingCurrency,
# MAGIC   ServiceName,
# MAGIC   SUM(EffectiveCost) AS TotalEffectiveCost,
# MAGIC   --SUM(ListCost) AS TotalListCost,
# MAGIC   SUM(BilledCost) AS TotalBilledCost,
# MAGIC   (SUM(EffectiveCost)/SUM(BilledCost))*100 AS EffectiveDiscount
# MAGIC FROM dm.discounts_applied_dt
# MAGIC WHERE BillingPeriodStart >= '06/01/2024' AND BillingPeriodEnd < '07/02/2024'
# MAGIC   AND ChargeClass != 'Correction'
# MAGIC GROUP BY
# MAGIC   ProviderName,
# MAGIC   BillingAccountId,
# MAGIC   BillingAccountName,
# MAGIC   BillingCurrency,
# MAGIC   ServiceName