# Databricks notebook source
# MAGIC %run /focus_dm/common_utilities/default_values

# COMMAND ----------

stage_data = spark.read.table("stg.ea_cost_actual_stg")

# COMMAND ----------

transform_data = stage_data.withColumn("LastDayOfMonth", last_day(col("BillingPeriodEndDate")))
transform_data = transform_data.withColumn("FirstDayOfNextMonth", date_add(col("LastDayOfMonth"), 1))

transform_data = transform_data.withColumn("LastDayOfMonth", date_format(col("LastDayOfMonth"),"MM/dd/yyyy"))
transform_data = transform_data.withColumn("FirstDayOfNextMonth", date_format(col("FirstDayOfNextMonth"),"MM/dd/yyyy"))

transform_data = transform_data.withColumn("BillingPeriodStartDate", date_format(col("BillingPeriodStartDate"),"MM/dd/yyyy"))
transform_data = transform_data.withColumn("BillingPeriodEndDate", date_format(col("BillingPeriodEndDate"),"MM/dd/yyyy"))

# COMMAND ----------

transform_data = transform_data.select( lit("Microsoft").alias("ProviderName")
                            ,coalesce(col("BillingProfileId"), lit(defaultString)).alias("BillingAccountId")
                            ,coalesce(col("BillingProfileName"), lit(defaultString)).alias("BillingAccountName")
                            ,coalesce(col("BillingCurrencyCode"), lit(defaultString)).alias("BillingCurrency")
                            ,coalesce(col("ConsumedService"), lit(defaultString)).alias("ServiceName")
                            ,coalesce(when(((col("ChargeType") == "Purchase") | (col("ChargeType") == "Refund")) & ((col("PricingModel") == "Reservation") | (col("PricingModel") == "SavingsPlan")), lit(0))
                            .otherwise(col("CostInBillingCurrency")), lit(defaultNumber)).alias("EffectiveCost")
                            ,coalesce(when((col("ChargeType") == "Usage") & ((col("PricingModel") == "Reservation") | (col("PricingModel") == "SavingsPlan")), lit(0.00))
                            .otherwise(col("CostInBillingCurrency")), lit(defaultNumber)).alias("BilledCost")
                            ,coalesce(col("BillingPeriodStartDate"), lit(defaultString)).alias("BillingPeriodStart")
                            ,coalesce(when(col("BillingPeriodEndDate") == col("LastDayOfMonth"), col("FirstDayOfNextMonth"))
                            .otherwise(col("BillingPeriodEndDate")), lit(defaultString)).alias("BillingPeriodEnd")
                            ,coalesce(when(col("ChargeType") == "Refund", lit('Correction'))
                             .otherwise(col("ChargeType")), lit(defaultString)).alias("ChargeClass")
                            )#.where(((col("ChargeType") == "Purchase") | (col("ChargeType") == "Refund")) & ((col("PricingModel") == "Reservation") | (col("PricingModel") == "SavingsPlan")))


# COMMAND ----------

transform_data.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("dm.discounts_applied_dt")