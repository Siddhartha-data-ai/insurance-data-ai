# Databricks notebook source
# MAGIC %md
# MAGIC # Claim Volume Forecasting Model
# MAGIC
# MAGIC **Predicts expected claim volumes for next 30/60/90 days**
# MAGIC
# MAGIC **Features:**
# MAGIC - Historical claim patterns
# MAGIC - Seasonal trends
# MAGIC - Policy growth trends
# MAGIC - Claim type distribution
# MAGIC - Geographic factors
# MAGIC
# MAGIC **Output:** Daily claim forecasts with confidence intervals

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, datediff, date_add, current_date, to_date, dayofweek, month, year
from pyspark.sql.window import Window
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
dbutils.widgets.dropdown(
    "silver_catalog",
    "insurance_dev_silver",
    ["insurance_dev_silver", "insurance_staging_silver", "insurance_prod_silver"],
    "Silver Catalog",
)
dbutils.widgets.dropdown(
    "gold_catalog",
    "insurance_dev_gold",
    ["insurance_dev_gold", "insurance_staging_gold", "insurance_prod_gold"],
    "Gold Catalog",
)
dbutils.widgets.dropdown("forecast_days", "90", ["30", "60", "90"], "Forecast Period (Days)")

silver_catalog = dbutils.widgets.get("silver_catalog")
gold_catalog = dbutils.widgets.get("gold_catalog")
forecast_days = int(dbutils.widgets.get("forecast_days"))

print(f"✅ Configuration:")
print(f"   Silver Catalog: {silver_catalog}")
print(f"   Gold Catalog: {gold_catalog}")
print(f"   Forecast Period: {forecast_days} days")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Historical Claim Data

# COMMAND ----------
print("📊 Loading historical claims...")

df_claims = spark.table(f"{silver_catalog}.claims.claim_fact")

# Aggregate claims by date
df_daily_claims = (
    df_claims.groupBy(to_date("report_date").alias("claim_date"), "claim_type")
    .agg(
        F.count("*").alias("claim_count"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.avg("claimed_amount").alias("avg_claim_amount"),
    )
    .orderBy("claim_date", "claim_type")
)

print(f"✅ Loaded {df_daily_claims.count():,} daily claim records")

# Get date range
date_range = df_daily_claims.agg(
    F.min("claim_date").alias("min_date"), F.max("claim_date").alias("max_date")
).collect()[0]

print(f"   Date range: {date_range['min_date']} to {date_range['max_date']}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Feature Engineering for Time Series

# COMMAND ----------
print("🔧 Engineering time series features...")

# Add temporal features
df_ts_features = (
    df_daily_claims.withColumn("day_of_week", dayofweek("claim_date"))
    .withColumn("month", month("claim_date"))
    .withColumn("year", year("claim_date"))
    .withColumn("is_weekend", when(col("day_of_week").isin(1, 7), 1).otherwise(0))
    .withColumn("is_month_end", when((datediff(F.last_day("claim_date"), col("claim_date")) <= 5), 1).otherwise(0))
)

# Calculate moving averages (7-day and 30-day)
window_7 = Window.partitionBy("claim_type").orderBy("claim_date").rowsBetween(-6, 0)
window_30 = Window.partitionBy("claim_type").orderBy("claim_date").rowsBetween(-29, 0)

df_ts_features = (
    df_ts_features.withColumn("claim_count_ma7", F.avg("claim_count").over(window_7))
    .withColumn("claim_count_ma30", F.avg("claim_count").over(window_30))
    .withColumn("amount_ma7", F.avg("total_claimed_amount").over(window_7))
    .withColumn("amount_ma30", F.avg("total_claimed_amount").over(window_30))
)

# Calculate trend (simple linear approximation)
window_trend = Window.partitionBy("claim_type").orderBy("claim_date").rowsBetween(-29, 0)
df_ts_features = df_ts_features.withColumn("recent_avg", F.avg("claim_count").over(window_trend)).withColumn(
    "trend", col("claim_count") - col("recent_avg")
)

print("✅ Time series features created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Calculate Historical Patterns

# COMMAND ----------
print("📈 Calculating historical patterns...")

# Convert to Pandas for advanced time series analysis
df_ts_pd = df_ts_features.toPandas()

# Calculate baseline by claim type and day of week
baseline_dow = (
    df_ts_pd.groupby(["claim_type", "day_of_week"])
    .agg({"claim_count": ["mean", "std"], "total_claimed_amount": "mean"})
    .reset_index()
)
baseline_dow.columns = ["claim_type", "day_of_week", "avg_claims", "std_claims", "avg_amount"]

# Calculate seasonal patterns by month
baseline_month = (
    df_ts_pd.groupby(["claim_type", "month"])
    .agg({"claim_count": ["mean", "std"], "total_claimed_amount": "mean"})
    .reset_index()
)
baseline_month.columns = ["claim_type", "month", "avg_claims_month", "std_claims_month", "avg_amount_month"]

# Overall baseline
baseline_overall = (
    df_ts_pd.groupby("claim_type")
    .agg({"claim_count": ["mean", "std"], "total_claimed_amount": "mean", "trend": "mean"})
    .reset_index()
)
baseline_overall.columns = ["claim_type", "overall_avg", "overall_std", "overall_avg_amount", "avg_trend"]

print("✅ Historical patterns calculated")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Forecasts

# COMMAND ----------
print(f"🎯 Generating {forecast_days}-day forecasts...")

# Get unique claim types
claim_types = df_ts_pd["claim_type"].unique()

# Generate future dates
today = pd.Timestamp(datetime.now().date())
future_dates = pd.date_range(start=today, periods=forecast_days, freq="D")

# Build forecast dataframe
forecasts = []

for claim_type in claim_types:
    # Get baselines for this claim type
    type_baseline = baseline_overall[baseline_overall["claim_type"] == claim_type].iloc[0]
    type_dow_baseline = baseline_dow[baseline_dow["claim_type"] == claim_type]
    type_month_baseline = baseline_month[baseline_month["claim_type"] == claim_type]

    for future_date in future_dates:
        dow = future_date.dayofweek + 1  # Pandas uses 0-6, Spark uses 1-7
        if dow == 0:
            dow = 7
        month_num = future_date.month

        # Get day of week adjustment
        dow_row = type_dow_baseline[type_dow_baseline["day_of_week"] == dow]
        if not dow_row.empty:
            dow_factor = dow_row.iloc[0]["avg_claims"] / type_baseline["overall_avg"]
        else:
            dow_factor = 1.0

        # Get monthly adjustment
        month_row = type_month_baseline[type_month_baseline["month"] == month_num]
        if not month_row.empty:
            month_factor = month_row.iloc[0]["avg_claims_month"] / type_baseline["overall_avg"]
        else:
            month_factor = 1.0

        # Calculate forecast with trend
        days_ahead = (future_date - today).days
        base_forecast = type_baseline["overall_avg"]
        trend_adjustment = type_baseline["avg_trend"] * (days_ahead / 30)  # Trend per 30 days

        predicted_count = base_forecast * dow_factor * month_factor + trend_adjustment
        predicted_count = max(0, predicted_count)  # No negative claims

        # Calculate confidence intervals (assuming normal distribution)
        std_dev = type_baseline["overall_std"]
        confidence_lower = max(0, predicted_count - 1.96 * std_dev)
        confidence_upper = predicted_count + 1.96 * std_dev

        # Estimate amount
        predicted_amount = predicted_count * type_baseline["overall_avg_amount"]

        forecasts.append(
            {
                "forecast_date": future_date,
                "claim_type": claim_type,
                "predicted_claim_count": round(predicted_count, 2),
                "confidence_lower_95": round(confidence_lower, 2),
                "confidence_upper_95": round(confidence_upper, 95),
                "predicted_total_amount": round(predicted_amount, 2),
                "day_of_week": dow,
                "month": month_num,
                "days_ahead": days_ahead,
            }
        )

df_forecast_pd = pd.DataFrame(forecasts)

# Convert back to Spark DataFrame
df_forecast = spark.createDataFrame(df_forecast_pd)

# Add metadata
df_forecast = (
    df_forecast.withColumn("forecast_generated_date", current_date())
    .withColumn("forecast_model", lit("Time Series - Seasonal Decomposition"))
    .withColumn("forecast_confidence", lit("95%"))
)

print(f"✅ Generated {df_forecast.count():,} forecast records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aggregate Forecasts

# COMMAND ----------
print("📊 Creating aggregate forecasts...")

# Daily total across all claim types
df_daily_total = (
    df_forecast.groupBy("forecast_date")
    .agg(
        F.sum("predicted_claim_count").alias("total_predicted_claims"),
        F.sum("confidence_lower_95").alias("total_confidence_lower"),
        F.sum("confidence_upper_95").alias("total_confidence_upper"),
        F.sum("predicted_total_amount").alias("total_predicted_amount"),
    )
    .withColumn("claim_type", lit("ALL_TYPES"))
)

# Add metadata
df_daily_total = (
    df_daily_total.withColumn("forecast_generated_date", current_date())
    .withColumn("forecast_model", lit("Time Series - Seasonal Decomposition"))
    .withColumn("forecast_confidence", lit("95%"))
    .withColumn("day_of_week", dayofweek("forecast_date"))
    .withColumn("month", month("forecast_date"))
    .withColumn("days_ahead", datediff(col("forecast_date"), current_date()))
)

# Rename columns to match
df_daily_total = df_daily_total.select(
    "forecast_date",
    "claim_type",
    col("total_predicted_claims").alias("predicted_claim_count"),
    col("total_confidence_lower").alias("confidence_lower_95"),
    col("total_confidence_upper").alias("confidence_upper_95"),
    col("total_predicted_amount").alias("predicted_total_amount"),
    "day_of_week",
    "month",
    "days_ahead",
    "forecast_generated_date",
    "forecast_model",
    "forecast_confidence",
)

# Combine with claim-type-specific forecasts
df_forecast_final = df_forecast.union(df_daily_total)

print("✅ Aggregate forecasts created")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save to Gold Layer

# COMMAND ----------
print("💾 Saving forecasts to gold layer...")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}.predictions")

table_name = f"{gold_catalog}.predictions.claim_forecast"

df_forecast_final.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

print(f"✅ Forecasts saved to: {table_name}")

# Show sample
print("\n📊 Next 7 Days Forecast (All Claims):")
display(df_forecast_final.filter(col("claim_type") == "ALL_TYPES").orderBy("forecast_date").limit(7))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------
print("=" * 70)
print("📈 CLAIM FORECASTING MODEL - SUMMARY")
print("=" * 70)

# 30-day summary by claim type
summary_30d = (
    df_forecast_final.filter((col("days_ahead") <= 30) & (col("claim_type") != "ALL_TYPES"))
    .groupBy("claim_type")
    .agg(
        F.sum("predicted_claim_count").alias("expected_claims_30d"),
        F.sum("predicted_total_amount").alias("expected_amount_30d"),
    )
    .orderBy(F.desc("expected_claims_30d"))
)

summary_pd = summary_30d.toPandas()

print(f"\n📊 30-Day Forecast by Claim Type:")
for _, row in summary_pd.iterrows():
    print(f"\n{row['claim_type']}:")
    print(f"  Expected Claims: {row['expected_claims_30d']:,.0f}")
    print(f"  Expected Amount: ${row['expected_amount_30d']:,.2f}")

# Overall totals
total_30d = (
    df_forecast_final.filter((col("days_ahead") <= 30) & (col("claim_type") == "ALL_TYPES"))
    .agg(F.sum("predicted_claim_count").alias("total_claims"), F.sum("predicted_total_amount").alias("total_amount"))
    .collect()[0]
)

print(f"\n{'='*70}")
print(f"📊 Total 30-Day Forecast:")
print(f"   Expected Claims: {total_30d['total_claims']:,.0f}")
print(f"   Expected Amount: ${total_30d['total_amount']:,.2f}")
print(f"\n✅ Forecasts saved to: {table_name}")
print("=" * 70)
