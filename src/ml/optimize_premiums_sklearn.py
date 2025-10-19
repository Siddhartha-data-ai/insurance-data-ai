# Databricks notebook source
# MAGIC %md
# MAGIC # Premium Optimization Model (Scikit-Learn Compatible)
# MAGIC
# MAGIC **Community Edition Compatible - Uses pandas + rule-based optimization**
# MAGIC
# MAGIC **Recommends optimal premium pricing for customers and segments**

import numpy as np
import pandas as pd

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_date, lit
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import when
from pyspark.sql.window import Window

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

silver_catalog = dbutils.widgets.get("silver_catalog")
gold_catalog = dbutils.widgets.get("gold_catalog")

print(f"✅ Configuration:")
print(f"   Silver Catalog: {silver_catalog}")
print(f"   Gold Catalog: {gold_catalog}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------
print("📊 Loading data...")

df_customers = spark.table(f"{silver_catalog}.customers.customer_dim").filter("is_current = true")
df_policies = spark.table(f"{silver_catalog}.policies.policy_dim").filter("is_active = true")
df_claims = spark.table(f"{silver_catalog}.claims.claim_fact")

# Try to load churn predictions
try:
    df_churn = spark.table(f"{gold_catalog}.predictions.customer_churn_risk")
    has_churn_data = True
    print("✅ Churn predictions loaded")
except:
    has_churn_data = False
    print("⚠️  Churn predictions not available - using defaults")

print(f"✅ Loaded:")
print(f"   Customers: {df_customers.count():,}")
print(f"   Active Policies: {df_policies.count():,}")
print(f"   Claims: {df_claims.count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Calculate Loss Ratios

# COMMAND ----------
print("🔧 Calculating risk metrics...")

df_policy_claims = df_claims.groupBy("policy_id").agg(
    F.count("*").alias("claim_count"),
    F.sum("claimed_amount").alias("total_claimed"),
    F.sum("paid_amount").alias("total_paid"),
    F.avg("fraud_score").alias("avg_fraud_score"),
)

df_policy_analysis = (
    df_policies.alias("p")
    .join(df_policy_claims.alias("c"), "policy_id", "left")
    .join(
        df_customers.alias("cust").select(
            "customer_id",
            col("age_years").alias("customer_age_years"),
            col("customer_tenure_months").alias("cust_tenure_months"),
            col("credit_tier").alias("customer_credit_tier"),
        ),
        "customer_id",
        "left",
    )
)

df_policy_analysis = (
    df_policy_analysis.withColumn("claim_count", F.coalesce(col("claim_count"), lit(0)))
    .withColumn("total_claimed", F.coalesce(col("total_claimed"), lit(0)))
    .withColumn("total_paid", F.coalesce(col("total_paid"), lit(0)))
    .withColumn("avg_fraud_score", F.coalesce(col("avg_fraud_score"), lit(0)))
)

df_policy_analysis = df_policy_analysis.withColumn(
    "loss_ratio",
    when(
        (col("annual_premium") > 0) & (col("policy_age_days") > 0),
        col("total_paid") / (col("annual_premium") * col("policy_age_days") / 365.0),
    ).otherwise(0),
)

df_policy_analysis = df_policy_analysis.withColumn(
    "risk_score",
    spark_round(
        (
            col("loss_ratio") * 40
            + col("claim_count") * 15
            + col("avg_fraud_score") * 100 * 20
            + when(col("customer_credit_tier") == "Poor", 25)
            .when(col("customer_credit_tier") == "Fair", 15)
            .when(col("customer_credit_tier") == "Good", 5)
            .otherwise(0)
        ),
        2,
    ),
).withColumn("risk_score", when(col("risk_score") > 100, 100).otherwise(col("risk_score")))

# Market benchmarks
df_market_benchmark = df_policy_analysis.groupBy("policy_type", "state_code").agg(
    F.avg("annual_premium").alias("market_avg_premium"),
    F.percentile_approx("annual_premium", 0.25).alias("market_25th_pct"),
    F.percentile_approx("annual_premium", 0.75).alias("market_75th_pct"),
    F.avg("loss_ratio").alias("segment_avg_loss_ratio"),
)

df_policy_analysis = df_policy_analysis.join(df_market_benchmark, ["policy_type", "state_code"], "left")

print("✅ Risk metrics calculated")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Calculate CLV

# COMMAND ----------
print("💰 Calculating Customer Lifetime Value...")

df_customer_value = df_policy_analysis.groupBy("customer_id").agg(
    F.sum("annual_premium").alias("current_annual_premium"),
    F.avg("loss_ratio").alias("avg_loss_ratio"),
    F.sum("total_paid").alias("lifetime_claims_paid"),
    F.count("policy_id").alias("policy_count"),
    F.avg("risk_score").alias("avg_risk_score"),
    F.first("customer_age_years").alias("age_years"),
    F.first("cust_tenure_months").alias("customer_tenure_months"),
    F.first("customer_credit_tier").alias("credit_tier"),
)

df_customer_value = (
    df_customer_value.withColumn(
        "expected_years",
        when(col("age_years") < 35, 10).when(col("age_years") < 50, 8).when(col("age_years") < 65, 6).otherwise(4),
    )
    .withColumn(
        "retention_probability",
        when(col("customer_tenure_months") > 60, 0.95)
        .when(col("customer_tenure_months") > 36, 0.90)
        .when(col("customer_tenure_months") > 12, 0.85)
        .otherwise(0.75),
    )
    .withColumn(
        "estimated_clv",
        when(
            col("current_annual_premium") > 0,
            spark_round(
                col("current_annual_premium")
                * col("expected_years")
                * F.greatest(1 - col("avg_loss_ratio"), lit(0))  # Ensure non-negative
                * col("retention_probability"),
                2,
            ),
        ).otherwise(0),
    )
)

df_policy_analysis = df_policy_analysis.join(
    df_customer_value.select("customer_id", "estimated_clv", "retention_probability"), "customer_id", "left"
)

# Join churn if available
if has_churn_data:
    df_policy_analysis = df_policy_analysis.join(
        df_churn.select("customer_id", col("churn_probability").alias("churn_risk"), col("churn_risk_category")),
        "customer_id",
        "left",
    )
    df_policy_analysis = df_policy_analysis.withColumn(
        "adjusted_retention_prob", spark_round((1 - col("churn_risk") / 100) * col("retention_probability"), 4)
    )
else:
    df_policy_analysis = (
        df_policy_analysis.withColumn("churn_risk", lit(20))
        .withColumn("churn_risk_category", lit("Unknown"))
        .withColumn("adjusted_retention_prob", col("retention_probability"))
    )

print("✅ CLV calculated")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Premium Recommendations

# COMMAND ----------
print("🎯 Generating premium recommendations...")

# Filter out invalid policies (zero premium, zero age, or missing data)
df_policy_analysis_valid = df_policy_analysis.filter(
    (col("annual_premium") > 0) & (col("policy_age_days") > 0) & (col("market_avg_premium").isNotNull())
)

df_recommendations = (
    df_policy_analysis_valid.withColumn(
        "loss_ratio_adjustment",
        when(col("loss_ratio") > 0.8, 1.15)
        .when(col("loss_ratio") > 0.6, 1.08)
        .when(col("loss_ratio") < 0.3, 0.95)
        .otherwise(1.0),
    )
    .withColumn(
        "risk_adjustment",
        when(col("risk_score") > 70, 1.10)
        .when(col("risk_score") > 50, 1.05)
        .when(col("risk_score") < 30, 0.98)
        .otherwise(1.0),
    )
    .withColumn(
        "market_adjustment",
        when(col("annual_premium") > col("market_75th_pct"), 0.92)
        .when(col("annual_premium") < col("market_25th_pct"), 1.05)
        .otherwise(1.0),
    )
    .withColumn(
        "retention_adjustment",
        when((col("estimated_clv") > 50000) & (col("adjusted_retention_prob") < 0.85), 0.95)
        .when((col("churn_risk") > 60) & (col("estimated_clv") > 30000), 0.93)
        .otherwise(1.0),
    )
    .withColumn(
        "combined_adjustment",
        when(
            col("loss_ratio_adjustment")
            * col("risk_adjustment")
            * col("market_adjustment")
            * col("retention_adjustment")
            > 1.25,
            1.25,
        )
        .when(
            col("loss_ratio_adjustment")
            * col("risk_adjustment")
            * col("market_adjustment")
            * col("retention_adjustment")
            < 0.75,
            0.75,
        )
        .otherwise(
            col("loss_ratio_adjustment")
            * col("risk_adjustment")
            * col("market_adjustment")
            * col("retention_adjustment")
        ),
    )
    .withColumn("recommended_premium", spark_round(col("annual_premium") * col("combined_adjustment"), 2))
    .withColumn("premium_change_amount", spark_round(col("recommended_premium") - col("annual_premium"), 2))
    .withColumn("premium_change_percent", spark_round((col("combined_adjustment") - 1) * 100, 2))
    .withColumn("annual_revenue_impact", spark_round(col("premium_change_amount") * col("adjusted_retention_prob"), 2))
    .withColumn(
        "recommendation_category",
        when(col("premium_change_percent") > 10, "Increase")
        .when(col("premium_change_percent") < -5, "Decrease")
        .otherwise("Maintain"),
    )
    .withColumn(
        "implementation_priority",
        when((F.abs(col("premium_change_percent")) > 15) & (col("estimated_clv") > 30000), "High")
        .when((F.abs(col("premium_change_percent")) > 10) | (col("estimated_clv") > 50000), "Medium")
        .otherwise("Low"),
    )
    .withColumn(
        "rationale",
        when(
            col("recommendation_category") == "Increase",
            F.concat(
                lit("Loss ratio: "),
                spark_round(col("loss_ratio"), 2),
                lit(", Risk: "),
                spark_round(col("risk_score"), 0),
            ),
        )
        .when(
            col("recommendation_category") == "Decrease",
            F.concat(
                lit("Retention risk: "),
                spark_round(col("churn_risk"), 0),
                lit("%, CLV: $"),
                spark_round(col("estimated_clv"), 0),
            ),
        )
        .otherwise("Pricing aligned with risk and market"),
    )
    .withColumn("recommendation_date", current_date())
)

final_recommendations = df_recommendations.select(
    "policy_id",
    "policy_number",
    "customer_id",
    "policy_type",
    "state_code",
    "annual_premium",
    "recommended_premium",
    "premium_change_amount",
    "premium_change_percent",
    "recommendation_category",
    "implementation_priority",
    "loss_ratio",
    "risk_score",
    "estimated_clv",
    "churn_risk",
    "adjusted_retention_prob",
    "annual_revenue_impact",
    "rationale",
    "recommendation_date",
)

print(f"✅ Generated recommendations for {final_recommendations.count():,} policies")

print("\n📊 Recommendation Distribution:")
final_recommendations.groupBy("recommendation_category", "implementation_priority").count().orderBy(
    "recommendation_category", "implementation_priority"
).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save

# COMMAND ----------
print("💾 Saving premium recommendations...")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}.predictions")

table_name = f"{gold_catalog}.predictions.premium_optimization"

final_recommendations.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

print(f"✅ Saved to: {table_name}")

print("\n💰 High-Priority Opportunities:")
display(
    final_recommendations.filter(col("implementation_priority") == "High")
    .orderBy(F.desc(F.abs(col("annual_revenue_impact"))))
    .limit(10)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print("=" * 70)
print("💰 PREMIUM OPTIMIZATION MODEL - SUMMARY")
print("=" * 70)

summary = (
    final_recommendations.groupBy("recommendation_category")
    .agg(
        F.count("*").alias("policy_count"),
        F.sum("annual_revenue_impact").alias("total_revenue_impact"),
        F.avg("premium_change_percent").alias("avg_change_percent"),
    )
    .orderBy("recommendation_category")
    .toPandas()
)

for _, row in summary.iterrows():
    print(f"\n{row['recommendation_category']}:")
    print(f"  Policies: {int(row['policy_count']):,}")
    print(f"  Avg Change: {row['avg_change_percent']:.1f}%")
    print(f"  Revenue Impact: ${row['total_revenue_impact']:,.2f}")

high_priority = final_recommendations.filter(col("implementation_priority") == "High")
high_priority_count = high_priority.count()
high_priority_impact = high_priority.agg(F.sum("annual_revenue_impact").alias("impact")).collect()[0]["impact"]

print(f"\n{'='*70}")
print(f"🎯 High Priority Opportunities:")
print(f"   Policies: {high_priority_count:,}")
print(f"   Potential Annual Impact: ${high_priority_impact:,.2f}")
print(f"\n✅ Recommendations saved to: {table_name}")
print("=" * 70)
