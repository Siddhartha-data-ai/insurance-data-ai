# Databricks notebook source
# MAGIC %md
# MAGIC # Build Customer 360 Analytics
# MAGIC Comprehensive customer view with all metrics, predictions, and insights

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------
# Create widgets for parameters
dbutils.widgets.dropdown("silver_catalog", "insurance_dev_silver", 
                         ["insurance_dev_silver", "insurance_staging_silver", "insurance_prod_silver"], 
                         "Silver Catalog Name")
dbutils.widgets.dropdown("gold_catalog", "insurance_dev_gold", 
                         ["insurance_dev_gold", "insurance_staging_gold", "insurance_prod_gold"], 
                         "Gold Catalog Name")

# Get widget values
silver_catalog = dbutils.widgets.get("silver_catalog")
gold_catalog = dbutils.widgets.get("gold_catalog")

print(f"Using silver catalog: {silver_catalog}")
print(f"Using gold catalog: {gold_catalog}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Silver Tables

# COMMAND ----------
# Load silver tables (using SCD Type 2 for customers)
df_customers = spark.table(f"{silver_catalog}.customers.customer_dim").filter("is_current = true")
df_policies = spark.table(f"{silver_catalog}.policies.policy_dim")
df_claims = spark.table(f"{silver_catalog}.claims.claim_fact")

# Check if payments table exists (optional for enhanced analytics)
try:
    df_payments = spark.table(f"{silver_catalog}.payments.payment_fact")
    has_payments = True
except:
    print("ℹ️  Note: Payments table not found - skipping payment metrics")
    has_payments = False
    df_payments = None

print(f"✅ Loaded {df_customers.count():,} current customers")
print(f"Loaded {df_policies.count():,} policies")
print(f"Loaded {df_claims.count():,} claims")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aggregate Policy Metrics per Customer

# COMMAND ----------
df_policy_metrics = df_policies.groupBy("customer_id").agg(
    F.count("*").alias("total_policies"),
    F.sum(F.when(F.col("policy_status") == "Active", 1).otherwise(0)).alias("active_policies"),
    F.sum(F.when(F.col("policy_status") == "Lapsed", 1).otherwise(0)).alias("lapsed_policies"),
    F.sum(F.when(F.col("policy_status") == "Cancelled", 1).otherwise(0)).alias("cancelled_policies"),
    F.sum(F.when(F.col("policy_type") == "Auto", 1).otherwise(0)).alias("auto_policies"),
    F.sum(F.when(F.col("policy_type") == "Home", 1).otherwise(0)).alias("home_policies"),
    F.sum(F.when(F.col("policy_type") == "Life", 1).otherwise(0)).alias("life_policies"),
    F.sum(F.when(F.col("policy_type") == "Health", 1).otherwise(0)).alias("health_policies"),
    F.sum(F.when(F.col("policy_type") == "Commercial", 1).otherwise(0)).alias("commercial_policies"),
    F.sum(F.when(F.col("policy_status") == "Active", F.col("annual_premium")).otherwise(0)).alias("total_annual_premium"),
    F.sum(F.col("annual_premium")).alias("total_lifetime_premium"),
    F.avg(F.col("annual_premium")).alias("average_policy_premium")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Aggregate Claims Metrics per Customer

# COMMAND ----------
df_claims_metrics = df_claims.groupBy("customer_id").agg(
    F.count("*").alias("total_claims_count"),
    F.sum(F.when(~F.col("is_closed"), 1).otherwise(0)).alias("open_claims_count"),
    F.sum(F.when(F.col("claim_status") == "Closed", 1).otherwise(0)).alias("closed_claims_count"),
    F.sum(F.when(F.col("claim_status") == "Denied", 1).otherwise(0)).alias("denied_claims_count"),
    F.sum(F.col("claimed_amount")).alias("total_claims_amount"),
    F.sum(F.col("paid_amount")).alias("total_claims_paid"),
    F.avg(F.col("claimed_amount")).alias("average_claim_amount"),
    F.max(F.col("loss_date")).alias("last_claim_date"),
    F.avg(F.col("fraud_score")).alias("avg_fraud_score")
)

# Add days since last claim
df_claims_metrics = df_claims_metrics.withColumn(
    "days_since_last_claim",
    F.datediff(F.current_date(), F.col("last_claim_date"))
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Calculate Customer Lifetime Value

# COMMAND ----------
# Generate surrogate key and select base customer attributes
df_customer_base = df_customers \
    .withColumn("customer_key", F.monotonically_increasing_id()) \
    .select(
        "customer_key",
        "customer_id",
        "full_name",
        "age_years",
        "state_code",
        "customer_segment",
        "customer_status",
        "customer_since_date",
        "credit_tier",
        "risk_profile"
    )

# Add age bracket
df_customer_base = df_customer_base.withColumn(
    "age_bracket",
    F.when(F.col("age_years") < 25, "18-24")
     .when(F.col("age_years") < 35, "25-34")
     .when(F.col("age_years") < 45, "35-44")
     .when(F.col("age_years") < 55, "45-54")
     .when(F.col("age_years") < 65, "55-64")
     .otherwise("65+")
)

# Calculate tenure
df_customer_base = df_customer_base.withColumn(
    "tenure_years",
    F.round(F.months_between(F.current_date(), F.col("customer_since_date")) / 12, 2)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Join All Metrics

# COMMAND ----------
df_customer_360 = df_customer_base \
    .join(df_policy_metrics, "customer_id", "left") \
    .join(df_claims_metrics, "customer_id", "left")

# Fill nulls for customers with no policies or claims
numeric_cols = [
    "total_policies", "active_policies", "lapsed_policies", "cancelled_policies",
    "auto_policies", "home_policies", "life_policies", "health_policies", "commercial_policies",
    "total_annual_premium", "total_lifetime_premium", "average_policy_premium",
    "total_claims_count", "open_claims_count", "closed_claims_count", "denied_claims_count",
    "total_claims_amount", "total_claims_paid", "average_claim_amount"
]

for col in numeric_cols:
    if col in df_customer_360.columns:
        df_customer_360 = df_customer_360.withColumn(col, F.coalesce(F.col(col), F.lit(0)))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Calculate Derived Metrics

# COMMAND ----------
# Loss ratio
df_customer_360 = df_customer_360.withColumn(
    "loss_ratio",
    F.when(F.col("total_lifetime_premium") > 0,
           F.round(F.col("total_claims_paid") / F.col("total_lifetime_premium"), 4))
     .otherwise(F.lit(0))
)

# Customer lifetime value (simple calculation)
df_customer_360 = df_customer_360.withColumn(
    "customer_lifetime_value",
    F.round(F.col("total_lifetime_premium") - F.col("total_claims_paid"), 2)
)

# Predicted lifetime value (simplified - would use ML model in production)
df_customer_360 = df_customer_360.withColumn(
    "predicted_lifetime_value",
    F.round(
        F.col("total_annual_premium") * F.col("tenure_years") * 1.5 * 
        F.when(F.col("customer_segment") == "Platinum", 1.3)
         .when(F.col("customer_segment") == "Gold", 1.15)
         .when(F.col("customer_segment") == "Silver", 1.0)
         .otherwise(0.85),
        2
    )
)

# Value tier
df_customer_360 = df_customer_360.withColumn(
    "value_tier",
    F.when(F.col("customer_lifetime_value") > 50000, "High Value")
     .when(F.col("customer_lifetime_value") > 20000, "Medium Value")
     .otherwise("Low Value")
)

# Profitability
df_customer_360 = df_customer_360.withColumn(
    "profitability",
    F.round(F.col("total_lifetime_premium") * 0.7 - F.col("total_claims_paid"), 2)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Churn Risk Scoring

# COMMAND ----------
# Simple churn risk score based on multiple factors
df_customer_360 = df_customer_360.withColumn(
    "churn_risk_score",
    F.round(
        (F.when(F.col("lapsed_policies") > 0, 0.30).otherwise(0.0) +
         F.when(F.col("cancelled_policies") > 0, 0.25).otherwise(0.0) +
         F.when(F.col("total_claims_count") > 3, 0.15).otherwise(0.0) +
         F.when(F.col("customer_status") == "Inactive", 0.20).otherwise(0.0) +
         F.when(F.col("days_since_last_claim").isNull(), 0.0).when(F.col("days_since_last_claim") < 90, 0.10).otherwise(0.0)),
        4
    )
)

df_customer_360 = df_customer_360.withColumn(
    "churn_risk_category",
    F.when(F.col("churn_risk_score") > 0.60, "High")
     .when(F.col("churn_risk_score") > 0.30, "Medium")
     .otherwise("Low")
)

# Retention probability (inverse of churn risk)
df_customer_360 = df_customer_360.withColumn(
    "retention_probability",
    F.round(1.0 - F.col("churn_risk_score"), 4)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Cross-sell Opportunity

# COMMAND ----------
df_customer_360 = df_customer_360.withColumn(
    "cross_sell_score",
    F.round(
        (F.when(F.col("auto_policies") == 0, 20.0).otherwise(0.0) +
         F.when(F.col("home_policies") == 0, 20.0).otherwise(0.0) +
         F.when(F.col("life_policies") == 0, 15.0).otherwise(0.0) +
         F.when(F.col("health_policies") == 0, 15.0).otherwise(0.0)) *
        F.when(F.col("customer_segment").isin(["Platinum", "Gold"]), 1.5).otherwise(1.0),
        2
    )
)

# Recommended products
df_customer_360 = df_customer_360.withColumn(
    "recommended_products",
    F.concat_ws(", ",
        F.when(F.col("auto_policies") == 0, F.lit("Auto")).otherwise(F.lit(None)),
        F.when(F.col("home_policies") == 0, F.lit("Home")).otherwise(F.lit(None)),
        F.when(F.col("life_policies") == 0, F.lit("Life")).otherwise(F.lit(None)),
        F.when(F.col("health_policies") == 0, F.lit("Health")).otherwise(F.lit(None))
    )
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Add Additional Metrics

# COMMAND ----------
# Add region mapping
df_customer_360 = df_customer_360.withColumn(
    "region",
    F.when(F.col("state_code").isin(["CA", "OR", "WA", "NV", "AZ"]), "West")
     .when(F.col("state_code").isin(["TX", "OK", "AR", "LA"]), "Southwest")
     .when(F.col("state_code").isin(["IL", "IN", "MI", "OH", "WI"]), "Midwest")
     .when(F.col("state_code").isin(["NY", "PA", "NJ", "MA", "CT"]), "Northeast")
     .when(F.col("state_code").isin(["FL", "GA", "NC", "SC", "VA"]), "Southeast")
     .otherwise("Other")
)

# Mock values for some fields
df_customer_360 = df_customer_360 \
    .withColumn("last_contact_date", F.date_sub(F.current_date(), F.expr("cast(rand() * 90 as int)"))) \
    .withColumn("days_since_last_contact", F.datediff(F.current_date(), F.col("last_contact_date"))) \
    .withColumn("contact_count_last_12m", F.expr("cast(rand() * 20 + 2 as int)")) \
    .withColumn("complaints_count", F.expr("cast(rand() * 3 as int)")) \
    .withColumn("satisfaction_score", F.round(F.expr("rand() * 2 + 3"), 2)) \
    .withColumn("nps_score", F.expr("cast(rand() * 60 - 10 as int)")) \
    .withColumn("overall_risk_score", F.expr("cast(rand() * 500 + 400 as int)")) \
    .withColumn("risk_tier", 
                F.when(F.col("overall_risk_score") > 700, "Low Risk")
                 .when(F.col("overall_risk_score") > 600, "Medium Risk")
                 .otherwise("High Risk")) \
    .withColumn("fraud_risk_score", F.coalesce(F.col("avg_fraud_score"), F.lit(25.0))) \
    .withColumn("payment_method_preference", F.lit("Auto Pay - Bank")) \
    .withColumn("payment_frequency", F.lit("Monthly")) \
    .withColumn("on_time_payment_rate", F.round(F.expr("rand() * 0.2 + 0.80"), 4)) \
    .withColumn("late_payments_count", F.expr("cast(rand() * 3 as int)")) \
    .withColumn("total_late_fees", F.round(F.col("late_payments_count") * 25, 2)) \
    .withColumn("payment_risk_score", F.round(F.expr("rand() * 30 + 20"), 2)) \
    .withColumn("upsell_opportunity", F.round(F.col("total_annual_premium") * 0.25, 2)) \
    .withColumn("primary_agent_id", F.lit("AGT000001")) \
    .withColumn("primary_agent_name", F.lit("John Smith")) \
    .withColumn("agent_relationship_years", F.col("tenure_years")) \
    .withColumn("agent_satisfaction_score", F.round(F.expr("rand() * 1.5 + 3.5"), 2)) \
    .withColumn("next_renewal_date", F.add_months(F.current_date(), 6)) \
    .withColumn("days_to_renewal", F.datediff(F.col("next_renewal_date"), F.current_date())) \
    .withColumn("renewal_likelihood", F.round(F.col("retention_probability") * F.expr("rand() * 0.1 + 0.9"), 4))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Final Selection and Write

# COMMAND ----------
df_customer_360 = df_customer_360 \
    .withColumn("snapshot_date", F.current_date()) \
    .withColumn("created_timestamp", F.current_timestamp()) \
    .withColumn("updated_timestamp", F.current_timestamp())

# Keep customer_key as surrogate key for analytics

# COMMAND ----------
# Write to gold layer
table_name = f"{gold_catalog}.customer_analytics.customer_360"

# Create schema if needed
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}.customer_analytics")

# Write to table (using liquid clustering defined in setup, not partitioning)
df_customer_360.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"✅ Successfully wrote customer 360 records to {table_name}")
print(f"   📊 Table uses liquid clustering for optimal query performance")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate and Display Sample

# COMMAND ----------
display(spark.table(table_name).limit(10))

# COMMAND ----------
# Summary statistics
print("Customer 360 Summary:")
spark.sql(f"""
    SELECT 
        value_tier,
        churn_risk_category,
        COUNT(*) as customer_count,
        ROUND(AVG(total_annual_premium), 2) as avg_annual_premium,
        ROUND(AVG(customer_lifetime_value), 2) as avg_clv,
        ROUND(AVG(cross_sell_score), 2) as avg_cross_sell_score
    FROM {table_name}
    GROUP BY value_tier, churn_risk_category
    ORDER BY customer_count DESC
""").show()

