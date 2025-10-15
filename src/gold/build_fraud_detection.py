# Databricks notebook source
# MAGIC %md
# MAGIC # Claims Fraud Detection Analytics
# MAGIC Build comprehensive fraud detection analytics with ML scores and rule-based indicators

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
# Load silver tables (using SCD Type 2 for customers)
df_claims = spark.table(f"{silver_catalog}.claims.claim_fact")
df_customers = spark.table(f"{silver_catalog}.customers.customer_dim").filter("is_current = true")
df_policies = spark.table(f"{silver_catalog}.policies.policy_dim")

print(f"✅ Analyzing {df_claims.count():,} claims for fraud detection")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Calculate Fraud Indicators

# COMMAND ----------
# Join claims with policies for additional context (including state for location mismatch check)
df_fraud = df_claims.alias("c") \
    .join(df_policies.alias("p").select("policy_id", "annual_premium", "coverage_amount", "state_code"), "policy_id", "left")

# Calculate fraud indicators
df_fraud = df_fraud \
    .withColumn("late_reporting_flag", F.col("days_to_report") > 7) \
    .withColumn("excessive_amount_flag", F.col("claimed_amount") > F.col("coverage_amount") * 0.8) \
    .withColumn("location_mismatch_flag", 
                (F.col("loss_location_state").isNotNull()) & 
                (F.col("loss_location_state") != F.col("state_code")))

# Detect duplicate/similar claims (simplified)
window_customer = Window.partitionBy("customer_id")
df_fraud = df_fraud \
    .withColumn("customer_total_claims", F.count("*").over(window_customer)) \
    .withColumn("frequent_claimant_flag", F.col("customer_total_claims") > 3)

# Medical billing anomaly (for health claims)
df_fraud = df_fraud \
    .withColumn("medical_billing_anomaly_flag",
                F.when((F.col("claim_type") == "Health") & 
                      (F.col("claimed_amount") > 30000), True)
                 .otherwise(False))

# Suspicious injury pattern (simplified)
df_fraud = df_fraud \
    .withColumn("suspicious_injury_pattern_flag",
                F.when((F.col("claim_type") == "Auto") & 
                      (F.col("claimed_amount") > 20000) &
                      (F.col("days_to_report") > 5), True)
                 .otherwise(False))

# Count total fraud indicators
fraud_indicator_cols = [
    "late_reporting_flag", "excessive_amount_flag", "location_mismatch_flag",
    "frequent_claimant_flag", "medical_billing_anomaly_flag", "suspicious_injury_pattern_flag"
]

df_fraud = df_fraud.withColumn(
    "total_fraud_indicators",
    sum([F.when(F.col(c), 1).otherwise(0) for c in fraud_indicator_cols])
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Calculate Fraud Scores

# COMMAND ----------
# Rule-based score (0-100)
df_fraud = df_fraud.withColumn(
    "rule_based_score",
    F.round(
        (F.when(F.col("late_reporting_flag"), 15).otherwise(0) +
         F.when(F.col("excessive_amount_flag"), 20).otherwise(0) +
         F.when(F.col("location_mismatch_flag"), 10).otherwise(0) +
         F.when(F.col("frequent_claimant_flag"), 20).otherwise(0) +
         F.when(F.col("medical_billing_anomaly_flag"), 15).otherwise(0) +
         F.when(F.col("suspicious_injury_pattern_flag"), 20).otherwise(0)),
        2
    )
)

# Behavioral score (simplified - would use ML in production)
df_fraud = df_fraud.withColumn(
    "behavioral_score",
    F.round(F.expr("rand() * 30 + 20 + total_fraud_indicators * 5"), 2)
)

# Network score (detecting fraud rings - simplified)
df_fraud = df_fraud.withColumn(
    "network_score",
    F.round(F.expr("rand() * 20 + 10"), 2)
)

# ML fraud prediction (mock - would use actual ML model)
df_fraud = df_fraud.withColumn(
    "ml_fraud_prediction",
    F.round(
        (F.col("rule_based_score") * 0.4 + 
         F.col("behavioral_score") * 0.3 + 
         F.col("network_score") * 0.3) / 100,
        4
    )
)

# Overall fraud score
df_fraud = df_fraud.withColumn(
    "overall_fraud_score",
    F.round(
        F.col("rule_based_score") * 0.5 + 
        F.col("behavioral_score") * 0.3 + 
        F.col("network_score") * 0.2,
        2
    )
)

# Fraud risk category
df_fraud = df_fraud.withColumn(
    "fraud_risk_category",
    F.when(F.col("overall_fraud_score") >= 80, "Critical")
     .when(F.col("overall_fraud_score") >= 60, "High")
     .when(F.col("overall_fraud_score") >= 40, "Medium")
     .otherwise("Low")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Customer and Provider Fraud History

# COMMAND ----------
# Customer fraud history
customer_fraud_history = df_fraud.groupBy("customer_id").agg(
    F.count("*").alias("customer_total_claims_history"),
    F.sum(F.when(F.col("siu_referral"), 1).otherwise(0)).alias("customer_siu_referrals"),
    F.sum(F.when(F.col("claim_status") == "Denied", 1).otherwise(0)).alias("customer_denied_claims"),
    F.avg(F.col("overall_fraud_score")).alias("customer_fraud_score")
)

df_fraud = df_fraud.join(customer_fraud_history, "customer_id", "left")

# Provider fraud risk (mock data)
df_fraud = df_fraud \
    .withColumn("provider_id", 
                F.when(F.col("claim_type").isin(["Auto", "Property"]), 
                      F.concat(F.lit("PROV"), F.lpad(F.expr("cast(rand() * 1000 as int)"), 4, "0")))
                 .otherwise(F.lit(None))) \
    .withColumn("provider_name", 
                F.when(F.col("provider_id").isNotNull(),
                      F.concat(F.lit("Provider "), F.col("provider_id")))
                 .otherwise(F.lit(None))) \
    .withColumn("provider_fraud_history", F.expr("cast(rand() * 5 as int)")) \
    .withColumn("provider_fraud_score", F.round(F.expr("rand() * 40 + 10"), 2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Financial Anomaly Detection

# COMMAND ----------
# Claim to premium ratio
df_fraud = df_fraud.withColumn(
    "claim_to_premium_ratio",
    F.when(F.col("annual_premium") > 0,
           F.round(F.col("claimed_amount") / F.col("annual_premium"), 4))
     .otherwise(F.lit(0))
)

# Amount vs similar claims percentile (mock)
df_fraud = df_fraud.withColumn(
    "amount_vs_similar_claims_percentile",
    F.round(F.expr("rand() * 100"), 2)
)

df_fraud = df_fraud.withColumn(
    "settlement_speed_percentile",
    F.round(F.expr("rand() * 100"), 2)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Investigation and Actions

# COMMAND ----------
df_fraud = df_fraud \
    .withColumn("siu_referral_date",
                F.when(F.col("siu_referral"), F.col("report_date") + F.expr("INTERVAL 2 DAY"))
                 .otherwise(F.lit(None))) \
    .withColumn("investigation_status",
                F.when(F.col("siu_referral"),
                      F.element_at(F.array([F.lit("Pending"), F.lit("In Progress"), F.lit("Completed")]),
                                  F.expr("cast(rand() * 3 as int) + 1")))
                 .otherwise(F.lit(None))) \
    .withColumn("investigation_result",
                F.when(F.col("investigation_status") == "Completed",
                      F.element_at(F.array([F.lit("Confirmed Fraud"), F.lit("Not Fraud"), F.lit("Suspicious")]),
                                  F.when(F.col("overall_fraud_score") > 70, 1)
                                  .when(F.col("overall_fraud_score") > 50, 3)
                                  .otherwise(2)))
                 .otherwise(F.lit(None))) \
    .withColumn("investigator_id",
                F.when(F.col("siu_referral"), F.concat(F.lit("INV"), F.lpad(F.expr("cast(rand() * 50 as int)"), 3, "0")))
                 .otherwise(F.lit(None)))

# Recommended action
df_fraud = df_fraud.withColumn(
    "recommended_action",
    F.when(F.col("overall_fraud_score") >= 80, "Refer to SIU")
     .when(F.col("overall_fraud_score") >= 60, "Investigate")
     .when(F.col("overall_fraud_score") >= 40, "Additional Review")
     .otherwise("Approve")
)

df_fraud = df_fraud.withColumn(
    "action_priority",
    F.when(F.col("overall_fraud_score") >= 80, "Urgent")
     .when(F.col("overall_fraud_score") >= 60, "High")
     .when(F.col("overall_fraud_score") >= 40, "Medium")
     .otherwise("Low")
)

# Estimated exposure and potential recovery
df_fraud = df_fraud \
    .withColumn("estimated_exposure", 
                F.round(F.col("claimed_amount") * F.col("ml_fraud_prediction"), 2)) \
    .withColumn("potential_recovery",
                F.round(F.col("estimated_exposure") * 0.6, 2))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Model Metadata

# COMMAND ----------
df_fraud = df_fraud \
    .withColumn("model_version", F.lit("v1.0.0")) \
    .withColumn("model_confidence", F.round(F.expr("rand() * 0.3 + 0.65"), 4)) \
    .withColumn("model_explanation",
                F.concat(
                    F.lit("Top factors: "),
                    F.when(F.col("late_reporting_flag"), F.lit("Late Reporting, ")).otherwise(F.lit("")),
                    F.when(F.col("excessive_amount_flag"), F.lit("Excessive Amount, ")).otherwise(F.lit("")),
                    F.when(F.col("frequent_claimant_flag"), F.lit("Frequent Claimant")).otherwise(F.lit(""))
                ))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Final Selection and Write

# COMMAND ----------
df_fraud = df_fraud \
    .withColumn("analysis_date", F.current_date()) \
    .withColumn("created_timestamp", F.current_timestamp()) \
    .withColumn("updated_timestamp", F.current_timestamp()) \
    .withColumn("claim_key", F.monotonically_increasing_id())

# Select final columns
final_columns = [
    "claim_key", "claim_id", "claim_number", "policy_id", "customer_id",
    "claim_type", "loss_type", "claim_status", "loss_date", "report_date",
    "claimed_amount", "paid_amount",
    # Fraud scores
    "overall_fraud_score", "fraud_risk_category", "ml_fraud_prediction",
    "rule_based_score", "behavioral_score", "network_score",
    # Fraud indicators
    "late_reporting_flag", "excessive_amount_flag", "location_mismatch_flag",
    "frequent_claimant_flag", "medical_billing_anomaly_flag", "suspicious_injury_pattern_flag",
    "total_fraud_indicators",
    # Customer history
    "customer_total_claims_history", "customer_siu_referrals", "customer_denied_claims", "customer_fraud_score",
    # Provider
    "provider_id", "provider_name", "provider_fraud_history", "provider_fraud_score",
    # Financial anomalies
    "claim_to_premium_ratio", "amount_vs_similar_claims_percentile", "settlement_speed_percentile",
    # Investigation
    "siu_referral_flag", "siu_referral_date", "investigation_status", "investigation_result", "investigator_id",
    # Actions
    "recommended_action", "action_priority", "estimated_exposure", "potential_recovery",
    # Model metadata
    "model_version", "model_confidence", "model_explanation",
    # Timestamps
    "analysis_date", "created_timestamp", "updated_timestamp"
]

# Rename siu_referral to siu_referral_flag for consistency
df_fraud = df_fraud.withColumnRenamed("siu_referral", "siu_referral_flag")

df_fraud_final = df_fraud.select([c for c in final_columns if c in df_fraud.columns])

# COMMAND ----------
table_name = f"{gold_catalog}.claims_analytics.claims_fraud_detection"

# Create schema if needed
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}.claims_analytics")

# Write to table (using liquid clustering defined in setup, not partitioning)
df_fraud_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"✅ Successfully wrote fraud detection records to {table_name}")
print(f"   📊 Table uses liquid clustering for optimal query performance")

# COMMAND ----------
display(spark.table(table_name).orderBy(F.desc("overall_fraud_score")).limit(20))

# COMMAND ----------
# Fraud detection summary
print("Fraud Detection Summary:")
spark.sql(f"""
    SELECT 
        fraud_risk_category,
        COUNT(*) as claim_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
        ROUND(AVG(overall_fraud_score), 2) as avg_fraud_score,
        ROUND(SUM(estimated_exposure), 2) as total_exposure,
        ROUND(SUM(potential_recovery), 2) as total_potential_recovery
    FROM {table_name}
    GROUP BY fraud_risk_category
    ORDER BY 
        CASE fraud_risk_category
            WHEN 'Critical' THEN 1
            WHEN 'High' THEN 2
            WHEN 'Medium' THEN 3
            ELSE 4
        END
""").show()

