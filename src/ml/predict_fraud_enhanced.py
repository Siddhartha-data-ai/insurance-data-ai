# Databricks notebook source
# MAGIC %md
# MAGIC # Enhanced Fraud Detection Model
# MAGIC
# MAGIC **ML-powered fraud detection to identify suspicious claims**
# MAGIC
# MAGIC **Features:**
# MAGIC - Claim characteristics and patterns
# MAGIC - Customer history and behavior
# MAGIC - Network analysis (provider, adjuster)
# MAGIC - Temporal patterns
# MAGIC - Amount anomalies
# MAGIC
# MAGIC **Output:** Enhanced fraud score, confidence level, and investigation priority

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, datediff, current_date
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pandas as pd

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

df_claims = spark.table(f"{silver_catalog}.claims.claim_fact")
df_customers = spark.table(f"{silver_catalog}.customers.customer_dim").filter("is_current = true")
df_policies = spark.table(f"{silver_catalog}.policies.policy_dim")

print(f"✅ Loaded:")
print(f"   Claims: {df_claims.count():,}")
print(f"   Customers: {df_customers.count():,}")
print(f"   Policies: {df_policies.count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Feature Engineering for Fraud Detection

# COMMAND ----------
print("🔧 Engineering fraud detection features...")

# Join claims with policies and customers
df_fraud_features = (
    df_claims.alias("c")
    .join(
        df_policies.alias("p").select("policy_id", "annual_premium", "coverage_amount", "state_code", "policy_type"),
        "policy_id",
        "left",
    )
    .join(
        df_customers.alias("cust").select(
            "customer_id", "age_years", "customer_tenure_months", "customer_segment", "credit_tier"
        ),
        "customer_id",
        "left",
    )
)

# Claim-level features
df_fraud_features = (
    df_fraud_features.withColumn(
        "claim_to_premium_ratio",
        when(col("annual_premium") > 0, col("claimed_amount") / col("annual_premium")).otherwise(0),
    )
    .withColumn(
        "claim_to_coverage_ratio",
        when(col("coverage_amount") > 0, col("claimed_amount") / col("coverage_amount")).otherwise(0),
    )
    .withColumn("excessive_claim", when(col("claim_to_coverage_ratio") > 0.8, 1).otherwise(0))
    .withColumn("late_reporting", when(col("days_to_report") > 7, 1).otherwise(0))
    .withColumn("location_risk", when(col("loss_location_state") != col("state_code"), 1).otherwise(0))
    .withColumn("high_amount", when(col("claimed_amount") > 50000, 1).otherwise(0))
    .withColumn("quick_settlement", when(col("days_open") < 5, 1).otherwise(0))
)

# Customer history features
window_cust = Window.partitionBy("customer_id")
df_fraud_features = (
    df_fraud_features.withColumn("customer_claim_count", F.count("*").over(window_cust))
    .withColumn("customer_total_claimed", F.sum("claimed_amount").over(window_cust))
    .withColumn("customer_avg_claim", F.avg("claimed_amount").over(window_cust))
    .withColumn("frequent_claimant", when(col("customer_claim_count") > 3, 1).otherwise(0))
)

# Adjuster pattern features (simplified)
df_fraud_features = df_fraud_features.withColumn(
    "adjuster_id", concat(lit("ADJ"), F.lpad((F.rand() * 50).cast("int"), 3, "0"))
)

window_adj = Window.partitionBy("adjuster_id")
df_fraud_features = df_fraud_features.withColumn("adjuster_claim_count", F.count("*").over(window_adj)).withColumn(
    "adjuster_avg_amount", F.avg("claimed_amount").over(window_adj)
)

# Create target variable (use existing fraud_score as proxy for actual fraud)
# In real scenario, you'd have historical fraud labels
df_fraud_features = df_fraud_features.withColumn(
    "is_fraud", when((col("fraud_score") > 0.7) | (col("siu_referral") == True), 1).otherwise(0)
)

# Fill nulls
numeric_features = [
    "claim_to_premium_ratio",
    "claim_to_coverage_ratio",
    "excessive_claim",
    "late_reporting",
    "location_risk",
    "high_amount",
    "quick_settlement",
    "customer_claim_count",
    "customer_total_claimed",
    "customer_avg_claim",
    "frequent_claimant",
    "adjuster_claim_count",
    "adjuster_avg_amount",
    "days_to_report",
    "days_open",
    "age_years",
    "customer_tenure_months",
]

for c in numeric_features:
    if c in df_fraud_features.columns:
        df_fraud_features = df_fraud_features.withColumn(c, F.coalesce(col(c), lit(0)))

print(f"✅ Feature engineering complete")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Train Model

# COMMAND ----------
print("🤖 Training enhanced fraud detection model...")

# Split data
train_data, test_data = df_fraud_features.randomSplit([0.8, 0.2], seed=42)

print(f"Training set: {train_data.count():,} claims")
print(f"Test set: {test_data.count():,} claims")
print(f"Fraud rate: {train_data.filter(col('is_fraud') == 1).count() / train_data.count() * 100:.2f}%")

# Select features
feature_cols = [
    "claimed_amount",
    "paid_amount",
    "days_to_report",
    "days_open",
    "claim_to_premium_ratio",
    "claim_to_coverage_ratio",
    "excessive_claim",
    "late_reporting",
    "location_risk",
    "high_amount",
    "quick_settlement",
    "customer_claim_count",
    "customer_total_claimed",
    "customer_avg_claim",
    "frequent_claimant",
    "adjuster_claim_count",
    "adjuster_avg_amount",
    "age_years",
    "customer_tenure_months",
    "fraud_score",
]

# Build pipeline
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw", handleInvalid="skip")
scaler = StandardScaler(inputCol="features_raw", outputCol="features")
rf = RandomForestClassifier(labelCol="is_fraud", featuresCol="features", numTrees=100, maxDepth=10, seed=42)

pipeline = Pipeline(stages=[assembler, scaler, rf])
model = pipeline.fit(train_data)

print("✅ Model trained!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Evaluate Model

# COMMAND ----------
print("📈 Evaluating model...")

predictions_test = model.transform(test_data)

evaluator_auc = BinaryClassificationEvaluator(labelCol="is_fraud", metricName="areaUnderROC")
auc = evaluator_auc.evaluate(predictions_test)

tp = predictions_test.filter((col("is_fraud") == 1) & (col("prediction") == 1)).count()
fp = predictions_test.filter((col("is_fraud") == 0) & (col("prediction") == 1)).count()
tn = predictions_test.filter((col("is_fraud") == 0) & (col("prediction") == 0)).count()
fn = predictions_test.filter((col("is_fraud") == 1) & (col("prediction") == 0)).count()

precision = tp / (tp + fp) if (tp + fp) > 0 else 0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0
f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

print(f"✅ Model Performance:")
print(f"   AUC-ROC: {auc:.3f}")
print(f"   Precision: {precision:.3f}")
print(f"   Recall: {recall:.3f}")
print(f"   F1 Score: {f1:.3f}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Predictions for All Open Claims

# COMMAND ----------
print("🎯 Generating fraud predictions for all claims...")

# Filter for claims that need investigation
df_predict = df_fraud_features.filter(~col("is_closed"))

predictions = model.transform(df_predict)

# Create enhanced fraud scores and alerts
predictions = (
    predictions.withColumn("ml_fraud_score", F.round(F.col("probability")[1] * 100, 2))
    .withColumn("combined_fraud_score", F.round((col("ml_fraud_score") + col("fraud_score") * 100) / 2, 2))
    .withColumn(
        "fraud_risk_category",
        when(col("combined_fraud_score") >= 70, "Critical")
        .when(col("combined_fraud_score") >= 50, "High")
        .when(col("combined_fraud_score") >= 30, "Medium")
        .otherwise("Low"),
    )
    .withColumn(
        "investigation_priority",
        when(col("combined_fraud_score") >= 70, 1)
        .when(col("combined_fraud_score") >= 50, 2)
        .when(col("combined_fraud_score") >= 30, 3)
        .otherwise(4),
    )
    .withColumn(
        "recommended_action",
        when(col("fraud_risk_category") == "Critical", "Immediate SIU Investigation")
        .when(col("fraud_risk_category") == "High", "Detailed Review Required")
        .when(col("fraud_risk_category") == "Medium", "Additional Documentation")
        .otherwise("Standard Processing"),
    )
    .withColumn("estimated_fraud_amount", F.round(col("claimed_amount") * col("combined_fraud_score") / 100, 2))
    .withColumn("prediction_date", current_date())
    .withColumn("model_confidence", F.round(F.greatest(F.col("probability")[0], F.col("probability")[1]) * 100, 2))
)

# Select final columns
final_predictions = predictions.select(
    "claim_id",
    "claim_number",
    "customer_id",
    "policy_id",
    "claim_type",
    "claim_status",
    "claimed_amount",
    "loss_date",
    "report_date",
    "fraud_score",
    "ml_fraud_score",
    "combined_fraud_score",
    "fraud_risk_category",
    "investigation_priority",
    "recommended_action",
    "estimated_fraud_amount",
    "model_confidence",
    "excessive_claim",
    "late_reporting",
    "location_risk",
    "frequent_claimant",
    "prediction_date",
)

print(f"✅ Generated predictions for {final_predictions.count():,} claims")

# Show distribution
print("\n📊 Fraud Risk Distribution:")
final_predictions.groupBy("fraud_risk_category").count().orderBy("investigation_priority").show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save to Gold Layer

# COMMAND ----------
print("💾 Saving fraud predictions...")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}.predictions")

table_name = f"{gold_catalog}.predictions.fraud_alerts"

final_predictions.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

print(f"✅ Saved to: {table_name}")

# Show critical cases
print("\n🚨 Critical Fraud Cases:")
display(
    final_predictions.filter(col("fraud_risk_category") == "Critical").orderBy(F.desc("combined_fraud_score")).limit(10)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print("=" * 70)
print("🚨 FRAUD DETECTION MODEL - SUMMARY")
print("=" * 70)

stats = (
    final_predictions.groupBy("fraud_risk_category")
    .agg(
        F.count("*").alias("claim_count"),
        F.avg("combined_fraud_score").alias("avg_fraud_score"),
        F.sum("estimated_fraud_amount").alias("total_potential_fraud"),
    )
    .orderBy("investigation_priority")
)

stats_pd = stats.toPandas()

for _, row in stats_pd.iterrows():
    print(f"\n{row['fraud_risk_category']} Risk:")
    print(f"  Claims: {row['claim_count']:,}")
    print(f"  Avg Fraud Score: {row['avg_fraud_score']:.1f}")
    print(f"  Potential Fraud Amount: ${row['total_potential_fraud']:,.2f}")

print("\n" + "=" * 70)
print(f"✅ Model AUC: {auc:.3f}, F1: {f1:.3f}")
print(f"✅ Predictions saved to: {table_name}")
print("=" * 70)
