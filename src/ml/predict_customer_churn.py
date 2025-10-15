# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Churn Prediction Model
# MAGIC 
# MAGIC **Predicts which customers are at risk of canceling their policies in the next 30 days**
# MAGIC 
# MAGIC **Features:**
# MAGIC - Customer demographics and tenure
# MAGIC - Policy characteristics and premium history
# MAGIC - Claims history and patterns
# MAGIC - Payment behavior
# MAGIC - Customer engagement metrics
# MAGIC 
# MAGIC **Output:** Churn probability, risk category, and recommended actions

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, datediff, current_date, months_between
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier, GBTClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pandas as pd
import numpy as np

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
# Create widgets
dbutils.widgets.dropdown("silver_catalog", "insurance_dev_silver", 
                         ["insurance_dev_silver", "insurance_staging_silver", "insurance_prod_silver"], 
                         "Silver Catalog")
dbutils.widgets.dropdown("gold_catalog", "insurance_dev_gold", 
                         ["insurance_dev_gold", "insurance_staging_gold", "insurance_prod_gold"], 
                         "Gold Catalog")
dbutils.widgets.dropdown("churn_window_days", "30", ["30", "60", "90"], "Churn Prediction Window (Days)")

silver_catalog = dbutils.widgets.get("silver_catalog")
gold_catalog = dbutils.widgets.get("gold_catalog")
churn_window = int(dbutils.widgets.get("churn_window_days"))

print(f"✅ Configuration:")
print(f"   Silver Catalog: {silver_catalog}")
print(f"   Gold Catalog: {gold_catalog}")
print(f"   Churn Window: {churn_window} days")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Data

# COMMAND ----------
print("📊 Loading data from silver layer...")

# Load customer dimension (current records only)
df_customers = spark.table(f"{silver_catalog}.customers.customer_dim").filter("is_current = true")

# Load policies
df_policies = spark.table(f"{silver_catalog}.policies.policy_dim")

# Load claims
df_claims = spark.table(f"{silver_catalog}.claims.claim_fact")

print(f"✅ Loaded data:")
print(f"   Customers: {df_customers.count():,}")
print(f"   Policies: {df_policies.count():,}")
print(f"   Claims: {df_claims.count():,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------
print("🔧 Building features...")

# Customer-level features from customer_dim
df_features = df_customers.select(
    "customer_id",
    "age_years",
    "customer_tenure_months",
    "credit_tier",
    "customer_segment",
    "customer_status",
    "state_code"
).withColumn(
    "credit_score_numeric",
    when(col("credit_tier") == "Excellent", 4)
    .when(col("credit_tier") == "Good", 3)
    .when(col("credit_tier") == "Fair", 2)
    .otherwise(1)
)

# Target variable: customer_status == 'Cancelled' as historical churn indicator
df_features = df_features.withColumn(
    "churned",
    when(col("customer_status").isin("Cancelled", "Inactive"), 1).otherwise(0)
)

# Policy aggregations per customer
df_policy_agg = df_policies.groupBy("customer_id").agg(
    F.count("*").alias("total_policies"),
    F.sum(when(col("is_active"), 1).otherwise(0)).alias("active_policies"),
    F.sum(when(~col("is_active"), 1).otherwise(0)).alias("inactive_policies"),
    F.avg("annual_premium").alias("avg_annual_premium"),
    F.sum("annual_premium").alias("total_annual_premium"),
    F.avg("policy_age_days").alias("avg_policy_age_days"),
    F.max("policy_age_days").alias("max_policy_age_days"),
    F.min("policy_age_days").alias("min_policy_age_days"),
    F.countDistinct("policy_type").alias("policy_type_diversity")
)

df_features = df_features.join(df_policy_agg, "customer_id", "left")

# Claims aggregations per customer
df_claims_agg = df_claims.groupBy("customer_id").agg(
    F.count("*").alias("total_claims"),
    F.sum(when(~col("is_closed"), 1).otherwise(0)).alias("open_claims"),
    F.sum("claimed_amount").alias("total_claimed_amount"),
    F.sum("paid_amount").alias("total_paid_amount"),
    F.avg("fraud_score").alias("avg_fraud_score"),
    F.max("loss_date").alias("last_claim_date")
).withColumn(
    "days_since_last_claim",
    datediff(current_date(), col("last_claim_date"))
).withColumn(
    "claim_paid_ratio",
    when(col("total_claimed_amount") > 0, col("total_paid_amount") / col("total_claimed_amount")).otherwise(0)
)

df_features = df_features.join(df_claims_agg, "customer_id", "left")

# Fill nulls for customers with no policies or claims
numeric_cols = [
    "total_policies", "active_policies", "inactive_policies",
    "avg_annual_premium", "total_annual_premium",
    "avg_policy_age_days", "max_policy_age_days", "min_policy_age_days",
    "policy_type_diversity", "total_claims", "open_claims",
    "total_claimed_amount", "total_paid_amount", "avg_fraud_score",
    "days_since_last_claim", "claim_paid_ratio"
]

for c in numeric_cols:
    if c in df_features.columns:
        df_features = df_features.withColumn(c, F.coalesce(col(c), lit(0)))

# Derived risk indicators
df_features = df_features \
    .withColumn("policy_per_claim_ratio", 
                when(col("total_claims") > 0, col("total_policies") / col("total_claims")).otherwise(999)) \
    .withColumn("has_open_claims", when(col("open_claims") > 0, 1).otherwise(0)) \
    .withColumn("high_fraud_score", when(col("avg_fraud_score") > 0.5, 1).otherwise(0)) \
    .withColumn("low_premium", when(col("avg_annual_premium") < 1000, 1).otherwise(0)) \
    .withColumn("short_tenure", when(col("customer_tenure_months") < 12, 1).otherwise(0)) \
    .withColumn("young_customer", when(col("age_years") < 30, 1).otherwise(0)) \
    .withColumn("inactive_ratio", 
                when(col("total_policies") > 0, col("inactive_policies") / col("total_policies")).otherwise(0))

print(f"✅ Feature engineering complete: {len(df_features.columns)} features")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Train-Test Split

# COMMAND ----------
print("📊 Splitting data for training...")

# Filter for active and cancelled customers (exclude inactive for training)
df_train_data = df_features.filter(col("customer_status").isin("Active", "Cancelled"))

# Split 80-20
train_data, test_data = df_train_data.randomSplit([0.8, 0.2], seed=42)

print(f"✅ Data split:")
print(f"   Training set: {train_data.count():,} customers")
print(f"   Test set: {test_data.count():,} customers")
print(f"   Churn rate in training: {train_data.filter(col('churned') == 1).count() / train_data.count() * 100:.2f}%")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Train ML Model

# COMMAND ----------
print("🤖 Training churn prediction model...")

# Select features for model
feature_cols = [
    "age_years", "customer_tenure_months", "credit_score_numeric",
    "total_policies", "active_policies", "inactive_policies",
    "avg_annual_premium", "total_annual_premium",
    "avg_policy_age_days", "policy_type_diversity",
    "total_claims", "open_claims", "total_claimed_amount", "total_paid_amount",
    "avg_fraud_score", "days_since_last_claim", "claim_paid_ratio",
    "policy_per_claim_ratio", "has_open_claims", "high_fraud_score",
    "low_premium", "short_tenure", "young_customer", "inactive_ratio"
]

# Build ML pipeline
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
scaler = StandardScaler(inputCol="features_raw", outputCol="features")
rf = RandomForestClassifier(
    labelCol="churned",
    featuresCol="features",
    numTrees=100,
    maxDepth=10,
    seed=42
)

pipeline = Pipeline(stages=[assembler, scaler, rf])

# Train model
model = pipeline.fit(train_data)

print("✅ Model trained successfully!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Evaluate Model

# COMMAND ----------
print("📈 Evaluating model performance...")

# Make predictions on test set
predictions_test = model.transform(test_data)

# Calculate metrics
evaluator_auc = BinaryClassificationEvaluator(labelCol="churned", metricName="areaUnderROC")
evaluator_pr = BinaryClassificationEvaluator(labelCol="churned", metricName="areaUnderPR")

auc = evaluator_auc.evaluate(predictions_test)
pr_auc = evaluator_pr.evaluate(predictions_test)

# Confusion matrix
tp = predictions_test.filter((col("churned") == 1) & (col("prediction") == 1)).count()
fp = predictions_test.filter((col("churned") == 0) & (col("prediction") == 1)).count()
tn = predictions_test.filter((col("churned") == 0) & (col("prediction") == 0)).count()
fn = predictions_test.filter((col("churned") == 1) & (col("prediction") == 0)).count()

accuracy = (tp + tn) / (tp + fp + tn + fn)
precision = tp / (tp + fp) if (tp + fp) > 0 else 0
recall = tp / (tp + fn) if (tp + fn) > 0 else 0
f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

print(f"✅ Model Performance:")
print(f"   AUC-ROC: {auc:.3f}")
print(f"   AUC-PR: {pr_auc:.3f}")
print(f"   Accuracy: {accuracy:.3f}")
print(f"   Precision: {precision:.3f}")
print(f"   Recall: {recall:.3f}")
print(f"   F1 Score: {f1:.3f}")
print(f"\n   Confusion Matrix:")
print(f"   True Positives: {tp:,}  | False Positives: {fp:,}")
print(f"   False Negatives: {fn:,} | True Negatives: {tn:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Predictions for All Active Customers

# COMMAND ----------
print("🎯 Generating churn predictions for all active customers...")

# Filter for active customers only
df_active_customers = df_features.filter(col("customer_status") == "Active")

# Make predictions
predictions = model.transform(df_active_customers)

# Extract probability and create risk categories
predictions = predictions.withColumn(
    "churn_probability",
    F.round(F.col("probability")[1] * 100, 2)
).withColumn(
    "churn_risk_category",
    when(col("churn_probability") >= 70, "High Risk")
    .when(col("churn_probability") >= 40, "Medium Risk")
    .otherwise("Low Risk")
).withColumn(
    "recommended_action",
    when(col("churn_probability") >= 70, "Immediate Retention Campaign")
    .when(col("churn_probability") >= 40, "Proactive Engagement")
    .otherwise("Monitor")
).withColumn(
    "prediction_date", current_date()
).withColumn(
    "prediction_window_days", lit(churn_window)
)

# Select final columns
final_predictions = predictions.select(
    "customer_id",
    "age_years",
    "customer_tenure_months",
    "credit_tier",
    "customer_segment",
    "state_code",
    "total_policies",
    "active_policies",
    "total_annual_premium",
    "total_claims",
    "avg_fraud_score",
    "churn_probability",
    "churn_risk_category",
    "recommended_action",
    "prediction_date",
    "prediction_window_days"
)

print(f"✅ Generated predictions for {final_predictions.count():,} active customers")

# Show risk distribution
print("\n📊 Churn Risk Distribution:")
final_predictions.groupBy("churn_risk_category").count().orderBy(F.desc("count")).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Predictions to Gold Layer

# COMMAND ----------
print("💾 Saving predictions to gold layer...")

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}.predictions")

# Save predictions
table_name = f"{gold_catalog}.predictions.customer_churn_risk"

final_predictions.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"✅ Predictions saved to: {table_name}")

# Display sample
print("\n📊 Sample High-Risk Customers:")
display(final_predictions.filter(col("churn_risk_category") == "High Risk").orderBy(F.desc("churn_probability")).limit(10))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------
print("=" * 70)
print("📊 CHURN PREDICTION MODEL - SUMMARY")
print("=" * 70)

stats = final_predictions.groupBy("churn_risk_category").agg(
    F.count("*").alias("customer_count"),
    F.avg("churn_probability").alias("avg_churn_prob"),
    F.sum("total_annual_premium").alias("premium_at_risk")
).orderBy(F.desc("avg_churn_prob"))

stats_pd = stats.toPandas()

for _, row in stats_pd.iterrows():
    print(f"\n{row['churn_risk_category']}:")
    print(f"  Customers: {row['customer_count']:,}")
    print(f"  Avg Churn Probability: {row['avg_churn_prob']:.1f}%")
    print(f"  Premium at Risk: ${row['premium_at_risk']:,.2f}")

print("\n" + "=" * 70)
print(f"✅ Model Performance: AUC-ROC = {auc:.3f}, F1 = {f1:.3f}")
print(f"✅ Predictions saved to: {table_name}")
print("=" * 70)

