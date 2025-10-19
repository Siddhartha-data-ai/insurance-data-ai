# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Churn Prediction Model (Scikit-Learn)
# MAGIC
# MAGIC **Community Edition Compatible - Uses pandas + scikit-learn**
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
from pyspark.sql.functions import col, lit, when, datediff, current_date
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score, confusion_matrix

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

df_customers = spark.table(f"{silver_catalog}.customers.customer_dim").filter("is_current = true")
df_policies = spark.table(f"{silver_catalog}.policies.policy_dim")
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

# Customer-level features
df_features = (
    df_customers.select(
        "customer_id",
        "age_years",
        "customer_tenure_months",
        "credit_tier",
        "customer_segment",
        "customer_status",
        "state_code",
    )
    .withColumn(
        "credit_score_numeric",
        when(col("credit_tier") == "Excellent", 4)
        .when(col("credit_tier") == "Good", 3)
        .when(col("credit_tier") == "Fair", 2)
        .otherwise(1),
    )
    .withColumn("churned", when(col("customer_status").isin("Cancelled", "Inactive"), 1).otherwise(0))
)

# Policy aggregations
df_policy_agg = df_policies.groupBy("customer_id").agg(
    F.count("*").alias("total_policies"),
    F.sum(when(col("is_active"), 1).otherwise(0)).alias("active_policies"),
    F.sum(when(~col("is_active"), 1).otherwise(0)).alias("inactive_policies"),
    F.avg("annual_premium").alias("avg_annual_premium"),
    F.sum("annual_premium").alias("total_annual_premium"),
    F.avg("policy_age_days").alias("avg_policy_age_days"),
    F.countDistinct("policy_type").alias("policy_type_diversity"),
)

df_features = df_features.join(df_policy_agg, "customer_id", "left")

# Claims aggregations
df_claims_agg = (
    df_claims.groupBy("customer_id")
    .agg(
        F.count("*").alias("total_claims"),
        F.sum(when(~col("is_closed"), 1).otherwise(0)).alias("open_claims"),
        F.sum("claimed_amount").alias("total_claimed_amount"),
        F.sum("paid_amount").alias("total_paid_amount"),
        F.avg("fraud_score").alias("avg_fraud_score"),
        F.max("loss_date").alias("last_claim_date"),
    )
    .withColumn("days_since_last_claim", datediff(current_date(), col("last_claim_date")))
    .withColumn(
        "claim_paid_ratio",
        when(col("total_claimed_amount") > 0, col("total_paid_amount") / col("total_claimed_amount")).otherwise(0),
    )
)

df_features = df_features.join(df_claims_agg, "customer_id", "left")

# Fill nulls
numeric_cols = [
    "total_policies",
    "active_policies",
    "inactive_policies",
    "avg_annual_premium",
    "total_annual_premium",
    "avg_policy_age_days",
    "policy_type_diversity",
    "total_claims",
    "open_claims",
    "total_claimed_amount",
    "total_paid_amount",
    "avg_fraud_score",
    "days_since_last_claim",
    "claim_paid_ratio",
]

for c in numeric_cols:
    if c in df_features.columns:
        df_features = df_features.withColumn(c, F.coalesce(col(c), lit(0)))

# Derived features
df_features = (
    df_features.withColumn("has_open_claims", when(col("open_claims") > 0, 1).otherwise(0))
    .withColumn("high_fraud_score", when(col("avg_fraud_score") > 0.5, 1).otherwise(0))
    .withColumn("low_premium", when(col("avg_annual_premium") < 1000, 1).otherwise(0))
    .withColumn("short_tenure", when(col("customer_tenure_months") < 12, 1).otherwise(0))
    .withColumn("young_customer", when(col("age_years") < 30, 1).otherwise(0))
    .withColumn(
        "inactive_ratio", when(col("total_policies") > 0, col("inactive_policies") / col("total_policies")).otherwise(0)
    )
)

print(f"✅ Feature engineering complete")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Convert to Pandas for Scikit-Learn

# COMMAND ----------
print("🔄 Converting to pandas...")

# Convert to pandas
df_pandas = df_features.toPandas()

# Fill any remaining NaNs
df_pandas = df_pandas.fillna(0)

# Convert all numeric columns from Decimal to float (Spark Decimal causes pandas arithmetic issues)
numeric_cols_to_convert = df_pandas.select_dtypes(include=["object"]).columns
for col_name in numeric_cols_to_convert:
    try:
        df_pandas[col_name] = pd.to_numeric(df_pandas[col_name], errors="coerce")
    except:
        pass

df_pandas = df_pandas.fillna(0)

print(f"✅ Converted to pandas: {len(df_pandas):,} rows, {len(df_pandas.columns)} columns")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Train-Test Split

# COMMAND ----------
print("📊 Preparing data for training...")

# Filter for training (active and cancelled customers)
df_train = df_pandas[df_pandas["customer_status"].isin(["Active", "Cancelled"])].copy()

# Select feature columns
feature_cols = [
    "age_years",
    "customer_tenure_months",
    "credit_score_numeric",
    "total_policies",
    "active_policies",
    "inactive_policies",
    "avg_annual_premium",
    "total_annual_premium",
    "avg_policy_age_days",
    "policy_type_diversity",
    "total_claims",
    "open_claims",
    "total_claimed_amount",
    "total_paid_amount",
    "avg_fraud_score",
    "days_since_last_claim",
    "claim_paid_ratio",
    "has_open_claims",
    "high_fraud_score",
    "low_premium",
    "short_tenure",
    "young_customer",
    "inactive_ratio",
]

X = df_train[feature_cols].values
y = df_train["churned"].values

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Scale features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

print(f"✅ Data prepared:")
print(f"   Training samples: {len(X_train):,}")
print(f"   Test samples: {len(X_test):,}")
print(f"   Churn rate: {y_train.mean()*100:.2f}%")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Train Model

# COMMAND ----------
print("🤖 Training Random Forest model...")

# Train model
model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)

model.fit(X_train_scaled, y_train)

print("✅ Model trained successfully!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Evaluate Model

# COMMAND ----------
print("📈 Evaluating model performance...")

# Predictions
y_pred = model.predict(X_test_scaled)
y_pred_proba = model.predict_proba(X_test_scaled)[:, 1]

# Calculate metrics
auc = roc_auc_score(y_test, y_pred_proba)
precision = precision_score(y_test, y_pred)
recall = recall_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)

# Confusion matrix
cm = confusion_matrix(y_test, y_pred)
tn, fp, fn, tp = cm.ravel()

accuracy = (tp + tn) / (tp + fp + tn + fn)

print(f"✅ Model Performance:")
print(f"   AUC-ROC: {auc:.3f}")
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

# Filter active customers
df_active = df_pandas[df_pandas["customer_status"] == "Active"].copy()

# Prepare features
X_active = df_active[feature_cols].values
X_active_scaled = scaler.transform(X_active)

# Make predictions
churn_proba = model.predict_proba(X_active_scaled)[:, 1] * 100  # Convert to percentage

# Add predictions to dataframe
df_active["churn_probability"] = churn_proba
df_active["churn_risk_category"] = pd.cut(
    df_active["churn_probability"], bins=[0, 40, 70, 100], labels=["Low Risk", "Medium Risk", "High Risk"]
)
df_active["recommended_action"] = df_active["churn_risk_category"].map(
    {"Low Risk": "Monitor", "Medium Risk": "Proactive Engagement", "High Risk": "Immediate Retention Campaign"}
)
df_active["prediction_date"] = pd.Timestamp.now().date()
df_active["prediction_window_days"] = churn_window

# Select final columns
final_cols = [
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
    "prediction_window_days",
]

df_predictions = df_active[final_cols]

print(f"✅ Generated predictions for {len(df_predictions):,} active customers")
print(f"\n📊 Churn Risk Distribution:")
print(df_predictions["churn_risk_category"].value_counts())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Predictions to Gold Layer

# COMMAND ----------
print("💾 Saving predictions to gold layer...")

# Convert back to Spark DataFrame
df_predictions_spark = spark.createDataFrame(df_predictions)

# Create schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}.predictions")

# Save
table_name = f"{gold_catalog}.predictions.customer_churn_risk"

df_predictions_spark.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

print(f"✅ Predictions saved to: {table_name}")

# Display sample
print("\n📊 Sample High-Risk Customers:")
display(
    df_predictions_spark.filter(col("churn_risk_category") == "High Risk")
    .orderBy(F.desc("churn_probability"))
    .limit(10)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary Statistics

# COMMAND ----------
print("=" * 70)
print("📊 CHURN PREDICTION MODEL - SUMMARY")
print("=" * 70)

summary = (
    df_predictions.groupby("churn_risk_category")
    .agg({"customer_id": "count", "churn_probability": "mean", "total_annual_premium": "sum"})
    .round(2)
)

summary.columns = ["customer_count", "avg_churn_prob", "premium_at_risk"]

for category in ["High Risk", "Medium Risk", "Low Risk"]:
    if category in summary.index:
        row = summary.loc[category]
        print(f"\n{category}:")
        print(f"  Customers: {int(row['customer_count']):,}")
        print(f"  Avg Churn Probability: {row['avg_churn_prob']:.1f}%")
        print(f"  Premium at Risk: ${row['premium_at_risk']:,.2f}")

print("\n" + "=" * 70)
print(f"✅ Model Performance: AUC-ROC = {auc:.3f}, F1 = {f1:.3f}")
print(f"✅ Predictions saved to: {table_name}")
print("=" * 70)
