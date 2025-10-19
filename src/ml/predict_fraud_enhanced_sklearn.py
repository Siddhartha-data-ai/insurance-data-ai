# Databricks notebook source
# MAGIC %md
# MAGIC # Enhanced Fraud Detection Model (Scikit-Learn)
# MAGIC
# MAGIC **Community Edition Compatible - Uses pandas + scikit-learn**
# MAGIC
# MAGIC **ML-powered fraud detection to identify suspicious claims**

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, datediff, current_date, concat, lpad
from pyspark.sql.window import Window
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
# MAGIC ## Feature Engineering

# COMMAND ----------
print("🔧 Engineering fraud detection features...")

# Join tables
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

# Claim features
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

# Customer history
window_cust = Window.partitionBy("customer_id")
df_fraud_features = (
    df_fraud_features.withColumn("customer_claim_count", F.count("*").over(window_cust))
    .withColumn("customer_total_claimed", F.sum("claimed_amount").over(window_cust))
    .withColumn("customer_avg_claim", F.avg("claimed_amount").over(window_cust))
    .withColumn("frequent_claimant", when(col("customer_claim_count") > 3, 1).otherwise(0))
)

# Target variable - use lower threshold to ensure we have positive examples
df_fraud_features = df_fraud_features.withColumn(
    "is_fraud",
    when(
        (col("fraud_score") > 0.5)
        | (col("siu_referral") == True)
        | (col("excessive_claim") == 1)
        | (col("late_reporting") == 1),
        1,
    ).otherwise(0),
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
    "days_to_report",
    "days_open",
    "age_years",
    "customer_tenure_months",
    "claimed_amount",
    "paid_amount",
    "fraud_score",
]

for c in numeric_features:
    if c in df_fraud_features.columns:
        df_fraud_features = df_fraud_features.withColumn(c, F.coalesce(col(c), lit(0)))

print(f"✅ Feature engineering complete")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Convert to Pandas

# COMMAND ----------
print("🔄 Converting to pandas...")

df_pandas = df_fraud_features.toPandas()
df_pandas = df_pandas.fillna(0)

# Convert all numeric columns from Decimal to float (Spark Decimal causes pandas arithmetic issues)
numeric_cols_to_convert = df_pandas.select_dtypes(include=["object"]).columns
for col_name in numeric_cols_to_convert:
    if col_name in numeric_features or col_name in [
        "fraud_score",
        "claimed_amount",
        "paid_amount",
        "annual_premium",
        "coverage_amount",
    ]:
        try:
            df_pandas[col_name] = pd.to_numeric(df_pandas[col_name], errors="coerce")
        except:
            pass

df_pandas = df_pandas.fillna(0)

print(f"✅ Converted: {len(df_pandas):,} claims")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Train Model

# COMMAND ----------
print("🤖 Training fraud detection model...")

# Feature columns
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
    "age_years",
    "customer_tenure_months",
    "fraud_score",
]

X = df_pandas[feature_cols].values
y = df_pandas["is_fraud"].values

# Check class balance
fraud_count = y.sum()
fraud_rate = y.mean()

print(f"📊 Class Distribution:")
print(f"   Total claims: {len(y):,}")
print(f"   Fraud cases: {fraud_count:,} ({fraud_rate*100:.2f}%)")
print(f"   Non-fraud: {len(y) - fraud_count:,} ({(1-fraud_rate)*100:.2f}%)")

# Check if we have both classes
if fraud_count == 0:
    print("\n⚠️  WARNING: No fraud cases found in training data!")
    print("   All claims will be predicted as non-fraudulent.")
    print("   Adjusting fraud detection to use existing fraud_score...")

    # Skip ML model, just use rule-based approach
    has_ml_model = False
elif fraud_count == len(y):
    print("\n⚠️  WARNING: All cases labeled as fraud!")
    print("   This is unusual. Using rule-based approach instead.")
    has_ml_model = False
else:
    # Split - use stratify only if we have both classes
    try:
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    except ValueError:
        # Fallback if stratify fails
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Scale
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train
    model = RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)
    model.fit(X_train_scaled, y_train)

    has_ml_model = True

    print(f"\n✅ Model trained!")
    print(f"   Training samples: {len(X_train):,}")
    print(f"   Fraud rate: {y_train.mean()*100:.2f}%")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Evaluate

# COMMAND ----------
print("📈 Evaluating model...")

if has_ml_model:
    y_pred = model.predict(X_test_scaled)
    y_pred_proba = model.predict_proba(X_test_scaled)

    # Handle single-class case
    if y_pred_proba.shape[1] == 1:
        print("⚠️  Model only learned one class - using rule-based fallback")
        has_ml_model = False
        auc = 0.5
        precision = 0.0
        recall = 0.0
        f1 = 0.0
    else:
        y_pred_proba = y_pred_proba[:, 1]

        try:
            auc = roc_auc_score(y_test, y_pred_proba)
        except:
            auc = 0.5

        try:
            precision = precision_score(y_test, y_pred, zero_division=0)
            recall = recall_score(y_test, y_pred, zero_division=0)
            f1 = f1_score(y_test, y_pred, zero_division=0)
        except:
            precision = 0.0
            recall = 0.0
            f1 = 0.0

        print(f"✅ Model Performance:")
        print(f"   AUC-ROC: {auc:.3f}")
        print(f"   Precision: {precision:.3f}")
        print(f"   Recall: {recall:.3f}")
        print(f"   F1 Score: {f1:.3f}")
else:
    print("⚠️  Using rule-based fraud detection (no ML model)")
    print("   Fraud scores based on existing fraud_score and risk indicators")
    auc = 0.5
    precision = 0.0
    recall = 0.0
    f1 = 0.0

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Predictions

# COMMAND ----------
print("🎯 Generating fraud predictions...")

# Filter open claims
df_open = df_pandas[df_pandas["is_closed"] == False].copy()

if has_ml_model:
    # Use ML model predictions
    X_open = df_open[feature_cols].values
    X_open_scaled = scaler.transform(X_open)

    ml_fraud_proba_array = model.predict_proba(X_open_scaled)

    # Handle single-class case
    if ml_fraud_proba_array.shape[1] == 1:
        ml_fraud_proba = np.full(len(df_open), 50.0)  # Neutral 50% if can't predict
    else:
        ml_fraud_proba = ml_fraud_proba_array[:, 1] * 100

    df_open["ml_fraud_score"] = ml_fraud_proba
    df_open["combined_fraud_score"] = (df_open["ml_fraud_score"] + df_open["fraud_score"] * 100) / 2
else:
    # Use rule-based approach only
    df_open["ml_fraud_score"] = df_open["fraud_score"] * 100  # Use existing score

    # Enhanced rule-based scoring
    df_open["combined_fraud_score"] = (
        df_open["fraud_score"] * 100 * 0.4  # 40% original score
        + df_open["excessive_claim"] * 25  # 25 points if excessive
        + df_open["late_reporting"] * 20  # 20 points if late
        + df_open["location_risk"] * 15  # 15 points if location mismatch
        + df_open["frequent_claimant"] * 20  # 20 points if frequent
    )

    # Cap at 100
    df_open["combined_fraud_score"] = df_open["combined_fraud_score"].clip(0, 100)

df_open["fraud_risk_category"] = pd.cut(
    df_open["combined_fraud_score"], bins=[0, 30, 50, 70, 100], labels=["Low", "Medium", "High", "Critical"]
)

df_open["investigation_priority"] = df_open["fraud_risk_category"].map(
    {"Low": 4, "Medium": 3, "High": 2, "Critical": 1}
)

df_open["recommended_action"] = df_open["fraud_risk_category"].map(
    {
        "Critical": "Immediate SIU Investigation",
        "High": "Detailed Review Required",
        "Medium": "Additional Documentation",
        "Low": "Standard Processing",
    }
)

df_open["estimated_fraud_amount"] = df_open["claimed_amount"] * df_open["combined_fraud_score"] / 100
df_open["prediction_date"] = pd.Timestamp.now().date()

# Select final columns
final_cols = [
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
    "excessive_claim",
    "late_reporting",
    "location_risk",
    "frequent_claimant",
    "prediction_date",
]

df_predictions = df_open[final_cols]

print(f"✅ Generated predictions for {len(df_predictions):,} claims")
print(f"\n📊 Fraud Risk Distribution:")
print(df_predictions["fraud_risk_category"].value_counts())

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save

# COMMAND ----------
print("💾 Saving fraud predictions...")

df_predictions_spark = spark.createDataFrame(df_predictions)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_catalog}.predictions")

table_name = f"{gold_catalog}.predictions.fraud_alerts"

df_predictions_spark.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

print(f"✅ Saved to: {table_name}")

print("\n🚨 Critical Fraud Cases:")
display(
    df_predictions_spark.filter(col("fraud_risk_category") == "Critical")
    .orderBy(F.desc("combined_fraud_score"))
    .limit(10)
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print("=" * 70)
print("🚨 FRAUD DETECTION MODEL - SUMMARY")
print("=" * 70)

summary = (
    df_predictions.groupby("fraud_risk_category")
    .agg({"claim_id": "count", "combined_fraud_score": "mean", "estimated_fraud_amount": "sum"})
    .round(2)
)

summary.columns = ["claim_count", "avg_fraud_score", "total_potential_fraud"]

for category in ["Critical", "High", "Medium", "Low"]:
    if category in summary.index:
        row = summary.loc[category]
        print(f"\n{category} Risk:")
        print(f"  Claims: {int(row['claim_count']):,}")
        print(f"  Avg Fraud Score: {row['avg_fraud_score']:.1f}")
        print(f"  Potential Fraud Amount: ${row['total_potential_fraud']:,.2f}")

print("\n" + "=" * 70)
print(f"✅ Model AUC: {auc:.3f}, F1: {f1:.3f}")
print(f"✅ Predictions saved to: {table_name}")
print("=" * 70)
