# Databricks notebook source
# MAGIC %md
# MAGIC # Check Prerequisites for ML Notebooks
# MAGIC
# MAGIC **This notebook verifies all required source tables exist before running ML predictions**

# COMMAND ----------
from pyspark.sql import functions as F

print("=" * 70)
print(" 🔍 CHECKING PREREQUISITES FOR ML PREDICTIONS")
print("=" * 70)
print()

# Define required tables for each model
requirements = {
    "Churn Prediction": [
        "insurance_dev_silver.customers.customer_dim",
        "insurance_dev_silver.policies.policy_dim",
        "insurance_dev_silver.claims.claim_fact",
    ],
    "Fraud Detection": [
        "insurance_dev_silver.customers.customer_dim",
        "insurance_dev_silver.policies.policy_dim",
        "insurance_dev_silver.claims.claim_fact",
    ],
    "Claim Forecasting": ["insurance_dev_silver.claims.claim_fact"],
    "Premium Optimization": [
        "insurance_dev_silver.customers.customer_dim",
        "insurance_dev_silver.policies.policy_dim",
        "insurance_dev_silver.claims.claim_fact",
    ],
}

# Check each model's requirements
results = {}

for model_name, tables in requirements.items():
    print(f"📊 {model_name}")
    print("-" * 70)

    model_ok = True

    for table in tables:
        try:
            count = spark.table(table).count()
            print(f"   ✅ {table}: {count:,} rows")
        except Exception as e:
            print(f"   ❌ {table}: NOT FOUND")
            print(f"      Error: {str(e)[:100]}")
            model_ok = False

    results[model_name] = model_ok
    print()

# COMMAND ----------
# Summary
print("=" * 70)
print(" 📋 SUMMARY")
print("=" * 70)
print()

for model_name, is_ok in results.items():
    status = "✅ Ready" if is_ok else "❌ Missing Data"
    print(f"{status}  {model_name}")

print()
print("=" * 70)

# Check if customer_360 exists (optional but helpful for churn)
print()
print("📊 Additional Gold Tables (Optional but Recommended):")
print("-" * 70)

optional_tables = [
    "insurance_dev_gold.customer_analytics.customer_360",
    "insurance_dev_gold.claims_analytics.claims_fraud_detection",
]

for table in optional_tables:
    try:
        count = spark.table(table).count()
        print(f"   ✅ {table}: {count:,} rows")
    except:
        print(f"   ⚠️  {table}: Not found (models will still work)")

print()
print("=" * 70)

# COMMAND ----------
# Recommendations
print()
print("🎯 RECOMMENDATIONS:")
print("=" * 70)
print()

all_ready = all(results.values())

if all_ready:
    print("✅ All prerequisites met! You can run all ML notebooks.")
    print()
    print("Next steps:")
    print("1. Run notebooks individually to test")
    print("2. Or run master orchestrator: run_all_predictions")
else:
    print("⚠️  Some tables are missing. Here's what to do:")
    print()

    if not results.get("Churn Prediction", True):
        print("For Churn Prediction:")
        print("   1. Run: /Workspace/Shared/insurance-analytics/bronze/generate_customers_data")
        print("   2. Run: /Workspace/Shared/insurance-analytics/bronze/generate_policies_data")
        print("   3. Run: /Workspace/Shared/insurance-analytics/bronze/generate_claims_data")
        print("   4. Run: /Workspace/Shared/insurance-analytics/transformations/transform_bronze_to_silver")
        print()

    if not results.get("Fraud Detection", True):
        print("For Fraud Detection:")
        print("   Same as above - needs silver layer tables")
        print()

    if not results.get("Claim Forecasting", True):
        print("For Claim Forecasting:")
        print("   1. Run: /Workspace/Shared/insurance-analytics/bronze/generate_claims_data")
        print("   2. Run: /Workspace/Shared/insurance-analytics/transformations/transform_bronze_to_silver")
        print()

print("=" * 70)
