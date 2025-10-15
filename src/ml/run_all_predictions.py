# Databricks notebook source
# MAGIC %md
# MAGIC # Run All ML Predictions - Master Orchestrator
# MAGIC 
# MAGIC **This notebook runs all 4 ML prediction models in sequence:**
# MAGIC 1. 🔴 Customer Churn Prediction
# MAGIC 2. 🚨 Enhanced Fraud Detection
# MAGIC 3. 📈 Claim Volume Forecasting
# MAGIC 4. 💰 Premium Optimization
# MAGIC 
# MAGIC **Total Expected Runtime: 15-30 minutes**
# MAGIC 
# MAGIC **Usage:**
# MAGIC - Click "Run All" to execute all models
# MAGIC - Or run cells individually to execute specific models
# MAGIC 
# MAGIC **Output:**
# MAGIC - Updates all prediction tables in `insurance_dev_gold.predictions`
# MAGIC - Displays summary statistics for each model
# MAGIC - Shows overall execution status

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
from datetime import datetime
import time

# Track execution
start_time = time.time()
execution_results = {}

print("=" * 70)
print(" 🚀 ML PREDICTION PIPELINE - STARTING")
print("=" * 70)
print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 70)
print()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Model 1: Customer Churn Prediction
# MAGIC **Predicts which customers are at risk of canceling policies**

# COMMAND ----------
print("🔄 Running Model 1: Customer Churn Prediction...")
print("-" * 70)

model1_start = time.time()

try:
    # Run churn prediction notebook
    result = dbutils.notebook.run(
        "/Workspace/Shared/insurance-analytics/ml/predict_customer_churn",
        timeout_seconds=900,  # 15 minutes
        arguments={}
    )
    
    model1_duration = time.time() - model1_start
    execution_results['churn_prediction'] = {
        'status': 'SUCCESS',
        'duration_seconds': round(model1_duration, 2),
        'output': result
    }
    
    print(f"✅ Churn Prediction Complete ({round(model1_duration, 1)}s)")
    print()
    
except Exception as e:
    model1_duration = time.time() - model1_start
    execution_results['churn_prediction'] = {
        'status': 'FAILED',
        'duration_seconds': round(model1_duration, 2),
        'error': str(e)
    }
    
    print(f"❌ Churn Prediction Failed: {str(e)}")
    print()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Model 2: Enhanced Fraud Detection
# MAGIC **Identifies suspicious claims for investigation**

# COMMAND ----------
print("🔄 Running Model 2: Enhanced Fraud Detection...")
print("-" * 70)

model2_start = time.time()

try:
    result = dbutils.notebook.run(
        "/Workspace/Shared/insurance-analytics/ml/predict_fraud_enhanced",
        timeout_seconds=600,  # 10 minutes
        arguments={}
    )
    
    model2_duration = time.time() - model2_start
    execution_results['fraud_detection'] = {
        'status': 'SUCCESS',
        'duration_seconds': round(model2_duration, 2),
        'output': result
    }
    
    print(f"✅ Fraud Detection Complete ({round(model2_duration, 1)}s)")
    print()
    
except Exception as e:
    model2_duration = time.time() - model2_start
    execution_results['fraud_detection'] = {
        'status': 'FAILED',
        'duration_seconds': round(model2_duration, 2),
        'error': str(e)
    }
    
    print(f"❌ Fraud Detection Failed: {str(e)}")
    print()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Model 3: Claim Volume Forecasting
# MAGIC **Forecasts expected claims for next 30-90 days**

# COMMAND ----------
print("🔄 Running Model 3: Claim Volume Forecasting...")
print("-" * 70)

model3_start = time.time()

try:
    result = dbutils.notebook.run(
        "/Workspace/Shared/insurance-analytics/ml/forecast_claims",
        timeout_seconds=600,  # 10 minutes
        arguments={}
    )
    
    model3_duration = time.time() - model3_start
    execution_results['claim_forecasting'] = {
        'status': 'SUCCESS',
        'duration_seconds': round(model3_duration, 2),
        'output': result
    }
    
    print(f"✅ Claim Forecasting Complete ({round(model3_duration, 1)}s)")
    print()
    
except Exception as e:
    model3_duration = time.time() - model3_start
    execution_results['claim_forecasting'] = {
        'status': 'FAILED',
        'duration_seconds': round(model3_duration, 2),
        'error': str(e)
    }
    
    print(f"❌ Claim Forecasting Failed: {str(e)}")
    print()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Model 4: Premium Optimization
# MAGIC **Recommends optimal pricing for policies**

# COMMAND ----------
print("🔄 Running Model 4: Premium Optimization...")
print("-" * 70)

model4_start = time.time()

try:
    result = dbutils.notebook.run(
        "/Workspace/Shared/insurance-analytics/ml/optimize_premiums",
        timeout_seconds=900,  # 15 minutes
        arguments={}
    )
    
    model4_duration = time.time() - model4_start
    execution_results['premium_optimization'] = {
        'status': 'SUCCESS',
        'duration_seconds': round(model4_duration, 2),
        'output': result
    }
    
    print(f"✅ Premium Optimization Complete ({round(model4_duration, 1)}s)")
    print()
    
except Exception as e:
    model4_duration = time.time() - model4_start
    execution_results['premium_optimization'] = {
        'status': 'FAILED',
        'duration_seconds': round(model4_duration, 2),
        'error': str(e)
    }
    
    print(f"❌ Premium Optimization Failed: {str(e)}")
    print()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Summary

# COMMAND ----------
total_duration = time.time() - start_time

print("=" * 70)
print(" 📊 ML PREDICTION PIPELINE - EXECUTION SUMMARY")
print("=" * 70)
print(f"End Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"Total Duration: {round(total_duration, 1)} seconds ({round(total_duration/60, 1)} minutes)")
print("=" * 70)
print()

# Count successes and failures
success_count = sum(1 for r in execution_results.values() if r['status'] == 'SUCCESS')
failure_count = sum(1 for r in execution_results.values() if r['status'] == 'FAILED')

print("📋 Model Execution Results:")
print()

for model_name, result in execution_results.items():
    status_icon = "✅" if result['status'] == 'SUCCESS' else "❌"
    print(f"{status_icon} {model_name.replace('_', ' ').title()}")
    print(f"   Status: {result['status']}")
    print(f"   Duration: {result['duration_seconds']}s")
    
    if result['status'] == 'FAILED':
        print(f"   Error: {result.get('error', 'Unknown error')}")
    
    print()

print("=" * 70)
print(f"✅ Successful: {success_count} / 4")
print(f"❌ Failed: {failure_count} / 4")
print("=" * 70)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify Prediction Tables

# COMMAND ----------
print("🔍 Verifying prediction tables...")
print("-" * 70)

# Check each prediction table
tables_to_check = [
    "insurance_dev_gold.predictions.customer_churn_risk",
    "insurance_dev_gold.predictions.fraud_alerts",
    "insurance_dev_gold.predictions.claim_forecast",
    "insurance_dev_gold.predictions.premium_optimization"
]

table_stats = []

for table_name in tables_to_check:
    try:
        count = spark.table(table_name).count()
        table_stats.append({
            'table': table_name.split('.')[-1],
            'status': '✅ Exists',
            'row_count': f"{count:,}"
        })
        print(f"✅ {table_name}: {count:,} rows")
    except Exception as e:
        table_stats.append({
            'table': table_name.split('.')[-1],
            'status': '❌ Missing',
            'row_count': 'N/A'
        })
        print(f"❌ {table_name}: Not found")

print()
print("=" * 70)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Quick Analytics Summary

# COMMAND ----------
print("📊 PREDICTION ANALYTICS SUMMARY")
print("=" * 70)
print()

try:
    # Churn Summary
    churn_summary = spark.sql("""
        SELECT 
            churn_risk_category,
            COUNT(*) as customer_count,
            ROUND(SUM(total_annual_premium), 2) as premium_at_risk
        FROM insurance_dev_gold.predictions.customer_churn_risk
        WHERE churn_risk_category = 'High Risk'
        GROUP BY churn_risk_category
    """).collect()
    
    if churn_summary:
        row = churn_summary[0]
        print(f"🔴 Customer Churn:")
        print(f"   High Risk Customers: {row['customer_count']:,}")
        print(f"   Premium at Risk: ${row['premium_at_risk']:,.2f}")
        print()
except Exception as e:
    print(f"⚠️  Churn summary unavailable: {str(e)}")
    print()

try:
    # Fraud Summary
    fraud_summary = spark.sql("""
        SELECT 
            COUNT(*) as critical_cases,
            ROUND(SUM(estimated_fraud_amount), 2) as potential_fraud
        FROM insurance_dev_gold.predictions.fraud_alerts
        WHERE fraud_risk_category IN ('Critical', 'High')
    """).collect()
    
    if fraud_summary:
        row = fraud_summary[0]
        print(f"🚨 Fraud Detection:")
        print(f"   Critical Cases: {row['critical_cases']:,}")
        print(f"   Potential Fraud: ${row['potential_fraud']:,.2f}")
        print()
except Exception as e:
    print(f"⚠️  Fraud summary unavailable: {str(e)}")
    print()

try:
    # Forecast Summary
    forecast_summary = spark.sql("""
        SELECT 
            ROUND(SUM(predicted_claim_count), 0) as forecast_30d_claims,
            ROUND(SUM(predicted_total_amount), 2) as forecast_30d_amount
        FROM insurance_dev_gold.predictions.claim_forecast
        WHERE claim_type = 'ALL_TYPES' AND days_ahead <= 30
    """).collect()
    
    if forecast_summary:
        row = forecast_summary[0]
        print(f"📈 Claim Forecast (30 Days):")
        print(f"   Expected Claims: {int(row['forecast_30d_claims']):,}")
        print(f"   Expected Amount: ${row['forecast_30d_amount']:,.2f}")
        print()
except Exception as e:
    print(f"⚠️  Forecast summary unavailable: {str(e)}")
    print()

try:
    # Premium Optimization Summary
    pricing_summary = spark.sql("""
        SELECT 
            COUNT(*) as high_priority_count,
            ROUND(SUM(annual_revenue_impact), 2) as revenue_opportunity
        FROM insurance_dev_gold.predictions.premium_optimization
        WHERE implementation_priority = 'High'
    """).collect()
    
    if pricing_summary:
        row = pricing_summary[0]
        print(f"💰 Premium Optimization:")
        print(f"   High Priority Policies: {row['high_priority_count']:,}")
        print(f"   Revenue Opportunity: ${row['revenue_opportunity']:,.2f}")
        print()
except Exception as e:
    print(f"⚠️  Pricing summary unavailable: {str(e)}")
    print()

print("=" * 70)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Next Steps

# COMMAND ----------
print("🎯 NEXT STEPS")
print("=" * 70)
print()
print("1. ✅ Review prediction results in individual model notebooks")
print("2. ✅ Open Databricks SQL Dashboard to visualize predictions")
print("3. ✅ Export high-priority cases for action:")
print("    - High-risk churn customers → Retention team")
print("    - Critical fraud cases → Investigation team")
print("    - High-priority pricing → Pricing team")
print("4. ✅ Schedule this notebook to run daily (if using Standard/Premium)")
print("5. ✅ Monitor prediction accuracy and retrain models as needed")
print()
print("=" * 70)
print()
print("📊 Dashboard Location:")
print("   Databricks SQL → Dashboards → Insurance Analytics - AI Predictions")
print()
print("📚 Setup Guide:")
print("   See DASHBOARD_SETUP_GUIDE.md for detailed instructions")
print()
print("=" * 70)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Status
# MAGIC 
# MAGIC **Overall Status:** {0}/{1} models completed successfully

# COMMAND ----------
if failure_count == 0:
    print("✅ ALL MODELS EXECUTED SUCCESSFULLY!")
    print(f"🎉 All prediction tables updated and ready for dashboard")
    dbutils.notebook.exit("SUCCESS: All predictions completed")
else:
    print(f"⚠️  WARNING: {failure_count} model(s) failed")
    print(f"✅ {success_count} model(s) completed successfully")
    print(f"Please review error messages above and re-run failed models")
    dbutils.notebook.exit(f"PARTIAL SUCCESS: {success_count}/{len(execution_results)} models completed")

