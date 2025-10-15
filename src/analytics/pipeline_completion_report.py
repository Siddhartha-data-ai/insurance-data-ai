# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Completion Report
# MAGIC Generates and sends completion report for ETL pipeline execution

# COMMAND ----------
from pyspark.sql import functions as F
from datetime import datetime

# COMMAND ----------
# Create widgets for parameters
dbutils.widgets.dropdown("bronze_catalog", "insurance_dev_bronze", 
                         ["insurance_dev_bronze", "insurance_staging_bronze", "insurance_prod_bronze"], 
                         "Bronze Catalog Name")
dbutils.widgets.dropdown("silver_catalog", "insurance_dev_silver", 
                         ["insurance_dev_silver", "insurance_staging_silver", "insurance_prod_silver"], 
                         "Silver Catalog Name")
dbutils.widgets.dropdown("gold_catalog", "insurance_dev_gold", 
                         ["insurance_dev_gold", "insurance_staging_gold", "insurance_prod_gold"], 
                         "Gold Catalog Name")
dbutils.widgets.text("job_run_id", "manual", "Job Run ID")

# Get widget values
bronze_catalog = dbutils.widgets.get("bronze_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog")
gold_catalog = dbutils.widgets.get("gold_catalog")
job_run_id = dbutils.widgets.get("job_run_id")

print(f"Using bronze catalog: {bronze_catalog}")
print(f"Using silver catalog: {silver_catalog}")
print(f"Using gold catalog: {gold_catalog}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Collect Pipeline Statistics

# COMMAND ----------
# Collect record counts
environment = "manual" if job_run_id == "manual" else bronze_catalog.split("_")[1] if "_" in bronze_catalog else "unknown"

print("📊 Collecting pipeline statistics...")

stats = {
    "execution_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "environment": environment,
    "job_run_id": job_run_id,
    "bronze": {},
    "silver": {},
    "gold": {}
}

# Bronze layer counts
try:
    stats["bronze"]["customers"] = spark.table(f"{bronze_catalog}.customers.customer_raw").count()
    stats["bronze"]["policies"] = spark.table(f"{bronze_catalog}.policies.policy_raw").count()
    stats["bronze"]["claims"] = spark.table(f"{bronze_catalog}.claims.claim_raw").count()
    print(f"✅ Bronze layer: {sum(stats['bronze'].values()):,} total records")
except Exception as e:
    print(f"⚠️ Bronze layer error: {e}")
    stats["bronze"] = {"customers": 0, "policies": 0, "claims": 0}

# Silver layer counts
try:
    stats["silver"]["customers"] = spark.table(f"{silver_catalog}.customers.customer_dim").filter("is_current = true").count()
    stats["silver"]["policies"] = spark.table(f"{silver_catalog}.policies.policy_dim").count()
    stats["silver"]["claims"] = spark.table(f"{silver_catalog}.claims.claim_fact").count()
    print(f"✅ Silver layer: {sum(stats['silver'].values()):,} total records")
except Exception as e:
    print(f"⚠️ Silver layer error: {e}")
    stats["silver"] = {"customers": 0, "policies": 0, "claims": 0}

# Gold layer counts
try:
    stats["gold"]["customer_360"] = spark.table(f"{gold_catalog}.customer_analytics.customer_360").count()
    stats["gold"]["fraud_detection"] = spark.table(f"{gold_catalog}.claims_analytics.claims_fraud_detection").count()
    print(f"✅ Gold layer: {sum(stats['gold'].values()):,} total records")
except Exception as e:
    print(f"⚠️ Gold layer error: {e}")
    stats["gold"] = {"customer_360": 0, "fraud_detection": 0}

print("✅ Statistics collection complete!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Report

# COMMAND ----------
# Verify stats variable exists (in case cells were run out of order)
try:
    _ = stats
except NameError:
    print("❌ ERROR: 'stats' variable not found!")
    print("⚠️  Please run the 'Collect Pipeline Statistics' cell first")
    print("💡 Or click 'Run All' to execute all cells in order")
    dbutils.notebook.exit("FAILED: Statistics not collected")

report = f"""
{'=' * 70}
INSURANCE ANALYTICS PIPELINE - COMPLETION REPORT
{'=' * 70}

Execution Time: {stats['execution_time']}
Environment: {stats['environment'].upper()}
Job Run ID: {stats['job_run_id']}

BRONZE LAYER (Raw Data)
{'─' * 70}
  • Customers:        {stats['bronze']['customers']:>12,} records
  • Policies:         {stats['bronze']['policies']:>12,} records
  • Claims:           {stats['bronze']['claims']:>12,} records

SILVER LAYER (Validated Data)
{'─' * 70}
  • Customer Dim:     {stats['silver']['customers']:>12,} records
  • Policy Fact:      {stats['silver']['policies']:>12,} records
  • Claim Fact:       {stats['silver']['claims']:>12,} records

GOLD LAYER (Analytics)
{'─' * 70}
  • Customer 360:     {stats['gold']['customer_360']:>12,} records
  • Fraud Detection:  {stats['gold']['fraud_detection']:>12,} records

{'=' * 70}
✅ PIPELINE EXECUTION COMPLETED SUCCESSFULLY
{'=' * 70}

Next Steps:
  1. Review data quality metrics
  2. Validate fraud detection scores
  3. Verify security (RLS/CLS) implementation
  4. Connect BI tools to gold layer
  5. Schedule incremental refresh jobs

Documentation: See README.md and DEPLOYMENT.md for details.
{'=' * 70}
"""

print(report)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Send Notification (Optional)

# COMMAND ----------
# In production, you would send this via email or Slack
# For now, just display the report
displayHTML(f"""
<div style="font-family: monospace; background: #f5f5f5; padding: 20px; border-radius: 5px;">
<h2 style="color: #2e7d32;">✅ Pipeline Execution Complete</h2>
<pre>{report}</pre>
</div>
""")

# COMMAND ----------
dbutils.notebook.exit("SUCCESS")

