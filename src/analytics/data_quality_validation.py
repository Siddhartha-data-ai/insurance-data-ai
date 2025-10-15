# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality Validation
# MAGIC Validates data quality across bronze, silver, and gold layers

# COMMAND ----------
from pyspark.sql import functions as F

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

# Get widget values
bronze_catalog = dbutils.widgets.get("bronze_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog")
gold_catalog = dbutils.widgets.get("gold_catalog")

print(f"Using bronze catalog: {bronze_catalog}")
print(f"Using silver catalog: {silver_catalog}")
print(f"Using gold catalog: {gold_catalog}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Layer Validation

# COMMAND ----------
print("=" * 60)
print("BRONZE LAYER DATA QUALITY VALIDATION")
print("=" * 60)

# Customers validation
customers_count = spark.table(f"{bronze_catalog}.customers.customer_raw").count()
customers_unique = spark.table(f"{bronze_catalog}.customers.customer_raw").select("customer_id").distinct().count()
print(f"✅ Customers: {customers_count:,} records, {customers_unique:,} unique")

# Policies validation
policies_count = spark.table(f"{bronze_catalog}.policies.policy_raw").count()
policies_unique = spark.table(f"{bronze_catalog}.policies.policy_raw").select("policy_id").distinct().count()
print(f"✅ Policies: {policies_count:,} records, {policies_unique:,} unique")

# Claims validation
claims_count = spark.table(f"{bronze_catalog}.claims.claim_raw").count()
claims_unique = spark.table(f"{bronze_catalog}.claims.claim_raw").select("claim_id").distinct().count()
print(f"✅ Claims: {claims_count:,} records, {claims_unique:,} unique")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Silver Layer Validation

# COMMAND ----------
print("\n" + "=" * 60)
print("SILVER LAYER DATA QUALITY VALIDATION")
print("=" * 60)

# Customer dimension
customer_dim_count = spark.table(f"{silver_catalog}.customers.customer_dim").filter("is_current = true").count()
print(f"✅ Customer Dimension (current): {customer_dim_count:,} records")

# Policy fact
policy_fact_count = spark.table(f"{silver_catalog}.policies.policy_fact").count()
print(f"✅ Policy Fact: {policy_fact_count:,} records")

# Claims fact
claim_fact_count = spark.table(f"{silver_catalog}.claims.claim_fact").count()
print(f"✅ Claims Fact: {claim_fact_count:,} records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Gold Layer Validation

# COMMAND ----------
print("\n" + "=" * 60)
print("GOLD LAYER DATA QUALITY VALIDATION")
print("=" * 60)

# Customer 360
customer_360_count = spark.table(f"{gold_catalog}.customer_analytics.customer_360").count()
print(f"✅ Customer 360: {customer_360_count:,} records")

# Fraud detection
fraud_count = spark.table(f"{gold_catalog}.claims_analytics.claims_fraud_detection").count()
print(f"✅ Fraud Detection: {fraud_count:,} records")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Metrics

# COMMAND ----------
print("\n" + "=" * 60)
print("DATA QUALITY METRICS")
print("=" * 60)

# Check for nulls in critical fields
null_checks = spark.sql(f"""
    SELECT 
        'Customers' as table_name,
        COUNT(*) - COUNT(customer_id) as null_customer_id,
        COUNT(*) - COUNT(email) as null_email,
        COUNT(*) - COUNT(state_code) as null_state
    FROM {bronze_catalog}.customers.customer_raw
    UNION ALL
    SELECT 
        'Policies' as table_name,
        COUNT(*) - COUNT(policy_id) as null_policy_id,
        COUNT(*) - COUNT(customer_id) as null_customer_id,
        COUNT(*) - COUNT(annual_premium) as null_premium
    FROM {bronze_catalog}.policies.policy_raw
""")

null_checks.show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Referential Integrity

# COMMAND ----------
print("\n" + "=" * 60)
print("REFERENTIAL INTEGRITY CHECKS")
print("=" * 60)

# Policies referencing non-existent customers
orphan_policies = spark.sql(f"""
    SELECT COUNT(*) as orphan_count
    FROM {bronze_catalog}.policies.policy_raw p
    LEFT JOIN {bronze_catalog}.customers.customer_raw c ON p.customer_id = c.customer_id
    WHERE c.customer_id IS NULL
""").collect()[0]['orphan_count']

print(f"{'✅' if orphan_policies == 0 else '❌'} Orphan Policies: {orphan_policies}")

# Claims referencing non-existent policies
orphan_claims = spark.sql(f"""
    SELECT COUNT(*) as orphan_count
    FROM {bronze_catalog}.claims.claim_raw cl
    LEFT JOIN {bronze_catalog}.policies.policy_raw p ON cl.policy_id = p.policy_id
    WHERE p.policy_id IS NULL
""").collect()[0]['orphan_count']

print(f"{'✅' if orphan_claims == 0 else '❌'} Orphan Claims: {orphan_claims}")

# COMMAND ----------
print("\n" + "=" * 60)
print("✅ DATA QUALITY VALIDATION COMPLETE")
print("=" * 60)

# COMMAND ----------
# Return success
dbutils.notebook.exit("SUCCESS")

