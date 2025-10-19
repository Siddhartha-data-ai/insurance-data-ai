# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver Transformation with SCD Type 2
# MAGIC Transforms raw bronze data into validated silver layer with **SCD Type 2 change tracking**
# MAGIC
# MAGIC **Community Edition Compatible** - Uses standard PySpark Delta merge operations

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, concat, current_date, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
# Create widgets for parameters
dbutils.widgets.dropdown(
    "bronze_catalog",
    "insurance_dev_bronze",
    ["insurance_dev_bronze", "insurance_staging_bronze", "insurance_prod_bronze"],
    "Bronze Catalog Name",
)
dbutils.widgets.dropdown(
    "silver_catalog",
    "insurance_dev_silver",
    ["insurance_dev_silver", "insurance_staging_silver", "insurance_prod_silver"],
    "Silver Catalog Name",
)

# Get widget values
bronze_catalog = dbutils.widgets.get("bronze_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog")

print(f"✅ Using bronze catalog: {bronze_catalog}")
print(f"✅ Using silver catalog: {silver_catalog}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Helper Function: Apply SCD Type 2


# COMMAND ----------
def apply_scd_type2(source_df, target_table, business_key, tracking_columns, schema_name):
    """
    Apply SCD Type 2 logic to maintain history of changes

    Parameters:
    - source_df: Source DataFrame with new/updated records
    - target_table: Full table name (catalog.schema.table)
    - business_key: Column(s) that uniquely identify a record (e.g., customer_id)
    - tracking_columns: Columns to track for changes
    - schema_name: Schema name for creating if doesn't exist
    """

    # Prepare source with SCD Type 2 columns
    df_new = (
        source_df.withColumn("effective_start_date", current_date())
        .withColumn("effective_end_date", lit(None).cast("date"))
        .withColumn("is_current", lit(True))
        .withColumn("record_version", lit(1))
        .withColumn("created_timestamp", current_timestamp())
        .withColumn("updated_timestamp", current_timestamp())
    )

    # Check if target table exists and has SCD Type 2 columns
    table_exists = spark.catalog.tableExists(target_table)
    has_scd_columns = False

    if table_exists:
        df_existing = spark.table(target_table)
        existing_columns = df_existing.columns
        has_scd_columns = all(
            col in existing_columns for col in ["is_current", "effective_start_date", "record_version"]
        )

    if table_exists and has_scd_columns:
        print(f"   📊 Table exists with SCD Type 2 - applying merge...")

        # Load existing table
        delta_table = DeltaTable.forName(spark, target_table)

        # Find records that changed (by comparing tracking columns)
        if isinstance(business_key, list):
            join_condition = " AND ".join([f"source.{k} = target.{k}" for k in business_key])
        else:
            join_condition = f"source.{business_key} = target.{business_key}"

        # Build comparison condition for tracking columns
        change_conditions = []
        for col_name in tracking_columns:
            change_conditions.append(
                f"(source.{col_name} <> target.{col_name} OR (source.{col_name} IS NULL AND target.{col_name} IS NOT NULL) OR (source.{col_name} IS NOT NULL AND target.{col_name} IS NULL))"
            )

        change_condition = " OR ".join(change_conditions)

        # Get current records that will be closed
        df_to_close = (
            df_existing.alias("target")
            .join(df_new.alias("source"), [business_key] if isinstance(business_key, str) else business_key, "inner")
            .where(f"target.is_current = true AND ({change_condition})")
            .select("target.*")
            .withColumn("effective_end_date", current_date())
            .withColumn("is_current", lit(False))
            .withColumn("updated_timestamp", current_timestamp())
        )

        # Get new records (inserts + updates)
        df_updates = (
            df_new.alias("source")
            .join(
                df_existing.alias("target").filter("is_current = true"),
                [business_key] if isinstance(business_key, str) else business_key,
                "left",
            )
            .where(f"target.{business_key} IS NULL OR ({change_condition})")
            .select("source.*")
            .withColumn(
                "record_version",
                when(col(f"target.{business_key}").isNull(), lit(1)).otherwise(col("target.record_version") + 1),
            )
        )

        # Union closed records + new/updated records
        df_final = df_to_close.unionByName(df_updates, allowMissingColumns=True)

        # Write using merge
        df_final.write.format("delta").mode("append").saveAsTable(target_table)

        closed_count = df_to_close.count()
        new_count = df_updates.count()
        print(f"   ✅ Closed {closed_count:,} old records, inserted {new_count:,} new/updated records")

    else:
        if table_exists:
            print(f"   ⚠️  Table exists but missing SCD Type 2 columns - recreating with SCD Type 2...")
        else:
            print(f"   📊 Table doesn't exist - creating with initial load...")

        # Create schema if needed
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        # Initial load or recreate - all records are current
        df_new.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

        count = df_new.count()
        print(f"   ✅ Created table with {count:,} records (all as current)")


# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Transform Customers (Bronze → Silver with SCD Type 2)

# COMMAND ----------
print("=" * 80)
print("TRANSFORMING CUSTOMERS: Bronze → Silver (SCD Type 2)")
print("=" * 80)

# Read bronze customer data
df_customers = spark.table(f"{bronze_catalog}.customers.customer_raw")
print(f"📊 Loaded {df_customers.count():,} raw customer records")

# Apply data quality filters
df_customers_validated = (
    df_customers.filter("customer_id IS NOT NULL")
    .filter("first_name IS NOT NULL AND last_name IS NOT NULL")
    .filter("email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\\\.[a-zA-Z]{2,}$'")
    .filter("state_code IS NOT NULL AND LENGTH(state_code) = 2")
    .filter("credit_score BETWEEN 300 AND 850 OR credit_score IS NULL")
    .filter("annual_income >= 0 OR annual_income IS NULL")
)

print(f"✅ Validated: {df_customers_validated.count():,} records passed quality checks")

# Add transformations and enrichments
df_customers_silver = (
    df_customers_validated.withColumn("full_name", concat(col("first_name"), lit(" "), col("last_name")))
    .withColumn("age_years", F.floor(F.months_between(current_date(), col("date_of_birth")) / 12))
    .withColumn("ssn_masked", concat(lit("XXX-XX-"), F.substring(col("ssn"), -4, 4)))
    .withColumn("ssn_encrypted", F.sha2(col("ssn"), 256))
    .withColumn("email_domain", F.element_at(F.split(col("email"), "@"), 2))
    .withColumn("phone_masked", concat(lit("XXX-XXX-"), F.substring(col("phone"), -4, 4)))
    .withColumn(
        "credit_tier",
        when(col("credit_score") >= 750, "Excellent")
        .when(col("credit_score") >= 700, "Good")
        .when(col("credit_score") >= 650, "Fair")
        .otherwise("Poor"),
    )
    .withColumn(
        "income_bracket",
        when(col("annual_income") >= 100000, "$100K+")
        .when(col("annual_income") >= 75000, "$75K-$100K")
        .when(col("annual_income") >= 50000, "$50K-$75K")
        .when(col("annual_income") >= 30000, "$30K-$50K")
        .otherwise("Under $30K"),
    )
    .withColumn("customer_tenure_months", F.floor(F.months_between(current_date(), col("customer_since_date"))))
    .withColumn("is_active", when(col("customer_status") == "Active", True).otherwise(False))
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("data_source", lit("bronze.customers.customer_raw"))
)

print(f"✅ Applied transformations and enrichments")

# Apply SCD Type 2
table_name = f"{silver_catalog}.customers.customer_dim"
tracking_cols = [
    "first_name",
    "last_name",
    "email",
    "phone",
    "address_line1",
    "city",
    "state_code",
    "customer_status",
    "customer_segment",
    "annual_income",
]

apply_scd_type2(
    source_df=df_customers_silver,
    target_table=table_name,
    business_key="customer_id",
    tracking_columns=tracking_cols,
    schema_name=f"{silver_catalog}.customers",
)

print(f"✅ Customer dimension complete: {table_name}")
print("")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Transform Policies (Bronze → Silver - No SCD Type 2)

# COMMAND ----------
print("=" * 80)
print("TRANSFORMING POLICIES: Bronze → Silver")
print("=" * 80)

# Read bronze policy data
df_policies = spark.table(f"{bronze_catalog}.policies.policy_raw")
print(f"📊 Loaded {df_policies.count():,} raw policy records")

# Apply data quality filters
df_policies_validated = (
    df_policies.filter("policy_id IS NOT NULL")
    .filter("customer_id IS NOT NULL")
    .filter("policy_type IS NOT NULL")
    .filter("effective_date IS NOT NULL")
)

print(f"✅ Validated: {df_policies_validated.count():,} records passed quality checks")

# Add transformations
df_policies_silver = (
    df_policies_validated.withColumn("policy_age_days", F.datediff(current_date(), col("effective_date")))
    .withColumn("policy_age_years", F.floor(F.datediff(current_date(), col("effective_date")) / 365))
    .withColumn("is_active", when(col("policy_status") == "Active", True).otherwise(False))
    .withColumn("is_expired", when(col("expiration_date") < current_date(), True).otherwise(False))
    .withColumn("days_to_expiration", F.datediff(col("expiration_date"), current_date()))
    .withColumn(
        "annual_premium_tier",
        when(col("annual_premium") >= 5000, "High").when(col("annual_premium") >= 2000, "Medium").otherwise("Low"),
    )
    .withColumn(
        "coverage_tier",
        when(col("coverage_amount") >= 500000, "Premium")
        .when(col("coverage_amount") >= 250000, "Standard")
        .otherwise("Basic"),
    )
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("data_source", lit("bronze.policies.policy_raw"))
)

print(f"✅ Applied transformations")

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_catalog}.policies")

# Write to silver layer (policies are immutable - no SCD needed)
table_name = f"{silver_catalog}.policies.policy_dim"
print(f"💾 Writing to {table_name}...")

df_policies_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

print(f"✅ Successfully wrote {df_policies_silver.count():,} policy records to silver layer")
print("")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Transform Claims (Bronze → Silver - No SCD Type 2)

# COMMAND ----------
print("=" * 80)
print("TRANSFORMING CLAIMS: Bronze → Silver")
print("=" * 80)

# Read bronze claim data
df_claims = spark.table(f"{bronze_catalog}.claims.claim_raw")
print(f"📊 Loaded {df_claims.count():,} raw claim records")

# Apply data quality filters
df_claims_validated = (
    df_claims.filter("claim_id IS NOT NULL")
    .filter("policy_id IS NOT NULL")
    .filter("customer_id IS NOT NULL")
    .filter("claim_type IS NOT NULL")
    .filter("loss_date IS NOT NULL")
)

print(f"✅ Validated: {df_claims_validated.count():,} records passed quality checks")

# Add transformations
df_claims_silver = (
    df_claims_validated.withColumn("claim_age_days", F.datediff(current_date(), col("report_date")))
    .withColumn("days_to_report", F.datediff(col("report_date"), col("loss_date")))
    .withColumn("is_closed", when(col("claim_status") == "Closed", True).otherwise(False))
    .withColumn("is_pending", when(col("claim_status").isin("Reported", "Under Investigation"), True).otherwise(False))
    .withColumn(
        "claim_amount_tier",
        when(col("claimed_amount") >= 50000, "Large").when(col("claimed_amount") >= 10000, "Medium").otherwise("Small"),
    )
    .withColumn(
        "fraud_risk", when(col("fraud_score") >= 0.7, "High").when(col("fraud_score") >= 0.4, "Medium").otherwise("Low")
    )
    .withColumn(
        "settlement_ratio",
        when(col("claimed_amount") > 0, col("paid_amount") / col("claimed_amount")).otherwise(lit(None)),
    )
    .withColumn("load_timestamp", current_timestamp())
    .withColumn("data_source", lit("bronze.claims.claim_raw"))
)

print(f"✅ Applied transformations")

# Create schema if not exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver_catalog}.claims")

# Write to silver layer (claims are transactional - no SCD needed)
table_name = f"{silver_catalog}.claims.claim_fact"
print(f"💾 Writing to {table_name}...")

df_claims_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)

print(f"✅ Successfully wrote {df_claims_silver.count():,} claim records to silver layer")
print("")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------
print("=" * 80)
print("🎉 BRONZE → SILVER TRANSFORMATION COMPLETE!")
print("=" * 80)
print(f"✅ Customers: {silver_catalog}.customers.customer_dim (SCD Type 2)")
print(f"✅ Policies: {silver_catalog}.policies.policy_dim")
print(f"✅ Claims: {silver_catalog}.claims.claim_fact")
print("=" * 80)

# Display current customers (SCD Type 2)
print("\n📊 Sample Current Customer Records:")
display(spark.table(f"{silver_catalog}.customers.customer_dim").filter("is_current = true").limit(5))

print("\n📊 Sample Customer History (showing SCD Type 2 tracking):")
sample_customer = spark.table(f"{silver_catalog}.customers.customer_dim").select("customer_id").limit(1).collect()[0][0]
display(
    spark.table(f"{silver_catalog}.customers.customer_dim")
    .filter(f"customer_id = '{sample_customer}'")
    .select(
        "customer_id",
        "full_name",
        "email",
        "customer_status",
        "is_current",
        "effective_start_date",
        "effective_end_date",
        "record_version",
    )
    .orderBy("effective_start_date")
)
