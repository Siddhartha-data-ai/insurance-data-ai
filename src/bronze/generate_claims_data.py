# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Realistic Claims Data
# MAGIC Generate claims with realistic patterns, fraud indicators, and financial metrics

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, concat, lpad, array, element_at, date_add, current_date
from pyspark.sql.types import *
from datetime import datetime, timedelta

# COMMAND ----------
# Create widgets for parameters
dbutils.widgets.dropdown("catalog", "insurance_dev_bronze", 
                         ["insurance_dev_bronze", "insurance_staging_bronze", "insurance_prod_bronze"], 
                         "Bronze Catalog Name")

# Get widget values
catalog_name = dbutils.widgets.get("catalog")

print(f"Using catalog: {catalog_name}")

# Realistic claims frequency: ~15% of policies have claims
# Load policies
df_policies = spark.table(f"{catalog_name}.policies.policy_raw").filter("policy_status IN ('Active', 'Cancelled', 'Lapsed')")
num_policies = df_policies.count()
NUM_CLAIMS = int(num_policies * 0.15)  # 15% claim frequency

print(f"Generating {NUM_CLAIMS:,} claims from {num_policies:,} policies")

# COMMAND ----------
# Create base claims
df_base = spark.range(0, NUM_CLAIMS) \
    .withColumn("claim_id", F.concat(F.lit("CLM"), F.lpad(F.col("id"), 10, "0"))) \
    .withColumn("claim_number", F.concat(F.lit("C"), F.lpad((F.rand() * 9999999).cast("int"), 7, "0")))

# Assign to random policies
df_policies_indexed = df_policies \
    .withColumn("policy_row", F.monotonically_increasing_id()) \
    .select("policy_row", "policy_id", "customer_id", "policy_type", "effective_date", 
            "expiration_date", "coverage_amount", "deductible_amount", "state_code")

df_claims = df_base \
    .withColumn("policy_row", (F.rand() * num_policies).cast("int")) \
    .join(df_policies_indexed, "policy_row", "left") \
    .drop("policy_row")

# Claim type based on policy type
df_claims = df_claims \
    .withColumn("claim_type", 
                when(F.col("policy_type") == "Auto", 
                     F.element_at(F.array([F.lit("Auto"), F.lit("Liability")]), 
                                 when(F.rand() < 0.70, 1).otherwise(2)))
                .when(F.col("policy_type") == "Home", F.lit("Property"))
                .when(F.col("policy_type") == "Health", F.lit("Health"))
                .otherwise(F.lit("Liability")))

# Loss type
df_claims = df_claims \
    .withColumn("loss_type",
                when(F.col("claim_type") == "Auto",
                     F.element_at(F.array([F.lit("Accident"), F.lit("Theft"), F.lit("Vandalism")]),
                                 when(F.rand() < 0.70, 1)
                                 .when(F.rand() < 0.90, 2)
                                 .otherwise(3)))
                .when(F.col("claim_type") == "Property",
                     F.element_at(F.array([F.lit("Fire"), F.lit("Flood"), F.lit("Theft"), F.lit("Storm")]),
                                 ((F.rand() * 4).cast("int") + 1)))
                .when(F.col("claim_type") == "Health",
                     F.element_at(F.array([F.lit("Medical"), F.lit("Hospital"), F.lit("Surgical")]),
                                 ((F.rand() * 3).cast("int") + 1)))
                .otherwise(F.lit("Other")))

# Loss date (within policy period)
df_claims = df_claims \
    .withColumn("days_into_policy", (F.datediff(F.col("expiration_date"), F.col("effective_date")) * F.rand())) \
    .withColumn("loss_date", 
                F.expr("effective_date + CAST(days_into_policy AS INT)")) \
    .drop("days_into_policy")

# Report date (0-30 days after loss, fraud cases report later)
df_claims = df_claims \
    .withColumn("is_potential_fraud", F.rand() < 0.05) \
    .withColumn("days_to_report", 
                when(F.col("is_potential_fraud"), ((F.rand() * 20).cast("int") + 10))
                .otherwise(((F.rand() * 5).cast("int") + 1))) \
    .withColumn("report_date", F.expr("loss_date + CAST(days_to_report AS INT)")) \
    .withColumn("fnol_date", F.col("report_date"))

# Claim status
df_claims = df_claims \
    .withColumn("claim_status",
                F.element_at(F.array([F.lit("Reported"), F.lit("Under Investigation"), F.lit("Approved"), 
                                     F.lit("Denied"), F.lit("Closed")]),
                            when(F.rand() < 0.05, 1)
                            .when(F.rand() < 0.15, 2)
                            .when(F.rand() < 0.25, 3)
                            .when(F.rand() < 0.30, 4)
                            .otherwise(5)))

# Closed date
df_claims = df_claims \
    .withColumn("closed_date",
                when(F.col("claim_status") == "Closed",
                     F.expr("report_date + CAST(rand() * 90 + 10 AS INT)"))
                .otherwise(F.lit(None)))

# Claimed amount (realistic distributions)
df_claims = df_claims \
    .withColumn("claimed_amount_base",
                when(F.col("claim_type") == "Auto", (F.rand() * 15000 + 2000))
                .when(F.col("claim_type") == "Property", (F.rand() * 50000 + 5000))
                .when(F.col("claim_type") == "Health", (F.rand() * 25000 + 1000))
                .otherwise((F.rand() * 20000 + 3000))) \
    .withColumn("claimed_amount",
                when(F.col("is_potential_fraud"), F.col("claimed_amount_base") * 1.5)
                .otherwise(F.col("claimed_amount_base"))) \
    .withColumn("claimed_amount", F.round(F.col("claimed_amount"), 2).cast("decimal(15,2)")) \
    .drop("claimed_amount_base")

# Other financial amounts
df_claims = df_claims \
    .withColumn("estimated_loss_amount", (F.col("claimed_amount") * (F.rand() * 0.2 + 0.90)).cast("decimal(15,2)")) \
    .withColumn("reserved_amount", (F.col("estimated_loss_amount") * (F.rand() * 0.15 + 1.0)).cast("decimal(15,2)")) \
    .withColumn("paid_amount",
                when(F.col("claim_status").isin(["Approved", "Closed"]),
                     F.least(F.col("claimed_amount") - F.col("deductible_amount"), 
                            F.col("coverage_amount") * 0.8))
                .otherwise(F.lit(0))) \
    .withColumn("paid_amount", F.round(F.col("paid_amount"), 2).cast("decimal(15,2)"))

# Fraud indicators
df_claims = df_claims \
    .withColumn("fraud_score",
                when(F.col("is_potential_fraud"), (F.rand() * 30 + 65))
                .otherwise((F.rand() * 40 + 10))) \
    .withColumn("fraud_score", F.round(F.col("fraud_score"), 2).cast("decimal(5,2)")) \
    .withColumn("siu_referral", F.col("fraud_score") > 70) \
    .withColumn("investigation_required", F.col("fraud_score") > 60)

# Assign adjusters
df_claims = df_claims \
    .withColumn("assigned_adjuster_id", F.concat(F.lit("ADJ"), F.lpad(((F.rand() * 300).cast("int") + 1), 4, "0")))

# Claimant information
df_claims = df_claims \
    .withColumn("claimant_relationship",
                F.element_at(F.array([F.lit("Self"), F.lit("Spouse"), F.lit("Child"), F.lit("Third Party")]),
                            when(F.rand() < 0.60, 1)
                            .when(F.rand() < 0.80, 2)
                            .when(F.rand() < 0.90, 3)
                            .otherwise(4)))

# Location (same as policy state mostly)
df_claims = df_claims \
    .withColumn("loss_location_state", 
                when(F.rand() < 0.95, F.col("state_code"))
                .otherwise(F.element_at(F.array([F.lit("CA"), F.lit("TX"), F.lit("FL"), F.lit("NY")]),
                                       ((F.rand() * 4).cast("int") + 1))))

# Police report filed for certain claim types
df_claims = df_claims \
    .withColumn("police_report_filed",
                when(F.col("loss_type").isin(["Accident", "Theft", "Vandalism"]), 
                     F.rand() < 0.80)
                .otherwise(F.rand() < 0.20)) \
    .withColumn("police_report_number",
                when(F.col("police_report_filed"), 
                     F.concat(F.lit("PR"), F.lpad((F.rand() * 999999).cast("int"), 6, "0")))
                .otherwise(F.lit(None)))

# Days open calculation
df_claims = df_claims \
    .withColumn("days_open",
                when(F.col("claim_status") == "Closed", F.datediff(F.col("closed_date"), F.col("report_date")))
                .otherwise(F.datediff(F.current_date(), F.col("report_date"))))

# Source metadata
df_claims = df_claims \
    .withColumn("source_system", F.lit("ClaimsManagement")) \
    .withColumn("source_record_id", F.col("claim_id")) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("record_hash", F.md5(F.concat(F.col("claim_id"), F.col("policy_id"), F.col("claim_number")))) \
    .withColumn("data_lineage", F.lit('{"source": "claim_generation_script", "version": "1.0"}'))

# Final columns
final_columns = [
    "claim_id", "claim_number", "policy_id", "customer_id", "claim_type", "loss_type",
    "claim_status", "loss_date", "report_date", "fnol_date", "closed_date",
    "loss_location_state", "claimed_amount", "estimated_loss_amount", "reserved_amount", 
    "paid_amount", "deductible_amount", "claimant_relationship", "assigned_adjuster_id",
    "investigation_required", "siu_referral", "fraud_score", "police_report_filed",
    "police_report_number", "days_open", "source_system", "source_record_id",
    "ingestion_timestamp", "record_hash", "data_lineage"
]

df_claims_final = df_claims.select(final_columns)

# COMMAND ----------
table_name = f"{catalog_name}.claims.claim_raw"

# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.claims")

# Drop table first
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Write data
df_claims_final.write.format("delta").saveAsTable(table_name)

print(f"✅ Successfully wrote claim records to {table_name}")

# COMMAND ----------
display(spark.table(table_name).limit(10))

# COMMAND ----------
spark.sql(f"""
    SELECT 
        claim_type,
        claim_status,
        COUNT(*) as count,
        ROUND(AVG(claimed_amount), 2) as avg_claimed,
        ROUND(AVG(paid_amount), 2) as avg_paid,
        ROUND(AVG(fraud_score), 2) as avg_fraud_score
    FROM {table_name}
    GROUP BY claim_type, claim_status
    ORDER BY claim_type, count DESC
""").show(50)

