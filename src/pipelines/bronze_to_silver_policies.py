# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline: Bronze to Silver - Policies
# MAGIC Transforms raw policy data into validated silver layer fact table

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
# Create widgets for parameters
dbutils.widgets.text("bronze_catalog", "insurance_dev_bronze", "Bronze Catalog Name")
dbutils.widgets.text("silver_catalog", "insurance_dev_silver", "Silver Catalog Name")

# Get widget values
bronze_catalog = dbutils.widgets.get("bronze_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog")

print(f"Using bronze catalog: {bronze_catalog}")
print(f"Using silver catalog: {silver_catalog}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Source Table


# COMMAND ----------
@dlt.table(name="policy_raw_stream", comment="Streaming view of raw policy data")
def policy_raw_stream():
    return (
        spark.readStream.format("delta").option("readChangeFeed", "true").table(f"{bronze_catalog}.policies.policy_raw")
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Expectations


# COMMAND ----------
@dlt.table(
    name="policy_validated",
    comment="Validated policy data with quality checks",
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "policy_id,customer_id,policy_type"},
)
@dlt.expect_or_drop("valid_policy_id", "policy_id IS NOT NULL")
@dlt.expect_or_drop("valid_policy_number", "policy_number IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_policy_type", "policy_type IS NOT NULL")
@dlt.expect_or_fail("valid_state", "state_code IS NOT NULL")
@dlt.expect("valid_premium", "annual_premium > 0")
@dlt.expect("valid_dates", "effective_date <= expiration_date")
@dlt.expect("valid_coverage", "coverage_amount >= 0")
def policy_validated():
    df = dlt.read_stream("policy_raw_stream")

    # Data validation and transformations
    return (
        df.withColumn(
            "policy_status_code",
            F.when(F.col("policy_status") == "Active", 1)
            .when(F.col("policy_status") == "Lapsed", 2)
            .when(F.col("policy_status") == "Cancelled", 3)
            .when(F.col("policy_status") == "Pending", 4)
            .otherwise(0),
        )
        .withColumn("days_in_force", F.datediff(F.current_date(), F.col("effective_date")))
        .withColumn(
            "earned_premium",
            F.when(
                F.col("policy_status") == "Active",
                F.col("annual_premium") * F.datediff(F.current_date(), F.col("effective_date")) / 365,
            ).otherwise(0),
        )
        .withColumn("unearned_premium", F.col("annual_premium") - F.col("earned_premium"))
        .withColumn(
            "days_overdue",
            F.when(
                F.col("premium_payment_status") == "Overdue", F.datediff(F.current_date(), F.col("effective_date"))
            ).otherwise(0),
        )
        .withColumn(
            "cancellation_category",
            F.when(F.col("cancellation_reason").like("%non-payment%"), "Non-Payment")
            .when(F.col("cancellation_reason").like("%sold%"), "Vehicle Sold")
            .when(F.col("cancellation_reason").like("%better rate%"), "Competitive Rate")
            .otherwise("Other"),
        )
        .withColumn("policy_value_score", (F.col("annual_premium") * 0.4 + F.col("coverage_amount") / 100000 * 0.6))
        .withColumn("data_quality_score", F.lit(0.95))
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Enrichment with Lookups


# COMMAND ----------
@dlt.table(name="policy_enriched", comment="Policy data enriched with reference data")
def policy_enriched():
    policies = dlt.read_stream("policy_validated")

    # Add state name
    states = (
        spark.table(f"{silver_catalog}.master_data.state_dim")
        if spark.catalog.tableExists(f"{silver_catalog}.master_data.state_dim")
        else None
    )

    if states:
        policies = policies.join(
            F.broadcast(states.select("state_code", F.col("state_name"), F.col("region").alias("region_code"))),
            "state_code",
            "left",
        )
    else:
        policies = policies.withColumn("state_name", F.lit(None)).withColumn("region_code", F.lit(None))

    # Add agent names
    policies = (
        policies.withColumn("writing_agent_name", F.concat(F.lit("Agent "), F.col("writing_agent_id")))
        .withColumn("servicing_agent_name", F.concat(F.lit("Agent "), F.col("servicing_agent_id")))
        .withColumn("underwriter_name", F.concat(F.lit("Underwriter "), F.col("underwriter_id")))
    )

    # Add product name
    policies = policies.withColumn(
        "product_name",
        F.when(F.col("policy_type") == "Auto", "Auto Insurance")
        .when(F.col("policy_type") == "Home", "Homeowners Insurance")
        .when(F.col("policy_type") == "Life", "Life Insurance")
        .when(F.col("policy_type") == "Health", "Health Insurance")
        .when(F.col("policy_type") == "Commercial", "Commercial Insurance")
        .otherwise("Other"),
    )

    return policies


# COMMAND ----------
# MAGIC %md
# MAGIC ## Target Silver Table

# COMMAND ----------
dlt.create_streaming_table(
    name="policy_fact",
    comment="Policy fact table with business metrics",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    partition_cols=["policy_type", "state_code"],
)


# Write to target
@dlt.view(name="policy_updates")
def policy_updates():
    return (
        dlt.read_stream("policy_enriched")
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
    )


dlt.apply_changes(
    target="policy_fact", source="policy_updates", keys=["policy_id"], sequence_by=F.col("updated_timestamp")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Metrics


# COMMAND ----------
@dlt.table(name="policy_quality_metrics", comment="Data quality metrics for policy pipeline")
def policy_quality_metrics():
    """Calculate data quality metrics for monitoring"""
    return (
        dlt.read("policy_fact")
        .groupBy("policy_type", "policy_status")
        .agg(
            F.count("*").alias("total_policies"),
            F.sum("annual_premium").alias("total_premium"),
            F.avg("annual_premium").alias("avg_premium"),
            F.sum("coverage_amount").alias("total_coverage"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.current_timestamp().alias("metric_timestamp"),
        )
    )
