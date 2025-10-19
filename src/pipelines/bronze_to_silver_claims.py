# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline: Bronze to Silver - Claims
# MAGIC Transforms raw claims data into validated silver layer fact table with fraud scoring

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
dbutils.widgets.text("bronze_catalog", "insurance_dev_bronze", "Bronze Catalog Name")
dbutils.widgets.text("silver_catalog", "insurance_dev_silver", "Silver Catalog Name")

bronze_catalog = dbutils.widgets.get("bronze_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog")

print(f"Using bronze catalog: {bronze_catalog}")
print(f"Using silver catalog: {silver_catalog}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Bronze Source Table


# COMMAND ----------
@dlt.table(name="claim_raw_stream", comment="Streaming view of raw claims data")
def claim_raw_stream():
    return spark.readStream.format("delta").option("readChangeFeed", "true").table(f"{bronze_catalog}.claims.claim_raw")


# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Expectations


# COMMAND ----------
@dlt.table(
    name="claim_validated",
    comment="Validated claims data with quality checks",
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "claim_id,policy_id,customer_id"},
)
@dlt.expect_or_drop("valid_claim_id", "claim_id IS NOT NULL")
@dlt.expect_or_drop("valid_claim_number", "claim_number IS NOT NULL")
@dlt.expect_or_drop("valid_policy_id", "policy_id IS NOT NULL")
@dlt.expect_or_fail("valid_claim_type", "claim_type IS NOT NULL")
@dlt.expect("valid_claimed_amount", "claimed_amount > 0")
@dlt.expect("valid_loss_date", "loss_date <= CURRENT_DATE()")
@dlt.expect("valid_reported_date", "reported_date >= loss_date")
def claim_validated():
    df = dlt.read_stream("claim_raw_stream")

    # Data validation and transformations
    return (
        df.withColumn("days_to_report", F.datediff(F.col("reported_date"), F.col("loss_date")))
        .withColumn(
            "days_to_close",
            F.when(F.col("is_closed"), F.datediff(F.col("closed_date"), F.col("reported_date"))).otherwise(
                F.datediff(F.current_date(), F.col("reported_date"))
            ),
        )
        .withColumn("outstanding_amount", F.col("claimed_amount") - F.col("paid_amount"))
        .withColumn(
            "claim_severity",
            F.when(F.col("claimed_amount") > 50000, "Critical")
            .when(F.col("claimed_amount") > 25000, "High")
            .when(F.col("claimed_amount") > 10000, "Medium")
            .otherwise("Low"),
        )
        .withColumn(
            "loss_category",
            F.when(F.col("loss_type").like("%Total Loss%"), "Total Loss")
            .when(F.col("loss_type").like("%Theft%"), "Theft")
            .when(F.col("loss_type").like("%Collision%"), "Collision")
            .when(F.col("loss_type").like("%Fire%"), "Fire")
            .when(F.col("loss_type").like("%Weather%"), "Weather")
            .otherwise("Other"),
        )
        .withColumn(
            "payment_ratio",
            F.when(F.col("claimed_amount") > 0, F.col("paid_amount") / F.col("claimed_amount")).otherwise(0),
        )
        .withColumn(
            "is_suspicious",
            F.when(
                (F.col("fraud_score") > 70) | (F.col("days_to_report") > 30) | (F.col("claimed_amount") > 100000), True
            ).otherwise(False),
        )
        .withColumn("data_quality_score", F.lit(0.95))
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Enrichment and Fraud Indicators


# COMMAND ----------
@dlt.table(name="claim_enriched", comment="Claims enriched with fraud indicators and reference data")
def claim_enriched():
    claims = dlt.read_stream("claim_validated")

    # Add fraud indicators
    claims = (
        claims.withColumn("excessive_amount_flag", F.when(F.col("claimed_amount") > 100000, 1).otherwise(0))
        .withColumn("late_reporting_flag", F.when(F.col("days_to_report") > 30, 1).otherwise(0))
        .withColumn("weekend_incident_flag", F.when(F.dayofweek(F.col("loss_date")).isin([1, 7]), 1).otherwise(0))
        .withColumn("multiple_claims_flag", F.when(F.col("total_claims") > 3, 1).otherwise(0))
        .withColumn("new_policyholder_flag", F.when(F.col("policy_tenure_months") < 6, 1).otherwise(0))
        .withColumn("round_amount_flag", F.when(F.col("claimed_amount") % 1000 == 0, 1).otherwise(0))
    )

    # Calculate total fraud indicators
    claims = claims.withColumn(
        "total_fraud_indicators",
        F.col("excessive_amount_flag")
        + F.col("late_reporting_flag")
        + F.col("weekend_incident_flag")
        + F.col("multiple_claims_flag")
        + F.col("new_policyholder_flag")
        + F.col("round_amount_flag"),
    )

    # Enhanced fraud score
    claims = claims.withColumn(
        "enhanced_fraud_score", (F.col("fraud_score") * 0.7 + F.col("total_fraud_indicators") * 5)
    )

    # Add adjuster and provider names
    claims = claims.withColumn("adjuster_name", F.concat(F.lit("Adjuster "), F.col("assigned_adjuster_id"))).withColumn(
        "provider_name", F.concat(F.lit("Provider "), F.col("service_provider_id"))
    )

    return claims


# COMMAND ----------
# MAGIC %md
# MAGIC ## Target Silver Table

# COMMAND ----------
dlt.create_streaming_table(
    name="claim_fact",
    comment="Claims fact table with fraud indicators",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true", "sensitive_data": "true"},
    partition_cols=["claim_type", "claim_status"],
)


@dlt.view(name="claim_updates")
def claim_updates():
    return (
        dlt.read_stream("claim_enriched")
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
    )


dlt.apply_changes(
    target="claim_fact", source="claim_updates", keys=["claim_id"], sequence_by=F.col("updated_timestamp")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Metrics


# COMMAND ----------
@dlt.table(name="claim_quality_metrics", comment="Data quality metrics for claims pipeline")
def claim_quality_metrics():
    """Calculate data quality metrics for monitoring"""
    return (
        dlt.read("claim_fact")
        .groupBy("claim_type", "claim_status")
        .agg(
            F.count("*").alias("total_claims"),
            F.sum("claimed_amount").alias("total_claimed"),
            F.sum("paid_amount").alias("total_paid"),
            F.avg("fraud_score").alias("avg_fraud_score"),
            F.sum(F.when(F.col("is_suspicious"), 1).otherwise(0)).alias("suspicious_claims"),
            F.avg("days_to_close").alias("avg_days_to_close"),
            F.current_timestamp().alias("metric_timestamp"),
        )
    )
