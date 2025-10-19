# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline: Bronze to Silver - Payments
# MAGIC Transforms raw payment data into validated silver layer fact table

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
@dlt.table(name="payment_raw_stream", comment="Streaming view of raw payment data")
def payment_raw_stream():
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(f"{bronze_catalog}.payments.payment_raw")
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Expectations


# COMMAND ----------
@dlt.table(
    name="payment_validated",
    comment="Validated payment data with quality checks",
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "payment_id,policy_id,customer_id"},
)
@dlt.expect_or_drop("valid_payment_id", "payment_id IS NOT NULL")
@dlt.expect_or_drop("valid_policy_id", "policy_id IS NOT NULL")
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_payment_method", "payment_method IS NOT NULL")
@dlt.expect("valid_amount", "payment_amount > 0")
@dlt.expect("valid_payment_date", "payment_date <= CURRENT_DATE()")
def payment_validated():
    df = dlt.read_stream("payment_raw_stream")

    # Data validation and transformations
    return (
        df.withColumn(
            "payment_status_code",
            F.when(F.col("payment_status") == "Completed", 1)
            .when(F.col("payment_status") == "Pending", 2)
            .when(F.col("payment_status") == "Failed", 3)
            .when(F.col("payment_status") == "Refunded", 4)
            .otherwise(0),
        )
        .withColumn(
            "payment_channel",
            F.when(F.col("payment_method").isin("Credit Card", "Debit Card"), "Card")
            .when(F.col("payment_method") == "Bank Transfer", "ACH")
            .when(F.col("payment_method") == "Check", "Check")
            .when(F.col("payment_method") == "Cash", "Cash")
            .otherwise("Other"),
        )
        .withColumn(
            "transaction_fee",
            F.when(F.col("payment_method") == "Credit Card", F.col("payment_amount") * 0.029 + 0.30)
            .when(F.col("payment_method") == "Debit Card", F.col("payment_amount") * 0.019 + 0.20)
            .when(F.col("payment_method") == "Bank Transfer", 0.25)
            .otherwise(0),
        )
        .withColumn("net_payment_amount", F.col("payment_amount") - F.col("transaction_fee"))
        .withColumn("is_late_payment", F.when(F.col("days_overdue") > 0, True).otherwise(False))
        .withColumn(
            "late_fee",
            F.when(F.col("days_overdue") > 30, 50.00)
            .when(F.col("days_overdue") > 15, 25.00)
            .when(F.col("days_overdue") > 0, 10.00)
            .otherwise(0),
        )
        .withColumn(
            "payment_size_category",
            F.when(F.col("payment_amount") >= 1000, "Large")
            .when(F.col("payment_amount") >= 500, "Medium")
            .otherwise("Small"),
        )
        .withColumn("data_quality_score", F.lit(0.95))
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Enrichment with Derived Metrics


# COMMAND ----------
@dlt.table(name="payment_enriched", comment="Payment data enriched with business metrics")
def payment_enriched():
    payments = dlt.read_stream("payment_validated")

    # Add time-based attributes
    payments = (
        payments.withColumn("payment_year", F.year(F.col("payment_date")))
        .withColumn("payment_quarter", F.quarter(F.col("payment_date")))
        .withColumn("payment_month", F.month(F.col("payment_date")))
        .withColumn("payment_day_of_week", F.dayofweek(F.col("payment_date")))
        .withColumn(
            "payment_day_name",
            F.when(F.dayofweek(F.col("payment_date")) == 1, "Sunday")
            .when(F.dayofweek(F.col("payment_date")) == 2, "Monday")
            .when(F.dayofweek(F.col("payment_date")) == 3, "Tuesday")
            .when(F.dayofweek(F.col("payment_date")) == 4, "Wednesday")
            .when(F.dayofweek(F.col("payment_date")) == 5, "Thursday")
            .when(F.dayofweek(F.col("payment_date")) == 6, "Friday")
            .otherwise("Saturday"),
        )
        .withColumn(
            "is_weekend_payment", F.when(F.dayofweek(F.col("payment_date")).isin([1, 7]), True).otherwise(False)
        )
    )

    # Add payment type classification
    payments = (
        payments.withColumn(
            "payment_type",
            F.when(F.col("payment_reason").like("%Premium%"), "Premium Payment")
            .when(F.col("payment_reason").like("%Claim%"), "Claim Payment")
            .when(F.col("payment_reason").like("%Refund%"), "Refund")
            .when(F.col("payment_reason").like("%Fee%"), "Fee Payment")
            .otherwise("Other"),
        )
        .withColumn("is_auto_payment", F.when(F.col("payment_method") == "Auto Pay", True).otherwise(False))
        .withColumn("is_recurring", F.when(F.col("payment_frequency").isNotNull(), True).otherwise(False))
    )

    # Add processing attributes
    payments = payments.withColumn(
        "processor_name",
        F.when(
            F.col("payment_processor_id").isNotNull(), F.concat(F.lit("Processor "), F.col("payment_processor_id"))
        ).otherwise("Direct"),
    ).withColumn(
        "gateway_name",
        F.when(
            F.col("payment_gateway_id").isNotNull(), F.concat(F.lit("Gateway "), F.col("payment_gateway_id"))
        ).otherwise("Internal"),
    )

    return payments


# COMMAND ----------
# MAGIC %md
# MAGIC ## Target Silver Table

# COMMAND ----------
dlt.create_streaming_table(
    name="payment_fact",
    comment="Payment fact table with transaction details",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true", "sensitive_data": "true"},
    partition_cols=["payment_year", "payment_month"],
)


@dlt.view(name="payment_updates")
def payment_updates():
    return (
        dlt.read_stream("payment_enriched")
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
    )


dlt.apply_changes(
    target="payment_fact", source="payment_updates", keys=["payment_id"], sequence_by=F.col("updated_timestamp")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Metrics


# COMMAND ----------
@dlt.table(name="payment_quality_metrics", comment="Data quality metrics for payment pipeline")
def payment_quality_metrics():
    """Calculate data quality metrics for monitoring"""
    return (
        dlt.read("payment_fact")
        .groupBy("payment_method", "payment_status")
        .agg(
            F.count("*").alias("total_payments"),
            F.sum("payment_amount").alias("total_payment_amount"),
            F.sum("net_payment_amount").alias("total_net_amount"),
            F.sum("transaction_fee").alias("total_fees"),
            F.avg("payment_amount").alias("avg_payment_amount"),
            F.sum(F.when(F.col("is_late_payment"), 1).otherwise(0)).alias("late_payments"),
            F.sum("late_fee").alias("total_late_fees"),
            F.current_timestamp().alias("metric_timestamp"),
        )
    )
