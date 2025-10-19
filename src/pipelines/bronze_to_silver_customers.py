# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline: Bronze to Silver - Customers
# MAGIC Transforms raw customer data into validated silver layer with SCD Type 2

# COMMAND ----------
import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import *

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
@dlt.table(name="customer_raw_stream", comment="Streaming view of raw customer data")
def customer_raw_stream():
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(f"{bronze_catalog}.customers.customer_raw")
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Expectations


# COMMAND ----------
@dlt.table(
    name="customer_validated",
    comment="Validated customer data with quality checks",
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "customer_id,state_code"},
)
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_name", "first_name IS NOT NULL AND last_name IS NOT NULL")
@dlt.expect_or_drop("valid_email", "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'")
@dlt.expect_or_fail("valid_state", "state_code IS NOT NULL AND LENGTH(state_code) = 2")
@dlt.expect("valid_credit_score", "credit_score BETWEEN 300 AND 850")
@dlt.expect("valid_income", "annual_income >= 0")
def customer_validated():
    df = dlt.read_stream("customer_raw_stream")

    # Data validation and cleansing
    return (
        df.withColumn("full_name", F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")))
        .withColumn("age_years", F.floor(F.months_between(F.current_date(), F.col("date_of_birth")) / 12))
        .withColumn("ssn_masked", F.concat(F.lit("XXX-XX-"), F.substring(F.col("ssn"), -4, 4)))
        .withColumn("ssn_encrypted", F.sha2(F.col("ssn"), 256))
        .withColumn("email_domain", F.element_at(F.split(F.col("email"), "@"), 2))
        .withColumn("phone_masked", F.concat(F.lit("XXX-XXX-"), F.substring(F.col("phone"), -4, 4)))
        .withColumn(
            "credit_tier",
            F.when(F.col("credit_score") >= 750, "Excellent")
            .when(F.col("credit_score") >= 700, "Good")
            .when(F.col("credit_score") >= 650, "Fair")
            .otherwise("Poor"),
        )
        .withColumn(
            "income_bracket",
            F.when(F.col("annual_income") >= 100000, "$100K+")
            .when(F.col("annual_income") >= 75000, "$75K-$100K")
            .when(F.col("annual_income") >= 50000, "$50K-$75K")
            .when(F.col("annual_income") >= 30000, "$30K-$50K")
            .otherwise("Under $30K"),
        )
        .withColumn("customer_tenure_months", F.floor(F.months_between(F.current_date(), F.col("customer_since_date"))))
        .withColumn("data_quality_score", F.lit(0.95))
        .withColumn("data_quality_flags", F.lit(None))
    )


# COMMAND ----------
# MAGIC %md
# MAGIC ## Enrichment with Lookups


# COMMAND ----------
@dlt.table(name="customer_enriched", comment="Customer data enriched with reference data")
def customer_enriched():
    customers = dlt.read_stream("customer_validated")

    # Join with state reference data (if available)
    states = (
        spark.table(f"{silver_catalog}.master_data.state_dim")
        if spark.catalog.tableExists(f"{silver_catalog}.master_data.state_dim")
        else None
    )

    if states:
        customers = customers.join(
            F.broadcast(states.select("state_code", F.col("state_name"), F.col("region").alias("state_region"))),
            "state_code",
            "left",
        )
    else:
        customers = customers.withColumn("state_name", F.lit(None)).withColumn("state_region", F.lit(None))

    # Add assigned agent name (lookup from agent dimension)
    # Simplified - would join with agent table in real scenario
    customers = customers.withColumn("assigned_agent_name", F.concat(F.lit("Agent "), F.col("assigned_agent_id")))

    return customers


# COMMAND ----------
# MAGIC %md
# MAGIC ## SCD Type 2 - Target Silver Table


# COMMAND ----------
@dlt.view(name="customer_updates")
def customer_updates():
    """
    Prepares customer updates for SCD Type 2 processing
    """
    return (
        dlt.read_stream("customer_enriched")
        .withColumn("effective_start_date", F.current_date())
        .withColumn("effective_end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("record_version", F.lit(1))
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
        .withColumn("created_by", F.lit("DLT_PIPELINE"))
    )


# COMMAND ----------
# Apply SCD Type 2 using Delta merge
# Note: In real DLT, this would use APPLY CHANGES INTO for SCD Type 2
dlt.create_streaming_table(
    name="customer_dim",
    comment="Customer dimension with SCD Type 2 history",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"},
    partition_cols=["state_code", "is_current"],
)

dlt.apply_changes(
    target="customer_dim",
    source="customer_updates",
    keys=["customer_id"],
    sequence_by=F.col("updated_timestamp"),
    stored_as_scd_type="2",
    track_history_column_list=[
        "first_name",
        "last_name",
        "email",
        "phone",
        "address_line1",
        "city",
        "state_code",
        "customer_status",
        "customer_segment",
    ],
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Metrics


# COMMAND ----------
@dlt.table(name="customer_quality_metrics", comment="Data quality metrics for customer pipeline")
def customer_quality_metrics():
    """
    Calculate data quality metrics for monitoring
    """
    return (
        dlt.read("customer_dim")
        .filter(F.col("is_current") == True)
        .agg(
            F.count("*").alias("total_records"),
            F.count("customer_id").alias("non_null_customer_id"),
            F.count("email").alias("non_null_email"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("credit_score").alias("avg_credit_score"),
            F.avg("annual_income").alias("avg_annual_income"),
            F.count(F.when(F.col("data_quality_score") >= 0.9, 1)).alias("high_quality_records"),
            F.current_timestamp().alias("metric_timestamp"),
        )
    )
