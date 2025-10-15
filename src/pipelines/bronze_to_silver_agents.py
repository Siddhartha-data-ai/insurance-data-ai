# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Pipeline: Bronze to Silver - Agents
# MAGIC Transforms raw agent data into validated silver layer dimension with hierarchy

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
@dlt.table(
    name="agent_raw_stream",
    comment="Streaming view of raw agent data"
)
def agent_raw_stream():
    return (
        spark.readStream
            .format("delta")
            .option("readChangeFeed", "true")
            .table(f"{bronze_catalog}.agents.agent_raw")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Expectations

# COMMAND ----------
@dlt.table(
    name="agent_validated",
    comment="Validated agent data with quality checks",
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "agent_id,region_code"
    }
)
@dlt.expect_or_drop("valid_agent_id", "agent_id IS NOT NULL")
@dlt.expect_or_drop("valid_agent_name", "agent_name IS NOT NULL")
@dlt.expect_or_fail("valid_agent_type", "agent_type IS NOT NULL")
@dlt.expect("valid_license_number", "license_number IS NOT NULL")
@dlt.expect("valid_commission_rate", "commission_rate BETWEEN 0 AND 1")
def agent_validated():
    df = dlt.read_stream("agent_raw_stream")
    
    # Data validation and transformations
    return (
        df
        .withColumn("years_of_experience",
                    F.floor(F.months_between(F.current_date(), F.col("hire_date")) / 12))
        .withColumn("agent_tenure_months",
                    F.floor(F.months_between(F.current_date(), F.col("hire_date"))))
        .withColumn("performance_tier",
                    F.when(F.col("performance_rating") >= 4.5, "Excellent")
                     .when(F.col("performance_rating") >= 3.5, "Good")
                     .when(F.col("performance_rating") >= 2.5, "Average")
                     .otherwise("Needs Improvement"))
        .withColumn("is_active_agent",
                    F.when(F.col("agent_status") == "Active", True).otherwise(False))
        .withColumn("commission_tier",
                    F.when(F.col("commission_rate") >= 0.15, "High")
                     .when(F.col("commission_rate") >= 0.10, "Standard")
                     .otherwise("Low"))
        .withColumn("total_licenses",
                    F.size(F.split(F.col("licensed_states"), ",")))
        .withColumn("data_quality_score", F.lit(0.95))
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## Enrichment with Hierarchy

# COMMAND ----------
@dlt.table(
    name="agent_enriched",
    comment="Agent data enriched with hierarchy and territory"
)
def agent_enriched():
    agents = dlt.read_stream("agent_validated")
    
    # Add hierarchy levels
    agents = (agents
        .withColumn("team_lead_name",
                    F.when(F.col("team_lead_id").isNotNull(), 
                           F.concat(F.lit("Team Lead "), F.col("team_lead_id")))
                     .otherwise(None))
        .withColumn("regional_manager_name",
                    F.when(F.col("regional_manager_id").isNotNull(),
                           F.concat(F.lit("Regional Manager "), F.col("regional_manager_id")))
                     .otherwise(None))
        .withColumn("vp_name",
                    F.when(F.col("vp_id").isNotNull(),
                           F.concat(F.lit("VP "), F.col("vp_id")))
                     .otherwise(None))
    )
    
    # Add performance metrics
    agents = (agents
        .withColumn("ytd_policies_sold", F.coalesce(F.col("ytd_policies_sold"), F.lit(0)))
        .withColumn("ytd_premium_written", F.coalesce(F.col("ytd_premium_written"), F.lit(0)))
        .withColumn("retention_rate", F.coalesce(F.col("retention_rate"), F.lit(0.0)))
        .withColumn("avg_policy_size",
                    F.when(F.col("ytd_policies_sold") > 0,
                           F.col("ytd_premium_written") / F.col("ytd_policies_sold"))
                     .otherwise(0))
    )
    
    # Territory information
    agents = (agents
        .withColumn("primary_territory", F.element_at(F.split(F.col("territory_code"), "-"), 1))
        .withColumn("territory_type",
                    F.when(F.col("territory_code").like("%URBAN%"), "Urban")
                     .when(F.col("territory_code").like("%SUBURBAN%"), "Suburban")
                     .when(F.col("territory_code").like("%RURAL%"), "Rural")
                     .otherwise("Mixed"))
    )
    
    return agents

# COMMAND ----------
# MAGIC %md
# MAGIC ## Target Silver Table

# COMMAND ----------
dlt.create_streaming_table(
    name="agent_dim",
    comment="Agent dimension with hierarchy and performance metrics",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "hierarchy_enabled": "true"
    },
    partition_cols=["region_code", "agent_status"]
)

@dlt.view(name="agent_updates")
def agent_updates():
    return (
        dlt.read_stream("agent_enriched")
        .withColumn("created_timestamp", F.current_timestamp())
        .withColumn("updated_timestamp", F.current_timestamp())
    )

dlt.apply_changes(
    target="agent_dim",
    source="agent_updates",
    keys=["agent_id"],
    sequence_by=F.col("updated_timestamp")
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Data Quality Metrics

# COMMAND ----------
@dlt.table(
    name="agent_quality_metrics",
    comment="Data quality metrics for agent pipeline"
)
def agent_quality_metrics():
    """Calculate data quality metrics for monitoring"""
    return (
        dlt.read("agent_dim")
        .filter(F.col("is_active_agent") == True)
        .groupBy("region_code", "agent_type")
        .agg(
            F.count("*").alias("total_agents"),
            F.sum("ytd_premium_written").alias("total_premium_written"),
            F.avg("ytd_premium_written").alias("avg_premium_per_agent"),
            F.avg("retention_rate").alias("avg_retention_rate"),
            F.avg("performance_rating").alias("avg_performance_rating"),
            F.current_timestamp().alias("metric_timestamp")
        )
    )

