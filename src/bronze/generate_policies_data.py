# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Realistic Policy Data
# MAGIC Generate 2-3 million policy records with realistic relationships to customers

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, concat, lpad, array, element_at, date_add
from pyspark.sql.types import *
from datetime import datetime, timedelta

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
# Create widgets for parameters
dbutils.widgets.dropdown("catalog", "insurance_dev_bronze", 
                         ["insurance_dev_bronze", "insurance_staging_bronze", "insurance_prod_bronze"], 
                         "Bronze Catalog Name")

# Get widget values
catalog_name = dbutils.widgets.get("catalog")

# Average 2.5 policies per customer
NUM_POLICIES = 2_500_000

print(f"Using catalog: {catalog_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load Customer Data

# COMMAND ----------
df_customers = spark.table(f"{catalog_name}.customers.customer_raw")
num_customers = df_customers.count()

print(f"Loaded {num_customers:,} customers")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Policy Records

# COMMAND ----------
from pyspark.sql.functions import col, lit, expr, when, concat, lpad, date_add, array, element_at, months_between, datediff

# Create policy records (some customers have multiple policies)
# Average 2.5 policies per customer means we need distribution
df_base = spark.range(0, NUM_POLICIES) \
    .withColumn("policy_id", concat(lit("POL"), lpad(col("id"), 10, "0"))) \
    .withColumn("policy_number", concat(
        lit("P"),
        lpad(expr("cast(rand() * 9999999 as int)"), 7, "0")
    ))

# Assign customers (some get multiple policies)
df_base = df_base \
    .withColumn("customer_row", expr(f"cast(rand() * {num_customers} as int)"))

# Join with customers to get customer_id
df_customers_indexed = df_customers \
    .withColumn("customer_row", F.monotonically_increasing_id())

df_policies = df_base.join(
    df_customers_indexed.select("customer_row", "customer_id", "state_code", "customer_segment", "credit_score"),
    "customer_row",
    "left"
).drop("customer_row")

# Policy types with realistic distribution
df_policies = df_policies \
    .withColumn("policy_type_rand", expr("rand()")) \
    .withColumn("policy_type",
                when(col("policy_type_rand") < 0.40, "Auto")
                .when(col("policy_type_rand") < 0.65, "Home")
                .when(col("policy_type_rand") < 0.80, "Life")
                .when(col("policy_type_rand") < 0.90, "Health")
                .otherwise("Commercial")) \
    .drop("policy_type_rand")

# Product line based on policy type
df_policies = df_policies \
    .withColumn("product_line",
                when(col("policy_type") == "Auto", 
                     element_at(array([lit("Personal Auto"), lit("Commercial Auto")]),
                               when(expr("rand()") < 0.85, 1).otherwise(2)))
                .when(col("policy_type") == "Home",
                     element_at(array([lit("Homeowners"), lit("Renters"), lit("Condo")]),
                               when(expr("rand()") < 0.60, 1)
                               .when(expr("rand()") < 0.90, 2)
                               .otherwise(3)))
                .when(col("policy_type") == "Life",
                     element_at(array([lit("Term Life"), lit("Whole Life"), lit("Universal Life")]),
                               when(expr("rand()") < 0.60, 1)
                               .when(expr("rand()") < 0.85, 2)
                               .otherwise(3)))
                .when(col("policy_type") == "Health",
                     element_at(array([lit("Individual Health"), lit("Family Health")]),
                               when(expr("rand()") < 0.45, 1).otherwise(2)))
                .otherwise(element_at(array([lit("Business Owners Policy"), lit("General Liability"), lit("Commercial Property")]),
                                     expr("cast(rand() * 3 as int) + 1"))))

# Product code
df_policies = df_policies \
    .withColumn("product_code", concat(
        F.upper(F.substring(col("policy_type"), 1, 2)),
        lit("-"),
        lpad(expr("cast(rand() * 999 as int) + 1"), 3, "0")
    ))

# Policy status - realistic distribution
df_policies = df_policies \
    .withColumn("policy_status_rand", expr("rand()")) \
    .withColumn("policy_status",
                when(col("policy_status_rand") < 0.75, "Active")
                .when(col("policy_status_rand") < 0.85, "Pending")
                .when(col("policy_status_rand") < 0.92, "Lapsed")
                .otherwise("Cancelled")) \
    .drop("policy_status_rand")

# Generate policy dates
# Application date: between 5 years ago and 30 days from now
df_policies = df_policies \
    .withColumn("application_days_ago", expr("cast(rand() * {} as int)".format(365 * 5 - 30))) \
    .withColumn("application_date", date_add(lit(datetime.now().date()), -col("application_days_ago"))) \
    .drop("application_days_ago")

# Issue date: 5-15 days after application
df_policies = df_policies \
    .withColumn("issue_date", date_add(col("application_date"), expr("cast(rand() * 10 as int) + 5")))

# Effective date: 0-30 days after issue
df_policies = df_policies \
    .withColumn("effective_date", date_add(col("issue_date"), expr("cast(rand() * 30 as int)")))

# Bind date: same as or 1-2 days before effective date
df_policies = df_policies \
    .withColumn("bind_date", date_add(col("effective_date"), -expr("cast(rand() * 2 as int)")))

# Policy term (6, 12, or 24 months typically)
df_policies = df_policies \
    .withColumn("policy_term_months",
                when(col("policy_type").isin(["Auto", "Home"]), 
                     element_at(array([lit(6), lit(12)]),
                               when(expr("rand()") < 0.75, 2).otherwise(1)))
                .when(col("policy_type") == "Life",
                     element_at(array([lit(12), lit(120), lit(240)]),
                               when(expr("rand()") < 0.50, 1)
                               .when(expr("rand()") < 0.85, 2)
                               .otherwise(3)))
                .otherwise(lit(12)))

# Expiration date
df_policies = df_policies \
    .withColumn("expiration_date", expr("add_months(effective_date, policy_term_months)"))

# Cancellation date for cancelled/lapsed policies
df_policies = df_policies \
    .withColumn("cancellation_date",
                when(col("policy_status").isin(["Cancelled", "Lapsed"]),
                     date_add(col("effective_date"), 
                             expr("cast(rand() * datediff(expiration_date, effective_date) as int)")))
                .otherwise(lit(None))) \
    .withColumn("cancellation_reason",
                when(col("policy_status") == "Cancelled",
                     element_at(array([
                         lit("Non-Payment"), lit("Customer Request"), lit("Underwriting"), 
                         lit("Fraud"), lit("Loss of Interest"), lit("Found Better Rate")
                     ]), expr("cast(rand() * 6 as int) + 1")))
                .when(col("policy_status") == "Lapsed", lit("Non-Payment"))
                .otherwise(lit(None)))

# Premium calculations based on policy type, coverage, risk
df_policies = df_policies \
    .withColumn("base_premium_auto", expr("rand() * 1500 + 600")) \
    .withColumn("base_premium_home", expr("rand() * 2000 + 800")) \
    .withColumn("base_premium_life", expr("rand() * 3000 + 500")) \
    .withColumn("base_premium_health", expr("rand() * 5000 + 2000")) \
    .withColumn("base_premium_commercial", expr("rand() * 8000 + 3000")) \
    .withColumn("annual_premium",
                when(col("policy_type") == "Auto", col("base_premium_auto"))
                .when(col("policy_type") == "Home", col("base_premium_home"))
                .when(col("policy_type") == "Life", col("base_premium_life"))
                .when(col("policy_type") == "Health", col("base_premium_health"))
                .otherwise(col("base_premium_commercial"))) \
    .drop("base_premium_auto", "base_premium_home", "base_premium_life", 
          "base_premium_health", "base_premium_commercial")

# Adjust premium based on credit score and customer segment
df_policies = df_policies \
    .withColumn("annual_premium",
                when(col("customer_segment") == "Platinum", col("annual_premium") * 0.85)
                .when(col("customer_segment") == "Gold", col("annual_premium") * 0.90)
                .when(col("customer_segment") == "Silver", col("annual_premium") * 0.95)
                .otherwise(col("annual_premium") * 1.10)) \
    .withColumn("annual_premium", F.round(col("annual_premium"), 2).cast("decimal(12,2)"))

# Payment frequency
df_policies = df_policies \
    .withColumn("payment_frequency",
                element_at(array([lit("Annual"), lit("Semi-Annual"), lit("Quarterly"), lit("Monthly")]),
                          when(expr("rand()") < 0.15, 1)
                          .when(expr("rand()") < 0.30, 2)
                          .when(expr("rand()") < 0.50, 3)
                          .otherwise(4)))

# Payment method
df_policies = df_policies \
    .withColumn("payment_method",
                element_at(array([lit("Auto Pay - Bank"), lit("Auto Pay - Credit Card"), 
                                 lit("Credit Card"), lit("Check"), lit("Bank Transfer")]),
                          when(expr("rand()") < 0.45, 1)
                          .when(expr("rand()") < 0.75, 2)
                          .when(expr("rand()") < 0.90, 3)
                          .when(expr("rand()") < 0.97, 4)
                          .otherwise(5)))

# Premium payment status
df_policies = df_policies \
    .withColumn("premium_payment_status",
                when(col("policy_status") == "Active",
                     element_at(array([lit("Current"), lit("Overdue")]),
                               when(expr("rand()") < 0.90, 1).otherwise(2)))
                .when(col("policy_status") == "Pending", lit("Pending"))
                .otherwise(lit("Unpaid")))

# Coverage amount based on policy type
df_policies = df_policies \
    .withColumn("coverage_amount",
                when(col("policy_type") == "Auto",
                     element_at(array([lit(50000), lit(100000), lit(250000), lit(500000)]),
                               when(expr("rand()") < 0.30, 1)
                               .when(expr("rand()") < 0.60, 2)
                               .when(expr("rand()") < 0.90, 3)
                               .otherwise(4)))
                .when(col("policy_type") == "Home",
                     expr("cast(rand() * 700000 + 200000 as decimal(15,2))"))
                .when(col("policy_type") == "Life",
                     element_at(array([lit(100000), lit(250000), lit(500000), lit(1000000), lit(2000000)]),
                               when(expr("rand()") < 0.25, 1)
                               .when(expr("rand()") < 0.50, 2)
                               .when(expr("rand()") < 0.75, 3)
                               .when(expr("rand()") < 0.90, 4)
                               .otherwise(5)))
                .when(col("policy_type") == "Health",
                     element_at(array([lit(50000), lit(100000), lit(250000), lit(1000000)]),
                               expr("cast(rand() * 4 as int) + 1")))
                .otherwise(expr("cast(rand() * 2000000 + 500000 as decimal(15,2))")))

# Deductible amount
df_policies = df_policies \
    .withColumn("deductible_amount",
                when(col("policy_type") == "Auto",
                     element_at(array([lit(250), lit(500), lit(1000), lit(2500)]),
                               expr("cast(rand() * 4 as int) + 1")))
                .when(col("policy_type") == "Home",
                     element_at(array([lit(500), lit(1000), lit(2500), lit(5000)]),
                               expr("cast(rand() * 4 as int) + 1")))
                .when(col("policy_type") == "Health",
                     element_at(array([lit(1000), lit(2500), lit(5000), lit(10000)]),
                               expr("cast(rand() * 4 as int) + 1")))
                .otherwise(element_at(array([lit(1000), lit(2500), lit(5000)]),
                                     expr("cast(rand() * 3 as int) + 1"))))

# Coverage dates (same as policy dates for simplicity)
df_policies = df_policies \
    .withColumn("coverage_start_date", col("effective_date")) \
    .withColumn("coverage_end_date", col("expiration_date"))

# Assign agents
df_policies = df_policies \
    .withColumn("writing_agent_id", concat(lit("AGT"), lpad(expr("cast(rand() * 5000 as int) + 1"), 6, "0"))) \
    .withColumn("servicing_agent_id", 
                when(expr("rand()") < 0.80, col("writing_agent_id"))
                .otherwise(concat(lit("AGT"), lpad(expr("cast(rand() * 5000 as int) + 1"), 6, "0"))))

# Agency and territory
df_policies = df_policies \
    .withColumn("agency_code", concat(lit("AGY"), lpad(expr("cast(rand() * 500 as int) + 1"), 4, "0"))) \
    .withColumn("territory_code", concat(col("state_code"), lit("-"), lpad(expr("cast(rand() * 99 as int) + 1"), 2, "0"))) \
    .withColumn("county", 
                element_at(array([
                    lit("County A"), lit("County B"), lit("County C"), lit("County D"), lit("County E")
                ]), expr("cast(rand() * 5 as int) + 1")))

# Underwriting details
df_policies = df_policies \
    .withColumn("underwriter_id", concat(lit("UW"), lpad(expr("cast(rand() * 200 as int) + 1"), 4, "0"))) \
    .withColumn("underwriting_tier",
                when(col("credit_score") > 750, "Preferred")
                .when(col("credit_score") > 650, "Standard")
                .otherwise("Non-Standard"))

# Risk class based on tier
df_policies = df_policies \
    .withColumn("risk_class",
                when(col("underwriting_tier") == "Preferred",
                     element_at(array([lit("A+"), lit("A"), lit("A-")]), expr("cast(rand() * 3 as int) + 1")))
                .when(col("underwriting_tier") == "Standard",
                     element_at(array([lit("B+"), lit("B"), lit("B-")]), expr("cast(rand() * 3 as int) + 1")))
                .otherwise(element_at(array([lit("C+"), lit("C"), lit("C-")]), expr("cast(rand() * 3 as int) + 1"))))

# Rating factor
df_policies = df_policies \
    .withColumn("rating_factor",
                when(col("underwriting_tier") == "Preferred", expr("rand() * 0.2 + 0.80"))
                .when(col("underwriting_tier") == "Standard", expr("rand() * 0.2 + 0.95"))
                .otherwise(expr("rand() * 0.3 + 1.10"))) \
    .withColumn("rating_factor", F.round(col("rating_factor"), 4).cast("decimal(5,4)"))

# Commission calculations
df_policies = df_policies \
    .withColumn("commission_rate",
                when(col("policy_type") == "Auto", expr("rand() * 0.05 + 0.10"))
                .when(col("policy_type") == "Home", expr("rand() * 0.05 + 0.12"))
                .when(col("policy_type") == "Life", expr("rand() * 0.20 + 0.40"))
                .when(col("policy_type") == "Health", expr("rand() * 0.10 + 0.15"))
                .otherwise(expr("rand() * 0.08 + 0.10"))) \
    .withColumn("commission_rate", F.round(col("commission_rate"), 4).cast("decimal(5,4)")) \
    .withColumn("commission_amount", F.round(col("annual_premium") * col("commission_rate"), 2).cast("decimal(10,2)"))

# Renewal information
df_policies = df_policies \
    .withColumn("renewal_count", 
                when(col("policy_status") == "Active", expr("cast(rand() * 5 as int)"))
                .otherwise(0)) \
    .withColumn("is_renewal", col("renewal_count") > 0) \
    .withColumn("parent_policy_id",
                when(col("is_renewal"), concat(lit("POL"), lpad(expr("cast(rand() * 1000000 as int)"), 10, "0")))
                .otherwise(lit(None)))

# Source system metadata
df_policies = df_policies \
    .withColumn("source_system", lit("PolicyAdmin")) \
    .withColumn("source_record_id", col("policy_id")) \
    .withColumn("source_last_modified_timestamp", 
                F.from_unixtime(F.unix_timestamp(F.current_timestamp()) - (F.rand() * 86400).cast("long"))) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("record_hash", F.md5(concat(*[col(c) for c in ["policy_id", "customer_id", "policy_number"]]))) \
    .withColumn("data_lineage", lit('{"source": "policy_generation_script", "version": "1.0"}'))

# Select final columns
final_columns = [
    "policy_id", "policy_number", "customer_id", "policy_type", "product_line", "product_code",
    "policy_status", "effective_date", "expiration_date", "issue_date", "application_date",
    "bind_date", "cancellation_date", "cancellation_reason", "policy_term_months",
    "renewal_count", "is_renewal", "parent_policy_id",
    "annual_premium", "payment_frequency", "payment_method", "premium_payment_status",
    "coverage_amount", "deductible_amount", "coverage_start_date", "coverage_end_date",
    "writing_agent_id", "servicing_agent_id", "agency_code", "territory_code", "state_code", "county",
    "underwriter_id", "underwriting_tier", "risk_class", "rating_factor",
    "commission_rate", "commission_amount",
    "source_system", "source_record_id", "source_last_modified_timestamp",
    "ingestion_timestamp", "record_hash", "data_lineage"
]

# Note: Renamed _metadata to data_lineage to avoid reserved keyword conflict

df_policies_final = df_policies.select(final_columns)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write to Bronze Layer

# COMMAND ----------
table_name = f"{catalog_name}.policies.policy_raw"

print(f"Writing to table: {table_name}")

# Ensure catalog and schema exist
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.policies")

# Drop table first
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Create table from DataFrame
df_policies_final.write.format("delta").saveAsTable(table_name)

print(f"✅ Successfully wrote policy records to {table_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate Data Quality

# COMMAND ----------
display(spark.table(table_name).limit(10))

# COMMAND ----------
print("Policy Data Quality Summary:")
print("=" * 60)
spark.sql(f"""
    SELECT 
        policy_type,
        COUNT(*) as policy_count,
        ROUND(AVG(annual_premium), 2) as avg_premium,
        ROUND(AVG(coverage_amount), 2) as avg_coverage,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM {table_name}
    GROUP BY policy_type
    ORDER BY policy_count DESC
""").show()

# COMMAND ----------
print("Policy Status Distribution:")
spark.sql(f"""
    SELECT 
        policy_status,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
        ROUND(AVG(annual_premium), 2) as avg_premium
    FROM {table_name}
    GROUP BY policy_status
    ORDER BY count DESC
""").show()

