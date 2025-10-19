# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Realistic Customer Data
# MAGIC Generate 1 million+ customer records with realistic distributions

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when, concat, lpad, array, element_at, date_add
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
# Create widgets for parameters
dbutils.widgets.dropdown(
    "catalog",
    "insurance_dev_bronze",
    ["insurance_dev_bronze", "insurance_staging_bronze", "insurance_prod_bronze"],
    "Bronze Catalog Name",
)

# Get widget values
catalog_name = dbutils.widgets.get("catalog")

# Number of customers to generate
NUM_CUSTOMERS = 1_000_000
BATCH_SIZE = 100_000

print(f"Using catalog: {catalog_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Customer Records

# COMMAND ----------
# US State distribution (realistic population distribution)
state_distribution = [
    ("CA", 0.12),
    ("TX", 0.09),
    ("FL", 0.07),
    ("NY", 0.06),
    ("PA", 0.04),
    ("IL", 0.04),
    ("OH", 0.04),
    ("GA", 0.03),
    ("NC", 0.03),
    ("MI", 0.03),
    ("NJ", 0.03),
    ("VA", 0.03),
    ("WA", 0.02),
    ("AZ", 0.02),
    ("MA", 0.02),
    ("TN", 0.02),
    ("IN", 0.02),
    ("MO", 0.02),
    ("MD", 0.02),
    ("WI", 0.02),
    ("CO", 0.02),
    ("MN", 0.02),
    ("SC", 0.02),
    ("AL", 0.01),
    ("LA", 0.01),
    ("KY", 0.01),
    ("OR", 0.01),
    ("OK", 0.01),
    ("CT", 0.01),
    ("UT", 0.01),
    # Add more states to total 100%
    ("IA", 0.01),
    ("NV", 0.01),
    ("AR", 0.01),
    ("MS", 0.01),
    ("KS", 0.01),
    ("NM", 0.01),
    ("NE", 0.01),
    ("WV", 0.01),
    ("ID", 0.01),
    ("HI", 0.01),
]

# First names distribution
first_names_male = [
    "James",
    "Robert",
    "John",
    "Michael",
    "David",
    "William",
    "Richard",
    "Joseph",
    "Thomas",
    "Christopher",
    "Charles",
    "Daniel",
    "Matthew",
    "Anthony",
    "Mark",
    "Donald",
    "Steven",
    "Andrew",
    "Paul",
    "Joshua",
    "Kenneth",
    "Kevin",
    "Brian",
    "George",
    "Timothy",
    "Ronald",
    "Jason",
    "Edward",
    "Jeffrey",
    "Ryan",
    "Jacob",
]

first_names_female = [
    "Mary",
    "Patricia",
    "Jennifer",
    "Linda",
    "Elizabeth",
    "Barbara",
    "Susan",
    "Jessica",
    "Sarah",
    "Karen",
    "Lisa",
    "Nancy",
    "Betty",
    "Margaret",
    "Sandra",
    "Ashley",
    "Kimberly",
    "Emily",
    "Donna",
    "Michelle",
    "Carol",
    "Amanda",
    "Melissa",
    "Deborah",
    "Stephanie",
    "Dorothy",
    "Rebecca",
    "Sharon",
    "Laura",
    "Cynthia",
]

last_names = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson",
    "Walker",
    "Young",
    "Allen",
    "King",
    "Wright",
    "Scott",
    "Torres",
    "Nguyen",
    "Hill",
    "Flores",
]

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Data Using Spark

# COMMAND ----------
from pyspark.sql.functions import udf, col, lit, expr, when, concat, lpad, date_add, array, element_at

# Create base customer IDs
df_base = spark.range(0, NUM_CUSTOMERS).withColumn("customer_id", concat(lit("CUST"), lpad(col("id"), 10, "0")))

# Generate demographics
df_customers = (
    df_base.withColumn(
        "first_name",
        when(
            F.rand() < 0.5,
            element_at(array([lit(x) for x in first_names_male]), ((F.rand() * len(first_names_male)).cast("int") + 1)),
        ).otherwise(
            element_at(
                array([lit(x) for x in first_names_female]), ((F.rand() * len(first_names_female)).cast("int") + 1)
            )
        ),
    )
    .withColumn(
        "last_name", element_at(array([lit(x) for x in last_names]), ((F.rand() * len(last_names)).cast("int") + 1))
    )
    .withColumn("gender", when(F.rand() < 0.5, "M").otherwise("F"))
)

# Generate date of birth (ages 18-85)
df_customers = (
    df_customers.withColumn("days_old", ((F.rand() * (365 * 67)).cast("int") + (365 * 18)))
    .withColumn("date_of_birth", date_add(lit(datetime.now().date()), -col("days_old")))
    .drop("days_old")
)

# Generate SSN (fake format)
df_customers = df_customers.withColumn(
    "ssn",
    concat(
        lpad((F.rand() * 999).cast("int"), 3, "0"),
        lit("-"),
        lpad((F.rand() * 99).cast("int"), 2, "0"),
        lit("-"),
        lpad((F.rand() * 9999).cast("int"), 4, "0"),
    ),
)

# Generate contact information
df_customers = df_customers.withColumn(
    "email",
    concat(
        F.lower(col("first_name")),
        lit("."),
        F.lower(col("last_name")),
        lit("@"),
        element_at(
            array(
                [
                    lit("gmail.com"),
                    lit("yahoo.com"),
                    lit("hotmail.com"),
                    lit("outlook.com"),
                    lit("aol.com"),
                    lit("icloud.com"),
                ]
            ),
            ((F.rand() * 6).cast("int") + 1),
        ),
    ),
).withColumn(
    "phone",
    concat(
        lpad(((F.rand() * 899).cast("int") + 100), 3, "0"),
        lit("-"),
        lpad(((F.rand() * 899).cast("int") + 100), 3, "0"),
        lit("-"),
        lpad((F.rand() * 9999).cast("int"), 4, "0"),
    ),
)

# Generate state with realistic distribution
# Create state_code using weighted random (simplified)
state_codes = [s[0] for s in state_distribution]
df_customers = df_customers.withColumn(
    "state_code", element_at(array([lit(x) for x in state_codes]), ((F.rand() * len(state_codes)).cast("int") + 1))
)

# Generate address
df_customers = (
    df_customers.withColumn(
        "address_line1",
        concat(
            ((F.rand() * 9999 + 1).cast("int")),
            lit(" "),
            element_at(
                array(
                    [
                        lit("Main"),
                        lit("Oak"),
                        lit("Pine"),
                        lit("Maple"),
                        lit("Cedar"),
                        lit("Elm"),
                        lit("Washington"),
                        lit("Lake"),
                        lit("Hill"),
                        lit("Park"),
                    ]
                ),
                ((F.rand() * 10).cast("int") + 1),
            ),
            lit(" "),
            element_at(
                array([lit("St"), lit("Ave"), lit("Blvd"), lit("Dr"), lit("Ln"), lit("Rd")]),
                ((F.rand() * 6).cast("int") + 1),
            ),
        ),
    )
    .withColumn(
        "address_line2",
        when(F.rand() < 0.3, concat(lit("Apt "), ((F.rand() * 999 + 1).cast("int")))).otherwise(lit(None)),
    )
    .withColumn(
        "city",
        element_at(
            array(
                [
                    lit("Springfield"),
                    lit("Franklin"),
                    lit("Clinton"),
                    lit("Madison"),
                    lit("Georgetown"),
                    lit("Arlington"),
                    lit("Salem"),
                    lit("Fairview"),
                    lit("Riverside"),
                    lit("Oak Grove"),
                    lit("Hillside"),
                    lit("Lakewood"),
                ]
            ),
            ((F.rand() * 12).cast("int") + 1),
        ),
    )
    .withColumn("zip_code", lpad((F.rand() * 99999).cast("int"), 5, "0"))
    .withColumn("country", lit("USA"))
)

# Generate customer attributes
df_customers = df_customers.withColumn(
    "customer_type",
    element_at(
        array([lit("Individual"), lit("Family"), lit("Business")]),
        when(F.rand() < 0.6, 1).when(F.rand() < 0.9, 2).otherwise(3),
    ),
).withColumn(
    "marital_status",
    element_at(
        array([lit("Single"), lit("Married"), lit("Divorced"), lit("Widowed")]),
        when(F.rand() < 0.35, 1).when(F.rand() < 0.85, 2).when(F.rand() < 0.95, 3).otherwise(4),
    ),
)

# Generate credit score (300-850, normal distribution approximation)
df_customers = (
    df_customers.withColumn("credit_score_raw", (F.rand() + F.rand() + F.rand() + F.rand() + F.rand() + F.rand()) / 6.0)
    .withColumn("credit_score", F.expr("300 + credit_score_raw * 550").cast("int"))
    .drop("credit_score_raw")
)

# Generate occupation
occupations = [
    "Software Engineer",
    "Teacher",
    "Nurse",
    "Sales Manager",
    "Accountant",
    "Retired",
    "Business Owner",
    "Consultant",
    "Engineer",
    "Doctor",
    "Attorney",
    "Manager",
    "Analyst",
    "Technician",
    "Administrator",
    "Driver",
    "Mechanic",
    "Electrician",
    "Plumber",
    "Carpenter",
    "Student",
    "Unemployed",
    "Self Employed",
    "Executive",
    "Professor",
]
df_customers = df_customers.withColumn(
    "occupation", element_at(array([lit(x) for x in occupations]), ((F.rand() * len(occupations)).cast("int") + 1))
)

# Generate annual income (correlated with occupation and credit score)
df_customers = df_customers.withColumn(
    "annual_income",
    when(col("occupation") == "Retired", ((F.rand() * 40000 + 30000).cast("decimal(15,2)")))
    .when(col("occupation") == "Student", ((F.rand() * 20000 + 10000).cast("decimal(15,2)")))
    .when(col("occupation") == "Unemployed", ((F.rand() * 15000).cast("decimal(15,2)")))
    .when(
        col("occupation").isin(["Doctor", "Attorney", "Executive"]),
        ((F.rand() * 200000 + 100000).cast("decimal(15,2)")),
    )
    .when(
        col("occupation").isin(["Software Engineer", "Engineer", "Manager"]),
        ((F.rand() * 100000 + 60000).cast("decimal(15,2)")),
    )
    .otherwise(((F.rand() * 60000 + 30000).cast("decimal(15,2)"))),
)

# Generate customer segment based on income and credit
df_customers = df_customers.withColumn(
    "customer_segment",
    when((col("annual_income") > 100000) & (col("credit_score") > 750), "Platinum")
    .when((col("annual_income") > 75000) & (col("credit_score") > 700), "Gold")
    .when((col("annual_income") > 50000) & (col("credit_score") > 650), "Silver")
    .otherwise("Bronze"),
)

# Generate customer lifecycle data
df_customers = (
    df_customers.withColumn("customer_since_days", (F.rand() * (365 * 10)).cast("int"))
    .withColumn("customer_since_date", date_add(lit(datetime.now().date()), -col("customer_since_days")))
    .drop("customer_since_days")
    .withColumn(
        "customer_status", when(F.rand() < 0.92, "Active").when(F.rand() < 0.97, "Inactive").otherwise("Cancelled")
    )
)

# Communication preferences
df_customers = (
    df_customers.withColumn(
        "communication_preference",
        element_at(array([lit("Email"), lit("Phone"), lit("Text"), lit("Mail")]), ((F.rand() * 4).cast("int") + 1)),
    )
    .withColumn(
        "preferred_language", when(F.rand() < 0.85, "English").when(F.rand() < 0.95, "Spanish").otherwise("Other")
    )
    .withColumn("marketing_opt_in", F.rand() < 0.6)
)

# Risk profile
df_customers = df_customers.withColumn(
    "risk_profile",
    when(col("credit_score") > 750, "Low Risk").when(col("credit_score") > 650, "Medium Risk").otherwise("High Risk"),
)

# Assign to regions and agents (simplified)
df_customers = df_customers.withColumn(
    "assigned_region",
    when(col("state_code").isin(["CA", "OR", "WA", "NV", "AZ"]), "West")
    .when(col("state_code").isin(["TX", "OK", "AR", "LA"]), "Southwest")
    .when(col("state_code").isin(["IL", "IN", "MI", "OH", "WI"]), "Midwest")
    .when(col("state_code").isin(["NY", "PA", "NJ", "MA", "CT"]), "Northeast")
    .when(col("state_code").isin(["FL", "GA", "NC", "SC", "VA"]), "Southeast")
    .otherwise("Other"),
).withColumn("assigned_agent_id", concat(lit("AGT"), lpad(((F.rand() * 5000).cast("int") + 1), 6, "0")))

# Source system metadata
df_customers = (
    df_customers.withColumn("source_system", lit("CRM"))
    .withColumn("source_record_id", col("customer_id"))
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("record_hash", F.md5(concat(*[col(c) for c in ["customer_id", "first_name", "last_name", "email"]])))
    .withColumn("data_lineage", lit('{"source": "customer_generation_script", "version": "1.0"}'))
)

# Select final columns in order
final_columns = [
    "customer_id",
    "first_name",
    "last_name",
    "date_of_birth",
    "ssn",
    "email",
    "phone",
    "address_line1",
    "address_line2",
    "city",
    "state_code",
    "zip_code",
    "country",
    "customer_type",
    "credit_score",
    "marital_status",
    "occupation",
    "annual_income",
    "communication_preference",
    "preferred_language",
    "marketing_opt_in",
    "customer_segment",
    "customer_since_date",
    "customer_status",
    "assigned_agent_id",
    "assigned_region",
    "risk_profile",
    "source_system",
    "source_record_id",
    "ingestion_timestamp",
    "record_hash",
    "data_lineage",
]

df_customers_final = df_customers.select(final_columns)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Write to Bronze Layer

# COMMAND ----------
# Write to Delta table
table_name = f"{catalog_name}.customers.customer_raw"

# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.customers")

# Drop table first
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# Write data
df_customers_final.write.format("delta").saveAsTable(table_name)

print(f"✅ Successfully wrote customer records to {table_name}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Validate Data Quality

# COMMAND ----------
# Display sample records
display(spark.table(table_name).limit(10))

# COMMAND ----------
# Data quality checks
print("Data Quality Summary:")
print("=" * 60)
spark.sql(
    f"""
    SELECT 
        COUNT(*) as total_customers,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(DISTINCT email) as unique_emails,
        AVG(credit_score) as avg_credit_score,
        AVG(annual_income) as avg_annual_income,
        COUNT(DISTINCT state_code) as states_count
    FROM {table_name}
"""
).show()

# COMMAND ----------
# Distribution by state
print("Top 10 States by Customer Count:")
spark.sql(
    f"""
    SELECT 
        state_code,
        COUNT(*) as customer_count,
        AVG(annual_income) as avg_income,
        AVG(credit_score) as avg_credit_score
    FROM {table_name}
    GROUP BY state_code
    ORDER BY customer_count DESC
    LIMIT 10
"""
).show()

# COMMAND ----------
# Distribution by segment
print("Customer Segment Distribution:")
spark.sql(
    f"""
    SELECT 
        customer_segment,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
        AVG(annual_income) as avg_income,
        AVG(credit_score) as avg_credit_score
    FROM {table_name}
    GROUP BY customer_segment
    ORDER BY count DESC
"""
).show()
