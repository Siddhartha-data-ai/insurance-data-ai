# Databricks notebook source
# MAGIC %md
# MAGIC # 🔍 Data Quality Monitoring Dashboard
# MAGIC
# MAGIC **Automated Data Quality Checks Across All Layers**
# MAGIC
# MAGIC This notebook provides:
# MAGIC - Automated quality checks for bronze, silver, and gold layers
# MAGIC - Quality metrics tracking over time
# MAGIC - Alerting on quality threshold violations
# MAGIC - Interactive DQ dashboard
# MAGIC
# MAGIC **Run Schedule:** Daily (recommended)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ⚙️ Configuration

from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from IPython.display import HTML, display

# COMMAND ----------
# Import libraries
from pyspark.sql import functions as F
from pyspark.sql.functions import col, count, current_timestamp, isnan, lit, when

# Configuration
dbutils.widgets.dropdown(
    "environment_catalog",
    "insurance_dev_gold",
    ["insurance_dev_gold", "insurance_staging_gold", "insurance_prod_gold"],
    "📊 Environment Catalog",
)

catalog = dbutils.widgets.get("environment_catalog")
env = "dev" if "dev" in catalog else "staging" if "staging" in catalog else "prod"
bronze_catalog = catalog.replace("_gold", "_bronze")
silver_catalog = catalog.replace("_gold", "_silver")
gold_catalog = catalog

print(f"✅ Environment: {env.upper()}")
print(f"📊 Bronze Catalog: {bronze_catalog}")
print(f"📊 Silver Catalog: {silver_catalog}")
print(f"📊 Gold Catalog: {gold_catalog}")

# Quality thresholds (configurable)
THRESHOLDS = {
    "null_rate": 0.05,  # Max 5% nulls allowed
    "duplicate_rate": 0.01,  # Max 1% duplicates allowed
    "freshness_hours": 48,  # Data older than 48 hours = stale
    "min_row_count": 100,  # Minimum expected rows
    "schema_change": False,  # Alert on schema changes
}

print(f"\n⚙️  Quality Thresholds: {THRESHOLDS}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 Data Quality Check Functions


# COMMAND ----------
def check_null_rate(df, table_name, layer):
    """Calculate null rate for each column"""
    total_rows = df.count()

    if total_rows == 0:
        return pd.DataFrame(
            {
                "layer": [layer],
                "table": [table_name],
                "column": ["N/A"],
                "null_count": [0],
                "null_rate": [0.0],
                "status": ["WARNING"],
                "message": ["Table is empty"],
            }
        )

    results = []
    for column in df.columns:
        null_count = df.filter(col(column).isNull() | isnan(column)).count()
        null_rate = null_count / total_rows

        status = "PASS" if null_rate <= THRESHOLDS["null_rate"] else "FAIL"

        results.append(
            {
                "layer": layer,
                "table": table_name,
                "column": column,
                "null_count": null_count,
                "null_rate": round(null_rate, 4),
                "status": status,
                "message": f"Null rate: {null_rate:.2%}",
            }
        )

    return pd.DataFrame(results)


def check_duplicate_rate(df, table_name, layer, key_columns):
    """Calculate duplicate rate based on key columns"""
    total_rows = df.count()

    if total_rows == 0:
        return {
            "layer": layer,
            "table": table_name,
            "total_rows": 0,
            "duplicate_rows": 0,
            "duplicate_rate": 0.0,
            "status": "WARNING",
            "message": "Table is empty",
        }

    # Check if key columns exist
    existing_keys = [k for k in key_columns if k in df.columns]
    if not existing_keys:
        return {
            "layer": layer,
            "table": table_name,
            "total_rows": total_rows,
            "duplicate_rows": 0,
            "duplicate_rate": 0.0,
            "status": "SKIP",
            "message": f"Key columns {key_columns} not found",
        }

    distinct_rows = df.select(existing_keys).distinct().count()
    duplicate_rows = total_rows - distinct_rows
    duplicate_rate = duplicate_rows / total_rows if total_rows > 0 else 0

    status = "PASS" if duplicate_rate <= THRESHOLDS["duplicate_rate"] else "FAIL"

    return {
        "layer": layer,
        "table": table_name,
        "total_rows": total_rows,
        "duplicate_rows": duplicate_rows,
        "duplicate_rate": round(duplicate_rate, 4),
        "status": status,
        "message": f"Duplicate rate: {duplicate_rate:.2%} on {existing_keys}",
    }


def check_freshness(df, table_name, layer, timestamp_column):
    """Check data freshness based on timestamp column"""

    # Check if timestamp column exists
    if timestamp_column not in df.columns:
        return {
            "layer": layer,
            "table": table_name,
            "latest_timestamp": None,
            "hours_old": None,
            "status": "SKIP",
            "message": f"Timestamp column {timestamp_column} not found",
        }

    latest_timestamp = df.agg(F.max(col(timestamp_column))).collect()[0][0]

    if latest_timestamp is None:
        return {
            "layer": layer,
            "table": table_name,
            "latest_timestamp": None,
            "hours_old": None,
            "status": "WARNING",
            "message": "No timestamp data available",
        }

    hours_old = (datetime.now() - latest_timestamp).total_seconds() / 3600

    status = "PASS" if hours_old <= THRESHOLDS["freshness_hours"] else "FAIL"

    return {
        "layer": layer,
        "table": table_name,
        "latest_timestamp": latest_timestamp,
        "hours_old": round(hours_old, 2),
        "status": status,
        "message": f"Data is {hours_old:.1f} hours old",
    }


def check_row_count(df, table_name, layer):
    """Check if table has minimum expected row count"""
    row_count = df.count()

    status = "PASS" if row_count >= THRESHOLDS["min_row_count"] else "FAIL"

    return {
        "layer": layer,
        "table": table_name,
        "row_count": row_count,
        "min_expected": THRESHOLDS["min_row_count"],
        "status": status,
        "message": f"Row count: {row_count:,}",
    }


def check_value_ranges(df, table_name, layer, range_checks):
    """Check if numeric values are within expected ranges"""
    results = []

    for column, (min_val, max_val) in range_checks.items():
        if column not in df.columns:
            continue

        out_of_range = df.filter((col(column) < min_val) | (col(column) > max_val)).count()

        total_rows = df.count()
        out_of_range_rate = out_of_range / total_rows if total_rows > 0 else 0

        status = "PASS" if out_of_range == 0 else "FAIL"

        results.append(
            {
                "layer": layer,
                "table": table_name,
                "column": column,
                "out_of_range_count": out_of_range,
                "out_of_range_rate": round(out_of_range_rate, 4),
                "expected_range": f"[{min_val}, {max_val}]",
                "status": status,
                "message": f"{out_of_range} rows out of range",
            }
        )

    return pd.DataFrame(results) if results else pd.DataFrame()


print("✅ Data quality check functions loaded")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔍 Run Quality Checks - Bronze Layer

# COMMAND ----------
print("🔍 Running Bronze Layer Quality Checks...")
print("=" * 70)

bronze_results = {"null_checks": [], "duplicate_checks": [], "freshness_checks": [], "row_count_checks": []}

# Bronze tables to check
bronze_tables = {
    "customers.customer_raw": {"key_columns": ["customer_id"], "timestamp_column": "created_timestamp"},
    "policies.policy_raw": {"key_columns": ["policy_id"], "timestamp_column": "policy_effective_date"},
    "claims.claim_raw": {"key_columns": ["claim_id"], "timestamp_column": "reported_date"},
}

for table_path, config in bronze_tables.items():
    full_table = f"{bronze_catalog}.{table_path}"
    table_name = table_path.split(".")[-1]

    try:
        print(f"\n📋 Checking: {full_table}")
        df = spark.table(full_table)

        # Null rate check
        null_results = check_null_rate(df, table_name, "bronze")
        bronze_results["null_checks"].append(null_results)
        failed_nulls = null_results[null_results["status"] == "FAIL"]
        if not failed_nulls.empty:
            print(f"  ⚠️  FAIL: High null rates in {len(failed_nulls)} columns")
        else:
            print(f"  ✅ PASS: Null rate checks")

        # Duplicate check
        dup_result = check_duplicate_rate(df, table_name, "bronze", config["key_columns"])
        bronze_results["duplicate_checks"].append(dup_result)
        if dup_result["status"] == "FAIL":
            print(f"  ⚠️  FAIL: High duplicate rate ({dup_result['duplicate_rate']:.2%})")
        else:
            print(f"  ✅ PASS: Duplicate check")

        # Freshness check
        fresh_result = check_freshness(df, table_name, "bronze", config["timestamp_column"])
        bronze_results["freshness_checks"].append(fresh_result)
        if fresh_result["status"] == "FAIL":
            print(f"  ⚠️  FAIL: Data is stale ({fresh_result['hours_old']:.1f} hours old)")
        else:
            print(f"  ✅ PASS: Freshness check")

        # Row count check
        count_result = check_row_count(df, table_name, "bronze")
        bronze_results["row_count_checks"].append(count_result)
        if count_result["status"] == "FAIL":
            print(f"  ⚠️  FAIL: Low row count ({count_result['row_count']:,})")
        else:
            print(f"  ✅ PASS: Row count check ({count_result['row_count']:,} rows)")

    except Exception as e:
        print(f"  ❌ ERROR: {str(e)}")
        bronze_results["row_count_checks"].append(
            {"layer": "bronze", "table": table_name, "row_count": 0, "status": "ERROR", "message": str(e)}
        )

print("\n" + "=" * 70)
print("✅ Bronze layer checks complete")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔍 Run Quality Checks - Silver Layer

# COMMAND ----------
print("🔍 Running Silver Layer Quality Checks...")
print("=" * 70)

silver_results = {
    "null_checks": [],
    "duplicate_checks": [],
    "freshness_checks": [],
    "row_count_checks": [],
    "value_range_checks": [],
}

# Silver tables to check
silver_tables = {
    "customers.customer_dim": {
        "key_columns": ["customer_id"],
        "timestamp_column": "effective_start_date",
        "range_checks": {"age_years": (18, 120), "customer_tenure_months": (0, 600)},
    },
    "policies.policy_dim": {
        "key_columns": ["policy_id"],
        "timestamp_column": "policy_effective_date",
        "range_checks": {"annual_premium": (100, 100000), "coverage_amount": (1000, 10000000)},
    },
    "claims.claim_fact": {
        "key_columns": ["claim_id"],
        "timestamp_column": "reported_date",
        "range_checks": {"claimed_amount": (0, 5000000), "paid_amount": (0, 5000000)},
    },
}

for table_path, config in silver_tables.items():
    full_table = f"{silver_catalog}.{table_path}"
    table_name = table_path.split(".")[-1]

    try:
        print(f"\n📋 Checking: {full_table}")
        df = spark.table(full_table)

        # Null rate check
        null_results = check_null_rate(df, table_name, "silver")
        silver_results["null_checks"].append(null_results)
        failed_nulls = null_results[null_results["status"] == "FAIL"]
        if not failed_nulls.empty:
            print(f"  ⚠️  FAIL: High null rates in {len(failed_nulls)} columns")
        else:
            print(f"  ✅ PASS: Null rate checks")

        # Duplicate check
        dup_result = check_duplicate_rate(df, table_name, "silver", config["key_columns"])
        silver_results["duplicate_checks"].append(dup_result)
        if dup_result["status"] == "FAIL":
            print(f"  ⚠️  FAIL: High duplicate rate")
        else:
            print(f"  ✅ PASS: Duplicate check")

        # Freshness check
        fresh_result = check_freshness(df, table_name, "silver", config["timestamp_column"])
        silver_results["freshness_checks"].append(fresh_result)
        if fresh_result["status"] == "FAIL":
            print(f"  ⚠️  FAIL: Data is stale")
        else:
            print(f"  ✅ PASS: Freshness check")

        # Row count check
        count_result = check_row_count(df, table_name, "silver")
        silver_results["row_count_checks"].append(count_result)
        print(f"  {'✅' if count_result['status'] == 'PASS' else '⚠️ '} Row count: {count_result['row_count']:,}")

        # Value range checks
        if "range_checks" in config:
            range_results = check_value_ranges(df, table_name, "silver", config["range_checks"])
            if not range_results.empty:
                silver_results["value_range_checks"].append(range_results)
                failed_ranges = range_results[range_results["status"] == "FAIL"]
                if not failed_ranges.empty:
                    print(f"  ⚠️  FAIL: {len(failed_ranges)} columns have out-of-range values")
                else:
                    print(f"  ✅ PASS: Value range checks")

    except Exception as e:
        print(f"  ❌ ERROR: {str(e)}")

print("\n" + "=" * 70)
print("✅ Silver layer checks complete")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔍 Run Quality Checks - Gold Layer (Predictions)

# COMMAND ----------
print("🔍 Running Gold Layer Quality Checks...")
print("=" * 70)

gold_results = {
    "null_checks": [],
    "duplicate_checks": [],
    "freshness_checks": [],
    "row_count_checks": [],
    "value_range_checks": [],
}

# Gold prediction tables to check
gold_tables = {
    "predictions.customer_churn_risk": {
        "key_columns": ["customer_id"],
        "timestamp_column": "prediction_timestamp",
        "range_checks": {"churn_probability": (0, 1), "total_annual_premium": (0, 500000)},
    },
    "predictions.fraud_alerts": {
        "key_columns": ["claim_id"],
        "timestamp_column": "prediction_timestamp",
        "range_checks": {"combined_fraud_score": (0, 100), "estimated_fraud_amount": (0, 5000000)},
    },
    "predictions.claim_forecast": {
        "key_columns": ["forecast_date", "claim_type"],
        "timestamp_column": "prediction_timestamp",
        "range_checks": {"predicted_claim_count": (0, 10000), "predicted_total_amount": (0, 100000000)},
    },
    "predictions.premium_optimization": {
        "key_columns": ["policy_id"],
        "timestamp_column": "prediction_timestamp",
        "range_checks": {"annual_premium": (100, 100000), "recommended_premium": (100, 100000)},
    },
}

for table_path, config in gold_tables.items():
    full_table = f"{gold_catalog}.{table_path}"
    table_name = table_path.split(".")[-1]

    try:
        print(f"\n📋 Checking: {full_table}")
        df = spark.table(full_table)

        # Null rate check
        null_results = check_null_rate(df, table_name, "gold")
        gold_results["null_checks"].append(null_results)
        failed_nulls = null_results[null_results["status"] == "FAIL"]
        print(f"  {'⚠️ ' if not failed_nulls.empty else '✅'} Null checks: {len(failed_nulls)} failures")

        # Duplicate check
        dup_result = check_duplicate_rate(df, table_name, "gold", config["key_columns"])
        gold_results["duplicate_checks"].append(dup_result)
        print(f"  {'⚠️ ' if dup_result['status'] == 'FAIL' else '✅'} Duplicate check")

        # Freshness check (critical for predictions!)
        fresh_result = check_freshness(df, table_name, "gold", config["timestamp_column"])
        gold_results["freshness_checks"].append(fresh_result)
        if fresh_result["status"] == "FAIL":
            print(f"  🚨 CRITICAL: Predictions are stale! ({fresh_result['hours_old']:.1f} hours old)")
        else:
            print(f"  ✅ Freshness: {fresh_result['hours_old']:.1f} hours old")

        # Row count
        count_result = check_row_count(df, table_name, "gold")
        gold_results["row_count_checks"].append(count_result)
        print(f"  {'✅' if count_result['status'] == 'PASS' else '⚠️ '} Row count: {count_result['row_count']:,}")

        # Value range checks (important for ML predictions)
        if "range_checks" in config:
            range_results = check_value_ranges(df, table_name, "gold", config["range_checks"])
            if not range_results.empty:
                gold_results["value_range_checks"].append(range_results)
                failed_ranges = range_results[range_results["status"] == "FAIL"]
                if not failed_ranges.empty:
                    print(f"  🚨 CRITICAL: Invalid prediction values detected!")
                else:
                    print(f"  ✅ Value range checks passed")

    except Exception as e:
        print(f"  ❌ ERROR: {str(e)}")

print("\n" + "=" * 70)
print("✅ Gold layer checks complete")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 Generate Data Quality Report

# COMMAND ----------
# Compile all results (handle empty lists gracefully)
null_checks_list = bronze_results["null_checks"] + silver_results["null_checks"] + gold_results["null_checks"]
if null_checks_list:
    all_null_checks = pd.concat(null_checks_list, ignore_index=True)
else:
    all_null_checks = pd.DataFrame(columns=["layer", "table", "column", "null_count", "null_rate", "status", "message"])

duplicate_checks_list = (
    bronze_results["duplicate_checks"] + silver_results["duplicate_checks"] + gold_results["duplicate_checks"]
)
all_duplicate_checks = (
    pd.DataFrame(duplicate_checks_list)
    if duplicate_checks_list
    else pd.DataFrame(columns=["layer", "table", "total_rows", "duplicate_rows", "duplicate_rate", "status", "message"])
)

freshness_checks_list = (
    bronze_results["freshness_checks"] + silver_results["freshness_checks"] + gold_results["freshness_checks"]
)
all_freshness_checks = (
    pd.DataFrame(freshness_checks_list)
    if freshness_checks_list
    else pd.DataFrame(columns=["layer", "table", "latest_timestamp", "hours_old", "status", "message"])
)

row_count_checks_list = (
    bronze_results["row_count_checks"] + silver_results["row_count_checks"] + gold_results["row_count_checks"]
)
all_row_count_checks = (
    pd.DataFrame(row_count_checks_list)
    if row_count_checks_list
    else pd.DataFrame(columns=["layer", "table", "row_count", "min_expected", "status", "message"])
)

# Calculate summary statistics
total_checks = len(all_null_checks) + len(all_duplicate_checks) + len(all_freshness_checks) + len(all_row_count_checks)

# Check if any checks were run
if total_checks == 0:
    displayHTML(
        """
    <div style="background: #fff3cd; border: 2px solid #ffc107; padding: 30px; border-radius: 10px; margin: 20px 0;">
        <h2 style="color: #856404; margin: 0;">⚠️ No Tables Found</h2>
        <p style="font-size: 16px;">No tables were found in the {env} environment.</p>
        <p><strong>Possible Reasons:</strong></p>
        <ul>
            <li>Tables haven't been created yet (run setup notebooks first)</li>
            <li>Wrong environment selected</li>
            <li>Pipeline hasn't run yet</li>
        </ul>
        <p><strong>Next Steps:</strong></p>
        <ol>
            <li>Verify environment selection (check catalog dropdown above)</li>
            <li>Run bronze data generation notebooks</li>
            <li>Run silver transformation notebooks</li>
            <li>Run gold notebooks</li>
            <li>Run ML prediction notebooks</li>
            <li>Re-run this monitoring notebook</li>
        </ol>
    </div>
    """.replace(
            "{env}", env.upper()
        )
    )
    dbutils.notebook.exit("No tables found - nothing to monitor")

failed_checks = (
    len(all_null_checks[all_null_checks["status"] == "FAIL"])
    + len(all_duplicate_checks[all_duplicate_checks["status"] == "FAIL"])
    + len(all_freshness_checks[all_freshness_checks["status"] == "FAIL"])
    + len(all_row_count_checks[all_row_count_checks["status"] == "FAIL"])
)
passed_checks = total_checks - failed_checks
pass_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0

# Display summary
displayHTML(
    f"""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 15px; margin: 20px 0;">
    <h1 style="margin: 0;">📊 Data Quality Report</h1>
    <p style="font-size: 16px; opacity: 0.9;">Environment: {env.upper()} | Run Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
</div>

<div style="display: flex; gap: 20px; margin: 20px 0;">
    <div style="flex: 1; background: {'#90EE90' if pass_rate >= 90 else '#FFD700' if pass_rate >= 70 else '#FF6B6B'}; padding: 20px; border-radius: 10px;">
        <h2 style="margin: 0;">Overall Pass Rate</h2>
        <div style="font-size: 48px; font-weight: bold; margin: 10px 0;">{pass_rate:.1f}%</div>
        <p>{passed_checks} / {total_checks} checks passed</p>
    </div>
    <div style="flex: 1; background: #e3f2fd; padding: 20px; border-radius: 10px;">
        <h2 style="margin: 0;">Total Checks Run</h2>
        <div style="font-size: 48px; font-weight: bold; margin: 10px 0; color: #1976d2;">{total_checks}</div>
        <p>Across all layers</p>
    </div>
    <div style="flex: 1; background: {'#ffebee' if failed_checks > 0 else '#e8f5e9'}; padding: 20px; border-radius: 10px;">
        <h2 style="margin: 0;">{'⚠️ ' if failed_checks > 0 else '✅ '}Failed Checks</h2>
        <div style="font-size: 48px; font-weight: bold; margin: 10px 0; color: {'#c62828' if failed_checks > 0 else '#2e7d32'};">{failed_checks}</div>
        <p>{'Needs attention!' if failed_checks > 0 else 'All passing!'}</p>
    </div>
</div>
"""
)

print("\n📊 DETAILED RESULTS:\n")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📋 Failed Checks Summary

# COMMAND ----------
print("🚨 FAILED CHECKS - IMMEDIATE ATTENTION REQUIRED:")
print("=" * 70)

# Collect all failures
failures = []

# Null rate failures
failed_nulls = all_null_checks[all_null_checks["status"] == "FAIL"]
if not failed_nulls.empty:
    print(f"\n⚠️  HIGH NULL RATES ({len(failed_nulls)} columns):")
    for _, row in failed_nulls.iterrows():
        print(f"  • {row['layer']}.{row['table']}.{row['column']}: {row['null_rate']:.2%} nulls")
        failures.append(f"{row['layer']}.{row['table']}.{row['column']}: {row['null_rate']:.2%} nulls")

# Duplicate failures
failed_dups = all_duplicate_checks[all_duplicate_checks["status"] == "FAIL"]
if not failed_dups.empty:
    print(f"\n⚠️  HIGH DUPLICATE RATES ({len(failed_dups)} tables):")
    for _, row in failed_dups.iterrows():
        print(
            f"  • {row['layer']}.{row['table']}: {row['duplicate_rate']:.2%} duplicates ({row['duplicate_rows']:,} rows)"
        )
        failures.append(f"{row['layer']}.{row['table']}: {row['duplicate_rate']:.2%} duplicates")

# Freshness failures
failed_fresh = all_freshness_checks[all_freshness_checks["status"] == "FAIL"]
if not failed_fresh.empty:
    print(f"\n🚨 STALE DATA ({len(failed_fresh)} tables):")
    for _, row in failed_fresh.iterrows():
        print(
            f"  • {row['layer']}.{row['table']}: {row['hours_old']:.1f} hours old (threshold: {THRESHOLDS['freshness_hours']} hours)"
        )
        failures.append(f"{row['layer']}.{row['table']}: {row['hours_old']:.1f} hours stale")

# Row count failures
failed_counts = all_row_count_checks[all_row_count_checks["status"] == "FAIL"]
if not failed_counts.empty:
    print(f"\n⚠️  LOW ROW COUNTS ({len(failed_counts)} tables):")
    for _, row in failed_counts.iterrows():
        print(f"  • {row['layer']}.{row['table']}: {row['row_count']:,} rows (min expected: {row['min_expected']:,})")
        failures.append(f"{row['layer']}.{row['table']}: Only {row['row_count']:,} rows")

if not failures:
    print("\n✅ NO FAILURES - ALL QUALITY CHECKS PASSED!")

print("\n" + "=" * 70)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📈 Quality Trends Visualization

# COMMAND ----------
# Create visualizations
if not all_null_checks.empty:
    # Null rate by layer
    null_by_layer = all_null_checks.groupby("layer")["null_rate"].mean().reset_index()
    fig1 = px.bar(
        null_by_layer,
        x="layer",
        y="null_rate",
        title="Average Null Rate by Layer",
        labels={"null_rate": "Null Rate", "layer": "Layer"},
        color="null_rate",
        color_continuous_scale="Reds",
    )
    fig1.update_layout(height=400)
    fig1.show()

# Row counts by layer
if not all_row_count_checks.empty:
    fig2 = px.bar(
        all_row_count_checks,
        x="table",
        y="row_count",
        color="layer",
        title="Row Counts by Table",
        labels={"row_count": "Row Count", "table": "Table"},
        barmode="group",
    )
    fig2.update_layout(height=400, xaxis_tickangle=-45)
    fig2.show()

# Freshness by table
if not all_freshness_checks[all_freshness_checks["hours_old"].notna()].empty:
    fresh_df = all_freshness_checks[all_freshness_checks["hours_old"].notna()].copy()
    fig3 = px.bar(
        fresh_df,
        x="table",
        y="hours_old",
        color="layer",
        title="Data Freshness (Hours Old)",
        labels={"hours_old": "Hours Since Last Update", "table": "Table"},
    )
    fig3.add_hline(
        y=THRESHOLDS["freshness_hours"], line_dash="dash", annotation_text="Freshness Threshold", line_color="red"
    )
    fig3.update_layout(height=400, xaxis_tickangle=-45)
    fig3.show()

print("✅ Visualizations generated")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🚨 Alert Generation


# COMMAND ----------
def generate_alert_message(failures, pass_rate):
    """Generate alert message if quality thresholds breached"""

    if not failures:
        return None

    severity = "🚨 CRITICAL" if pass_rate < 70 else "⚠️  WARNING"

    alert_html = f"""
    <div style="background: {'#ffebee' if pass_rate < 70 else '#fff3e0'}; border-left: 5px solid {'#c62828' if pass_rate < 70 else '#f57c00'}; padding: 20px; margin: 20px 0; border-radius: 5px;">
        <h2 style="margin: 0 0 10px 0;">{severity}: Data Quality Issues Detected</h2>
        <p><strong>Environment:</strong> {env.upper()}</p>
        <p><strong>Overall Pass Rate:</strong> {pass_rate:.1f}% ({len(failures)} failures)</p>
        <p><strong>Time:</strong> {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
        
        <h3>Failed Checks:</h3>
        <ul>
            {"".join([f"<li>{failure}</li>" for failure in failures[:10]])}
            {f"<li><em>...and {len(failures)-10} more</em></li>" if len(failures) > 10 else ""}
        </ul>
        
        <p><strong>Action Required:</strong></p>
        <ul>
            <li>Review failed checks above</li>
            <li>Investigate root causes</li>
            <li>Re-run ML predictions if gold layer affected</li>
            <li>Contact data engineering team if issues persist</li>
        </ul>
    </div>
    """

    return alert_html


# Generate alert if needed
alert = generate_alert_message(failures, pass_rate)

if alert:
    displayHTML(alert)

    # TODO: Send alert via email/Slack
    print("\n🚨 ALERT GENERATED!")
    print("📧 TODO: Configure email/Slack alerts to notify team")
    print(f"   Failures: {len(failures)}")
    print(f"   Pass Rate: {pass_rate:.1f}%")
else:
    displayHTML(
        """
    <div style="background: #e8f5e9; border-left: 5px solid #2e7d32; padding: 20px; margin: 20px 0; border-radius: 5px;">
        <h2 style="margin: 0; color: #2e7d32;">✅ All Quality Checks Passed!</h2>
        <p>No data quality issues detected. All systems operating normally.</p>
    </div>
    """
    )
    print("\n✅ No alerts needed - all checks passed!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 💾 Save Quality Metrics (for trending)

# COMMAND ----------
# Create quality metrics table if it doesn't exist
metrics_table = f"{gold_catalog}.data_quality.quality_metrics_history"

try:
    spark.sql(
        f"""
    CREATE TABLE IF NOT EXISTS {metrics_table} (
        check_timestamp TIMESTAMP,
        environment STRING,
        layer STRING,
        table_name STRING,
        check_type STRING,
        metric_name STRING,
        metric_value DOUBLE,
        status STRING,
        message STRING
    )
    PARTITIONED BY (DATE(check_timestamp))
    """
    )

    print(f"✅ Quality metrics table ready: {metrics_table}")
except Exception as e:
    print(f"⚠️  Could not create metrics table: {e}")
    print("   Skipping metrics persistence")
    metrics_table = None

# Save current run metrics
if metrics_table:
    try:
        # Prepare metrics for storage
        timestamp = current_timestamp()
        metrics_data = []

        # Null rate metrics
        for _, row in all_null_checks.iterrows():
            metrics_data.append(
                (
                    timestamp,
                    env,
                    row["layer"],
                    row["table"],
                    "null_rate",
                    row["column"],
                    float(row["null_rate"]),
                    row["status"],
                    row["message"],
                )
            )

        # Duplicate rate metrics
        for _, row in all_duplicate_checks.iterrows():
            metrics_data.append(
                (
                    timestamp,
                    env,
                    row["layer"],
                    row["table"],
                    "duplicate_rate",
                    "overall",
                    float(row["duplicate_rate"]),
                    row["status"],
                    row["message"],
                )
            )

        # Freshness metrics
        for _, row in all_freshness_checks[all_freshness_checks["hours_old"].notna()].iterrows():
            metrics_data.append(
                (
                    timestamp,
                    env,
                    row["layer"],
                    row["table"],
                    "freshness",
                    "hours_old",
                    float(row["hours_old"]),
                    row["status"],
                    row["message"],
                )
            )

        # Row count metrics
        for _, row in all_row_count_checks.iterrows():
            metrics_data.append(
                (
                    timestamp,
                    env,
                    row["layer"],
                    row["table"],
                    "row_count",
                    "count",
                    float(row["row_count"]),
                    row["status"],
                    row["message"],
                )
            )

        # Create DataFrame and write
        if metrics_data:
            columns = [
                "check_timestamp",
                "environment",
                "layer",
                "table_name",
                "check_type",
                "metric_name",
                "metric_value",
                "status",
                "message",
            ]
            metrics_df = spark.createDataFrame(metrics_data, columns)

            metrics_df.write.mode("append").saveAsTable(metrics_table)

            print(f"✅ Saved {len(metrics_data)} metrics to history")

    except Exception as e:
        print(f"⚠️  Could not save metrics: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📝 Summary & Recommendations

# COMMAND ----------
displayHTML(
    f"""
<div style="background: #f5f5f5; padding: 30px; border-radius: 10px; margin: 20px 0;">
    <h2 style="color: #333;">📋 Data Quality Summary</h2>
    
    <h3>Overall Health: {'🟢 GOOD' if pass_rate >= 90 else '🟡 FAIR' if pass_rate >= 70 else '🔴 POOR'}</h3>
    <ul>
        <li><strong>Total Checks:</strong> {total_checks}</li>
        <li><strong>Passed:</strong> {passed_checks} ({pass_rate:.1f}%)</li>
        <li><strong>Failed:</strong> {failed_checks}</li>
    </ul>
    
    <h3>Layer-Level Summary:</h3>
    <ul>
        <li><strong>Bronze Layer:</strong> Raw data ingestion
            <ul>
                <li>{len(bronze_tables)} tables checked</li>
                <li>Focus: Null rates, duplicates, freshness</li>
            </ul>
        </li>
        <li><strong>Silver Layer:</strong> Cleaned & standardized
            <ul>
                <li>{len(silver_tables)} tables checked</li>
                <li>Focus: Data quality, value ranges, SCD Type 2</li>
            </ul>
        </li>
        <li><strong>Gold Layer:</strong> ML predictions
            <ul>
                <li>{len(gold_tables)} tables checked</li>
                <li>Focus: Prediction validity, freshness (critical!)</li>
            </ul>
        </li>
    </ul>
    
    <h3>📌 Recommendations:</h3>
    <ul>
        <li><strong>Schedule:</strong> Run this notebook daily (automated)</li>
        <li><strong>Alerts:</strong> Configure email/Slack notifications for failures</li>
        <li><strong>Thresholds:</strong> Adjust based on your business requirements</li>
        <li><strong>Trending:</strong> Review historical metrics weekly</li>
        <li><strong>Action:</strong> Investigate and fix any failed checks immediately</li>
    </ul>
    
    <h3>🔗 Next Steps:</h3>
    <ol>
        <li>Review any failed checks above</li>
        <li>Fix data quality issues at source</li>
        <li>Re-run affected ML predictions if needed</li>
        <li>Schedule this notebook to run daily</li>
        <li>Set up alerting for critical failures</li>
    </ol>
</div>
"""
)

print("\n" + "=" * 70)
print("✅ DATA QUALITY MONITORING COMPLETE")
print("=" * 70)
print(f"\n📊 Pass Rate: {pass_rate:.1f}%")
print(f"✅ Passed: {passed_checks}/{total_checks}")
print(f"{'⚠️ ' if failed_checks > 0 else '✅ '}Failed: {failed_checks}/{total_checks}")
print(f"\n⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📚 Tips & Best Practices
# MAGIC
# MAGIC **Running the Monitoring:**
# MAGIC - Run daily (automated via job scheduling)
# MAGIC - Run after each data pipeline execution
# MAGIC - Run before critical business reports
# MAGIC
# MAGIC **Interpreting Results:**
# MAGIC - Pass rate ≥ 90% = Good health
# MAGIC - Pass rate 70-90% = Needs attention
# MAGIC - Pass rate < 70% = Critical issues
# MAGIC
# MAGIC **Taking Action:**
# MAGIC - Failed null checks → Investigate source data
# MAGIC - Failed duplicate checks → Check deduplication logic
# MAGIC - Failed freshness checks → Check pipeline scheduling
# MAGIC - Failed range checks → Validate business rules
# MAGIC
# MAGIC **Customization:**
# MAGIC - Adjust thresholds in THRESHOLDS dictionary
# MAGIC - Add custom checks for your specific needs
# MAGIC - Modify range checks for your business rules
# MAGIC - Add more tables as your platform grows
# MAGIC
# MAGIC **Questions? Review the detailed logs above!** 📊
