# Databricks notebook source
# MAGIC %md
# MAGIC # 💰 Cost Optimization Analysis Dashboard
# MAGIC
# MAGIC **Comprehensive Cost Tracking and Optimization**
# MAGIC
# MAGIC This notebook provides:
# MAGIC - **Compute Cost Analysis:** Cluster usage, idle time, cost per job
# MAGIC - **Storage Cost Analysis:** Data volume, growth trends, optimization opportunities
# MAGIC - **Job Performance Metrics:** Runtime, cost efficiency, bottlenecks
# MAGIC - **Optimization Recommendations:** Automated cost-saving suggestions
# MAGIC
# MAGIC **Run Schedule:** Weekly (recommended)

# COMMAND ----------
# MAGIC %md
# MAGIC ## ⚙️ Configuration

import json
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from IPython.display import HTML, display
from plotly.subplots import make_subplots

# COMMAND ----------
# Import libraries
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_timestamp,
)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum

# Configuration
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "📊 Environment")
dbutils.widgets.text("days_lookback", "30", "📅 Days to Analyze")

ENVIRONMENT = dbutils.widgets.get("environment")
DAYS_LOOKBACK = int(dbutils.widgets.get("days_lookback"))

catalog_prefix = f"insurance_{ENVIRONMENT}"
bronze_catalog = f"{catalog_prefix}_bronze"
silver_catalog = f"{catalog_prefix}_silver"
gold_catalog = f"{catalog_prefix}_gold"

print(f"✅ Environment: {ENVIRONMENT.upper()}")
print(f"📅 Analysis Period: Last {DAYS_LOOKBACK} days")
print(f"📊 Catalogs: {bronze_catalog}, {silver_catalog}, {gold_catalog}")

# Cost constants (adjust based on your Databricks pricing)
COST_PER_DBU = 0.55  # Standard pricing (adjust for your region/contract)
COST_PER_GB_STORAGE_MONTH = 0.023  # Delta Lake storage cost per GB per month

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 Storage Cost Analysis


# COMMAND ----------
def get_table_storage_cost(catalog, schema):
    """Calculate storage cost for all tables in a schema"""
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").toPandas()
        storage_data = []

        for table_name in tables["tableName"]:
            try:
                full_table = f"{catalog}.{schema}.{table_name}"

                # Get table details
                details = spark.sql(f"DESCRIBE DETAIL {full_table}").collect()[0]

                # Extract metrics
                size_bytes = details["sizeInBytes"] if "sizeInBytes" in details.asDict() else 0
                num_files = details["numFiles"] if "numFiles" in details.asDict() else 0

                # Convert to GB
                size_gb = size_bytes / (1024**3)

                # Calculate monthly cost
                monthly_cost = size_gb * COST_PER_GB_STORAGE_MONTH

                storage_data.append(
                    {
                        "catalog": catalog,
                        "schema": schema,
                        "table": table_name,
                        "size_gb": round(size_gb, 2),
                        "num_files": num_files,
                        "monthly_storage_cost": round(monthly_cost, 2),
                    }
                )
            except Exception as e:
                print(f"Warning: Could not analyze {table_name}: {str(e)}")

        return pd.DataFrame(storage_data)
    except Exception as e:
        print(f"Error accessing {catalog}.{schema}: {str(e)}")
        return pd.DataFrame()


# Collect storage data for all layers
print("🔍 Analyzing storage costs across all layers...")

all_storage_data = []

# Bronze layer
for schema in ["customers"]:
    df = get_table_storage_cost(bronze_catalog, schema)
    if not df.empty:
        all_storage_data.append(df)

# Silver layer
for schema in ["customers", "policies", "claims", "agents", "payments"]:
    df = get_table_storage_cost(silver_catalog, schema)
    if not df.empty:
        all_storage_data.append(df)

# Gold layer
for schema in ["analytics", "predictions"]:
    df = get_table_storage_cost(gold_catalog, schema)
    if not df.empty:
        all_storage_data.append(df)

# Combine all storage data
if all_storage_data:
    df_storage = pd.concat(all_storage_data, ignore_index=True)

    # Add layer column
    df_storage["layer"] = df_storage["catalog"].apply(
        lambda x: "Bronze" if "bronze" in x else "Silver" if "silver" in x else "Gold"
    )

    print(f"✅ Analyzed {len(df_storage)} tables across all layers")
else:
    df_storage = pd.DataFrame()
    print("⚠️ No storage data available")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 💻 Compute Cost Analysis

# COMMAND ----------
# Get Databricks API credentials
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_url = ctx.apiUrl().getOrElse(None)
api_token = ctx.apiToken().getOrElse(None)

import requests


def get_cluster_usage(days=30):
    """Fetch cluster usage from Databricks API"""
    try:
        headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}

        # Get all clusters
        clusters_url = f"{api_url}/api/2.0/clusters/list"
        response = requests.get(clusters_url, headers=headers)
        clusters = response.json().get("clusters", [])

        cluster_costs = []
        for cluster in clusters:
            cluster_id = cluster.get("cluster_id")
            cluster_name = cluster.get("cluster_name")

            # Get cluster events to calculate usage
            events_url = f"{api_url}/api/2.0/clusters/events"
            params = {"cluster_id": cluster_id, "limit": 100}
            events_response = requests.post(events_url, headers=headers, json=params)
            events = events_response.json().get("events", [])

            # Calculate uptime from events
            total_uptime_hours = 0
            start_time = None

            for event in sorted(events, key=lambda x: x.get("timestamp", 0)):
                event_type = event.get("type")
                timestamp = event.get("timestamp", 0) / 1000  # Convert to seconds

                if event_type in ["CREATING", "STARTING", "RUNNING"]:
                    if start_time is None:
                        start_time = timestamp
                elif event_type in ["TERMINATING", "TERMINATED"]:
                    if start_time:
                        uptime = (timestamp - start_time) / 3600  # Convert to hours
                        total_uptime_hours += uptime
                        start_time = None

            # Estimate DBUs (simplified: assuming 1 DBU per hour per node)
            num_workers = cluster.get("num_workers", 1)
            driver_node_type = cluster.get("driver_node_type_id", "Standard")

            # Estimate DBUs per hour based on instance type
            dbus_per_hour = num_workers + 1  # Workers + driver (simplified)

            total_dbus = total_uptime_hours * dbus_per_hour
            total_cost = total_dbus * COST_PER_DBU

            cluster_costs.append(
                {
                    "cluster_id": cluster_id,
                    "cluster_name": cluster_name,
                    "num_workers": num_workers,
                    "uptime_hours": round(total_uptime_hours, 2),
                    "total_dbus": round(total_dbus, 2),
                    "estimated_cost": round(total_cost, 2),
                    "driver_type": driver_node_type,
                }
            )

        return pd.DataFrame(cluster_costs)
    except Exception as e:
        print(f"Error fetching cluster usage: {str(e)}")
        return pd.DataFrame()


print("🔍 Analyzing compute costs...")
df_compute = get_cluster_usage(DAYS_LOOKBACK)

if not df_compute.empty:
    print(f"✅ Analyzed {len(df_compute)} clusters")
else:
    print("⚠️ No compute data available")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📈 Job Performance Analysis


# COMMAND ----------
def get_job_performance(days=30):
    """Analyze job performance and costs"""
    try:
        headers = {"Authorization": f"Bearer {api_token}", "Content-Type": "application/json"}

        # Get recent job runs
        runs_url = f"{api_url}/api/2.1/jobs/runs/list"
        params = {"limit": 100, "expand_tasks": False}
        response = requests.get(runs_url, headers=headers, params=params)
        runs = response.json().get("runs", [])

        job_performance = []
        for run in runs:
            run_id = run.get("run_id")
            job_name = run.get("run_name", "Unknown")

            # Get run details
            start_time = run.get("start_time", 0) / 1000
            end_time = run.get("end_time", 0) / 1000 if run.get("end_time") else None

            if end_time:
                runtime_seconds = end_time - start_time
                runtime_hours = runtime_seconds / 3600

                # Estimate cost (simplified)
                cluster_spec = run.get("cluster_spec", {})
                num_workers = cluster_spec.get("num_workers", 1)
                dbus_used = runtime_hours * (num_workers + 1)
                estimated_cost = dbus_used * COST_PER_DBU

                state = run.get("state", {}).get("result_state", "UNKNOWN")

                job_performance.append(
                    {
                        "run_id": run_id,
                        "job_name": job_name,
                        "runtime_minutes": round(runtime_seconds / 60, 2),
                        "runtime_hours": round(runtime_hours, 2),
                        "dbus_used": round(dbus_used, 2),
                        "estimated_cost": round(estimated_cost, 2),
                        "status": state,
                        "start_time": datetime.fromtimestamp(start_time).strftime("%Y-%m-%d %H:%M"),
                    }
                )

        return pd.DataFrame(job_performance)
    except Exception as e:
        print(f"Error fetching job performance: {str(e)}")
        return pd.DataFrame()


print("🔍 Analyzing job performance...")
df_jobs = get_job_performance(DAYS_LOOKBACK)

if not df_jobs.empty:
    print(f"✅ Analyzed {len(df_jobs)} job runs")
else:
    print("⚠️ No job data available")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 Cost Summary Dashboard

# COMMAND ----------
print("\n" + "=" * 80)
print("💰 COST OPTIMIZATION ANALYSIS - SUMMARY")
print("=" * 80 + "\n")

# Storage costs
if not df_storage.empty:
    total_storage_gb = df_storage["size_gb"].sum()
    total_storage_cost = df_storage["monthly_storage_cost"].sum()

    print("📦 STORAGE COSTS:")
    print(f"  • Total Data Size: {total_storage_gb:,.2f} GB")
    print(f"  • Monthly Storage Cost: ${total_storage_cost:,.2f}")
    print(f"  • Annual Storage Cost: ${total_storage_cost * 12:,.2f}")

    # Breakdown by layer
    print("\n  Breakdown by Layer:")
    for layer in ["Bronze", "Silver", "Gold"]:
        layer_data = df_storage[df_storage["layer"] == layer]
        if not layer_data.empty:
            layer_size = layer_data["size_gb"].sum()
            layer_cost = layer_data["monthly_storage_cost"].sum()
            print(f"    - {layer}: {layer_size:,.2f} GB → ${layer_cost:,.2f}/month")

# Compute costs
if not df_compute.empty:
    total_compute_cost = df_compute["estimated_cost"].sum()
    total_uptime = df_compute["uptime_hours"].sum()

    print(f"\n💻 COMPUTE COSTS ({DAYS_LOOKBACK} days):")
    print(f"  • Total Cluster Uptime: {total_uptime:,.2f} hours")
    print(f"  • Total DBUs Consumed: {df_compute['total_dbus'].sum():,.2f}")
    print(f"  • Total Compute Cost: ${total_compute_cost:,.2f}")
    print(f"  • Projected Monthly Cost: ${total_compute_cost * (30/DAYS_LOOKBACK):,.2f}")

# Job costs
if not df_jobs.empty:
    total_job_cost = df_jobs["estimated_cost"].sum()
    successful_jobs = len(df_jobs[df_jobs["status"] == "SUCCESS"])
    failed_jobs = len(df_jobs[df_jobs["status"] == "FAILED"])

    print(f"\n🔄 JOB PERFORMANCE ({DAYS_LOOKBACK} days):")
    print(f"  • Total Jobs Analyzed: {len(df_jobs)}")
    print(f"  • Successful: {successful_jobs} | Failed: {failed_jobs}")
    print(f"  • Total Job Runtime: {df_jobs['runtime_hours'].sum():,.2f} hours")
    print(f"  • Total Job Cost: ${total_job_cost:,.2f}")
    print(f"  • Avg Cost per Job: ${df_jobs['estimated_cost'].mean():,.2f}")

# Total costs
print("\n" + "=" * 80)
if not df_storage.empty and not df_compute.empty:
    monthly_total = total_storage_cost + (total_compute_cost * (30 / DAYS_LOOKBACK))
    annual_total = monthly_total * 12

    print(f"💰 TOTAL ESTIMATED COSTS:")
    print(f"  • Monthly: ${monthly_total:,.2f}")
    print(f"  • Annual: ${annual_total:,.2f}")
print("=" * 80 + "\n")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📈 Visualizations

# COMMAND ----------
# Storage cost visualization
if not df_storage.empty:
    # Top 10 most expensive tables
    top_tables = df_storage.nlargest(10, "monthly_storage_cost")

    fig1 = px.bar(
        top_tables,
        x="table",
        y="monthly_storage_cost",
        color="layer",
        title="Top 10 Most Expensive Tables (Monthly Storage Cost)",
        labels={"monthly_storage_cost": "Monthly Cost ($)", "table": "Table Name"},
        color_discrete_map={"Bronze": "#FF6B6B", "Silver": "#4ECDC4", "Gold": "#FFD93D"},
    )
    fig1.update_layout(xaxis_tickangle=-45, height=500)
    fig1.show()

    # Storage distribution by layer
    layer_summary = df_storage.groupby("layer").agg({"size_gb": "sum", "monthly_storage_cost": "sum"}).reset_index()

    fig2 = go.Figure()
    fig2.add_trace(
        go.Bar(
            name="Storage Size (GB)",
            x=layer_summary["layer"],
            y=layer_summary["size_gb"],
            yaxis="y",
            marker_color="#4ECDC4",
        )
    )
    fig2.add_trace(
        go.Bar(
            name="Monthly Cost ($)",
            x=layer_summary["layer"],
            y=layer_summary["monthly_storage_cost"],
            yaxis="y2",
            marker_color="#FF6B6B",
        )
    )

    fig2.update_layout(
        title="Storage Size vs Cost by Layer",
        xaxis=dict(title="Layer"),
        yaxis=dict(title="Storage Size (GB)", side="left"),
        yaxis2=dict(title="Monthly Cost ($)", overlaying="y", side="right"),
        barmode="group",
        height=500,
    )
    fig2.show()

# COMMAND ----------
# Compute cost visualization
if not df_compute.empty:
    # Cluster cost breakdown
    fig3 = px.bar(
        df_compute.sort_values("estimated_cost", ascending=True),
        y="cluster_name",
        x="estimated_cost",
        orientation="h",
        title=f"Compute Cost by Cluster (Last {DAYS_LOOKBACK} days)",
        labels={"estimated_cost": "Cost ($)", "cluster_name": "Cluster"},
        color="estimated_cost",
        color_continuous_scale="Reds",
    )
    fig3.update_layout(height=400)
    fig3.show()

    # Cluster efficiency
    df_compute["cost_per_hour"] = df_compute["estimated_cost"] / df_compute["uptime_hours"]

    fig4 = px.scatter(
        df_compute,
        x="uptime_hours",
        y="estimated_cost",
        size="total_dbus",
        color="num_workers",
        hover_data=["cluster_name"],
        title="Cluster Efficiency: Uptime vs Cost",
        labels={"uptime_hours": "Uptime (hours)", "estimated_cost": "Total Cost ($)"},
        color_continuous_scale="Viridis",
    )
    fig4.show()

# COMMAND ----------
# Job performance visualization
if not df_jobs.empty:
    # Top 10 most expensive jobs
    top_jobs = df_jobs.nlargest(10, "estimated_cost")

    fig5 = px.bar(
        top_jobs,
        x="job_name",
        y="estimated_cost",
        color="status",
        title="Top 10 Most Expensive Jobs",
        labels={"estimated_cost": "Cost ($)", "job_name": "Job Name"},
        color_discrete_map={"SUCCESS": "#00C851", "FAILED": "#ff4444", "TIMEOUT": "#ffaa00"},
    )
    fig5.update_layout(xaxis_tickangle=-45, height=500)
    fig5.show()

    # Job runtime distribution
    fig6 = px.box(
        df_jobs,
        y="runtime_minutes",
        x="status",
        color="status",
        title="Job Runtime Distribution by Status",
        labels={"runtime_minutes": "Runtime (minutes)", "status": "Job Status"},
        color_discrete_map={"SUCCESS": "#00C851", "FAILED": "#ff4444", "TIMEOUT": "#ffaa00"},
    )
    fig6.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 💡 Optimization Recommendations

# COMMAND ----------
print("\n" + "=" * 80)
print("💡 COST OPTIMIZATION RECOMMENDATIONS")
print("=" * 80 + "\n")

recommendations = []

# Storage optimizations
if not df_storage.empty:
    # Check for small files
    small_file_tables = df_storage[df_storage["num_files"] > 1000]
    if not small_file_tables.empty:
        recommendations.append(
            {
                "category": "Storage",
                "priority": "High",
                "issue": "Small File Problem",
                "tables_affected": len(small_file_tables),
                "potential_savings": f"${small_file_tables['monthly_storage_cost'].sum() * 0.2:,.2f}/month",
                "action": "Run OPTIMIZE and Z-ORDER on affected tables",
                "command": f"OPTIMIZE {small_file_tables.iloc[0]['catalog']}.{small_file_tables.iloc[0]['schema']}.{small_file_tables.iloc[0]['table']} ZORDER BY (primary_key)",
            }
        )

    # Check for large tables without partitioning
    large_tables = df_storage[df_storage["size_gb"] > 100]
    if not large_tables.empty:
        recommendations.append(
            {
                "category": "Storage",
                "priority": "Medium",
                "issue": "Large Unpartitioned Tables",
                "tables_affected": len(large_tables),
                "potential_savings": f"${large_tables['monthly_storage_cost'].sum() * 0.15:,.2f}/month",
                "action": "Implement table partitioning by date or key columns",
                "command": "ALTER TABLE <table> ADD PARTITION ...",
            }
        )

# Compute optimizations
if not df_compute.empty:
    # Check for idle clusters
    idle_clusters = df_compute[df_compute["uptime_hours"] < 1]
    if not idle_clusters.empty:
        recommendations.append(
            {
                "category": "Compute",
                "priority": "High",
                "issue": "Idle/Underutilized Clusters",
                "tables_affected": len(idle_clusters),
                "potential_savings": f"${idle_clusters['estimated_cost'].sum():,.2f} (one-time)",
                "action": "Enable auto-termination and reduce cluster idle time",
                "command": "Set auto-termination to 10-15 minutes in cluster config",
            }
        )

    # Check for oversized clusters
    expensive_clusters = df_compute[df_compute["estimated_cost"] > df_compute["estimated_cost"].quantile(0.75)]
    if not expensive_clusters.empty:
        recommendations.append(
            {
                "category": "Compute",
                "priority": "Medium",
                "issue": "Potentially Oversized Clusters",
                "tables_affected": len(expensive_clusters),
                "potential_savings": f"${expensive_clusters['estimated_cost'].sum() * 0.3:,.2f}/month",
                "action": "Right-size clusters based on workload requirements",
                "command": "Reduce worker count or switch to smaller instance types",
            }
        )

# Job optimizations
if not df_jobs.empty:
    # Check for failed jobs
    failed = df_jobs[df_jobs["status"] == "FAILED"]
    if not failed.empty:
        wasted_cost = failed["estimated_cost"].sum()
        recommendations.append(
            {
                "category": "Jobs",
                "priority": "High",
                "issue": "Failed Jobs Wasting Resources",
                "tables_affected": len(failed),
                "potential_savings": f"${wasted_cost:,.2f} (wasted)",
                "action": "Fix failing jobs and implement retry logic",
                "command": "Review job logs and add error handling",
            }
        )

    # Check for long-running jobs
    long_jobs = df_jobs[df_jobs["runtime_hours"] > df_jobs["runtime_hours"].quantile(0.9)]
    if not long_jobs.empty:
        recommendations.append(
            {
                "category": "Jobs",
                "priority": "Medium",
                "issue": "Long-Running Jobs",
                "tables_affected": len(long_jobs),
                "potential_savings": f"${long_jobs['estimated_cost'].sum() * 0.25:,.2f}/month",
                "action": "Optimize query performance and enable caching",
                "command": "Use CACHE TABLE, broadcast joins, and predicate pushdown",
            }
        )

# Display recommendations
if recommendations:
    df_recommendations = pd.DataFrame(recommendations)

    print("📋 ACTIONABLE RECOMMENDATIONS:\n")
    for idx, rec in enumerate(recommendations, 1):
        print(f"{idx}. [{rec['priority']} Priority] {rec['category']}: {rec['issue']}")
        print(f"   • Tables/Jobs Affected: {rec['tables_affected']}")
        print(f"   • Potential Savings: {rec['potential_savings']}")
        print(f"   • Action: {rec['action']}")
        print(f"   • Command: {rec['command']}")
        print()

    # Summary of potential savings
    print("=" * 80)
    print("💰 TOTAL POTENTIAL SAVINGS:")

    # Extract numeric savings (simplified)
    total_potential = sum(
        [float(rec["potential_savings"].replace("$", "").replace(",", "").split("/")[0]) for rec in recommendations]
    )
    print(f"  • Estimated Monthly Savings: ${total_potential:,.2f}")
    print(f"  • Estimated Annual Savings: ${total_potential * 12:,.2f}")
    print("=" * 80)
else:
    print("✅ No major optimization opportunities found. Current setup is well-optimized!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📝 Export Cost Report

# COMMAND ----------
# Create comprehensive cost report
cost_report = {
    "report_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "environment": ENVIRONMENT,
    "analysis_period_days": DAYS_LOOKBACK,
    "storage_summary": {
        "total_size_gb": df_storage["size_gb"].sum() if not df_storage.empty else 0,
        "monthly_cost": df_storage["monthly_storage_cost"].sum() if not df_storage.empty else 0,
        "table_count": len(df_storage) if not df_storage.empty else 0,
    },
    "compute_summary": {
        "total_uptime_hours": df_compute["uptime_hours"].sum() if not df_compute.empty else 0,
        "total_cost": df_compute["estimated_cost"].sum() if not df_compute.empty else 0,
        "cluster_count": len(df_compute) if not df_compute.empty else 0,
    },
    "job_summary": {
        "total_runs": len(df_jobs) if not df_jobs.empty else 0,
        "successful_runs": len(df_jobs[df_jobs["status"] == "SUCCESS"]) if not df_jobs.empty else 0,
        "total_cost": df_jobs["estimated_cost"].sum() if not df_jobs.empty else 0,
    },
    "recommendations": recommendations,
}

# Save as JSON
report_json = json.dumps(cost_report, indent=2)

print("✅ Cost Report Generated")
print("\nTo save the report:")
report_date = datetime.now().strftime("%Y%m%d")
print(f"# dbutils.fs.put('/tmp/cost_report_{ENVIRONMENT}_{report_date}.json', report_json, True)")

# Display sample
print("\n📄 Report Preview:")
print(report_json[:500] + "...")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🎯 Summary & Next Steps
# MAGIC
# MAGIC **This analysis provides:**
# MAGIC - ✅ Complete storage cost breakdown across all layers
# MAGIC - ✅ Compute cost analysis by cluster
# MAGIC - ✅ Job performance and cost metrics
# MAGIC - ✅ Actionable optimization recommendations
# MAGIC - ✅ Estimated cost savings opportunities
# MAGIC
# MAGIC **Recommended Actions:**
# MAGIC 1. Review and implement high-priority recommendations
# MAGIC 2. Schedule this notebook to run weekly
# MAGIC 3. Track cost trends over time
# MAGIC 4. Set up alerts for cost anomalies
# MAGIC 5. Regularly optimize Delta tables (OPTIMIZE, VACUUM)
# MAGIC
# MAGIC **Next Run:** Schedule for next week to track improvements
