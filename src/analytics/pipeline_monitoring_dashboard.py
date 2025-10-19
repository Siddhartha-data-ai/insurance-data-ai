# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Pipeline Monitoring Dashboard
# MAGIC
# MAGIC **Real-Time Job Execution Monitoring**
# MAGIC
# MAGIC This dashboard provides:
# MAGIC - Current pipeline health status
# MAGIC - Recent job run history
# MAGIC - Performance metrics and trends
# MAGIC - Failure analysis and alerts
# MAGIC - SLA compliance tracking
# MAGIC
# MAGIC **Auto-refreshes** when jobs complete!

# COMMAND ----------
# MAGIC %md
# MAGIC ## ⚙️ Configuration

import json
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# COMMAND ----------
import requests
from IPython.display import HTML, display
from plotly.subplots import make_subplots

# Configuration
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "📊 Environment")
ENVIRONMENT = dbutils.widgets.get("environment")

# Get Databricks context
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
api_url = ctx.apiUrl().getOrElse(None)
api_token = ctx.apiToken().getOrElse(None)

print(f"✅ Environment: {ENVIRONMENT.upper()}")
print(f"🌐 API URL: {api_url}")
print(f"🔑 Token: {'✓ Found' if api_token else '✗ Not found'}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔌 Databricks Jobs API


# COMMAND ----------
class DatabricksJobsAPI:
    def __init__(self, host: str, token: str):
        self.host = host.rstrip("/")
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    def list_jobs(self, name_filter=None):
        """List all jobs"""
        url = f"{self.host}/api/2.1/jobs/list"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        jobs = response.json().get("jobs", [])

        if name_filter:
            jobs = [j for j in jobs if name_filter in j.get("settings", {}).get("name", "")]

        return jobs

    def get_job_runs(self, job_id=None, limit=25, offset=0):
        """Get job runs (recent executions)"""
        url = f"{self.host}/api/2.1/jobs/runs/list"
        params = {"limit": limit, "offset": offset}
        if job_id:
            params["job_id"] = job_id

        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        return response.json().get("runs", [])

    def get_run_output(self, run_id):
        """Get output from a specific run"""
        url = f"{self.host}/api/2.1/jobs/runs/get-output"
        response = requests.get(url, headers=self.headers, params={"run_id": run_id})
        response.raise_for_status()
        return response.json()


# Initialize API client
if api_url and api_token:
    api = DatabricksJobsAPI(api_url, api_token)
    print("✅ Jobs API client initialized")
else:
    print("❌ Cannot initialize API client - missing credentials")
    dbutils.notebook.exit("Missing API credentials")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 Fetch Job Data

# COMMAND ----------
print(f"🔍 Fetching jobs for environment: {ENVIRONMENT.upper()}")
print("=" * 70)

# Get all jobs for this environment
all_jobs = api.list_jobs(name_filter=f"[{ENVIRONMENT.upper()}]")

print(f"\n✅ Found {len(all_jobs)} jobs:")
for job in all_jobs:
    print(f"   • {job['settings']['name']} (ID: {job['job_id']})")

if not all_jobs:
    displayHTML(
        """
    <div style="background: #fff3cd; border: 2px solid #ffc107; padding: 30px; border-radius: 10px; margin: 20px 0;">
        <h2 style="color: #856404; margin: 0;">⚠️ No Jobs Found</h2>
        <p style="font-size: 16px;">No jobs found for environment: {}</p>
        <p><strong>Next Steps:</strong></p>
        <ol>
            <li>Run create_jobs.py script to create jobs</li>
            <li>Or select different environment above</li>
        </ol>
    </div>
    """.format(
            ENVIRONMENT.upper()
        )
    )
    dbutils.notebook.exit("No jobs found")

# Get recent runs for all jobs (last 50)
print(f"\n🔍 Fetching recent job runs...")
all_runs = api.get_job_runs(limit=100)

# Filter runs for our environment jobs
job_ids = {j["job_id"] for j in all_jobs}
env_runs = [r for r in all_runs if r.get("job_id") in job_ids]

print(f"✅ Found {len(env_runs)} recent runs for {ENVIRONMENT.upper()} jobs")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📈 Process Job Data


# COMMAND ----------
def process_run_data(runs):
    """Convert run data to DataFrame"""
    if not runs:
        return pd.DataFrame()

    data = []
    for run in runs:
        # Get job name
        job_id = run.get("job_id")
        job_name = next((j["settings"]["name"] for j in all_jobs if j["job_id"] == job_id), f"Job {job_id}")

        # Parse status
        state = run.get("state", {})
        life_cycle_state = state.get("life_cycle_state", "UNKNOWN")
        result_state = state.get("result_state", "UNKNOWN")

        # Determine final status
        if life_cycle_state in ["PENDING", "RUNNING", "TERMINATING"]:
            status = "RUNNING"
        elif result_state == "SUCCESS":
            status = "SUCCESS"
        elif result_state in ["FAILED", "TIMEDOUT", "CANCELED"]:
            status = "FAILED"
        else:
            status = "UNKNOWN"

        # Calculate duration
        start_time = run.get("start_time")
        end_time = run.get("end_time") or run.get("execution_duration", 0) + start_time if start_time else None

        if start_time:
            start_dt = datetime.fromtimestamp(start_time / 1000)
            if end_time:
                end_dt = datetime.fromtimestamp(end_time / 1000)
                duration_seconds = (end_dt - start_dt).total_seconds()
                duration_minutes = duration_seconds / 60
            else:
                duration_seconds = None
                duration_minutes = None
        else:
            start_dt = None
            duration_seconds = None
            duration_minutes = None

        # Get task info
        tasks = run.get("tasks", [])
        total_tasks = len(tasks)
        failed_tasks = sum(1 for t in tasks if t.get("state", {}).get("result_state") == "FAILED")

        data.append(
            {
                "run_id": run.get("run_id"),
                "job_id": job_id,
                "job_name": job_name,
                "status": status,
                "life_cycle_state": life_cycle_state,
                "result_state": result_state,
                "start_time": start_dt,
                "duration_seconds": duration_seconds,
                "duration_minutes": duration_minutes,
                "total_tasks": total_tasks,
                "failed_tasks": failed_tasks,
                "trigger_type": run.get("trigger", "UNKNOWN"),
                "run_page_url": run.get("run_page_url", ""),
            }
        )

    return pd.DataFrame(data)


# Process data
df_runs = process_run_data(env_runs)

if not df_runs.empty:
    # Sort by start time descending
    df_runs = df_runs.sort_values("start_time", ascending=False)
    print(f"✅ Processed {len(df_runs)} job runs")
else:
    print("⚠️  No run data available")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 Calculate Metrics

# COMMAND ----------
# Overall metrics
if not df_runs.empty:
    total_runs = len(df_runs)
    successful_runs = len(df_runs[df_runs["status"] == "SUCCESS"])
    failed_runs = len(df_runs[df_runs["status"] == "FAILED"])
    running_runs = len(df_runs[df_runs["status"] == "RUNNING"])

    success_rate = (successful_runs / total_runs * 100) if total_runs > 0 else 0

    # Latest run per job
    latest_runs = df_runs.groupby("job_name").first().reset_index()
    jobs_healthy = len(latest_runs[latest_runs["status"] == "SUCCESS"])
    jobs_failing = len(latest_runs[latest_runs["status"] == "FAILED"])
    jobs_running = len(latest_runs[latest_runs["status"] == "RUNNING"])

    # Average duration
    completed_runs = df_runs[df_runs["duration_minutes"].notna()]
    avg_duration = completed_runs["duration_minutes"].mean() if not completed_runs.empty else 0

    # Recent runs (last 24 hours)
    now = datetime.now()
    recent_runs = df_runs[df_runs["start_time"] > (now - timedelta(days=1))]
    runs_last_24h = len(recent_runs)

    # Recent failures (last 7 days)
    recent_week = df_runs[df_runs["start_time"] > (now - timedelta(days=7))]
    failures_last_week = len(recent_week[recent_week["status"] == "FAILED"])

else:
    total_runs = successful_runs = failed_runs = running_runs = 0
    success_rate = avg_duration = runs_last_24h = failures_last_week = 0
    jobs_healthy = jobs_failing = jobs_running = 0
    latest_runs = pd.DataFrame()

print(f"📊 Metrics calculated:")
print(f"   Total runs: {total_runs}")
print(f"   Success rate: {success_rate:.1f}%")
print(f"   Avg duration: {avg_duration:.1f} minutes")
print(f"   Jobs healthy: {jobs_healthy}/{len(all_jobs)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🎨 Pipeline Health Dashboard

# COMMAND ----------
# Determine overall health
if jobs_failing > 0:
    health_status = "🔴 UNHEALTHY"
    health_color = "#ff6b6b"
elif jobs_running > 0:
    health_status = "🟡 RUNNING"
    health_color = "#ffd93d"
elif jobs_healthy == len(all_jobs):
    health_status = "🟢 HEALTHY"
    health_color = "#6bcf7f"
else:
    health_status = "⚪ UNKNOWN"
    health_color = "#95a5a6"

# Display main dashboard
displayHTML(
    f"""
<div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 15px; margin: 20px 0;">
    <h1 style="margin: 0;">📊 Pipeline Monitoring Dashboard</h1>
    <p style="font-size: 16px; opacity: 0.9;">Environment: {ENVIRONMENT.upper()} | Last Updated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
</div>

<div style="background: {health_color}; color: white; padding: 30px; border-radius: 15px; margin: 20px 0; text-align: center;">
    <h2 style="margin: 0; font-size: 48px;">{health_status}</h2>
    <p style="font-size: 20px; margin: 10px 0;">Pipeline Health Status</p>
    <p style="font-size: 16px; opacity: 0.9;">
        {jobs_healthy} of {len(all_jobs)} jobs healthy
        {f' | {jobs_failing} failing' if jobs_failing > 0 else ''}
        {f' | {jobs_running} running' if jobs_running > 0 else ''}
    </p>
</div>

<div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 20px 0;">
    <div style="background: #e3f2fd; padding: 25px; border-radius: 10px; text-align: center;">
        <h3 style="margin: 0; color: #1976d2;">Total Runs</h3>
        <div style="font-size: 48px; font-weight: bold; margin: 15px 0; color: #1976d2;">{total_runs}</div>
        <p style="margin: 0; color: #666;">{runs_last_24h} in last 24 hours</p>
    </div>
    
    <div style="background: #e8f5e9; padding: 25px; border-radius: 10px; text-align: center;">
        <h3 style="margin: 0; color: #2e7d32;">Success Rate</h3>
        <div style="font-size: 48px; font-weight: bold; margin: 15px 0; color: #2e7d32;">{success_rate:.1f}%</div>
        <p style="margin: 0; color: #666;">{successful_runs} successful runs</p>
    </div>
    
    <div style="background: {'#ffebee' if failed_runs > 0 else '#f5f5f5'}; padding: 25px; border-radius: 10px; text-align: center;">
        <h3 style="margin: 0; color: {'#c62828' if failed_runs > 0 else '#666'};">Failed Runs</h3>
        <div style="font-size: 48px; font-weight: bold; margin: 15px 0; color: {'#c62828' if failed_runs > 0 else '#666'};">{failed_runs}</div>
        <p style="margin: 0; color: #666;">{failures_last_week} in last 7 days</p>
    </div>
    
    <div style="background: #f3e5f5; padding: 25px; border-radius: 10px; text-align: center;">
        <h3 style="margin: 0; color: #7b1fa2;">Avg Duration</h3>
        <div style="font-size: 48px; font-weight: bold; margin: 15px 0; color: #7b1fa2;">{avg_duration:.1f}</div>
        <p style="margin: 0; color: #666;">minutes per job</p>
    </div>
</div>
"""
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📋 Recent Job Runs

# COMMAND ----------
print("📋 RECENT JOB RUNS")
print("=" * 70)

if not df_runs.empty:
    # Show last 10 runs
    recent_10 = df_runs.head(10)

    print(f"\n{'Job Name':<40} {'Status':<12} {'Duration':<12} {'Started':<20}")
    print("-" * 90)

    for _, row in recent_10.iterrows():
        status_emoji = {"SUCCESS": "✅", "FAILED": "❌", "RUNNING": "🔄", "UNKNOWN": "❓"}.get(row["status"], "❓")

        job_name = row["job_name"].replace(f"[{ENVIRONMENT.upper()}] ", "")[:38]
        status = f"{status_emoji} {row['status']}"
        duration = f"{row['duration_minutes']:.1f}m" if pd.notna(row["duration_minutes"]) else "N/A"
        started = row["start_time"].strftime("%Y-%m-%d %H:%M") if pd.notna(row["start_time"]) else "N/A"

        print(f"{job_name:<40} {status:<12} {duration:<12} {started:<20}")

    # Create interactive table
    display_df = recent_10[
        ["job_name", "status", "duration_minutes", "start_time", "total_tasks", "failed_tasks"]
    ].copy()
    display_df["job_name"] = display_df["job_name"].str.replace(f"[{ENVIRONMENT.upper()}] ", "")
    display_df["duration_minutes"] = display_df["duration_minutes"].round(1)
    display_df["start_time"] = display_df["start_time"].dt.strftime("%Y-%m-%d %H:%M")

    display_df.columns = ["Job Name", "Status", "Duration (min)", "Start Time", "Total Tasks", "Failed Tasks"]

    displayHTML("<h3>📊 Last 10 Job Runs:</h3>")
    display(display_df)
else:
    print("\n⚠️  No job runs found")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 Success Rate by Job

# COMMAND ----------
if not df_runs.empty:
    # Calculate success rate per job
    job_stats = (
        df_runs.groupby("job_name")
        .agg({"run_id": "count", "status": lambda x: (x == "SUCCESS").sum()})
        .rename(columns={"run_id": "total_runs", "status": "successful_runs"})
    )

    job_stats["success_rate"] = (job_stats["successful_runs"] / job_stats["total_runs"] * 100).round(1)
    job_stats["failed_runs"] = job_stats["total_runs"] - job_stats["successful_runs"]
    job_stats = job_stats.sort_values("success_rate")

    # Clean job names
    job_stats.index = job_stats.index.str.replace(f"[{ENVIRONMENT.upper()}] ", "")

    # Create chart
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            name="Success Rate",
            x=job_stats["success_rate"],
            y=job_stats.index,
            orientation="h",
            marker=dict(color=job_stats["success_rate"], colorscale="RdYlGn", showscale=True),
            text=job_stats["success_rate"].apply(lambda x: f"{x:.1f}%"),
            textposition="inside",
        )
    )

    fig.update_layout(
        title="Success Rate by Job",
        xaxis_title="Success Rate (%)",
        yaxis_title="Job Name",
        height=max(400, len(job_stats) * 50),
        showlegend=False,
    )

    fig.show()
else:
    print("⚠️  No data for chart")

# COMMAND ----------
# MAGIC %md
# MAGIC ## ⏱️ Average Duration by Job

# COMMAND ----------
if not df_runs.empty and not df_runs[df_runs["duration_minutes"].notna()].empty:
    # Calculate average duration per job
    duration_stats = (
        df_runs[df_runs["duration_minutes"].notna()]
        .groupby("job_name")
        .agg({"duration_minutes": ["mean", "min", "max", "count"]})
        .round(1)
    )

    duration_stats.columns = ["avg_duration", "min_duration", "max_duration", "run_count"]
    duration_stats = duration_stats.sort_values("avg_duration", ascending=False)

    # Clean job names
    duration_stats.index = duration_stats.index.str.replace(f"[{ENVIRONMENT.upper()}] ", "")

    # Create chart
    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            name="Average",
            x=duration_stats.index,
            y=duration_stats["avg_duration"],
            marker_color="#667eea",
            text=duration_stats["avg_duration"].apply(lambda x: f"{x:.1f}m"),
            textposition="outside",
        )
    )

    fig.update_layout(
        title="Average Duration by Job",
        xaxis_title="Job Name",
        yaxis_title="Duration (minutes)",
        height=400,
        showlegend=False,
        xaxis_tickangle=-45,
    )

    fig.show()
else:
    print("⚠️  No duration data for chart")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📈 Success/Failure Trend (Last 30 Days)

# COMMAND ----------
if not df_runs.empty:
    # Filter last 30 days
    thirty_days_ago = datetime.now() - timedelta(days=30)
    recent_runs = df_runs[df_runs["start_time"] > thirty_days_ago].copy()

    if not recent_runs.empty:
        # Group by date and status
        recent_runs["date"] = recent_runs["start_time"].dt.date
        trend_data = recent_runs.groupby(["date", "status"]).size().unstack(fill_value=0)

        # Create stacked area chart
        fig = go.Figure()

        if "SUCCESS" in trend_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=trend_data.index,
                    y=trend_data["SUCCESS"],
                    mode="lines",
                    name="Success",
                    line=dict(color="#6bcf7f", width=2),
                    fill="tozeroy",
                )
            )

        if "FAILED" in trend_data.columns:
            fig.add_trace(
                go.Scatter(
                    x=trend_data.index,
                    y=trend_data["FAILED"],
                    mode="lines",
                    name="Failed",
                    line=dict(color="#ff6b6b", width=2),
                    fill="tozeroy",
                )
            )

        fig.update_layout(
            title="Job Runs Trend (Last 30 Days)",
            xaxis_title="Date",
            yaxis_title="Number of Runs",
            height=400,
            hovermode="x unified",
        )

        fig.show()
    else:
        print("⚠️  No runs in last 30 days")
else:
    print("⚠️  No data for trend chart")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🚨 Failed Jobs Analysis

# COMMAND ----------
if not df_runs.empty:
    failed_runs_df = df_runs[df_runs["status"] == "FAILED"].copy()

    if not failed_runs_df.empty:
        displayHTML(
            f"""
        <div style="background: #ffebee; border-left: 5px solid #c62828; padding: 20px; margin: 20px 0; border-radius: 5px;">
            <h2 style="margin: 0; color: #c62828;">🚨 Failed Jobs Detected</h2>
            <p style="font-size: 16px; margin: 10px 0;">
                <strong>{len(failed_runs_df)} failed runs</strong> found in recent history
            </p>
        </div>
        """
        )

        print("\n🚨 FAILED JOBS ANALYSIS")
        print("=" * 70)

        # Group failures by job
        failure_counts = failed_runs_df.groupby("job_name").size().sort_values(ascending=False)

        print(f"\n📊 Failures by Job:")
        for job_name, count in failure_counts.items():
            clean_name = job_name.replace(f"[{ENVIRONMENT.upper()}] ", "")
            print(f"   • {clean_name}: {count} failures")

        # Show recent failures
        print(f"\n📋 Last 5 Failed Runs:")
        recent_failures = failed_runs_df.head(5)

        for _, row in recent_failures.iterrows():
            clean_name = row["job_name"].replace(f"[{ENVIRONMENT.upper()}] ", "")
            start_time = row["start_time"].strftime("%Y-%m-%d %H:%M") if pd.notna(row["start_time"]) else "N/A"
            print(f"\n   ❌ {clean_name}")
            print(f"      Started: {start_time}")
            print(f"      Run ID: {row['run_id']}")
            if row["run_page_url"]:
                print(f"      URL: {row['run_page_url']}")
    else:
        displayHTML(
            """
        <div style="background: #e8f5e9; border-left: 5px solid #2e7d32; padding: 20px; margin: 20px 0; border-radius: 5px;">
            <h2 style="margin: 0; color: #2e7d32;">✅ No Failed Jobs</h2>
            <p style="font-size: 16px;">All recent job runs completed successfully!</p>
        </div>
        """
        )
else:
    print("⚠️  No run data available")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📝 Summary & Recommendations

# COMMAND ----------
displayHTML(
    f"""
<div style="background: #f5f5f5; padding: 30px; border-radius: 10px; margin: 20px 0;">
    <h2 style="color: #333;">📋 Pipeline Monitoring Summary</h2>
    
    <h3>Overall Health: {health_status}</h3>
    <ul>
        <li><strong>Total Jobs:</strong> {len(all_jobs)}</li>
        <li><strong>Healthy Jobs:</strong> {jobs_healthy}</li>
        <li><strong>Failing Jobs:</strong> {jobs_failing}</li>
        <li><strong>Currently Running:</strong> {jobs_running}</li>
    </ul>
    
    <h3>Recent Activity:</h3>
    <ul>
        <li><strong>Total Runs:</strong> {total_runs}</li>
        <li><strong>Success Rate:</strong> {success_rate:.1f}%</li>
        <li><strong>Failed Runs:</strong> {failed_runs}</li>
        <li><strong>Average Duration:</strong> {avg_duration:.1f} minutes</li>
    </ul>
    
    <h3>📌 Recommendations:</h3>
    <ul>
        {'<li>🚨 <strong>Immediate Action:</strong> Investigate and fix ' + str(jobs_failing) + ' failing jobs</li>' if jobs_failing > 0 else ''}
        {'<li>⚠️ <strong>Warning:</strong> Success rate below 90% - review failure patterns</li>' if success_rate < 90 else ''}
        {'<li>✅ <strong>Good:</strong> All jobs are healthy!</li>' if jobs_failing == 0 and jobs_healthy == len(all_jobs) else ''}
        <li><strong>Monitor:</strong> Run this dashboard daily to track trends</li>
        <li><strong>Alerts:</strong> Ensure email notifications are configured for failures</li>
        <li><strong>Optimization:</strong> Review jobs taking longer than expected</li>
    </ul>
    
    <h3>🔗 Next Steps:</h3>
    <ol>
        <li>Review any failed jobs above and check logs</li>
        <li>Verify data quality monitoring results</li>
        <li>Check if schedules are appropriate</li>
        <li>Optimize slow-running jobs if needed</li>
        <li>Re-run this dashboard after addressing issues</li>
    </ol>
</div>
"""
)

print("\n" + "=" * 70)
print("✅ PIPELINE MONITORING COMPLETE")
print("=" * 70)
print(f"\n📊 Environment: {ENVIRONMENT.upper()}")
print(f"🎯 Pipeline Health: {health_status}")
print(f"✅ Jobs Healthy: {jobs_healthy}/{len(all_jobs)}")
print(f"📈 Success Rate: {success_rate:.1f}%")
print(f"\n⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📚 Dashboard Usage Tips
# MAGIC
# MAGIC **Running the Dashboard:**
# MAGIC - Run daily to monitor pipeline health
# MAGIC - Run after making changes to jobs
# MAGIC - Run when investigating issues
# MAGIC
# MAGIC **Interpreting Results:**
# MAGIC - 🟢 GREEN = All systems normal
# MAGIC - 🟡 YELLOW = Jobs running
# MAGIC - 🔴 RED = Action required
# MAGIC
# MAGIC **Taking Action:**
# MAGIC - Failed jobs → Check logs in job run URL
# MAGIC - Low success rate → Review failure patterns
# MAGIC - Slow jobs → Optimize queries or increase cluster
# MAGIC - Multiple failures → Check data dependencies
# MAGIC
# MAGIC **Integration:**
# MAGIC - This dashboard uses Databricks Jobs API
# MAGIC - Works with jobs created by create_jobs.py
# MAGIC - Auto-detects environment from job names
# MAGIC - Can be scheduled as a job itself!
# MAGIC
# MAGIC **Questions? Review the logs and job run history!** 📊
