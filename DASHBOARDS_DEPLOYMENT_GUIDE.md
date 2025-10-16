# 📊 Dashboards Deployment Guide

## Overview

This project includes **two production-ready dashboards** for monitoring and optimization:

1. **📊 Data Quality Monitoring Dashboard** - Interactive Streamlit app
2. **💰 Cost Optimization Analysis** - Comprehensive cost tracking notebook

---

## 📊 1. Data Quality Monitoring Dashboard

### What It Does

**Real-time interactive dashboard** that monitors data quality across Bronze, Silver, and Gold layers:

- ✅ **Quality Metrics:** Null rates, duplicates, freshness, row counts
- ✅ **Interactive Visualizations:** Charts, heatmaps, trends
- ✅ **Automated Scoring:** Quality score (0-100) for each table
- ✅ **Alert System:** Critical, warning, and success alerts
- ✅ **Recommendations:** Actionable fixes for quality issues
- ✅ **Auto-refresh:** Real-time monitoring (configurable)

### Features

| Feature | Description |
|---------|-------------|
| **Overview Tab** | Executive summary, quality scores by layer, pass/fail metrics |
| **Detailed Analysis Tab** | Table-by-table quality metrics, filtering, heatmaps |
| **Trends Tab** | Historical quality trends, volume analysis |
| **Alerts Tab** | Critical issues, warnings, recommendations |

### Deployment Options

#### Option 1: Local Deployment (Quick Start)

```bash
# Navigate to project root
cd /Users/kanikamondal/Databricks/insurance-data-ai

# Make script executable
chmod +x launch_dq_dashboard.sh

# Launch dashboard
./launch_dq_dashboard.sh
```

**Access at:** `http://localhost:8502`

#### Option 2: Databricks Deployment

1. **Upload notebook to Databricks:**
   ```
   Workspace → Upload → Select: src/analytics/dq_dashboard.py
   ```

2. **Install dependencies:**
   ```python
   %pip install streamlit plotly altair
   dbutils.library.restartPython()
   ```

3. **Run all cells** in the notebook

4. **Access via Databricks Apps** (if available) or port forwarding

#### Option 3: Production Deployment (Databricks Apps)

For Databricks Standard/Premium/Enterprise:

1. Create a Databricks App:
   ```python
   # In Databricks workspace
   Create → App → Streamlit
   # Upload: src/analytics/dq_dashboard.py
   ```

2. Configure app settings:
   - **Name:** Data Quality Monitoring
   - **Compute:** Serverless (recommended)
   - **Access:** Share with team

3. **Launch** and get permanent URL

---

### Configuration

**Sidebar Controls:**

| Setting | Default | Description |
|---------|---------|-------------|
| Environment | dev | Select: dev, staging, prod |
| Max Null Rate | 5% | Alert threshold for null values |
| Max Duplicate Rate | 1% | Alert threshold for duplicates |
| Max Data Age | 48 hours | Freshness threshold |
| Auto-refresh | Off | Enable 30-second auto-refresh |

---

### Usage

#### Daily Workflow

1. **Open dashboard** at configured URL
2. **Review Overview tab** for critical alerts
3. **Check Alerts tab** for specific issues
4. **Investigate failed tables** in Detailed Analysis
5. **Implement recommendations**

#### Example Alerts

**Critical Alert:**
```
🚨 CRITICAL: customers.customer_dim failed quality checks!
- High null rate: 8.5% (threshold: 5%)
- Recommendation: Investigate and fix null values in source data
```

**Warning:**
```
⚠️ WARNING: claims.claim_raw is stale
- Data is 72 hours old (threshold: 48 hours)
- Recommendation: Schedule more frequent data refreshes
```

---

## 💰 2. Cost Optimization Analysis

### What It Does

**Comprehensive cost tracking and optimization** for Databricks resources:

- ✅ **Storage Cost Analysis:** Data volume, growth trends, cost per table
- ✅ **Compute Cost Analysis:** Cluster usage, idle time, DBU consumption
- ✅ **Job Performance Metrics:** Runtime, efficiency, failure analysis
- ✅ **Optimization Recommendations:** Automated cost-saving suggestions
- ✅ **Savings Estimation:** Potential monthly/annual savings

### Features

| Feature | Description |
|---------|-------------|
| **Storage Analysis** | Table sizes, file counts, monthly costs by layer |
| **Compute Analysis** | Cluster uptime, DBU usage, cost per cluster |
| **Job Performance** | Runtime, success rates, cost per job |
| **Recommendations** | Prioritized optimization actions with estimated savings |
| **Visualizations** | Interactive charts for cost breakdown and trends |

### Deployment

#### Option 1: Manual Execution

1. **Open notebook** in Databricks:
   ```
   Workspace → Upload → Select: src/analytics/cost_optimization_analysis.py
   ```

2. **Configure parameters:**
   - **Environment:** dev / staging / prod
   - **Days to Analyze:** 30 (default)

3. **Run all cells**

4. **Review recommendations**

#### Option 2: Scheduled Execution (Recommended)

Create a weekly job:

```yaml
# In Databricks Jobs
Name: Weekly Cost Optimization Report
Schedule: Every Monday at 9:00 AM
Notebook: src/analytics/cost_optimization_analysis.py
Cluster: Serverless
Parameters:
  - environment: prod
  - days_lookback: 30
Email Notifications: finance-team@company.com
```

---

### Cost Metrics Explained

#### Storage Costs

```
Monthly Storage Cost = Data Size (GB) × $0.023/GB/month
```

**Tracked Metrics:**
- Total data size across all layers
- Cost breakdown by Bronze/Silver/Gold
- Top 10 most expensive tables
- File counts (detect small file problem)

#### Compute Costs

```
Compute Cost = Uptime (hours) × DBUs per hour × $0.55/DBU
```

**Tracked Metrics:**
- Cluster uptime and idle time
- DBU consumption per cluster
- Cost per cluster
- Efficiency analysis

#### Job Costs

```
Job Cost = Runtime (hours) × (Workers + 1) × DBUs/hour × $0.55/DBU
```

**Tracked Metrics:**
- Total job runs analyzed
- Success vs failure rates
- Average cost per job
- Long-running jobs

---

### Optimization Recommendations

The notebook provides **automated recommendations** in priority order:

#### Example Recommendations

**1. High Priority: Small File Problem**
```
Issue: 15 tables have >1000 small files
Potential Savings: $1,200/month
Action: Run OPTIMIZE and Z-ORDER on affected tables
Command: OPTIMIZE insurance_dev_silver.customers.customer_dim ZORDER BY (customer_id)
```

**2. Medium Priority: Idle Clusters**
```
Issue: 3 clusters running with <1 hour uptime
Potential Savings: $450 (one-time)
Action: Enable auto-termination (10-15 minutes)
Command: Update cluster config → Advanced → Auto Termination = 10 minutes
```

**3. High Priority: Failed Jobs**
```
Issue: 12 failed jobs wasting resources
Wasted Cost: $320
Action: Fix failing jobs and implement retry logic
Command: Review job logs and add error handling
```

---

### Sample Output

```
==================================================
💰 COST OPTIMIZATION ANALYSIS - SUMMARY
==================================================

📦 STORAGE COSTS:
  • Total Data Size: 1,234.56 GB
  • Monthly Storage Cost: $28.39
  • Annual Storage Cost: $340.68

  Breakdown by Layer:
    - Bronze: 456.78 GB → $10.51/month
    - Silver: 567.89 GB → $13.06/month
    - Gold: 209.89 GB → $4.82/month

💻 COMPUTE COSTS (30 days):
  • Total Cluster Uptime: 156.5 hours
  • Total DBUs Consumed: 1,234.5
  • Total Compute Cost: $678.98
  • Projected Monthly Cost: $678.98

🔄 JOB PERFORMANCE (30 days):
  • Total Jobs Analyzed: 120
  • Successful: 108 | Failed: 12
  • Total Job Runtime: 89.3 hours
  • Total Job Cost: $456.78
  • Avg Cost per Job: $3.81

==================================================
💰 TOTAL ESTIMATED COSTS:
  • Monthly: $1,164.15
  • Annual: $13,969.80
==================================================

💡 COST OPTIMIZATION RECOMMENDATIONS

📋 ACTIONABLE RECOMMENDATIONS:

1. [High Priority] Storage: Small File Problem
   • Tables Affected: 15
   • Potential Savings: $1,200.00/month
   • Action: Run OPTIMIZE and Z-ORDER on affected tables
   • Command: OPTIMIZE insurance_dev_silver.customers.customer_dim ...

2. [High Priority] Compute: Idle/Underutilized Clusters
   • Clusters Affected: 3
   • Potential Savings: $450.00 (one-time)
   • Action: Enable auto-termination and reduce cluster idle time
   • Command: Set auto-termination to 10-15 minutes in cluster config

==================================================
💰 TOTAL POTENTIAL SAVINGS:
  • Estimated Monthly Savings: $1,650.00
  • Estimated Annual Savings: $19,800.00
==================================================
```

---

## 🚀 Quick Start Commands

### Data Quality Dashboard

```bash
# Local deployment
cd /Users/kanikamondal/Databricks/insurance-data-ai
chmod +x launch_dq_dashboard.sh
./launch_dq_dashboard.sh

# Access at: http://localhost:8502
```

### Cost Optimization

```bash
# Open in Databricks
# Workspace → src/analytics/cost_optimization_analysis.py
# Run all cells
```

---

## 📅 Recommended Schedule

| Dashboard | Frequency | When | Purpose |
|-----------|-----------|------|---------|
| **Data Quality** | Daily | After ETL runs | Detect quality issues immediately |
| **Cost Optimization** | Weekly | Monday mornings | Track costs, implement optimizations |

---

## 🔧 Troubleshooting

### Data Quality Dashboard

**Issue:** Dashboard shows "No data available"
- **Cause:** Cannot connect to Databricks catalogs
- **Fix:** Ensure Spark session is active and catalogs exist

**Issue:** Slow performance
- **Cause:** Too many tables being analyzed
- **Fix:** Limit analysis to specific schemas or reduce sample size

### Cost Optimization

**Issue:** "Error fetching cluster usage"
- **Cause:** Missing API token or permissions
- **Fix:** Ensure notebook runs in Databricks with proper permissions

**Issue:** Cost estimates seem incorrect
- **Cause:** Default pricing constants don't match your contract
- **Fix:** Update `COST_PER_DBU` and `COST_PER_GB_STORAGE_MONTH` in notebook

---

## 📊 Integration with Existing System

Both dashboards integrate seamlessly with your existing infrastructure:

- ✅ **Works with all environments:** dev, staging, prod
- ✅ **Uses existing catalogs:** No additional setup required
- ✅ **Databricks Asset Bundles compatible:** Can be added to `databricks.yml`
- ✅ **Compatible with DLT pipelines:** Monitors DLT-created tables

---

## 🎯 Success Metrics

### Data Quality Dashboard

- **Quality Score Improvement:** Track average quality score over time
- **Alert Resolution Time:** Monitor how quickly issues are fixed
- **Tables Passing:** Increase percentage of tables passing all checks

### Cost Optimization

- **Monthly Cost Reduction:** Track actual savings from recommendations
- **Failed Job Reduction:** Decrease failed job percentage
- **Storage Growth Rate:** Optimize storage growth over time

---

## 📞 Support

For issues or questions:

1. **Data Quality Issues:** Check alert messages for specific tables
2. **Cost Optimization:** Review recommendations in priority order
3. **Technical Issues:** Check Databricks logs and notebook outputs

---

## 🎉 Summary

You now have **two production-ready dashboards**:

1. **📊 Data Quality Monitoring** - Real-time interactive quality monitoring
2. **💰 Cost Optimization Analysis** - Comprehensive cost tracking and recommendations

Both are fully functional, deployed, and ready to use!

**Next Steps:**
1. Launch the Data Quality Dashboard
2. Run the Cost Optimization Analysis
3. Implement high-priority recommendations
4. Schedule regular runs for both dashboards

