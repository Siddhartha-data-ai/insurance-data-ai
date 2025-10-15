# 🚀 Databricks Jobs Setup Guide

## ✅ **What You'll Get**

Automated creation of **6 production-ready jobs** for your insurance analytics pipeline:

1. **Bronze Data Generation** - 3 notebooks (customers, policies, claims)
2. **Silver Transformation** - 1 notebook (bronze → silver with SCD Type 2)
3. **Gold Analytics** - 2 notebooks (customer 360, fraud detection)
4. **ML Predictions** - 4 notebooks (churn, fraud, forecast, optimization)
5. **Data Quality Monitoring** - Automated DQ checks
6. **Pipeline Monitoring** - Execution tracking and alerts

**All with:**
- ✅ Proper dependencies (run in correct order)
- ✅ Email alerts on failures
- ✅ Scheduled execution (daily at 6 AM)
- ✅ Timeout protection
- ✅ Resource optimization

---

## 🎯 **Quick Start (5 Minutes)**

### **Step 1: Get Your Databricks Token** (2 minutes)

1. Go to your Databricks workspace
2. Click your profile icon (top right)
3. Select **User Settings**
4. Go to **Access Tokens** tab
5. Click **Generate New Token**
6. Give it a name: "Pipeline Jobs"
7. Set lifetime: 90 days (or longer)
8. Click **Generate**
9. **Copy the token** (you won't see it again!)

---

### **Step 2: Set Your Token** (30 seconds)

**On Mac/Linux:**
```bash
export DATABRICKS_TOKEN='dapi1234567890abcdef...'
```

**On Windows (PowerShell):**
```powershell
$env:DATABRICKS_TOKEN='dapi1234567890abcdef...'
```

**Or edit the script directly:**
```python
# In create_jobs.py, line 17
DATABRICKS_TOKEN = "dapi1234567890abcdef..."  # Paste your token here
```

---

### **Step 3: Run the Script** (2 minutes)

```bash
cd /Users/kanikamondal/Databricks/insurance-data-ai

python3 create_jobs.py
```

**What it does:**
- ✅ Checks for existing jobs
- ✅ Creates 6 new jobs
- ✅ Configures schedules & alerts
- ✅ Displays summary with job URLs

---

### **Step 4: Verify in Databricks** (1 minute)

1. Go to Databricks workspace
2. Click **Jobs & Pipelines** (left sidebar)
3. You should see 6 new jobs:
   - `[DEV] 1_Bronze_Data_Generation`
   - `[DEV] 2_Silver_Transformation`
   - `[DEV] 3_Gold_Analytics`
   - `[DEV] 4_ML_Predictions`
   - `[DEV] 5_Data_Quality_Monitoring`
   - `[DEV] 6_Pipeline_Monitoring`

---

## ⚙️ **Configuration Options**

### **Edit `create_jobs.py` to customize:**

#### **1. Environment**
```python
ENVIRONMENT = "dev"  # Change to "staging" or "prod"
```

#### **2. Alert Email**
```python
ALERT_EMAIL = "your-email@example.com"
```

#### **3. Schedule**
```python
SCHEDULE_CRON = "0 6 * * *"  # Daily at 6 AM
```

**Common schedules:**
```
"0 6 * * *"       # Daily at 6 AM
"0 */4 * * *"     # Every 4 hours
"0 6 * * 1"       # Weekly on Monday at 6 AM
"0 6 1 * *"       # Monthly on 1st at 6 AM
"0 6 * * 1-5"     # Weekdays only at 6 AM
```

#### **4. Timezone**
```python
SCHEDULE_TIMEZONE = "America/New_York"
```

**Common timezones:**
- `America/New_York` (EST/EDT)
- `America/Chicago` (CST/CDT)
- `America/Los_Angeles` (PST/PDT)
- `Europe/London` (GMT/BST)
- `Asia/Kolkata` (IST)
- `UTC` (Universal)

#### **5. Cluster Size**
```python
CLUSTER_CONFIG = {
    "node_type_id": "i3.xlarge",  # Smaller: m4.large, Bigger: i3.2xlarge
    "num_workers": 2,              # More workers = faster
}
```

---

## 📊 **Running Jobs**

### **Manual Run (For Testing)**

1. Go to **Jobs & Pipelines**
2. Click job name (e.g., `[DEV] 1_Bronze_Data_Generation`)
3. Click **Run now** button
4. Watch execution in real-time
5. Check **Task Runs** for details

**Recommended first-run order:**
1. Bronze → 2. Silver → 3. Gold → 4. ML → 5. DQ → 6. Monitoring

---

### **Enable Scheduled Runs**

All jobs start **PAUSED** for safety. To enable:

1. Click job name
2. Go to **Schedule** tab
3. Click **Resume** button
4. Job will now run on schedule!

**To pause schedule:**
- Click **Pause** in Schedule tab

---

### **View Job Runs**

**Recent runs:**
- Jobs & Pipelines → **Job Runs** tab

**Specific job history:**
- Click job name → **Runs** tab

**What you'll see:**
- ✅ Start/end time
- ✅ Duration
- ✅ Status (Success/Failed)
- ✅ Task details
- ✅ Logs

---

## 🚨 **Alerts & Notifications**

### **Email Alerts (Already Configured)**

You'll receive emails when:
- ❌ Job fails
- ⏰ Job times out

**Email contains:**
- Job name
- Failure reason
- Direct link to job run
- Logs

---

### **Add More Recipients**

Edit `create_jobs.py`:
```python
ALERT_EMAIL = "team@example.com"

# Or multiple emails in job config:
"email_notifications": {
    "on_failure": ["person1@example.com", "person2@example.com"]
}
```

Re-run script to update.

---

### **Add Slack Notifications**

1. Create Slack incoming webhook
2. In job config, add:
```python
"webhook_notifications": {
    "on_failure": [
        {
            "id": "slack-webhook-id"
        }
    ]
}
```

---

## 🔍 **Monitoring & Troubleshooting**

### **Check Job Status**

**Dashboard view:**
- Jobs & Pipelines → See all jobs at a glance
- Color coded: Green (success), Red (failed), Yellow (running)

**Job details:**
- Click job name → See configuration and history

---

### **View Logs**

1. Click job name
2. Click specific run
3. Click task name
4. Click **Logs** tab

**What to look for:**
- Error messages
- Stack traces
- Data volume issues
- Performance problems

---

### **Common Issues**

#### **Issue: "Invalid access token"**
**Solution:** 
- Regenerate token (they expire!)
- Update script with new token
- Re-run

#### **Issue: "Notebook not found"**
**Solution:**
- Verify notebook paths in script
- Check notebooks exist in workspace
- Update paths if needed

#### **Issue: "Cluster start failed"**
**Solution:**
- Check cluster configuration
- May need different node_type_id
- Try smaller cluster (1 worker)

#### **Issue: Job takes too long**
**Solution:**
- Increase timeout_seconds
- Use bigger cluster
- Optimize notebook code

#### **Issue: Email alerts not received**
**Solution:**
- Check spam folder
- Verify email address
- Check Databricks SMTP settings

---

## 📈 **Best Practices**

### **Development Workflow**

1. **Test manually first**
   - Run each job once
   - Verify output
   - Check data quality

2. **Start with dev environment**
   - Create jobs for `ENVIRONMENT = "dev"`
   - Test thoroughly
   - Then create staging/prod

3. **Monitor initial runs**
   - Watch first few scheduled runs
   - Verify timing is correct
   - Check alert delivery

4. **Adjust as needed**
   - Tune cluster sizes
   - Adjust timeouts
   - Refine schedules

---

### **Production Readiness**

Before going to production:

1. ✅ All jobs tested manually
2. ✅ Data quality checks passing
3. ✅ Alerts configured and tested
4. ✅ Cluster sizes optimized
5. ✅ Schedules validated
6. ✅ Team notified of schedule
7. ✅ Runbook documented

---

### **Cost Optimization**

**Use job clusters (already configured):**
- ✅ Start automatically for job
- ✅ Stop after completion
- ✅ Only pay for active time

**Right-size clusters:**
```python
# For testing/dev
"num_workers": 1

# For production
"num_workers": 2-4  # Adjust based on data volume
```

**Schedule wisely:**
- Run during off-peak hours
- Avoid overlapping heavy jobs
- Use appropriate frequency

---

## 🔄 **Updating Jobs**

### **Method 1: Delete and Recreate (Easiest)**

```bash
# Script will ask if you want to delete existing
python3 create_jobs.py
# Answer 'yes' to delete and recreate
```

---

### **Method 2: Manual Update**

1. Go to job in UI
2. Click **Edit** button
3. Modify settings
4. Click **Save**

---

### **Method 3: Programmatic Update**

Use Databricks API:
```python
# In create_jobs.py, add update_job() method
def update_job(self, job_id: int, new_config: Dict):
    url = f"{self.host}/api/2.1/jobs/update"
    response = requests.post(url, headers=self.headers, json={
        "job_id": job_id,
        "new_settings": new_config
    })
    response.raise_for_status()
```

---

## 📊 **Job Dependencies**

### **Current Setup:**

```
[1_Bronze_Data_Generation]
  ├─ generate_customers (runs first)
  ├─ generate_policies (depends on customers)
  └─ generate_claims (depends on policies)

[2_Silver_Transformation]
  └─ transform_bronze_to_silver (runs after bronze complete)

[3_Gold_Analytics]
  ├─ build_customer_360 (parallel)
  └─ build_fraud_detection (parallel)

[4_ML_Predictions]
  ├─ predict_churn (parallel)
  ├─ predict_fraud (parallel)
  ├─ forecast_claims (parallel)
  └─ optimize_premiums (parallel)

[5_Data_Quality_Monitoring]
  └─ data_quality_checks (runs after ML)

[6_Pipeline_Monitoring]
  └─ pipeline_monitoring (runs after all)
```

---

### **Cross-Job Dependencies (Not Yet Implemented)**

To make Job 2 wait for Job 1 to complete, you need **Databricks Workflows** (not available via simple job creation).

**Workaround:**
- Schedule jobs with time delays:
  - Bronze: 6:00 AM
  - Silver: 6:20 AM (20 min buffer)
  - Gold: 6:40 AM
  - ML: 7:00 AM
  - DQ: 7:30 AM
  - Monitoring: 8:00 AM

**Or:**
- Use master orchestrator notebook (I can build this!)

---

## 🎯 **Advanced: Master Orchestrator**

Want jobs to trigger each other automatically?

I can create a **master orchestrator notebook** that:
- ✅ Triggers jobs via API
- ✅ Waits for completion
- ✅ Handles failures
- ✅ Ensures proper order
- ✅ One job to rule them all!

Let me know if you want this! 🚀

---

## 📝 **Job Configuration Reference**

### **Full Job Structure:**

```python
{
    "name": "Job Name",
    "tags": {
        "key": "value"
    },
    "email_notifications": {
        "on_failure": ["email@example.com"],
        "on_success": [],
        "no_alert_for_skipped_runs": True
    },
    "timeout_seconds": 3600,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "unique_task_name",
            "description": "What this task does",
            "depends_on": [{"task_key": "previous_task"}],
            "notebook_task": {
                "notebook_path": "/path/to/notebook",
                "base_parameters": {
                    "param1": "value1"
                }
            },
            "new_cluster": {
                "spark_version": "13.3.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 2
            },
            "timeout_seconds": 900
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 6 * * *",
        "timezone_id": "America/New_York",
        "pause_status": "PAUSED"
    },
    "format": "MULTI_TASK"
}
```

---

## 🔗 **Useful Links**

- **Jobs API Documentation:** https://docs.databricks.com/api/workspace/jobs
- **Cron Expression Guide:** https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html
- **Cluster Configuration:** https://docs.databricks.com/clusters/configure.html
- **Email Notifications:** https://docs.databricks.com/workflows/jobs/jobs-notifications.html

---

## 💡 **Tips & Tricks**

### **Naming Convention:**
```
[ENV] number_LayerName
[DEV] 1_Bronze_Data_Generation
[STAGING] 1_Bronze_Data_Generation
[PROD] 1_Bronze_Data_Generation
```
Benefits:
- Easy filtering by environment
- Sorted execution order
- Clear purpose

### **Tagging:**
```python
"tags": {
    "environment": "dev",
    "layer": "bronze",
    "project": "insurance-analytics",
    "owner": "data-team",
    "cost_center": "engineering"
}
```
Benefits:
- Easy searching/filtering
- Cost tracking
- Ownership clarity

### **Testing:**
1. Create jobs with `ENVIRONMENT = "test"`
2. Test thoroughly
3. Delete test jobs
4. Create real jobs

---

## 🆘 **Getting Help**

**If jobs fail:**
1. Check job run logs
2. Look for error messages
3. Verify input data exists
4. Check cluster resources
5. Review notebook code

**If script fails:**
1. Check token is valid
2. Verify network connectivity
3. Confirm workspace URL
4. Check notebook paths exist

**If schedules don't trigger:**
1. Verify schedule is not paused
2. Check timezone is correct
3. Confirm cron expression
4. Review job permissions

---

## ✅ **Checklist**

**Before running script:**
- [ ] Databricks token generated
- [ ] Token set in environment or script
- [ ] Email address updated
- [ ] Schedule/timezone configured
- [ ] Cluster size appropriate

**After creating jobs:**
- [ ] All 6 jobs visible in UI
- [ ] Job configurations verified
- [ ] Manual test run successful
- [ ] Alerts received (test failure)
- [ ] Schedules paused (verify first)

**Before enabling schedules:**
- [ ] All jobs tested manually
- [ ] Data quality checks passing
- [ ] Alert delivery confirmed
- [ ] Team notified
- [ ] Schedule times confirmed

---

## 🎉 **You're All Set!**

Your professional data pipeline is now automated with:
- ✅ 6 production-ready jobs
- ✅ Proper dependencies
- ✅ Email alerts
- ✅ Scheduled execution
- ✅ Resource optimization

**Next:** Run the jobs and watch your pipeline execute automatically! 🚀

---

**Questions? Check the logs, review this guide, or reach out!** 💬

