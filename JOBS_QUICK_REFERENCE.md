# 🚀 Jobs Creation - Quick Reference

## ⚡ **5-Minute Setup**

### **1. Get Token (2 min)**
```
Databricks → Profile → User Settings → Access Tokens → Generate New Token
```

### **2. Set Token (30 sec)**
```bash
export DATABRICKS_TOKEN='dapi1234...'
```

### **3. Run Script (2 min)**
```bash
cd /Users/kanikamondal/Databricks/insurance-data-ai
python3 create_jobs.py
```

### **4. Verify (30 sec)**
```
Databricks → Jobs & Pipelines → See 6 new jobs
```

---

## 📋 **Jobs Created**

| # | Job Name | Notebooks | Schedule | Purpose |
|---|----------|-----------|----------|---------|
| 1 | Bronze_Data_Generation | 3 | Daily 6 AM | Generate raw data |
| 2 | Silver_Transformation | 1 | Daily 6 AM | Clean & transform |
| 3 | Gold_Analytics | 2 | Daily 6 AM | Build analytics |
| 4 | ML_Predictions | 4 | Daily 6 AM | Run ML models |
| 5 | Data_Quality_Monitoring | 1 | Daily 6 AM | Check quality |
| 6 | Pipeline_Monitoring | 1 | Daily 7 AM | Monitor pipeline |

**All start PAUSED** - you must enable schedules manually!

---

## ⚙️ **Common Edits**

### **Change Schedule**
```python
SCHEDULE_CRON = "0 6 * * *"  # Daily at 6 AM

# Every 4 hours:  "0 */4 * * *"
# Weekdays only:  "0 6 * * 1-5"
# Weekly:         "0 6 * * 1"
```

### **Change Email**
```python
ALERT_EMAIL = "your-email@example.com"
```

### **Change Environment**
```python
ENVIRONMENT = "dev"  # or "staging" or "prod"
```

### **Change Cluster Size**
```python
"num_workers": 2  # More = faster (but $$$)
```

---

## 🚀 **Running Jobs**

### **Manual Run (Testing)**
```
Jobs & Pipelines → Click job → Run now
```

### **Enable Schedule**
```
Jobs & Pipelines → Click job → Schedule tab → Resume
```

### **View Runs**
```
Jobs & Pipelines → Job Runs tab
```

---

## 🚨 **Alerts**

**Already configured:**
- ✅ Email on failure
- ✅ Email on timeout
- ✅ No spam on success

**Emails contain:**
- Error details
- Direct link to logs
- Job run summary

---

## 🔧 **Troubleshooting**

| Problem | Solution |
|---------|----------|
| "Invalid token" | Regenerate token (they expire!) |
| "Notebook not found" | Check paths in script |
| Job times out | Increase `timeout_seconds` |
| Alerts not received | Check spam folder |
| Job fails | Check logs in job run |

---

## 📊 **Recommended First Run**

Run manually in this order:
1. Bronze_Data_Generation (10 min)
2. Silver_Transformation (5 min)
3. Gold_Analytics (5 min)
4. ML_Predictions (15 min)
5. Data_Quality_Monitoring (3 min)
6. Pipeline_Monitoring (2 min)

**Total: ~40 minutes**

Then enable schedules! ✅

---

## 💡 **Pro Tips**

✅ **Start paused** - Test first, then enable schedules  
✅ **Watch first runs** - Monitor initial scheduled executions  
✅ **Check logs** - Always review logs after failures  
✅ **Right-size clusters** - Start small, scale up if needed  
✅ **Tag everything** - Makes tracking easier  

---

## 🔗 **Quick Links**

- **Jobs UI:** `{databricks-url}/#job/list`
- **Job Runs:** `{databricks-url}/#job/list` → Job Runs tab
- **Documentation:** https://docs.databricks.com/workflows/jobs/

---

## 📞 **Need Help?**

1. Check logs in job run
2. Review `JOBS_SETUP_GUIDE.md` (detailed guide)
3. Verify notebook paths exist
4. Confirm data exists for transformations

---

**Created by:** Jobs Creation Script  
**Total Setup Time:** 5 minutes  
**Total Jobs Created:** 6  
**Total Notebooks Orchestrated:** 13  

🎉 **Your pipeline is now automated!**

