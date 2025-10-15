# 🌍 AI Chatbot - Multi-Environment Guide

## ✅ **Environment Support Added!**

Your chatbot now works across **dev, staging, and prod** environments!

---

## 🎯 **What Changed?**

### **Before (Environment-Specific):**
```python
# Hardcoded to dev environment
FROM insurance_dev_gold.predictions.customer_churn_risk
```
❌ Only worked with dev catalogs

### **After (Environment-Aware):**
```python
# Dynamic catalog selection
FROM {gold_catalog}.predictions.customer_churn_risk
```
✅ Works with any environment!

---

## 📂 **Catalog Naming Convention**

Your catalogs follow this pattern:

| Environment | Bronze Catalog | Silver Catalog | Gold Catalog |
|-------------|---------------|----------------|--------------|
| **Dev** | `insurance_dev_bronze` | `insurance_dev_silver` | `insurance_dev_gold` |
| **Staging** | `insurance_staging_bronze` | `insurance_staging_silver` | `insurance_staging_gold` |
| **Prod** | `insurance_prod_bronze` | `insurance_prod_silver` | `insurance_prod_gold` |

The chatbot uses:
- **Gold Catalog** for predictions (ML outputs)
- **Silver Catalog** for source data (if needed)

---

## 🚀 **How to Use**

### **Step 1: Open the Chatbot**

Navigate to:
```
/Workspace/Shared/insurance-analytics/chatbot/insurance_chatbot_native
```

Or your personal copy:
```
/Workspace/Users/siddhartha013@gmail.com/insurance-analytics/chatbot/insurance_chatbot_native
```

---

### **Step 2: Select Environment**

At the **top of the notebook**, you'll see **3 new dropdown widgets:**

```
┌─────────────────────────────────────────────────┐
│ 🌍 Environment:           [dev ▼]               │
│ Gold Catalog (Predictions): [insurance_dev_gold ▼] │
│ Silver Catalog (Source):   [insurance_dev_silver ▼] │
└─────────────────────────────────────────────────┘
```

**Change the environment:**
1. Click the **Environment** dropdown
2. Select: `dev`, `staging`, or `prod`
3. The catalog dropdowns will auto-update (or manually select)

---

### **Step 3: Run All Cells**

1. Click **"Run All"**
2. Wait 30-60 seconds
3. Chatbot is ready!

---

### **Step 4: Ask Questions**

The chatbot automatically uses your selected environment!

```
🔍 Processing: Show me the executive summary
🌍 Environment: STAGING
══════════════════════════════════════════════════
```

---

## 🎯 **Use Cases**

### **Scenario 1: Testing in Dev**
```
1. Select: Environment = "dev"
2. Catalogs: insurance_dev_*
3. Test new predictions safely
```

### **Scenario 2: Validating in Staging**
```
1. Select: Environment = "staging"
2. Catalogs: insurance_staging_*
3. Verify before promoting to prod
```

### **Scenario 3: Production Analytics**
```
1. Select: Environment = "prod"
2. Catalogs: insurance_prod_*
3. Generate real business insights
```

### **Scenario 4: Comparing Environments**
```
1. Run chatbot with "dev"
2. Note the results
3. Change to "prod"
4. Re-run same question
5. Compare the outputs!
```

---

## 📊 **Widget Details**

### **🌍 Environment Dropdown**
- **Purpose**: Quick label for current environment
- **Options**: dev, staging, prod
- **Default**: dev
- **Note**: Mainly for display and context

### **📊 Gold Catalog Dropdown**
- **Purpose**: Where to find ML predictions
- **Options**: 
  - insurance_dev_gold
  - insurance_staging_gold
  - insurance_prod_gold
- **Default**: insurance_dev_gold
- **Usage**: All prediction queries use this catalog

### **📁 Silver Catalog Dropdown**
- **Purpose**: Where to find source data (if needed)
- **Options**: 
  - insurance_dev_silver
  - insurance_staging_silver
  - insurance_prod_silver
- **Default**: insurance_dev_silver
- **Usage**: For future enhancements requiring source data

---

## 🔧 **How It Works Internally**

### **1. Environment Configuration Cell**
```python
# Create dropdown widgets
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"])
dbutils.widgets.dropdown("gold_catalog", "insurance_dev_gold", [...])
dbutils.widgets.dropdown("silver_catalog", "insurance_dev_silver", [...])

# Read selected values
gold_catalog = dbutils.widgets.get("gold_catalog")
silver_catalog = dbutils.widgets.get("silver_catalog")
environment = dbutils.widgets.get("environment")
```

### **2. Dynamic SQL Generation**
```python
def generate_sql_query(intent, params, gold_catalog, silver_catalog):
    if intent == 'churn':
        return f"""
        SELECT * FROM {gold_catalog}.predictions.customer_churn_risk
        WHERE churn_risk_category = 'High Risk'
        """
```

### **3. Response Generation**
```python
response = generate_response(
    user_input, 
    gold_catalog,      # Uses selected gold catalog
    silver_catalog,    # Uses selected silver catalog
    environment        # For display/logging
)
```

---

## ✅ **Testing Different Environments**

### **Test 1: Dev Environment**
```
1. Set Environment: "dev"
2. Set Gold Catalog: "insurance_dev_gold"
3. Ask: "Show me the executive summary"
4. Verify: See dev prediction data
```

### **Test 2: Staging Environment**
```
1. Set Environment: "staging"
2. Set Gold Catalog: "insurance_staging_gold"
3. Ask: "Show me the executive summary"
4. Verify: See staging prediction data
```

### **Test 3: Prod Environment**
```
1. Set Environment: "prod"
2. Set Gold Catalog: "insurance_prod_gold"
3. Ask: "Show me the executive summary"
4. Verify: See production prediction data
```

---

## 🔄 **Switching Environments**

### **Method 1: Use Environment Dropdown**
```
1. Change "Environment" dropdown
2. Catalog dropdowns auto-suggest matching catalogs
3. Or manually select catalogs
4. Re-run "Process User Input" cell
```

### **Method 2: Direct Catalog Selection**
```
1. Directly change "Gold Catalog" dropdown
2. Select the target environment's catalog
3. Re-run "Process User Input" cell
```

**Important:** Always **re-run the Process User Input cell** after changing widgets!

---

## 📋 **Environment Setup Checklist**

Before using the chatbot in a specific environment:

### **Dev Environment ✅**
- [x] Catalogs exist: insurance_dev_*
- [x] ML predictions run successfully
- [x] Prediction tables populated
- [x] Chatbot tested and working

### **Staging Environment** (If you create it)
- [ ] Create catalogs: insurance_staging_*
- [ ] Run bronze data generation (with staging catalog)
- [ ] Run silver transformations (with staging catalog)
- [ ] Run gold notebooks (with staging catalog)
- [ ] Run ML predictions (with staging catalogs)
- [ ] Test chatbot with staging

### **Prod Environment** (If you create it)
- [ ] Create catalogs: insurance_prod_*
- [ ] Set up production data pipelines
- [ ] Schedule regular data refresh
- [ ] Run ML predictions on schedule
- [ ] Configure chatbot for prod access
- [ ] Set up monitoring and alerts

---

## 🎨 **Visual Indicators**

The chatbot now shows your current environment:

### **In Processing Output:**
```
🔍 Processing: Show me high-risk customers
🌍 Environment: STAGING
══════════════════════════════════════════════════
```

### **In Welcome Screen:**
```
┌─────────────────────────────────────────────────┐
│ 🤖 Insurance Analytics AI                       │
│                                                 │
│ 🌍 Current Environment: STAGING                 │
│ 📊 Gold Catalog: insurance_staging_gold         │
│ 📁 Silver Catalog: insurance_staging_silver     │
└─────────────────────────────────────────────────┘
```

---

## ⚠️ **Important Notes**

### **Catalog Must Exist**
If you select a catalog that doesn't exist, you'll get errors:
```
Error: Table 'insurance_prod_gold.predictions.customer_churn_risk' not found
```

**Solution:** Only select environments/catalogs that you've actually created!

---

### **Predictions Must Be Populated**
Empty prediction tables will return 0 metrics (handled gracefully):
```
┌─────────────────┐
│ 🔴 High Risk    │
│      0          │
│ $0 at risk      │
└─────────────────┘
```

**Solution:** Run ML prediction notebooks for that environment first!

---

### **Consistent Environment Selection**
Make sure your catalog selections match your environment:
```
✅ Good:
   Environment: staging
   Gold Catalog: insurance_staging_gold

❌ Bad:
   Environment: prod
   Gold Catalog: insurance_dev_gold  (mismatched!)
```

---

## 🚀 **Best Practices**

### **1. Start with Dev**
Always test new questions in dev first:
```
dev → test → verify → move to staging
```

### **2. Validate in Staging**
Before using in prod, validate in staging:
```
staging → validate → compare with dev → approve for prod
```

### **3. Use Prod Carefully**
Only use prod for actual business decisions:
```
prod → real data → real insights → real actions
```

### **4. Document Environment Differences**
Keep track of what's different between environments:
```
Dev:     1,000 customers, test data
Staging: 10,000 customers, historical data
Prod:    50,000 customers, live data
```

---

## 🔧 **Troubleshooting**

### **Issue 1: "Table not found" error**
```
Error: Table 'insurance_staging_gold.predictions.customer_churn_risk' not found
```

**Solutions:**
1. Check if staging catalogs exist: `SHOW CATALOGS LIKE 'insurance_staging%'`
2. Create catalogs if missing (run setup notebooks with staging configs)
3. Run ML predictions to populate tables
4. Or switch back to dev environment

---

### **Issue 2: All metrics show 0**
```
Executive Summary shows all zeros
```

**Solutions:**
1. Check if prediction tables are empty: `SELECT COUNT(*) FROM {catalog}.predictions.*`
2. Run ML prediction notebooks for that environment
3. Verify data pipeline completed successfully
4. Check for errors in ML notebook runs

---

### **Issue 3: Wrong environment data**
```
Selected "prod" but seeing dev data
```

**Solutions:**
1. Verify catalog selection matches environment
2. Re-run the "Environment Configuration" cell
3. Re-run the "Process User Input" cell
4. Check widget values at top of notebook

---

### **Issue 4: Widgets don't show**
```
No dropdown widgets visible
```

**Solutions:**
1. Run the "Environment Configuration" cell
2. Run the "Quick Actions" cell
3. Refresh the notebook page
4. Check cluster is attached and running

---

## 📚 **Summary**

### **What You Can Do Now:**
✅ Use chatbot with dev, staging, or prod  
✅ Switch environments on the fly  
✅ Compare results across environments  
✅ Test safely before production  
✅ Get environment-specific insights  

### **How to Use:**
1. Select environment/catalogs (widgets at top)
2. Run All cells
3. Ask questions
4. Get environment-specific answers!

### **Key Points:**
- **Gold catalog** = where predictions live
- **Silver catalog** = where source data lives (future use)
- **Environment** = label for context
- **Always re-run** Process User Input cell after changing environment

---

## 🎉 **You're Ready!**

Your chatbot now works across all environments!

**Try it:**
1. Open the chatbot notebook
2. Select your environment
3. Run All
4. Ask: "Show me the executive summary"
5. See environment-specific results! 🚀

---

**Questions? The chatbot is environment-aware and ready to answer them!** 🤖

