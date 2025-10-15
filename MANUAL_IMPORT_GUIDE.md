# Manual Import Guide - Updated with Widgets

## ✅ All files now have parameterized widgets!

You can now easily change catalog names using **Databricks widgets** without editing any code.

---

## 📋 Step-by-Step Manual Import

### Step 1: Open Databricks Workspace

1. Open your browser and go to your Databricks workspace
2. Log in to your account

---

### Step 2: Import the Project

1. Click **Workspace** in the left sidebar
2. Navigate to your home folder: `/Users/your-email@company.com/`
3. Right-click or click the **⋮** menu
4. Select **Import**
5. Click **browse** and select:
   ```
   /Users/kanikamondal/Databricks/insurance-data-ai/src
   ```
6. Click **Import**

All notebooks and SQL scripts are now imported! ✅

---

### Step 3: Create Unity Catalogs

1. Open **SQL Editor** (or create a new SQL notebook)
2. Run this SQL:

```sql
-- Create the three catalogs
CREATE CATALOG IF NOT EXISTS insurance_dev_bronze;
CREATE CATALOG IF NOT EXISTS insurance_dev_silver;
CREATE CATALOG IF NOT EXISTS insurance_dev_gold;

-- Verify creation
SHOW CATALOGS LIKE 'insurance_dev%';
```

---

### Step 4: Create Compute Cluster (if needed)

1. Click **Compute** in the left sidebar
2. Click **Create Compute**
3. Configure:
   - **Name**: `insurance-cluster`
   - **Policy**: Choose your policy (or Unrestricted)
   - **Cluster mode**: Single Node or Standard
   - **Databricks runtime**: 13.3 LTS or higher
   - **Node type**: Choose based on your needs (e.g., `m5.xlarge`)
   - **Workers**: 2-4 workers
4. Click **Create Compute**

---

### Step 5: Run Notebooks with Widgets

Navigate to **Workspace** → **src** (your imported folder)

#### 🎯 **A. Create Bronze Tables**

**File**: `setup/01_create_bronze_tables.sql`

1. Open the notebook
2. Attach to your SQL Warehouse or cluster
3. You'll see a **widget at the top**: `catalog`
4. **Widget value**: `insurance_dev_bronze` (already set as default)
5. Click **Run All** or press **Ctrl+Shift+Enter**

**Time**: ~2-3 minutes

---

#### 🎯 **B. Generate Customer Data**

**File**: `bronze/generate_customers_data.py`

1. Open the notebook
2. Attach to your compute cluster
3. You'll see a **widget at the top**: `catalog`
   - **Label**: "Bronze Catalog Name"
   - **Default value**: `insurance_dev_bronze`
4. **Change it if needed** (or leave default)
5. Click **Run All**

**Expected output**: 
```
Using catalog: insurance_dev_bronze
✅ Successfully generated and wrote 1,000,000 customer records
```

**Time**: ~5-10 minutes

---

#### 🎯 **C. Generate Policy Data**

**File**: `bronze/generate_policies_data.py`

1. Open the notebook
2. Attach to your compute cluster
3. You'll see a **widget at the top**: `catalog`
   - **Default**: `insurance_dev_bronze`
4. Click **Run All**

**Expected output**: 
```
Using catalog: insurance_dev_bronze
Loaded 1,000,000 customers
✅ Successfully generated and wrote 2,500,000 policy records
```

**Time**: ~10-15 minutes

---

#### 🎯 **D. Generate Claims Data**

**File**: `bronze/generate_claims_data.py`

1. Open the notebook
2. Attach to your compute cluster
3. You'll see a **widget at the top**: `catalog`
   - **Default**: `insurance_dev_bronze`
4. Click **Run All**

**Expected output**: 
```
Using catalog: insurance_dev_bronze
Generating 375,000 claims from 2,500,000 policies
✅ Successfully generated 375,000 claims
```

**Time**: ~5-8 minutes

---

#### 🎯 **E. Create Silver Tables**

**File**: `setup/02_create_silver_tables.sql`

1. Open the notebook
2. Attach to SQL Warehouse or cluster
3. You'll see a **widget at the top**: `catalog`
   - **Default**: `insurance_dev_silver`
4. Click **Run All**

**Time**: ~2-3 minutes

---

#### 🎯 **F. Apply Security (RLS/CLS)**

**File**: `setup/03_create_security_rls_cls.sql`

1. Open the notebook
2. Attach to SQL Warehouse or cluster
3. You'll see a **widget at the top**: `catalog`
   - **Default**: `insurance_dev_silver`
4. Click **Run All**

**Creates**:
- Security functions (masking SSN, email, phone)
- Secure views with RLS and CLS
- Audit logging table

**Time**: ~2-3 minutes

---

#### 🎯 **G. Create Gold Tables**

**File**: `setup/04_create_gold_tables.sql`

1. Open the notebook
2. Attach to SQL Warehouse or cluster
3. You'll see a **widget at the top**: `catalog`
   - **Default**: `insurance_dev_gold`
4. Click **Run All**

**Time**: ~2-3 minutes

---

#### 🎯 **H. Build Customer 360 Analytics**

**File**: `gold/build_customer_360.py`

1. Open the notebook
2. Attach to your compute cluster
3. You'll see **2 widgets at the top**:
   - `silver_catalog`: Default = `insurance_dev_silver`
   - `gold_catalog`: Default = `insurance_dev_gold`
4. Click **Run All**

**Expected output**: 
```
Using silver catalog: insurance_dev_silver
Using gold catalog: insurance_dev_gold
Loaded 1,000,000 customers
Loaded 2,500,000 policies
Loaded 375,000 claims
✅ Successfully wrote 1,000,000 customer 360 records
```

**Time**: ~10-15 minutes

---

#### 🎯 **I. Build Fraud Detection Analytics**

**File**: `gold/build_fraud_detection.py`

1. Open the notebook
2. Attach to your compute cluster
3. You'll see **2 widgets at the top**:
   - `silver_catalog`: Default = `insurance_dev_silver`
   - `gold_catalog`: Default = `insurance_dev_gold`
4. Click **Run All**

**Expected output**: 
```
Using silver catalog: insurance_dev_silver
Using gold catalog: insurance_dev_gold
Analyzing 375,000 claims for fraud detection
✅ Successfully wrote 375,000 fraud detection records
```

**Time**: ~10-15 minutes

---

### Step 6: Verify Your Data

Open **SQL Editor** and run:

```sql
-- Check bronze layer
SELECT COUNT(*) as customer_count FROM insurance_dev_bronze.customers.customer_raw;
-- Expected: 1,000,000

SELECT COUNT(*) as policy_count FROM insurance_dev_bronze.policies.policy_raw;
-- Expected: 2,500,000

SELECT COUNT(*) as claim_count FROM insurance_dev_bronze.claims.claim_raw;
-- Expected: 375,000

-- Check silver layer
SELECT COUNT(*) FROM insurance_dev_silver.customers.customer_dim WHERE is_current = true;
-- Expected: 1,000,000

-- Check gold layer
SELECT COUNT(*) FROM insurance_dev_gold.customer_analytics.customer_360;
-- Expected: 1,000,000

SELECT COUNT(*) FROM insurance_dev_gold.claims_analytics.claims_fraud_detection;
-- Expected: 375,000

-- View sample analytics
SELECT 
    customer_id,
    full_name,
    customer_lifetime_value,
    churn_risk_category,
    total_policies,
    recommended_products
FROM insurance_dev_gold.customer_analytics.customer_360
WHERE churn_risk_category = 'High'
ORDER BY customer_lifetime_value DESC
LIMIT 10;

-- View fraud cases
SELECT 
    claim_number,
    overall_fraud_score,
    fraud_risk_category,
    total_fraud_indicators,
    recommended_action
FROM insurance_dev_gold.claims_analytics.claims_fraud_detection
WHERE fraud_risk_category IN ('Critical', 'High')
ORDER BY overall_fraud_score DESC
LIMIT 10;

-- Test security (RLS/CLS)
SELECT * FROM insurance_dev_silver.customers.customer_secure LIMIT 10;
```

---

## 🎨 Understanding Widgets

### What are Widgets?

Widgets are **interactive input fields** at the top of Databricks notebooks that let you:
- ✅ Change parameters without editing code
- ✅ Reuse notebooks across environments
- ✅ Make notebooks user-friendly

### How to Use Widgets

1. **View widgets**: They appear at the top of the notebook when you open it
2. **Change values**: Click the widget and type a new value
3. **Run notebook**: The notebook uses your widget values

### Widget Examples in This Project

**Python notebooks**:
```python
# Creates a text widget named "catalog" with default "insurance_dev_bronze"
dbutils.widgets.text("catalog", "insurance_dev_bronze", "Bronze Catalog Name")

# Gets the current widget value
catalog_name = dbutils.widgets.get("catalog")
```

**SQL notebooks**:
```sql
-- Creates a text widget
CREATE WIDGET TEXT catalog DEFAULT "insurance_dev_bronze";

-- Uses the widget value
CREATE TABLE ${catalog}.customers.customer_raw (...);
```

---

## 🔄 Running for Different Environments

Want to run the same notebooks for **staging** or **prod**?

### Option 1: Change Widget Values

When opening any notebook:
1. Click the widget at the top
2. Change `insurance_dev_bronze` → `insurance_staging_bronze`
3. Click **Run All**

### Option 2: Create Copies

1. Duplicate notebooks for each environment
2. Set default widget values differently
3. Run environment-specific notebooks

---

## ⏱️ Total Time Estimate

| Step | Time | Widget |
|------|------|--------|
| Import folder | 1 min | - |
| Create catalogs | 1 min | - |
| Create bronze tables | 3 min | `catalog` |
| Generate customers | 5-10 min | `catalog` |
| Generate policies | 10-15 min | `catalog` |
| Generate claims | 5-8 min | `catalog` |
| Create silver tables | 3 min | `catalog` |
| Apply security | 3 min | `catalog` |
| Create gold tables | 3 min | `catalog` |
| Build Customer 360 | 10-15 min | `silver_catalog`, `gold_catalog` |
| Build Fraud Detection | 10-15 min | `silver_catalog`, `gold_catalog` |
| **TOTAL** | **~50-75 min** | |

---

## 💡 Tips & Best Practices

1. **Monitor Progress**: 
   - Check notebook output as it runs
   - Use SQL queries to count records in real-time

2. **Run in Background**: 
   - Start a notebook and work on other things
   - Come back to check results

3. **Save Widget Values**:
   - Databricks remembers your last widget values
   - No need to re-enter each time

4. **Troubleshooting**:
   - If a notebook fails, check the widget values
   - Ensure previous notebooks completed successfully
   - Check cluster is running and attached

5. **Cluster Usage**:
   - Attach all notebooks to the same cluster
   - No need to stop/start between notebooks
   - Cluster will auto-terminate when idle

---

## 🎯 Quick Execution Checklist

```
□ Step 1: Import src folder into Databricks workspace
□ Step 2: Create compute cluster (if needed)
□ Step 3: Create catalogs (SQL)
□ Step 4: Run setup/01_create_bronze_tables.sql
          Widget: catalog = insurance_dev_bronze
□ Step 5: Run bronze/generate_customers_data.py
          Widget: catalog = insurance_dev_bronze
□ Step 6: Run bronze/generate_policies_data.py
          Widget: catalog = insurance_dev_bronze
□ Step 7: Run bronze/generate_claims_data.py
          Widget: catalog = insurance_dev_bronze
□ Step 8: Run setup/02_create_silver_tables.sql
          Widget: catalog = insurance_dev_silver
□ Step 9: Run setup/03_create_security_rls_cls.sql
          Widget: catalog = insurance_dev_silver
□ Step 10: Run setup/04_create_gold_tables.sql
           Widget: catalog = insurance_dev_gold
□ Step 11: Run gold/build_customer_360.py
           Widgets: silver_catalog, gold_catalog
□ Step 12: Run gold/build_fraud_detection.py
           Widgets: silver_catalog, gold_catalog
□ Step 13: Verify data with SQL queries
□ Step 14: Test security views
```

---

## 🆘 Troubleshooting

### Widget Not Showing

**Solution**: Run the first cell of the notebook (the one that creates widgets)

### Wrong Catalog Name

**Solution**: Click the widget and change the value, then re-run affected cells

### Table Already Exists

**Solution**: Either drop the table or use `CREATE OR REPLACE TABLE`

### Permission Denied

**Solution**: Ensure you have CREATE CATALOG and CREATE TABLE permissions in Unity Catalog

---

## 🎉 Success!

Once complete, you'll have:

✅ **1M customers** with realistic demographics  
✅ **2.5M policies** across all product types  
✅ **375K claims** with fraud detection  
✅ **Row-level security** (RLS) implemented  
✅ **Column-level security** (CLS) with PII masking  
✅ **Customer 360** analytics with churn prediction  
✅ **Fraud detection** with ML scores  
✅ **Complete Unity Catalog** structure  

**Ready to analyze and build dashboards!** 📊

---

**Questions? Check `README.md` or `DEPLOYMENT.md` for more details.**

