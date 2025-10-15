# 🚀 Method 3 Deployment Guide: Databricks Asset Bundles

## 📋 Complete Step-by-Step Instructions

Follow these steps **in order** to deploy your DLT Pipeline using Databricks Asset Bundles.

---

## **STEP 1: Install New Databricks CLI** ⏱️ ~2 minutes

The NEW Databricks CLI (not the old one) is required for Asset Bundles.

### Option A: Using Homebrew (Recommended for macOS)

```bash
brew tap databricks/tap
brew install databricks
```

### Option B: Direct Installation

```bash
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh
```

### Verify Installation

```bash
databricks --version
```

**Expected output:** `databricks version 0.2xx.x` (version 0.200 or higher)

**If you see version 0.18.0** → That's the OLD CLI, you need the NEW one above

---

## **STEP 2: Authenticate with Databricks** ⏱️ ~1 minute

### Option A: Interactive Login (Easiest)

```bash
databricks auth login
```

This will:
1. Open your browser
2. Show available Databricks workspaces
3. Select your workspace
4. Automatically save credentials

### Option B: Manual Token Configuration

```bash
databricks configure --token
```

Then enter:
- **Host**: `https://your-workspace.cloud.databricks.com`
- **Token**: Your personal access token

**To get a token:**
1. Go to Databricks UI
2. Click your user icon (top right)
3. User Settings → Access Tokens
4. Generate New Token
5. Copy and paste when prompted

### Verify Authentication

```bash
databricks auth profiles
```

Should show your configured profile(s)

---

## **STEP 3: Navigate to Project Directory** ⏱️ ~10 seconds

```bash
cd /Users/kanikamondal/Databricks/insurance-data-ai
```

---

## **STEP 4: Run the Deployment Script** ⏱️ ~3-5 minutes

We've created an automated deployment script for you!

```bash
./deploy_dlt_pipeline.sh
```

**The script will:**
1. ✅ Check CLI installation
2. ✅ Verify authentication
3. ✅ Validate configuration
4. ✅ Upload DLT notebooks
5. ✅ Create DLT pipeline
6. ✅ Display pipeline information

**Expected output:**
```
====================================================================
🚀 DEPLOYING DLT PIPELINE WITH DATABRICKS ASSET BUNDLES
====================================================================

STEP 1: Checking Databricks CLI Installation
✅ Found: databricks version 0.xxx.x

STEP 2: Checking Databricks Authentication
✅ Authenticated with Databricks

STEP 3: Validating Bundle Configuration
✅ Configuration is valid!

STEP 4: Deploying Bundle to Databricks
Deploying...
✅ Deployment successful!

STEP 5: Pipeline Information
✅ Pipeline Created!

Pipeline ID: abc-123-def-456-789
Pipeline Name: insurance_dev_bronze_to_silver_pipeline

====================================================================
🎉 DEPLOYMENT COMPLETE!
====================================================================
```

---

## **STEP 5: Verify in Databricks UI** ⏱️ ~1 minute

1. **Open Databricks workspace** in your browser

2. **Navigate to**: Workflows → Delta Live Tables (left sidebar)

3. **You should see**: `insurance_dev_bronze_to_silver_pipeline`

4. **Click on it** to see:
   - Overview tab
   - Graph tab (visual data flow)
   - Tables tab
   - Event Log
   - Configuration

---

## **STEP 6: Start the Pipeline** ⏱️ ~15-25 minutes (pipeline execution)

### Method A: From Databricks UI (Recommended)

1. Click on `insurance_dev_bronze_to_silver_pipeline`
2. Click the **[Start]** button (top right)
3. Watch progress in the Graph tab

### Method B: From Command Line

```bash
# Get pipeline ID from saved file
PIPELINE_ID=$(cat .pipeline_id)

# Start the pipeline
databricks pipelines start --pipeline-id $PIPELINE_ID

# Check status
databricks pipelines get --pipeline-id $PIPELINE_ID
```

---

## **STEP 7: Monitor Execution** ⏱️ Active monitoring

### In Databricks UI - Graph Tab:

You'll see boxes for each table:
- 🟢 **Green** = Completed successfully
- 🔵 **Blue** = Currently running
- 🔴 **Red** = Failed
- ⚪ **Gray** = Not started yet

**Click on any box** to see:
- Row count
- Sample data
- Data quality expectations (passed/failed)
- Execution details

### Expected Flow:

```
bronze_catalog.customers.customer_raw
           ↓
    customer_raw_stream
           ↓
    customer_validated (with quality checks)
           ↓
    customer_enriched
           ↓
    customer_updates
           ↓
    customer_dim ✅ (SCD Type 2)
           ↓
    customer_quality_metrics

(Same pattern for: policies, claims, agents, payments)
```

---

## **STEP 8: Verify Results** ⏱️ ~2 minutes

After pipeline completes successfully, verify the output:

### In Databricks UI:

1. Go to **Data Explorer** (left sidebar)
2. Navigate to **Catalogs**
3. Find: `insurance_dev_silver`
4. You should see schemas:
   - `customers` (with `customer_dim`, `customer_quality_metrics`)
   - `policies` (with `policy_fact`, `policy_quality_metrics`)
   - `claims` (with `claim_fact`, `claim_quality_metrics`)
   - `agents` (with `agent_dim`, `agent_quality_metrics`)
   - `payments` (with `payment_fact`, `payment_quality_metrics`)

### Via SQL Queries:

```sql
-- Check customers
SELECT COUNT(*) as total_customers 
FROM insurance_dev_silver.customers.customer_dim 
WHERE is_current = TRUE;
-- Expected: ~1,000,000 rows

-- Check SCD Type 2 is working
SELECT 
    customer_id,
    email,
    is_current,
    effective_start_date,
    effective_end_date,
    record_version
FROM insurance_dev_silver.customers.customer_dim
WHERE customer_id = 'CUST0000000001'
ORDER BY effective_start_date;
-- Should show version history if customer has changes

-- Check policies
SELECT COUNT(*) as total_policies 
FROM insurance_dev_silver.policies.policy_fact;
-- Expected: ~2,500,000 rows

-- Check claims
SELECT COUNT(*) as total_claims 
FROM insurance_dev_silver.claims.claim_fact;
-- Expected: ~375,000 rows

-- Check agents
SELECT COUNT(*) as total_agents 
FROM insurance_dev_silver.agents.agent_dim;
-- Expected: ~5,000 rows

-- View quality metrics
SELECT * FROM insurance_dev_silver.customers.customer_quality_metrics;
SELECT * FROM insurance_dev_silver.claims.claim_quality_metrics;
```

---

## **🎉 SUCCESS! You've Deployed with Method 3!**

### **What You've Accomplished:**

✅ Installed new Databricks CLI  
✅ Authenticated with Databricks  
✅ Deployed using Infrastructure as Code (IaC)  
✅ Created DLT pipeline with 5 notebooks  
✅ Uploaded notebooks to workspace  
✅ Ran the pipeline successfully  
✅ Verified data in silver layer  
✅ SCD Type 2 implemented and working  

---

## **🔄 Future Updates**

### To Update the Pipeline:

1. **Make changes** to your DLT notebooks or configuration
2. **Redeploy**:
   ```bash
   databricks bundle deploy -c databricks_simplified.yml -t dev
   ```
3. **Run pipeline** again to see changes

### To Deploy to Other Environments:

```bash
# Staging
databricks bundle deploy -c databricks_simplified.yml -t staging

# Production
databricks bundle deploy -c databricks_simplified.yml -t prod
```

---

## **❓ Troubleshooting**

### Issue: "Bundle validation failed"

**Fix:** Make sure you're in the project directory:
```bash
cd /Users/kanikamondal/Databricks/insurance-data-ai
```

### Issue: "Authentication required"

**Fix:** Re-authenticate:
```bash
databricks auth login
```

### Issue: "Pipeline fails - table not found"

**Fix:** Make sure Bronze layer data exists first. Run:
1. `src/bronze/generate_customers_data.py` (on regular cluster)
2. `src/bronze/generate_policies_data.py` (on regular cluster)
3. `src/bronze/generate_claims_data.py` (on regular cluster)

### Issue: "Permission denied on catalog"

**Fix:** Create catalogs first:
```sql
CREATE CATALOG IF NOT EXISTS insurance_dev_bronze;
CREATE CATALOG IF NOT EXISTS insurance_dev_silver;

GRANT ALL PRIVILEGES ON CATALOG insurance_dev_bronze TO `your-email@domain.com`;
GRANT ALL PRIVILEGES ON CATALOG insurance_dev_silver TO `your-email@domain.com`;
```

### Issue: Pipeline runs but tables are empty

**Fix:** Check Event Log in DLT UI for errors. Usually:
- Data quality expectations are too strict
- Bronze tables don't have data
- Schema mismatch

---

## **📚 Next Steps**

After successful DLT pipeline deployment:

1. ✅ **Run Gold Layer Analytics**
   - `src/gold/build_customer_360.py`
   - `src/gold/build_fraud_detection.py`

2. ✅ **Run ML Models**
   - `src/ml/run_all_predictions.py`

3. ✅ **Set up Monitoring**
   - `src/analytics/data_quality_monitoring.py`

4. ✅ **Deploy to Staging/Production**
   - Follow same steps with `-t staging` or `-t prod`

---

## **🆘 Need Help?**

If you encounter issues:

1. Check the **Event Log** in DLT UI
2. Review **databricks_simplified.yml** configuration
3. Verify **Bronze layer has data**
4. Check **Unity Catalog permissions**
5. Review **pipeline configuration** in UI

---

**🎊 Congratulations! You've successfully deployed using Method 3 (Databricks Asset Bundles)!**

This is the modern, professional way to manage Databricks resources. You now have:
- ✅ Version-controlled infrastructure
- ✅ Reproducible deployments
- ✅ Multi-environment ready
- ✅ Production-grade setup

**Welcome to the world of Infrastructure as Code!** 🚀


