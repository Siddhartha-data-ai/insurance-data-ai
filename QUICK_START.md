# Quick Start Guide - Insurance Analytics DABs

## 🚀 Get Started in 5 Minutes

### Prerequisites
- Databricks workspace with Unity Catalog enabled
- Databricks CLI installed (`pip install databricks-cli`)
- Access to create catalogs and run jobs

---

## Step 1: Navigate to Project

```bash
cd /Users/kanikamondal/Databricks/insurance-data-ai
```

---

## Step 2: Configure Databricks CLI

```bash
databricks configure --token
```

Enter:
- **Host**: `https://your-workspace.cloud.databricks.com`
- **Token**: Your personal access token

---

## Step 3: Validate Bundle

```bash
databricks bundle validate -t dev
```

Expected output: `✓ Configuration is valid`

---

## Step 4: Deploy

```bash
databricks bundle deploy -t dev
```

This creates:
- ✅ 3 Unity Catalog catalogs (bronze, silver, gold)
- ✅ 21 schemas
- ✅ 2 volumes
- ✅ 1 Delta Live Tables pipeline
- ✅ 2 orchestration jobs

**Time**: ~2-3 minutes

---

## Step 5: Run Initial Data Load

```bash
databricks bundle run insurance_etl_full_refresh -t dev
```

This will:
1. Generate 1M customers (5-10 min)
2. Generate 2.5M policies (10-15 min)
3. Generate 375K claims (5-8 min)
4. Transform to silver layer (15-20 min)
5. Build gold analytics (10-15 min)

**Total time**: ~45-70 minutes

---

## Step 6: Verify

### Check in Databricks UI

1. **Catalog Explorer**: See `insurance_dev_bronze`, `insurance_dev_silver`, `insurance_dev_gold`
2. **Workflows → Jobs**: See `insurance_dev_etl_full_refresh`
3. **Workflows → Delta Live Tables**: See bronze to silver pipeline

### Check via SQL

```sql
-- Verify data
SELECT COUNT(*) FROM insurance_dev_bronze.customers.customer_raw;
SELECT COUNT(*) FROM insurance_dev_silver.customers.customer_dim WHERE is_current = true;
SELECT COUNT(*) FROM insurance_dev_gold.customer_analytics.customer_360;

-- Test security
SELECT * FROM insurance_dev_silver.customers.customer_secure LIMIT 10;

-- Fraud detection
SELECT * FROM insurance_dev_gold.claims_analytics.claims_fraud_detection 
WHERE fraud_risk_category = 'Critical' 
ORDER BY overall_fraud_score DESC 
LIMIT 10;
```

---

## What You Get

### 📊 Data
- 1,000,000 customers with realistic demographics
- 2,500,000 insurance policies across 5 product types
- 375,000 claims with fraud detection
- Complete referential integrity

### 🏛️ Architecture
- **Bronze**: Raw data from "source systems"
- **Silver**: Validated, cleaned data with SCD Type 2
- **Gold**: Business analytics and ML insights

### 🔒 Security
- **Row-Level Security**: Agent/region-based filtering
- **Column-Level Security**: PII masking, data redaction
- **Secure Views**: Dynamic filtering by user role

### 📈 Analytics
- Customer 360 view
- Fraud detection (ML + rules)
- Policy performance metrics
- Agent scorecards
- Financial summaries

---

## Common Commands

```bash
# Deploy changes
databricks bundle deploy -t dev

# Run full refresh
databricks bundle run insurance_etl_full_refresh -t dev

# Run incremental
databricks bundle run insurance_etl_incremental -t dev

# View job runs
databricks runs list --limit 10

# Check pipeline
databricks pipelines list | grep insurance

# Destroy (clean up)
databricks bundle destroy -t dev
```

---

## Sample Queries

### High-Value Churning Customers
```sql
SELECT customer_id, full_name, customer_lifetime_value, churn_risk_score
FROM insurance_dev_gold.customer_analytics.customer_360
WHERE churn_risk_category = 'High' AND value_tier = 'High Value'
ORDER BY churn_risk_score DESC;
```

### Critical Fraud Cases
```sql
SELECT claim_number, overall_fraud_score, recommended_action
FROM insurance_dev_gold.claims_analytics.claims_fraud_detection
WHERE fraud_risk_category IN ('Critical', 'High')
ORDER BY overall_fraud_score DESC;
```

### Top Performing Agents
```sql
SELECT agent_name, ytd_premium_written, performance_tier
FROM insurance_dev_gold.agent_analytics.agent_performance_scorecard
WHERE report_date = CURRENT_DATE()
ORDER BY ytd_premium_written DESC
LIMIT 10;
```

---

## Troubleshooting

### "Catalog already exists"
```bash
# Clean up first (dev only!)
databricks catalogs delete insurance_dev_bronze --cascade
databricks catalogs delete insurance_dev_silver --cascade
databricks catalogs delete insurance_dev_gold --cascade
databricks bundle deploy -t dev
```

### "Insufficient permissions"
- Verify you have CREATE CATALOG permission
- Contact workspace admin

### "Node type not available"
- Edit `databricks.yml` and change `cluster_node_type`

---

## Next Steps

1. ✅ Review `README.md` for complete documentation
2. ✅ Check `DEPLOYMENT.md` for advanced deployment options
3. ✅ Explore `PROJECT_SUMMARY.md` for feature details
4. ✅ Connect your BI tool (Power BI, Tableau)
5. ✅ Customize analytics for your needs
6. ✅ Deploy to staging/production

---

## Support

- 📖 **Full Documentation**: See `README.md`
- 🚀 **Deployment Guide**: See `DEPLOYMENT.md`
- 📊 **Project Details**: See `PROJECT_SUMMARY.md`

---

## File Structure

```
insurance-data-ai/
├── databricks.yml              # Main configuration
├── README.md                   # Full documentation
├── DEPLOYMENT.md               # Deployment guide
├── PROJECT_SUMMARY.md          # Feature summary
├── QUICK_START.md              # This file
├── resources/                  # DABs resources
│   ├── schemas/               # Catalog/schema definitions
│   ├── jobs/                  # Job orchestration
│   ├── pipelines/             # DLT pipelines
│   └── grants/                # Security grants
└── src/                       # Source code
    ├── setup/                 # DDL scripts
    ├── bronze/                # Data generation
    ├── pipelines/             # DLT notebooks
    ├── gold/                  # Analytics
    └── analytics/             # Reporting
```

---

**🎉 You're ready to go! Happy analyzing!**

