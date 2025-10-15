# ⚡ Quick Command Reference - Method 3 Deployment

## 🚀 One-Time Setup

```bash
# 1. Install Databricks CLI
brew tap databricks/tap && brew install databricks

# 2. Authenticate
databricks auth login

# 3. Navigate to project
cd /Users/kanikamondal/Databricks/insurance-data-ai

# 4. Run deployment script
./deploy_dlt_pipeline.sh
```

---

## 🔄 Regular Operations

```bash
# Deploy/Update pipeline
databricks bundle deploy -c databricks_simplified.yml -t dev

# Start pipeline (replace with your pipeline ID)
databricks pipelines start --pipeline-id $(cat .pipeline_id)

# Check pipeline status
databricks pipelines get --pipeline-id $(cat .pipeline_id)

# List all pipelines
databricks pipelines list
```

---

## 🔍 Verification Commands

```sql
-- Check customers (in Databricks SQL)
SELECT COUNT(*) FROM insurance_dev_silver.customers.customer_dim WHERE is_current = TRUE;

-- Check policies
SELECT COUNT(*) FROM insurance_dev_silver.policies.policy_fact;

-- Check claims
SELECT COUNT(*) FROM insurance_dev_silver.claims.claim_fact;

-- View quality metrics
SELECT * FROM insurance_dev_silver.customers.customer_quality_metrics;
```

---

## 🆘 Troubleshooting

```bash
# Re-authenticate
databricks auth login

# Validate configuration
databricks bundle validate -c databricks_simplified.yml -t dev

# Check logs
databricks pipelines get --pipeline-id $(cat .pipeline_id) | grep -i error
```

---

## 📍 Key Locations

- **Databricks UI Pipeline**: Workflows → Delta Live Tables → `insurance_dev_bronze_to_silver_pipeline`
- **Data Explorer**: Catalogs → `insurance_dev_silver`
- **DLT Notebooks**: Workspace → (your workspace path) → pipelines/

---

## 🎯 Expected Results

After successful deployment:
- ✅ Pipeline visible in Delta Live Tables
- ✅ 5 DLT notebooks uploaded
- ✅ Pipeline can be started from UI
- ✅ Tables created in `insurance_dev_silver` catalog

After successful execution:
- ✅ ~1M customer records in `customer_dim`
- ✅ ~2.5M policy records in `policy_fact`
- ✅ ~375K claim records in `claim_fact`
- ✅ Quality metrics tables populated
- ✅ SCD Type 2 working for customers

---

**Total Time**: ~30 minutes (first time deployment + pipeline execution)


