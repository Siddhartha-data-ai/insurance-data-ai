# Deployment Guide - Insurance Analytics Platform

## Quick Start

### 1. Prerequisites Check

```bash
# Check Databricks CLI version
databricks --version
# Should be >= 0.200.0

# Check authentication
databricks workspace list
# Should list your workspace folders

# Verify Unity Catalog access
databricks catalogs list
# Should return catalogs you have access to
```

### 2. Configure Variables

Create a file `config/dev.json` (or use environment variables):

```json
{
  "workspace_url": "https://your-workspace.cloud.databricks.com",
  "catalog_prefix": "insurance_dev",
  "storage_location": "s3://your-bucket/insurance-analytics",
  "cluster_node_type": "i3.xlarge",
  "service_principal_id": "your-sp-id",
  "sql_warehouse_id": "your-warehouse-id"
}
```

### 3. Deploy the Bundle

```bash
# Navigate to project directory
cd /Users/kanikamondal/Databricks/insurance-data-ai

# Validate configuration
databricks bundle validate -t dev

# Deploy all resources
databricks bundle deploy -t dev
```

**What happens during deployment:**
- ✅ Creates Unity Catalog catalogs (bronze, silver, gold)
- ✅ Creates schemas in each catalog
- ✅ Creates external volumes for documents
- ✅ Uploads all notebooks and SQL scripts
- ✅ Creates Delta Live Tables pipelines
- ✅ Creates orchestration jobs
- ✅ Applies initial security grants

---

## Step-by-Step Deployment

### Step 1: Create Unity Catalog Structure

```bash
# This is included in bundle deploy, but can be run separately
databricks bundle deploy -t dev --target-only resources.catalogs
```

### Step 2: Upload Source Code

```bash
# Upload notebooks and scripts
databricks bundle deploy -t dev --target-only artifacts
```

### Step 3: Create Delta Live Tables Pipeline

```bash
# Create DLT pipeline
databricks bundle deploy -t dev --target-only resources.pipelines
```

### Step 4: Create Jobs

```bash
# Create orchestration jobs
databricks bundle deploy -t dev --target-only resources.jobs
```

### Step 5: Run Initial Setup

#### Option A: Run via Databricks UI
1. Go to Workflows → Jobs
2. Find `insurance_dev_etl_full_refresh`
3. Click "Run now"

#### Option B: Run via CLI
```bash
databricks bundle run insurance_etl_full_refresh -t dev
```

---

## Monitoring Deployment

### Check Job Status

```bash
# List all jobs
databricks jobs list | grep insurance

# Get job details
databricks jobs get --job-id <job-id>

# View recent runs
databricks runs list --limit 10
```

### Check Pipeline Status

```bash
# List pipelines
databricks pipelines list | grep insurance

# Get pipeline details
databricks pipelines get --pipeline-id <pipeline-id>

# View pipeline updates
databricks pipelines list-updates --pipeline-id <pipeline-id>
```

### Check Data

```sql
-- Check bronze layer
SELECT COUNT(*) FROM insurance_dev_bronze.customers.customer_raw;
SELECT COUNT(*) FROM insurance_dev_bronze.policies.policy_raw;
SELECT COUNT(*) FROM insurance_dev_bronze.claims.claim_raw;

-- Check silver layer
SELECT COUNT(*) FROM insurance_dev_silver.customers.customer_dim;
SELECT COUNT(*) FROM insurance_dev_silver.policies.policy_fact;
SELECT COUNT(*) FROM insurance_dev_silver.claims.claim_fact;

-- Check gold layer
SELECT COUNT(*) FROM insurance_dev_gold.customer_analytics.customer_360;
SELECT COUNT(*) FROM insurance_dev_gold.claims_analytics.claims_fraud_detection;
```

---

## Troubleshooting

### Common Issues

#### 1. Catalog Already Exists

**Error:** `Catalog 'insurance_dev_bronze' already exists`

**Solution:**
```bash
# Drop existing catalogs (dev only!)
databricks catalogs delete insurance_dev_bronze --cascade
databricks catalogs delete insurance_dev_silver --cascade
databricks catalogs delete insurance_dev_gold --cascade

# Redeploy
databricks bundle deploy -t dev
```

#### 2. Insufficient Permissions

**Error:** `User does not have permission to create catalog`

**Solution:**
- Verify your user has `CREATE CATALOG` permission in Unity Catalog
- Contact your workspace admin to grant permissions
- Alternatively, use an existing catalog and modify `catalog_prefix`

#### 3. Cluster Node Type Not Available

**Error:** `Node type 'i3.xlarge' is not available in this region`

**Solution:**
Edit `databricks.yml` and change `cluster_node_type`:
```yaml
variables:
  cluster_node_type:
    default: "m5.xlarge"  # Or another available type
```

#### 4. Storage Location Access Denied

**Error:** `Cannot access s3://your-bucket/insurance-analytics`

**Solution:**
- Verify your workspace has access to the S3 bucket
- Check IAM roles and policies
- Update `storage_location` to an accessible path

#### 5. SQL Warehouse Not Found

**Error:** `SQL Warehouse with id 'xxx' not found`

**Solution:**
```bash
# List available warehouses
databricks sql warehouses list

# Update variable with correct warehouse ID
```

---

## Data Generation Time Estimates

| Task | Records | Estimated Time | Cluster Size |
|------|---------|---------------|--------------|
| Generate Customers | 1,000,000 | 5-10 minutes | 2 workers |
| Generate Policies | 2,500,000 | 10-15 minutes | 2 workers |
| Generate Claims | 375,000 | 5-8 minutes | 2 workers |
| Bronze to Silver DLT | All tables | 15-20 minutes | 2 workers |
| Build Gold Analytics | All tables | 10-15 minutes | 4 workers |
| **Total** | | **45-70 minutes** | |

---

## Production Deployment

### Pre-Production Checklist

- [ ] All dev tests passed
- [ ] Data quality validation completed
- [ ] Security rules tested
- [ ] Performance benchmarks met
- [ ] Documentation updated
- [ ] User access reviewed
- [ ] Backup strategy defined
- [ ] Monitoring configured
- [ ] Incident response plan ready

### Deploy to Production

```bash
# Deploy to production
databricks bundle deploy -t prod

# Run production data load (during maintenance window)
databricks bundle run insurance_etl_full_refresh -t prod
```

### Production Configuration Differences

| Setting | Dev | Prod |
|---------|-----|------|
| Catalog Prefix | `insurance_dev` | `insurance_prod` |
| Min/Max Workers | 1-2 | 2-10 |
| Schedule Status | PAUSED | ACTIVE |
| DLT Mode | Development | Production |
| Run As | User | Service Principal |
| Auto Optimization | Enabled | Enabled |
| Cluster Policy | Unrestricted | Restricted |

---

## Rollback Procedure

### If Deployment Fails

```bash
# Destroy current deployment
databricks bundle destroy -t dev

# Restore from previous version (if versioned)
git checkout <previous-commit>

# Redeploy
databricks bundle deploy -t dev
```

### If Data Issues Occur

```sql
-- Delta Lake time travel
SELECT * FROM insurance_dev_bronze.customers.customer_raw 
VERSION AS OF 1;

-- Restore previous version
RESTORE TABLE insurance_dev_bronze.customers.customer_raw 
TO VERSION AS OF 1;
```

---

## Performance Tuning

### Optimize Tables

```sql
-- Optimize bronze tables
OPTIMIZE insurance_dev_bronze.customers.customer_raw 
ZORDER BY (customer_id, state_code);

OPTIMIZE insurance_dev_bronze.policies.policy_raw 
ZORDER BY (policy_id, customer_id);

OPTIMIZE insurance_dev_bronze.claims.claim_raw 
ZORDER BY (claim_id, policy_id);

-- Analyze statistics
ANALYZE TABLE insurance_dev_bronze.customers.customer_raw 
COMPUTE STATISTICS FOR ALL COLUMNS;
```

### Enable Auto-Optimization

```sql
-- Already configured in table properties
ALTER TABLE insurance_dev_bronze.customers.customer_raw 
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true'
);
```

---

## Scaling Considerations

### For Larger Data Volumes

If scaling to 10M+ customers, 25M+ policies:

1. **Increase Cluster Size**
   ```yaml
   max_workers: 20
   cluster_node_type: "i3.2xlarge"
   ```

2. **Partition Strategy**
   - Bronze: Partition by ingestion_date, state_code
   - Silver: Partition by state_code, is_current (for dimensions)
   - Gold: Partition by report_date, region

3. **Use Liquid Clustering**
   ```sql
   ALTER TABLE ... CLUSTER BY (customer_id, state_code);
   ```

4. **Enable Photon**
   ```yaml
   photon: true
   ```

---

## Security Hardening

### Before Production

1. **Review Grants**
   ```sql
   SHOW GRANTS ON CATALOG insurance_prod_silver;
   SHOW GRANTS ON TABLE insurance_prod_silver.customers.customer_dim;
   ```

2. **Test RLS**
   ```sql
   -- Test as different users
   SELECT * FROM insurance_prod_silver.customers.customer_secure;
   ```

3. **Audit Logging**
   ```sql
   SELECT * FROM insurance_prod_silver.master_data.data_access_audit
   WHERE access_timestamp >= CURRENT_DATE()
   ORDER BY access_timestamp DESC;
   ```

4. **Enable Encryption**
   - S3: Server-side encryption enabled
   - Volumes: Encryption at rest
   - Network: TLS in transit

---

## Maintenance

### Daily
- Monitor job execution
- Check data quality metrics
- Review error logs

### Weekly
- Run OPTIMIZE on frequently accessed tables
- Review storage costs
- Update statistics (ANALYZE TABLE)

### Monthly
- Review security audit logs
- Update user permissions
- Performance tuning review
- Capacity planning

---

## Support Contacts

- **Databricks Support**: support@databricks.com
- **Project Lead**: [Your Name]
- **Data Engineering Team**: data-engineering@your-company.com
- **On-Call**: data-engineering-oncall@your-company.com

---

## Additional Resources

- [Databricks CLI Reference](https://docs.databricks.com/dev-tools/cli/index.html)
- [Unity Catalog Best Practices](https://docs.databricks.com/data-governance/unity-catalog/best-practices.html)
- [Delta Live Tables Guide](https://docs.databricks.com/workflows/delta-live-tables/delta-live-tables-cookbook.html)
- [Performance Tuning](https://docs.databricks.com/optimizations/index.html)

---

**Last Updated**: October 2025  
**Version**: 1.0.0

