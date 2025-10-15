# Delta Live Tables (DLT) Deployment Guide

## Overview
This project now has **dual ETL implementations**:
1. **PySpark Implementation** (`src/transformations/transform_bronze_to_silver.py`) - Manual SCD Type 2
2. **DLT Implementation** (`src/pipelines/`) - Native Databricks DLT with built-in SCD Type 2

Both work independently and are fully functional!

## DLT Pipeline Structure

### Created DLT Notebooks
The following 5 DLT notebooks have been created in `src/pipelines/`:

1. **bronze_to_silver_customers.py**
   - Customer dimension with SCD Type 2
   - Email validation, credit score checks
   - Customer segmentation and enrichment
   - Uses `dlt.apply_changes()` with `stored_as_scd_type="2"`

2. **bronze_to_silver_policies.py**
   - Policy fact table
   - Premium calculations, earned/unearned premium
   - Policy status tracking
   - Product categorization

3. **bronze_to_silver_claims.py**
   - Claims fact table with fraud indicators
   - 6 fraud detection flags
   - Enhanced fraud scoring
   - Claim severity classification

4. **bronze_to_silver_agents.py**
   - Agent dimension with hierarchy
   - Performance metrics and tiers
   - Territory management
   - Commission tracking

5. **bronze_to_silver_payments.py**
   - Payment fact table
   - Transaction fee calculations
   - Late payment tracking
   - Payment method analytics

## DLT Pipeline Configuration

The pipeline is defined in `resources/pipelines/bronze_to_silver_dlt.yml`:

```yaml
resources:
  pipelines:
    insurance_dlt_bronze_to_silver:
      name: insurance_${var.env}_bronze_to_silver_pipeline
      target: ${var.catalog_prefix}_silver
      libraries:
        - notebook:
            path: ../src/pipelines/bronze_to_silver_customers.py
        - notebook:
            path: ../src/pipelines/bronze_to_silver_policies.py
        - notebook:
            path: ../src/pipelines/bronze_to_silver_claims.py
        - notebook:
            path: ../src/pipelines/bronze_to_silver_agents.py
        - notebook:
            path: ../src/pipelines/bronze_to_silver_payments.py
```

## Sync to Databricks Workspace

### Method 1: Using Databricks Asset Bundles (DABs) - RECOMMENDED

```bash
# Navigate to project directory
cd /Users/kanikamondal/Databricks/insurance-data-ai

# Validate the bundle
databricks bundle validate -e dev

# Deploy to Databricks (this syncs all files)
databricks bundle deploy -e dev

# View deployed resources
databricks bundle summary -e dev
```

This will:
- ✅ Upload all 5 DLT notebooks to Databricks workspace
- ✅ Create/update the DLT pipeline configuration
- ✅ Set up all schemas, catalogs, and resources
- ✅ Configure permissions and grants

### Method 2: Manual Upload (Alternative)

If you prefer manual upload:

1. **Open Databricks Workspace**
2. **Navigate to Workspace > Shared**
3. **Create folder**: `insurance_data_ai/src/pipelines`
4. **Upload each notebook**:
   - `bronze_to_silver_customers.py`
   - `bronze_to_silver_policies.py`
   - `bronze_to_silver_claims.py`
   - `bronze_to_silver_agents.py`
   - `bronze_to_silver_payments.py`

5. **Create DLT Pipeline**:
   - Go to **Workflows > Delta Live Tables**
   - Click **Create Pipeline**
   - Name: `insurance_dev_bronze_to_silver_pipeline`
   - Add all 5 notebooks as libraries
   - Set target: `insurance_dev_silver`
   - Configuration:
     ```
     bronze_catalog: insurance_dev_bronze
     silver_catalog: insurance_dev_silver
     pipeline.environment: dev
     ```

### Method 3: Databricks CLI (Individual Files)

```bash
# Create directory in workspace
databricks workspace mkdirs /Shared/insurance_data_ai/src/pipelines

# Upload each notebook
databricks workspace import src/pipelines/bronze_to_silver_customers.py \
  /Shared/insurance_data_ai/src/pipelines/bronze_to_silver_customers.py \
  --language PYTHON --format SOURCE

databricks workspace import src/pipelines/bronze_to_silver_policies.py \
  /Shared/insurance_data_ai/src/pipelines/bronze_to_silver_policies.py \
  --language PYTHON --format SOURCE

databricks workspace import src/pipelines/bronze_to_silver_claims.py \
  /Shared/insurance_data_ai/src/pipelines/bronze_to_silver_claims.py \
  --language PYTHON --format SOURCE

databricks workspace import src/pipelines/bronze_to_silver_agents.py \
  /Shared/insurance_data_ai/src/pipelines/bronze_to_silver_agents.py \
  --language PYTHON --format SOURCE

databricks workspace import src/pipelines/bronze_to_silver_payments.py \
  /Shared/insurance_data_ai/src/pipelines/bronze_to_silver_payments.py \
  --language PYTHON --format SOURCE
```

## Running the DLT Pipeline

### Option A: Using Databricks UI
1. Go to **Workflows > Delta Live Tables**
2. Find pipeline: `insurance_dev_bronze_to_silver_pipeline`
3. Click **Start**

### Option B: Using Databricks CLI
```bash
# Start the pipeline
databricks pipelines start --pipeline-id <pipeline-id>

# Get pipeline ID
databricks pipelines list | grep insurance_dev_bronze_to_silver
```

### Option C: Using the ETL Orchestration Job
The pipeline is already integrated in your orchestration job:
```bash
databricks bundle run insurance_etl_full_refresh -e dev
```

## Key Features of DLT Implementation

### 1. Built-in Data Quality
```python
@dlt.expect_or_drop("valid_email", "email RLIKE '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$'")
@dlt.expect_or_fail("valid_state", "state_code IS NOT NULL AND LENGTH(state_code) = 2")
@dlt.expect("valid_credit_score", "credit_score BETWEEN 300 AND 850")
```

### 2. Native SCD Type 2
```python
dlt.apply_changes(
    target="customer_dim",
    source="customer_updates",
    keys=["customer_id"],
    sequence_by=F.col("updated_timestamp"),
    stored_as_scd_type="2",
    track_history_column_list=["first_name", "last_name", "email", ...]
)
```

### 3. Automatic Monitoring
- Data quality metrics tracked automatically
- Pipeline lineage visible in UI
- Detailed error reporting

### 4. Change Data Feed
All tables have CDC enabled:
```python
table_properties={
    "delta.enableChangeDataFeed": "true"
}
```

## Comparison: PySpark vs DLT

| Feature | PySpark (Existing) | DLT (New) |
|---------|-------------------|-----------|
| **Location** | `src/transformations/` | `src/pipelines/` |
| **SCD Type 2** | Manual merge logic | Native `dlt.apply_changes()` |
| **Data Quality** | Manual checks | Declarative expectations |
| **Monitoring** | Custom code | Built-in lineage & metrics |
| **Orchestration** | Spark jobs | DLT pipeline |
| **Cost** | Standard compute | Optimized DLT clusters |
| **Debugging** | Standard logs | Enhanced DLT UI |

## Verification After Deployment

### 1. Check DLT Pipeline Status
```sql
-- In Databricks SQL
SELECT * FROM event_log('<pipeline-id>')
ORDER BY timestamp DESC
LIMIT 100;
```

### 2. Verify Silver Tables Created
```sql
SHOW TABLES IN insurance_dev_silver.customers;
SHOW TABLES IN insurance_dev_silver.policies;
SHOW TABLES IN insurance_dev_silver.claims;
SHOW TABLES IN insurance_dev_silver.agents;
SHOW TABLES IN insurance_dev_silver.payments;
```

### 3. Check SCD Type 2 Implementation
```sql
-- Verify customer history tracking
SELECT 
    customer_id,
    first_name,
    email,
    __START_AT,
    __END_AT,
    is_current
FROM insurance_dev_silver.customers.customer_dim
WHERE customer_id = 'CUST001'
ORDER BY __START_AT DESC;
```

### 4. Data Quality Metrics
```sql
SELECT * FROM insurance_dev_silver.customers.customer_quality_metrics;
SELECT * FROM insurance_dev_silver.claims.claim_quality_metrics;
SELECT * FROM insurance_dev_silver.payments.payment_quality_metrics;
```

## Troubleshooting

### Issue: "Table not found" error
**Solution**: Ensure Bronze layer tables exist first
```bash
databricks bundle run generate_bronze_data -e dev
```

### Issue: "Permission denied"
**Solution**: Check Unity Catalog permissions
```sql
GRANT USAGE ON CATALOG insurance_dev_bronze TO `your-user@domain.com`;
GRANT USAGE ON CATALOG insurance_dev_silver TO `your-user@domain.com`;
```

### Issue: DLT pipeline fails on startup
**Solution**: Check cluster configuration in `resources/pipelines/bronze_to_silver_dlt.yml`

## Next Steps

1. ✅ **Deploy to Databricks** using Method 1 (DABs)
2. ✅ **Run the DLT Pipeline** to populate Silver layer
3. ✅ **Verify data quality** using the quality metrics tables
4. ✅ **Monitor pipeline** in DLT UI
5. ✅ **Schedule regular runs** via orchestration job

## Resources
- [Delta Live Tables Documentation](https://docs.databricks.com/delta-live-tables/index.html)
- [SCD Type 2 with DLT](https://docs.databricks.com/delta-live-tables/cdc.html)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)

---
**Status**: ✅ All DLT notebooks created and ready for deployment
**Last Updated**: October 15, 2025

