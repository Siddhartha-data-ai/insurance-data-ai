-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Create Unity Catalog Catalogs
-- MAGIC 
-- MAGIC This notebook creates the three Unity Catalog catalogs required for the insurance analytics platform:
-- MAGIC - **insurance_dev_bronze**: Raw data layer
-- MAGIC - **insurance_dev_silver**: Cleaned and validated data layer
-- MAGIC - **insurance_dev_gold**: Analytics and aggregated data layer
-- MAGIC 
-- MAGIC **Prerequisites:**
-- MAGIC - Unity Catalog must be enabled in the workspace
-- MAGIC - User must have CREATE CATALOG permission
-- MAGIC 
-- MAGIC **Usage:**
-- MAGIC Run this notebook before deploying the rest of the platform infrastructure.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Check Current Catalogs and Permissions

-- COMMAND ----------

-- Check what catalogs currently exist
SHOW CATALOGS;

-- COMMAND ----------

-- Check current user
SELECT current_user();

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Bronze Catalog
-- MAGIC Raw data ingestion layer for source system data

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS insurance_dev_bronze;

-- COMMAND ----------

-- Verify bronze catalog was created
DESCRIBE CATALOG insurance_dev_bronze;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Silver Catalog
-- MAGIC Cleaned, validated, and conformed data layer

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS insurance_dev_silver;

-- COMMAND ----------

-- Verify silver catalog was created
DESCRIBE CATALOG insurance_dev_silver;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Create Gold Catalog
-- MAGIC Business-ready analytics and aggregated data layer

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS insurance_dev_gold;

-- COMMAND ----------

-- Verify gold catalog was created
DESCRIBE CATALOG insurance_dev_gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Verify Catalog Creation

-- COMMAND ----------

-- Show all catalogs to verify
SHOW CATALOGS;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Troubleshooting
-- MAGIC 
-- MAGIC **If catalogs are not showing:**
-- MAGIC 
-- MAGIC 1. **Check Unity Catalog is enabled**: Contact your workspace admin
-- MAGIC 2. **Check permissions**: You need `CREATE CATALOG` privilege on the metastore
-- MAGIC 3. **Check for errors**: Review any error messages from the CREATE CATALOG commands above
-- MAGIC 4. **Try listing all catalogs**: Run `SHOW CATALOGS;` to see if they exist with different names
-- MAGIC 
-- MAGIC **Grant permission (Metastore Admin must run):**
-- MAGIC ```sql
-- MAGIC GRANT CREATE CATALOG ON METASTORE TO `your_user@email.com`;
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ✅ **Catalogs Created Successfully**
-- MAGIC 
-- MAGIC Next steps:
-- MAGIC 1. Run `01_create_bronze_tables.sql` to create bronze layer schemas and tables
-- MAGIC 2. Run `02_create_silver_tables.sql` to create silver layer schemas and tables
-- MAGIC 3. Run `03_create_security_rls_cls.sql` to implement security policies
-- MAGIC 4. Run `04_create_gold_tables.sql` to create gold layer analytics tables

