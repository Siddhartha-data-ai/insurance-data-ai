-- =====================================================
-- INSURANCE DATA AI - ENABLE CHANGE DATA FEED (CDF)
-- Enable Change Data Capture for real-time data tracking
-- =====================================================

-- Enable CDF on Bronze layer tables
ALTER TABLE ${catalog_name}_bronze.customers.customers_raw
SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE ${catalog_name}_bronze.policies.policies_raw
SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE ${catalog_name}_bronze.claims.claims_raw
SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

-- Enable CDF on Silver layer tables
ALTER TABLE ${catalog_name}_silver.customers.customers_dim
SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE ${catalog_name}_silver.policies.policies_fact
SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE ${catalog_name}_silver.claims.claims_fact
SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

-- Enable CDF on Gold layer tables
ALTER TABLE ${catalog_name}_gold.customer_analytics.customer_360
SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

ALTER TABLE ${catalog_name}_gold.fraud_analytics.fraud_detection
SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

-- =====================================================
-- USAGE NOTES
-- =====================================================

-- Query CDF changes:
-- SELECT * FROM table_changes('table_name', start_version, end_version);
-- SELECT * FROM table_changes('table_name', start_timestamp, end_timestamp);

-- Example:
-- SELECT * FROM table_changes('${catalog_name}_bronze.customers.customers_raw', 0)
-- WHERE _change_type IN ('insert', 'update_postimage');

-- CDF columns:
-- _change_type: insert, update_preimage, update_postimage, delete
-- _commit_version: Delta version number
-- _commit_timestamp: Commit timestamp

-- Use cases:
-- 1. Incremental ETL processing
-- 2. Real-time data replication
-- 3. Audit trail and compliance
-- 4. Event-driven architectures
-- 5. Data synchronization across systems

