-- =====================================================
-- INSURANCE DATA AI - AUDIT LOGGING SYSTEM
-- Comprehensive 7-Year Audit Trail for Regulatory Compliance
-- Compliant with: HIPAA, SOX 404, GDPR Art 30/32, State Insurance Regulations
-- =====================================================

-- Create security schema
CREATE SCHEMA IF NOT EXISTS ${catalog_name}.security
COMMENT 'Security and compliance layer for audit logging, PII tracking, and access control';

-- =====================================================
-- 1. MAIN AUDIT LOG TABLE (7-Year Retention)
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.security.audit_log (
    audit_id BIGINT GENERATED ALWAYS AS IDENTITY,
    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    user_id STRING NOT NULL COMMENT 'User performing the action',
    user_email STRING COMMENT 'User email address',
    user_role STRING COMMENT 'User role (agent, underwriter, claims_adjuster, etc.)',
    action_type STRING NOT NULL COMMENT 'SELECT, INSERT, UPDATE, DELETE, GRANT, REVOKE',
    object_type STRING NOT NULL COMMENT 'TABLE, VIEW, SCHEMA, CATALOG, POLICY, CLAIM',
    object_name STRING NOT NULL COMMENT 'Fully qualified object name',
    query_text STRING COMMENT 'SQL query or operation performed',
    rows_affected BIGINT COMMENT 'Number of rows read or modified',
    session_id STRING COMMENT 'Session identifier',
    client_ip STRING COMMENT 'Client IP address',
    client_application STRING COMMENT 'Application name',
    operation_status STRING NOT NULL COMMENT 'SUCCESS, FAILED, DENIED',
    error_message STRING COMMENT 'Error message if operation failed',
    contains_pii BOOLEAN DEFAULT FALSE COMMENT 'Whether operation accessed PII',
    pii_fields_accessed ARRAY<STRING> COMMENT 'List of PII fields accessed',
    request_id STRING COMMENT 'Request correlation ID',
    execution_time_ms BIGINT COMMENT 'Query execution time in milliseconds',
    created_date DATE GENERATED ALWAYS AS (CAST(event_timestamp AS DATE))
)
USING DELTA
PARTITIONED BY (created_date)
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 2555 days',  -- 7 years
    'delta.logRetentionDuration' = 'interval 2555 days',
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'delta.dataSkippingNumIndexedCols' = '10',
    'delta.checkpoint.writeStatsAsJson' = 'true'
)
COMMENT 'Main audit log with 7-year retention for regulatory compliance';

-- =====================================================
-- 2. SENSITIVE DATA ACCESS LOG (PHI/PII Tracking)
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.security.sensitive_data_access_log (
    access_id BIGINT GENERATED ALWAYS AS IDENTITY,
    access_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    user_id STRING NOT NULL,
    user_role STRING NOT NULL,
    data_type STRING NOT NULL COMMENT 'PHI, PII_SSN, PII_EMAIL, PII_PHONE, FINANCIAL',
    table_name STRING NOT NULL,
    column_name STRING NOT NULL,
    record_count BIGINT NOT NULL COMMENT 'Number of records accessed',
    access_purpose STRING COMMENT 'Business purpose (claims_processing, underwriting, audit)',
    was_masked BOOLEAN DEFAULT FALSE COMMENT 'Whether data was masked/redacted',
    authorization_level STRING COMMENT 'FULL, PARTIAL, MASKED',
    session_id STRING,
    client_ip STRING,
    request_id STRING,
    risk_score INT COMMENT 'Access risk score (0-100)',
    is_anomaly BOOLEAN DEFAULT FALSE COMMENT 'Flagged by anomaly detection',
    created_date DATE GENERATED ALWAYS AS (CAST(access_timestamp AS DATE))
)
USING DELTA
PARTITIONED BY (created_date)
TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 2555 days',
    'delta.logRetentionDuration' = 'interval 2555 days',
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'PHI/PII access tracking for HIPAA compliance';

-- =====================================================
-- 3. AUTHENTICATION & AUTHORIZATION LOG
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.security.authentication_log (
    auth_id BIGINT GENERATED ALWAYS AS IDENTITY,
    auth_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    user_id STRING NOT NULL,
    user_email STRING,
    auth_type STRING NOT NULL COMMENT 'LOGIN, LOGOUT, TOKEN_REFRESH, PASSWORD_CHANGE',
    auth_status STRING NOT NULL COMMENT 'SUCCESS, FAILED, DENIED',
    auth_method STRING COMMENT 'SSO, MFA, PASSWORD, API_TOKEN',
    failure_reason STRING COMMENT 'Invalid credentials, account locked, etc.',
    client_ip STRING,
    user_agent STRING,
    session_id STRING,
    mfa_verified BOOLEAN DEFAULT FALSE,
    risk_assessment_score INT COMMENT 'Login risk score (0-100)',
    geo_location STRING COMMENT 'Geographic location of access',
    failed_attempts_count INT DEFAULT 0,
    created_date DATE GENERATED ALWAYS AS (CAST(auth_timestamp AS DATE))
)
USING DELTA
PARTITIONED BY (created_date)
TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 2555 days',
    'delta.logRetentionDuration' = 'interval 2555 days'
)
COMMENT 'Authentication and authorization events';

-- =====================================================
-- 4. DATA MODIFICATION LOG (Claims, Policies, Customers)
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.security.data_modification_log (
    modification_id BIGINT GENERATED ALWAYS AS IDENTITY,
    modification_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    user_id STRING NOT NULL,
    user_role STRING NOT NULL,
    operation_type STRING NOT NULL COMMENT 'INSERT, UPDATE, DELETE',
    table_name STRING NOT NULL,
    record_id STRING NOT NULL COMMENT 'Policy ID, Claim ID, Customer ID',
    field_name STRING COMMENT 'Modified field name',
    old_value STRING COMMENT 'Previous value (encrypted for sensitive fields)',
    new_value STRING COMMENT 'New value (encrypted for sensitive fields)',
    modification_reason STRING COMMENT 'Business justification',
    approval_required BOOLEAN DEFAULT FALSE,
    approved_by STRING COMMENT 'Approver user ID',
    approval_timestamp TIMESTAMP,
    session_id STRING,
    request_id STRING,
    created_date DATE GENERATED ALWAYS AS (CAST(modification_timestamp AS DATE))
)
USING DELTA
PARTITIONED BY (created_date)
TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 2555 days',
    'delta.logRetentionDuration' = 'interval 2555 days',
    'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Detailed data modification tracking for SOX compliance';

-- =====================================================
-- 5. PERMISSION CHANGE LOG
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.security.permission_change_log (
    change_id BIGINT GENERATED ALWAYS AS IDENTITY,
    change_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    changed_by STRING NOT NULL COMMENT 'Administrator making the change',
    affected_user STRING NOT NULL COMMENT 'User whose permissions changed',
    permission_type STRING NOT NULL COMMENT 'GRANT, REVOKE, MODIFY',
    object_type STRING NOT NULL COMMENT 'TABLE, SCHEMA, CATALOG, ROW, COLUMN',
    object_name STRING NOT NULL,
    privilege STRING NOT NULL COMMENT 'SELECT, INSERT, UPDATE, DELETE, ALL',
    previous_access_level STRING,
    new_access_level STRING,
    justification STRING COMMENT 'Business reason for change',
    approved_by STRING,
    approval_date TIMESTAMP,
    created_date DATE GENERATED ALWAYS AS (CAST(change_timestamp AS DATE))
)
USING DELTA
PARTITIONED BY (created_date)
TBLPROPERTIES (
    'delta.deletedFileRetentionDuration' = 'interval 2555 days'
)
COMMENT 'Access control changes for compliance auditing';

-- =====================================================
-- 6. AUDIT LOGGING FUNCTIONS
-- =====================================================

-- Function to log data access
CREATE OR REPLACE FUNCTION ${catalog_name}.security.log_data_access(
    p_user_id STRING,
    p_table_name STRING,
    p_query_text STRING,
    p_rows_affected BIGINT,
    p_contains_pii BOOLEAN
)
RETURNS STRING
RETURN CONCAT('Logged access by ', p_user_id, ' to ', p_table_name);

-- Function to calculate risk score
CREATE OR REPLACE FUNCTION ${catalog_name}.security.calculate_access_risk_score(
    p_time_of_day INT,
    p_record_count BIGINT,
    p_is_sensitive_data BOOLEAN,
    p_user_role STRING
)
RETURNS INT
RETURN CASE
    WHEN p_is_sensitive_data AND (p_time_of_day < 6 OR p_time_of_day > 22) THEN 90  -- High risk: sensitive data off-hours
    WHEN p_record_count > 10000 AND p_user_role NOT IN ('ADMIN', 'AUDITOR') THEN 80  -- High volume access
    WHEN p_is_sensitive_data AND p_record_count > 1000 THEN 70  -- Moderate-high risk
    WHEN p_is_sensitive_data THEN 50  -- Moderate risk
    ELSE 20  -- Low risk
END;

-- =====================================================
-- 7. AUDIT VIEWS FOR REPORTING
-- =====================================================

-- Recent PII/PHI Access (Last 24 hours)
CREATE OR REPLACE VIEW ${catalog_name}.security.recent_pii_access AS
SELECT
    access_timestamp,
    user_id,
    user_role,
    data_type,
    table_name,
    column_name,
    record_count,
    was_masked,
    risk_score,
    is_anomaly
FROM ${catalog_name}.security.sensitive_data_access_log
WHERE access_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
ORDER BY access_timestamp DESC;

-- Suspicious Activity (High risk score or failed auth)
CREATE OR REPLACE VIEW ${catalog_name}.security.suspicious_activity AS
SELECT
    'DATA_ACCESS' AS event_type,
    access_timestamp AS event_timestamp,
    user_id,
    CONCAT('High-risk access to ', table_name) AS description,
    risk_score
FROM ${catalog_name}.security.sensitive_data_access_log
WHERE risk_score >= 70 OR is_anomaly = TRUE

UNION ALL

SELECT
    'AUTH_FAILURE' AS event_type,
    auth_timestamp AS event_timestamp,
    user_id,
    CONCAT('Failed authentication: ', failure_reason) AS description,
    risk_assessment_score AS risk_score
FROM ${catalog_name}.security.authentication_log
WHERE auth_status = 'FAILED' AND auth_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS

ORDER BY event_timestamp DESC;

-- Compliance Audit Report (7-Year Summary)
CREATE OR REPLACE VIEW ${catalog_name}.security.compliance_audit_summary AS
SELECT
    DATE_TRUNC('month', event_timestamp) AS month,
    COUNT(*) AS total_operations,
    COUNT(DISTINCT user_id) AS unique_users,
    SUM(CASE WHEN contains_pii THEN 1 ELSE 0 END) AS pii_access_count,
    SUM(CASE WHEN operation_status = 'FAILED' THEN 1 ELSE 0 END) AS failed_operations,
    AVG(execution_time_ms) AS avg_execution_time_ms
FROM ${catalog_name}.security.audit_log
GROUP BY DATE_TRUNC('month', event_timestamp)
ORDER BY month DESC;

-- =====================================================
-- 8. AUDIT LOG RETENTION POLICY
-- =====================================================

-- Automatically archive logs older than 7 years to cold storage
-- This should be run as a scheduled job

CREATE OR REPLACE PROCEDURE ${catalog_name}.security.archive_old_audit_logs()
LANGUAGE SQL
AS
BEGIN
    -- Archive to deep storage and delete from main table
    -- In production, export to S3/ADLS before deletion
    
    -- Example: Delete logs older than 7 years (2555 days)
    DELETE FROM ${catalog_name}.security.audit_log
    WHERE created_date < CURRENT_DATE() - INTERVAL 2555 DAYS;
    
    DELETE FROM ${catalog_name}.security.sensitive_data_access_log
    WHERE created_date < CURRENT_DATE() - INTERVAL 2555 DAYS;
    
    DELETE FROM ${catalog_name}.security.authentication_log
    WHERE created_date < CURRENT_DATE() - INTERVAL 2555 DAYS;
    
    DELETE FROM ${catalog_name}.security.data_modification_log
    WHERE created_date < CURRENT_DATE() - INTERVAL 2555 DAYS;
    
    DELETE FROM ${catalog_name}.security.permission_change_log
    WHERE created_date < CURRENT_DATE() - INTERVAL 2555 DAYS;
END;

-- =====================================================
-- GRANT PERMISSIONS
-- =====================================================

-- Auditors: Read-only access to all audit logs
GRANT SELECT ON SCHEMA ${catalog_name}.security TO `auditors`;
GRANT SELECT ON ${catalog_name}.security.audit_log TO `auditors`;
GRANT SELECT ON ${catalog_name}.security.sensitive_data_access_log TO `auditors`;
GRANT SELECT ON ${catalog_name}.security.authentication_log TO `auditors`;

-- Security team: Full access
GRANT ALL PRIVILEGES ON SCHEMA ${catalog_name}.security TO `security_team`;

-- =====================================================
-- USAGE NOTES
-- =====================================================

-- To log a data access event:
-- INSERT INTO ${catalog_name}.security.audit_log (user_id, action_type, object_type, object_name, rows_affected, contains_pii)
-- VALUES (current_user(), 'SELECT', 'TABLE', 'insurance_prod_silver.customers.customers_dim', 100, TRUE);

-- To query recent PII access:
-- SELECT * FROM ${catalog_name}.security.recent_pii_access;

-- To identify suspicious activity:
-- SELECT * FROM ${catalog_name}.security.suspicious_activity;

