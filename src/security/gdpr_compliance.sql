-- =====================================================
-- INSURANCE DATA AI - GDPR COMPLIANCE
-- Implementation of GDPR Articles 15-20, 30
-- European Data Protection Regulation
-- =====================================================

-- Create GDPR schema
CREATE SCHEMA IF NOT EXISTS ${catalog_name}.gdpr
COMMENT 'GDPR compliance layer for data subject rights and processing records';

-- =====================================================
-- 1. GDPR REQUEST TRACKING
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.gdpr.gdpr_requests (
    request_id BIGINT GENERATED ALWAYS AS IDENTITY,
    request_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    request_type STRING NOT NULL COMMENT 'ACCESS, RECTIFICATION, ERASURE, RESTRICTION, PORTABILITY, OBJECTION',
    data_subject_id STRING NOT NULL COMMENT 'Customer ID',
    data_subject_email STRING NOT NULL,
    data_subject_name STRING,
    article_number STRING NOT NULL COMMENT 'Art 15, Art 16, Art 17, Art 18, Art 20, Art 21',
    request_details STRING COMMENT 'Detailed description of request',
    request_channel STRING COMMENT 'EMAIL, PHONE, PORTAL, MAIL',
    status STRING NOT NULL DEFAULT 'PENDING' COMMENT 'PENDING, IN_PROGRESS, COMPLETED, REJECTED',
    assigned_to STRING COMMENT 'DPO or team member handling request',
    due_date DATE COMMENT '30 days from request_date',
    completion_date TIMESTAMP,
    rejection_reason STRING,
    legal_hold_check BOOLEAN DEFAULT FALSE COMMENT 'Whether legal hold was checked',
    has_active_policies BOOLEAN,
    has_open_claims BOOLEAN,
    has_fraud_investigation BOOLEAN,
    processing_notes STRING,
    created_by STRING DEFAULT CURRENT_USER(),
    updated_by STRING,
    updated_date TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.deletedFileRetentionDuration' = 'interval 2555 days'  -- 7 years
)
COMMENT 'GDPR data subject request tracking with 30-day SLA';

-- =====================================================
-- 2. GDPR ARTICLE 15 - RIGHT OF ACCESS
-- Export all customer data
-- =====================================================

CREATE OR REPLACE FUNCTION ${catalog_name}.gdpr.export_customer_data(p_customer_id STRING)
RETURNS TABLE(
    data_category STRING,
    field_name STRING,
    field_value STRING,
    source_table STRING,
    last_updated TIMESTAMP
)
RETURN
SELECT 'PERSONAL_INFO' AS data_category, 'customer_id' AS field_name, customer_id AS field_value,
       'customers_dim' AS source_table, updated_date AS last_updated
FROM ${catalog_name}_silver.customers.customers_dim
WHERE customer_id = p_customer_id

UNION ALL

SELECT 'POLICIES' AS data_category, 'policy_id' AS field_name, policy_id AS field_value,
       'policies_fact' AS source_table, updated_date AS last_updated
FROM ${catalog_name}_silver.policies.policies_fact
WHERE customer_id = p_customer_id

UNION ALL

SELECT 'CLAIMS' AS data_category, 'claim_id' AS field_name, claim_id AS field_value,
       'claims_fact' AS source_table, updated_date AS last_updated
FROM ${catalog_name}_silver.claims.claims_fact
WHERE customer_id = p_customer_id;

-- =====================================================
-- 3. GDPR ARTICLE 16 - RIGHT TO RECTIFICATION
-- Update customer data with audit trail
-- =====================================================

CREATE OR REPLACE PROCEDURE ${catalog_name}.gdpr.rectify_customer_data(
    p_customer_id STRING,
    p_field_name STRING,
    p_new_value STRING,
    p_reason STRING
)
LANGUAGE SQL
AS
BEGIN
    -- Log the change in modification log
    INSERT INTO ${catalog_name}.security.data_modification_log
    (user_id, user_role, operation_type, table_name, record_id, field_name, new_value, modification_reason)
    VALUES (CURRENT_USER(), 'DPO', 'UPDATE', 'customers_dim', p_customer_id, p_field_name, p_new_value, p_reason);
    
    -- Note: Actual UPDATE would be done based on field_name
    -- This is a template - implement specific field updates as needed
END;

-- =====================================================
-- 4. GDPR ARTICLE 17 - RIGHT TO ERASURE (Right to be Forgotten)
-- Anonymize customer data while maintaining referential integrity
-- =====================================================

CREATE OR REPLACE PROCEDURE ${catalog_name}.gdpr.anonymize_customer_data(
    p_customer_id STRING,
    p_request_id BIGINT
)
LANGUAGE SQL
AS
BEGIN
    DECLARE v_has_legal_hold BOOLEAN DEFAULT FALSE;
    DECLARE v_active_policies INT DEFAULT 0;
    DECLARE v_open_claims INT DEFAULT 0;
    DECLARE v_fraud_cases INT DEFAULT 0;
    
    -- Check for legal holds
    SELECT COUNT(*) INTO v_active_policies
    FROM ${catalog_name}_silver.policies.policies_fact
    WHERE customer_id = p_customer_id AND policy_status = 'Active';
    
    SELECT COUNT(*) INTO v_open_claims
    FROM ${catalog_name}_silver.claims.claims_fact
    WHERE customer_id = p_customer_id AND claim_status IN ('Pending', 'Under Review');
    
    SELECT COUNT(*) INTO v_fraud_cases
    FROM ${catalog_name}_gold.fraud_analytics.fraud_detection
    WHERE customer_id = p_customer_id AND risk_category IN ('High', 'Critical') AND investigation_closed = FALSE;
    
    SET v_has_legal_hold = (v_active_policies > 0 OR v_open_claims > 0 OR v_fraud_cases > 0);
    
    -- If legal hold exists, reject the request
    IF v_has_legal_hold THEN
        UPDATE ${catalog_name}.gdpr.gdpr_requests
        SET status = 'REJECTED',
            rejection_reason = CONCAT('Legal hold: Active policies (', v_active_policies, '), Open claims (', v_open_claims, '), Fraud investigations (', v_fraud_cases, ')'),
            has_active_policies = (v_active_policies > 0),
            has_open_claims = (v_open_claims > 0),
            has_fraud_investigation = (v_fraud_cases > 0)
        WHERE request_id = p_request_id;
        
        RAISE EXCEPTION 'Cannot anonymize: Legal hold exists';
    END IF;
    
    -- Anonymize customer data
    MERGE INTO ${catalog_name}_silver.customers.customers_dim AS target
    USING (SELECT p_customer_id AS customer_id) AS source
    ON target.customer_id = source.customer_id AND target.is_current = TRUE
    WHEN MATCHED THEN UPDATE SET
        first_name = 'ANONYMIZED',
        last_name = 'CUSTOMER',
        email = CONCAT('deleted_', p_customer_id, '@anonymized.local'),
        phone = 'XXX-XXX-XXXX',
        address_line1 = 'DELETED',
        address_line2 = 'DELETED',
        city = 'DELETED',
        state = 'XX',
        zip_code = '00000',
        ssn_last_4 = '0000',
        date_of_birth = '1900-01-01',
        is_anonymized = TRUE,
        anonymization_date = CURRENT_TIMESTAMP(),
        anonymization_request_id = p_request_id,
        updated_date = CURRENT_TIMESTAMP(),
        updated_by = CURRENT_USER();
    
    -- Update GDPR request status
    UPDATE ${catalog_name}.gdpr.gdpr_requests
    SET status = 'COMPLETED',
        completion_date = CURRENT_TIMESTAMP(),
        legal_hold_check = TRUE,
        processing_notes = 'Customer data successfully anonymized'
    WHERE request_id = p_request_id;
    
    -- Log in audit log
    INSERT INTO ${catalog_name}.security.data_modification_log
    (user_id, user_role, operation_type, table_name, record_id, modification_reason, approved_by)
    VALUES (CURRENT_USER(), 'DPO', 'ANONYMIZE', 'customers_dim', p_customer_id, 'GDPR Article 17 - Right to Erasure', CURRENT_USER());
    
END;

-- =====================================================
-- 5. GDPR ARTICLE 18 - RIGHT TO RESTRICTION OF PROCESSING
-- =====================================================

CREATE OR REPLACE PROCEDURE ${catalog_name}.gdpr.restrict_data_processing(
    p_customer_id STRING,
    p_restriction_reason STRING
)
LANGUAGE SQL
AS
BEGIN
    -- Mark customer for restricted processing
    UPDATE ${catalog_name}_silver.customers.customers_dim
    SET processing_restricted = TRUE,
        restriction_reason = p_restriction_reason,
        restriction_date = CURRENT_TIMESTAMP(),
        updated_date = CURRENT_TIMESTAMP(),
        updated_by = CURRENT_USER()
    WHERE customer_id = p_customer_id AND is_current = TRUE;
    
    -- Log restriction
    INSERT INTO ${catalog_name}.security.data_modification_log
    (user_id, user_role, operation_type, table_name, record_id, modification_reason)
    VALUES (CURRENT_USER(), 'DPO', 'RESTRICT', 'customers_dim', p_customer_id, p_restriction_reason);
END;

-- =====================================================
-- 6. GDPR ARTICLE 20 - RIGHT TO DATA PORTABILITY
-- Export customer data in machine-readable format
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}.gdpr.customer_data_export AS
SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.phone,
    c.date_of_birth,
    c.address_line1,
    c.city,
    c.state,
    c.zip_code,
    COLLECT_LIST(
        STRUCT(
            p.policy_id,
            p.policy_type,
            p.coverage_amount,
            p.premium_amount,
            p.start_date,
            p.end_date
        )
    ) AS policies,
    COLLECT_LIST(
        STRUCT(
            cl.claim_id,
            cl.claim_type,
            cl.claim_amount,
            cl.claim_date,
            cl.claim_status
        )
    ) AS claims
FROM ${catalog_name}_silver.customers.customers_dim c
LEFT JOIN ${catalog_name}_silver.policies.policies_fact p ON c.customer_id = p.customer_id
LEFT JOIN ${catalog_name}_silver.claims.claims_fact cl ON c.customer_id = cl.customer_id
WHERE c.is_current = TRUE
GROUP BY c.customer_id, c.first_name, c.last_name, c.email, c.phone, c.date_of_birth,
         c.address_line1, c.city, c.state, c.zip_code;

-- =====================================================
-- 7. GDPR ARTICLE 30 - RECORDS OF PROCESSING ACTIVITIES
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.gdpr.processing_activities_register (
    activity_id BIGINT GENERATED ALWAYS AS IDENTITY,
    activity_name STRING NOT NULL COMMENT 'Name of processing activity',
    purpose STRING NOT NULL COMMENT 'Purpose of processing',
    legal_basis STRING NOT NULL COMMENT 'CONSENT, CONTRACT, LEGAL_OBLIGATION, VITAL_INTERESTS, PUBLIC_TASK, LEGITIMATE_INTERESTS',
    data_categories ARRAY<STRING> COMMENT 'Categories of personal data',
    data_subjects ARRAY<STRING> COMMENT 'Categories of data subjects',
    recipients ARRAY<STRING> COMMENT 'Recipients of personal data',
    transfers_to_third_countries BOOLEAN DEFAULT FALSE,
    retention_period STRING COMMENT 'Data retention period',
    security_measures STRING COMMENT 'Technical and organizational security measures',
    data_controller STRING NOT NULL COMMENT 'Name of data controller',
    dpo_contact STRING COMMENT 'Data Protection Officer contact',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP
)
USING DELTA
COMMENT 'GDPR Article 30 - Records of processing activities';

-- Insert sample processing activities
INSERT INTO ${catalog_name}.gdpr.processing_activities_register
(activity_name, purpose, legal_basis, data_categories, data_subjects, recipients, retention_period, security_measures)
VALUES
('Policy Underwriting', 'Assess insurance risk and issue policies', 'CONTRACT',
 ARRAY('Name', 'DOB', 'Address', 'SSN', 'Medical history'), ARRAY('Policyholders', 'Applicants'),
 ARRAY('Underwriters', 'Actuaries'), '7 years after policy expiration', 'Encryption, Access controls, Audit logging'),
 
('Claims Processing', 'Process and settle insurance claims', 'CONTRACT',
 ARRAY('Name', 'Policy details', 'Claim documents', 'Health information'), ARRAY('Policyholders'),
 ARRAY('Claims adjusters', 'Medical providers'), '10 years after claim settlement', 'End-to-end encryption, Role-based access'),
 
('Fraud Detection', 'Detect and prevent insurance fraud', 'LEGITIMATE_INTERESTS',
 ARRAY('Claims history', 'Transaction patterns', 'Social graphs'), ARRAY('All customers'),
 ARRAY('Fraud investigators', 'Law enforcement (if required)'), '7 years', 'Anonymization, Audit trails'),
 
('Customer Service', 'Provide customer support and communications', 'CONTRACT',
 ARRAY('Contact info', 'Policy details', 'Communication history'), ARRAY('Customers'),
 ARRAY('Customer service agents'), '3 years after last contact', 'Access logging, Data masking'),
 
('Marketing & Analytics', 'Product recommendations and business analytics', 'CONSENT',
 ARRAY('Demographics', 'Purchase history', 'Preferences'), ARRAY('Opted-in customers'),
 ARRAY('Marketing team', 'Analytics team'), 'Until consent withdrawn', 'Pseudonymization, Aggregation');

-- =====================================================
-- 8. GDPR COMPLIANCE VIEWS
-- =====================================================

-- Open GDPR requests approaching deadline
CREATE OR REPLACE VIEW ${catalog_name}.gdpr.gdpr_open_requests AS
SELECT
    request_id,
    request_date,
    request_type,
    article_number,
    data_subject_id,
    data_subject_email,
    status,
    due_date,
    DATEDIFF(due_date, CURRENT_DATE()) AS days_remaining,
    CASE
        WHEN DATEDIFF(due_date, CURRENT_DATE()) < 0 THEN 'OVERDUE'
        WHEN DATEDIFF(due_date, CURRENT_DATE()) <= 5 THEN 'URGENT'
        WHEN DATEDIFF(due_date, CURRENT_DATE()) <= 10 THEN 'WARNING'
        ELSE 'ON_TRACK'
    END AS sla_status,
    assigned_to
FROM ${catalog_name}.gdpr.gdpr_requests
WHERE status IN ('PENDING', 'IN_PROGRESS')
ORDER BY due_date ASC;

-- GDPR compliance metrics
CREATE OR REPLACE VIEW ${catalog_name}.gdpr.gdpr_compliance_metrics AS
SELECT
    DATE_TRUNC('month', request_date) AS month,
    request_type,
    COUNT(*) AS total_requests,
    SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_requests,
    SUM(CASE WHEN status = 'REJECTED' THEN 1 ELSE 0 END) AS rejected_requests,
    SUM(CASE WHEN completion_date IS NOT NULL AND completion_date <= due_date THEN 1 ELSE 0 END) AS on_time_completions,
    AVG(DATEDIFF(completion_date, request_date)) AS avg_resolution_days,
    ROUND(SUM(CASE WHEN completion_date IS NOT NULL AND completion_date <= due_date THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS sla_compliance_rate
FROM ${catalog_name}.gdpr.gdpr_requests
GROUP BY DATE_TRUNC('month', request_date), request_type
ORDER BY month DESC, request_type;

-- =====================================================
-- GRANT PERMISSIONS
-- =====================================================

-- Data Protection Officer: Full access
GRANT ALL PRIVILEGES ON SCHEMA ${catalog_name}.gdpr TO `data_protection_officers`;

-- Compliance team: Read access
GRANT SELECT ON SCHEMA ${catalog_name}.gdpr TO `compliance_team`;

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================

-- Submit a GDPR request:
-- INSERT INTO ${catalog_name}.gdpr.gdpr_requests (request_type, data_subject_id, data_subject_email, article_number, due_date)
-- VALUES ('ERASURE', 'CUST123', 'customer@example.com', 'Art 17', CURRENT_DATE() + INTERVAL 30 DAYS);

-- Export customer data (Article 15):
-- SELECT * FROM ${catalog_name}.gdpr.export_customer_data('CUST123');

-- Anonymize customer (Article 17):
-- CALL ${catalog_name}.gdpr.anonymize_customer_data('CUST123', 12345);

-- Check open requests:
-- SELECT * FROM ${catalog_name}.gdpr.gdpr_open_requests;

