-- =====================================================
-- INSURANCE DATA AI - HIPAA COMPLIANCE
-- Health Insurance Portability and Accountability Act
-- Protected Health Information (PHI) Security and Privacy
-- =====================================================

-- Note: HIPAA compliance is required if handling health insurance or medical data

-- Create HIPAA schema
CREATE SCHEMA IF NOT EXISTS ${catalog_name}.hipaa
COMMENT 'HIPAA compliance layer for PHI protection and breach notification';

-- =====================================================
-- 1. PHI (Protected Health Information) INVENTORY
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.hipaa.phi_data_inventory (
    phi_id BIGINT GENERATED ALWAYS AS IDENTITY,
    catalog_name STRING NOT NULL,
    schema_name STRING NOT NULL,
    table_name STRING NOT NULL,
    column_name STRING NOT NULL,
    phi_category STRING NOT NULL COMMENT 'DEMOGRAPHIC, MEDICAL, FINANCIAL, TREATMENT, PRESCRIPTION',
    phi_identifier_type STRING NOT NULL COMMENT 'NAME, ADDRESS, SSN, MEDICAL_RECORD_NUM, HEALTH_PLAN_NUM, DIAGNOSIS_CODE, PRESCRIPTION, etc.',
    is_direct_identifier BOOLEAN DEFAULT TRUE COMMENT 'Direct identifiers require strict protection',
    hipaa_rule STRING NOT NULL DEFAULT 'PRIVACY_RULE' COMMENT 'PRIVACY_RULE, SECURITY_RULE, BREACH_NOTIFICATION_RULE',
    encryption_required BOOLEAN DEFAULT TRUE,
    encryption_method STRING COMMENT 'AES-256, Column-level encryption',
    access_restrictions STRING COMMENT 'Roles allowed to access',
    retention_period_days INT COMMENT 'HIPAA retention requirement: 6 years minimum',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP,
    audited_by STRING
)
USING DELTA
COMMENT 'Inventory of all PHI fields across the platform';

-- Pre-populate PHI inventory for insurance data
INSERT INTO ${catalog_name}.hipaa.phi_data_inventory
(catalog_name, schema_name, table_name, column_name, phi_category, phi_identifier_type, is_direct_identifier, retention_period_days)
VALUES
('${catalog_name}_silver', 'customers', 'customers_dim', 'customer_name', 'DEMOGRAPHIC', 'NAME', TRUE, 2555),
('${catalog_name}_silver', 'customers', 'customers_dim', 'date_of_birth', 'DEMOGRAPHIC', 'DATE_OF_BIRTH', TRUE, 2555),
('${catalog_name}_silver', 'customers', 'customers_dim', 'ssn_last_4', 'DEMOGRAPHIC', 'SSN', TRUE, 2555),
('${catalog_name}_silver', 'customers', 'customers_dim', 'email', 'DEMOGRAPHIC', 'EMAIL', TRUE, 2555),
('${catalog_name}_silver', 'customers', 'customers_dim', 'phone', 'DEMOGRAPHIC', 'PHONE', TRUE, 2555),
('${catalog_name}_silver', 'customers', 'customers_dim', 'address_line1', 'DEMOGRAPHIC', 'ADDRESS', TRUE, 2555),
('${catalog_name}_silver', 'claims', 'claims_fact', 'medical_diagnosis_code', 'MEDICAL', 'DIAGNOSIS_CODE', TRUE, 2555),
('${catalog_name}_silver', 'claims', 'claims_fact', 'treatment_description', 'TREATMENT', 'TREATMENT_INFO', TRUE, 2555),
('${catalog_name}_silver', 'policies', 'policies_fact', 'health_plan_number', 'FINANCIAL', 'HEALTH_PLAN_NUM', TRUE, 2555),
('${catalog_name}_silver', 'policies', 'policies_fact', 'policy_id', 'FINANCIAL', 'POLICY_NUMBER', TRUE, 2555);

-- =====================================================
-- 2. HIPAA BREACH DETECTION & NOTIFICATION
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.hipaa.breach_incidents (
    incident_id BIGINT GENERATED ALWAYS AS IDENTITY,
    incident_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    breach_type STRING NOT NULL COMMENT 'UNAUTHORIZED_ACCESS, DATA_LOSS, RANSOMWARE, SYSTEM_INTRUSION, THEFT, DISPOSAL_ERROR',
    severity STRING NOT NULL COMMENT 'LOW, MEDIUM, HIGH, CRITICAL',
    affected_individuals_count INT COMMENT 'Number of individuals affected',
    phi_compromised ARRAY<STRING> COMMENT 'Types of PHI compromised',
    breach_description STRING NOT NULL,
    discovery_date TIMESTAMP NOT NULL,
    root_cause STRING,
    containment_actions STRING,
    remediation_actions STRING,
    notification_required BOOLEAN COMMENT 'TRUE if affects 500+ individuals',
    hhs_notification_date TIMESTAMP COMMENT 'Must notify HHS within 60 days',
    media_notification_required BOOLEAN DEFAULT FALSE COMMENT 'Required if 500+ in same state/jurisdiction',
    affected_individuals_notified BOOLEAN DEFAULT FALSE,
    notification_date TIMESTAMP COMMENT 'Must notify within 60 days of discovery',
    reported_by STRING,
    investigated_by STRING,
    status STRING DEFAULT 'OPEN' COMMENT 'OPEN, INVESTIGATING, CONTAINED, REMEDIATED, CLOSED',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'HIPAA breach incident tracking and notification management';

-- =====================================================
-- 3. HIPAA MINIMUM NECESSARY RULE
-- Track access justification for PHI
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.hipaa.phi_access_justification (
    access_justification_id BIGINT GENERATED ALWAYS AS IDENTITY,
    user_id STRING NOT NULL,
    user_role STRING NOT NULL,
    access_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    phi_accessed STRING NOT NULL COMMENT 'Table.column accessed',
    records_accessed INT NOT NULL,
    business_justification STRING NOT NULL COMMENT 'Treatment, Payment, Healthcare Operations, Research, etc.',
    justification_category STRING NOT NULL COMMENT 'TREATMENT, PAYMENT, OPERATIONS, RESEARCH, AUDIT, LEGAL',
    approved_by STRING COMMENT 'Supervisor or compliance officer',
    approval_date TIMESTAMP,
    is_minimum_necessary BOOLEAN DEFAULT TRUE COMMENT 'Whether access followed minimum necessary rule',
    audit_review_status STRING DEFAULT 'PENDING' COMMENT 'PENDING, APPROVED, FLAGGED, INVESTIGATED',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
PARTITIONED BY (DATE(access_timestamp))
COMMENT 'HIPAA Minimum Necessary Rule - justify all PHI access';

-- =====================================================
-- 4. HIPAA BUSINESS ASSOCIATE AGREEMENTS (BAA) TRACKING
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.hipaa.business_associate_agreements (
    baa_id BIGINT GENERATED ALWAYS AS IDENTITY,
    associate_name STRING NOT NULL COMMENT 'Name of business associate (vendor/partner)',
    associate_type STRING NOT NULL COMMENT 'CLOUD_PROVIDER, ANALYTICS_VENDOR, CLAIMS_PROCESSOR, IT_SUPPORT',
    baa_signed_date DATE NOT NULL,
    baa_expiration_date DATE NOT NULL,
    baa_status STRING NOT NULL DEFAULT 'ACTIVE' COMMENT 'ACTIVE, EXPIRED, TERMINATED, PENDING_RENEWAL',
    phi_shared ARRAY<STRING> COMMENT 'Types of PHI shared with associate',
    services_provided STRING NOT NULL COMMENT 'Description of services',
    data_usage_restrictions STRING COMMENT 'How associate may use/disclose PHI',
    security_requirements STRING COMMENT 'Required security measures',
    breach_notification_obligations STRING,
    subcontractor_authorization BOOLEAN DEFAULT FALSE,
    audit_rights STRING COMMENT 'Rights to audit associate compliance',
    termination_data_return_obligations STRING,
    contact_person STRING,
    contact_email STRING,
    last_compliance_review_date DATE,
    next_review_date DATE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Track HIPAA Business Associate Agreements';

-- =====================================================
-- 5. HIPAA SECURITY RULE - ACCESS CONTROLS
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}.hipaa.phi_access_control_matrix AS
SELECT
    'claims_adjuster' AS role,
    ARRAY('customers.customer_name', 'claims.medical_diagnosis_code', 'claims.treatment_description') AS phi_access_allowed,
    'TREATMENT_PAYMENT' AS justification,
    TRUE AS minimum_necessary_applied

UNION ALL

SELECT 'underwriter' AS role,
       ARRAY('customers.customer_name', 'customers.date_of_birth', 'policies.health_plan_number') AS phi_access_allowed,
       'UNDERWRITING_OPERATIONS' AS justification,
       TRUE AS minimum_necessary_applied

UNION ALL

SELECT 'customer_service' AS role,
       ARRAY('customers.customer_name', 'customers.email', 'customers.phone') AS phi_access_allowed,
       'CUSTOMER_SUPPORT' AS justification,
       TRUE AS minimum_necessary_applied

UNION ALL

SELECT 'data_scientist' AS role,
       ARRAY() AS phi_access_allowed,  -- No direct PHI access, only de-identified/aggregated data
       'ANALYTICS_RESEARCH' AS justification,
       TRUE AS minimum_necessary_applied

UNION ALL

SELECT 'auditor' AS role,
       ARRAY('*') AS phi_access_allowed,  -- Full access for compliance audit
       'COMPLIANCE_AUDIT' AS justification,
       TRUE AS minimum_necessary_applied;

-- =====================================================
-- 6. HIPAA PRIVACY RULE - AUTHORIZATION TRACKING
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.hipaa.patient_authorizations (
    authorization_id BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_id STRING NOT NULL,
    authorization_type STRING NOT NULL COMMENT 'MARKETING, RESEARCH, THIRD_PARTY_DISCLOSURE',
    purpose STRING NOT NULL,
    phi_to_be_disclosed ARRAY<STRING>,
    authorized_recipient STRING NOT NULL,
    authorization_date DATE NOT NULL,
    expiration_date DATE COMMENT 'Date authorization expires or event that causes expiration',
    authorization_status STRING DEFAULT 'ACTIVE' COMMENT 'ACTIVE, EXPIRED, REVOKED',
    revocation_date DATE,
    revocation_reason STRING,
    consent_form_signed BOOLEAN DEFAULT TRUE,
    consent_form_location STRING COMMENT 'Document storage location',
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Track patient authorizations for PHI disclosure';

-- =====================================================
-- 7. HIPAA COMPLIANCE VIEWS
-- =====================================================

-- Detect potential HIPAA violations
CREATE OR REPLACE VIEW ${catalog_name}.hipaa.potential_violations AS
SELECT
    'EXCESSIVE_PHI_ACCESS' AS violation_type,
    user_id,
    user_role,
    COUNT(*) AS access_count,
    SUM(records_accessed) AS total_records,
    MAX(access_timestamp) AS last_access
FROM ${catalog_name}.hipaa.phi_access_justification
WHERE access_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
GROUP BY user_id, user_role
HAVING COUNT(*) > 100 OR SUM(records_accessed) > 10000  -- Threshold for suspicious activity

UNION ALL

SELECT
    'MISSING_JUSTIFICATION' AS violation_type,
    user_id,
    user_role,
    COUNT(*) AS access_count,
    SUM(records_accessed) AS total_records,
    MAX(access_timestamp) AS last_access
FROM ${catalog_name}.hipaa.phi_access_justification
WHERE business_justification IS NULL OR business_justification = ''
GROUP BY user_id, user_role;

-- Expired Business Associate Agreements
CREATE OR REPLACE VIEW ${catalog_name}.hipaa.expired_baas AS
SELECT
    baa_id,
    associate_name,
    associate_type,
    baa_expiration_date,
    DATEDIFF(CURRENT_DATE(), baa_expiration_date) AS days_expired,
    phi_shared,
    contact_person,
    contact_email
FROM ${catalog_name}.hipaa.business_associate_agreements
WHERE baa_status = 'ACTIVE' AND baa_expiration_date < CURRENT_DATE()
ORDER BY days_expired DESC;

-- HIPAA Compliance Dashboard Metrics
CREATE OR REPLACE VIEW ${catalog_name}.hipaa.compliance_metrics AS
SELECT
    'PHI Fields Protected' AS metric,
    COUNT(*) AS value,
    'Total PHI columns under protection' AS description
FROM ${catalog_name}.hipaa.phi_data_inventory

UNION ALL

SELECT 'Business Associates' AS metric,
       COUNT(*) AS value,
       'Active BAAs' AS description
FROM ${catalog_name}.hipaa.business_associate_agreements
WHERE baa_status = 'ACTIVE'

UNION ALL

SELECT 'Security Incidents' AS metric,
       COUNT(*) AS value,
       'Open incidents requiring investigation' AS description
FROM ${catalog_name}.hipaa.breach_incidents
WHERE status IN ('OPEN', 'INVESTIGATING')

UNION ALL

SELECT 'Breaches Requiring Notification' AS metric,
       COUNT(*) AS value,
       'Breaches affecting 500+ individuals' AS description
FROM ${catalog_name}.hipaa.breach_incidents
WHERE notification_required = TRUE AND affected_individuals_notified = FALSE;

-- =====================================================
-- 8. HIPAA COMPLIANCE PROCEDURES
-- =====================================================

-- Procedure to log PHI access
CREATE OR REPLACE PROCEDURE ${catalog_name}.hipaa.log_phi_access(
    p_user_id STRING,
    p_user_role STRING,
    p_phi_accessed STRING,
    p_records_accessed INT,
    p_justification STRING,
    p_justification_category STRING
)
LANGUAGE SQL
AS
BEGIN
    INSERT INTO ${catalog_name}.hipaa.phi_access_justification
    (user_id, user_role, phi_accessed, records_accessed, business_justification, justification_category)
    VALUES (p_user_id, p_user_role, p_phi_accessed, p_records_accessed, p_justification, p_justification_category);
END;

-- Procedure to report a breach
CREATE OR REPLACE PROCEDURE ${catalog_name}.hipaa.report_breach(
    p_breach_type STRING,
    p_severity STRING,
    p_affected_count INT,
    p_phi_compromised ARRAY<STRING>,
    p_description STRING
)
LANGUAGE SQL
AS
BEGIN
    DECLARE v_notification_required BOOLEAN;
    DECLARE v_media_notification_required BOOLEAN;
    
    -- Determine if HHS notification required (500+ individuals)
    SET v_notification_required = (p_affected_count >= 500);
    SET v_media_notification_required = (p_affected_count >= 500);
    
    INSERT INTO ${catalog_name}.hipaa.breach_incidents
    (breach_type, severity, affected_individuals_count, phi_compromised, breach_description,
     discovery_date, notification_required, media_notification_required, reported_by, status)
    VALUES (p_breach_type, p_severity, p_affected_count, p_phi_compromised, p_description,
            CURRENT_TIMESTAMP(), v_notification_required, v_media_notification_required, CURRENT_USER(), 'OPEN');
    
    -- If critical breach, send immediate alert
    IF p_severity = 'CRITICAL' OR v_notification_required THEN
        -- Trigger alert system (implement based on your alerting mechanism)
        SELECT 'CRITICAL BREACH - Immediate action required' AS alert_message;
    END IF;
END;

-- =====================================================
-- GRANT PERMISSIONS
-- =====================================================

-- Privacy/Security Officers: Full access
GRANT ALL PRIVILEGES ON SCHEMA ${catalog_name}.hipaa TO `privacy_officers`;
GRANT ALL PRIVILEGES ON SCHEMA ${catalog_name}.hipaa TO `security_officers`;

-- Compliance team: Read access
GRANT SELECT ON SCHEMA ${catalog_name}.hipaa TO `compliance_team`;

-- =====================================================
-- HIPAA COMPLIANCE CHECKLIST
-- =====================================================

-- ✅ Privacy Rule: PHI access controls and minimum necessary rule
-- ✅ Security Rule: Access audit trails and encryption requirements
-- ✅ Breach Notification Rule: Incident tracking and notification timelines
-- ✅ Business Associate Rule: BAA tracking and management
-- ✅ Patient Rights: Authorization tracking for disclosures
-- ✅ Retention: 6-year minimum retention (2555 days configured)

-- Next Steps:
-- 1. Enable encryption at rest and in transit
-- 2. Implement multi-factor authentication
-- 3. Configure automatic breach detection alerts
-- 4. Schedule quarterly compliance audits
-- 5. Conduct annual HIPAA training for all staff

