-- =====================================================
-- INSURANCE DATA AI - ROW-LEVEL SECURITY (RLS)
-- Dynamic data filtering based on user roles and attributes
-- =====================================================

-- =====================================================
-- 1. USER ROLE MANAGEMENT
-- =====================================================

CREATE SCHEMA IF NOT EXISTS ${catalog_name}.access_control
COMMENT 'Access control configuration for RLS/CLS';

CREATE TABLE IF NOT EXISTS ${catalog_name}.access_control.user_role_mapping (
    user_id STRING NOT NULL,
    user_email STRING NOT NULL,
    role STRING NOT NULL COMMENT 'EXECUTIVE, COMPLIANCE_OFFICER, FRAUD_ANALYST, UNDERWRITER, AGENT, CLAIMS_ADJUSTER, CUSTOMER_SERVICE, ANALYST, AUDITOR',
    region STRING COMMENT 'Geographic region (Northeast, Southeast, Midwest, West)',
    state_code STRING COMMENT 'State code for state-level access',
    branch_id STRING COMMENT 'Branch ID for branch-level access',
    agent_id STRING COMMENT 'Agent ID for agent-specific access',
    effective_date DATE NOT NULL DEFAULT CURRENT_DATE(),
    expiration_date DATE,
    is_active BOOLEAN DEFAULT TRUE,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    created_by STRING DEFAULT CURRENT_USER()
)
USING DELTA
COMMENT 'User role assignments for RLS enforcement';

-- Sample user role assignments
INSERT INTO ${catalog_name}.access_control.user_role_mapping
(user_id, user_email, role, region, state_code, branch_id)
VALUES
('user001', 'executive@insurance.com', 'EXECUTIVE', NULL, NULL, NULL),
('user002', 'compliance@insurance.com', 'COMPLIANCE_OFFICER', NULL, NULL, NULL),
('user003', 'fraud@insurance.com', 'FRAUD_ANALYST', NULL, NULL, NULL),
('user004', 'underwriter@insurance.com', 'UNDERWRITER', 'Northeast', 'NY', 'BR001'),
('user005', 'agent@insurance.com', 'AGENT', 'Northeast', 'NY', 'BR001'),
('user006', 'claims@insurance.com', 'CLAIMS_ADJUSTER', 'Northeast', NULL, NULL),
('user007', 'service@insurance.com', 'CUSTOMER_SERVICE', 'West', 'CA', 'BR005'),
('user008', 'analyst@insurance.com', 'ANALYST', NULL, NULL, NULL),
('user009', 'auditor@insurance.com', 'AUDITOR', NULL, NULL, NULL);

-- =====================================================
-- 2. USER ATTRIBUTE FUNCTIONS
-- =====================================================

-- Get current user's role
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.get_user_role()
RETURNS STRING
RETURN (
    SELECT role
    FROM ${catalog_name}.access_control.user_role_mapping
    WHERE user_email = CURRENT_USER() AND is_active = TRUE
    LIMIT 1
);

-- Get current user's region
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.get_user_region()
RETURNS STRING
RETURN (
    SELECT region
    FROM ${catalog_name}.access_control.user_role_mapping
    WHERE user_email = CURRENT_USER() AND is_active = TRUE
    LIMIT 1
);

-- Get current user's state
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.get_user_state()
RETURNS STRING
RETURN (
    SELECT state_code
    FROM ${catalog_name}.access_control.user_role_mapping
    WHERE user_email = CURRENT_USER() AND is_active = TRUE
    LIMIT 1
);

-- Get current user's branch
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.get_user_branch()
RETURNS STRING
RETURN (
    SELECT branch_id
    FROM ${catalog_name}.access_control.user_role_mapping
    WHERE user_email = CURRENT_USER() AND is_active = TRUE
    LIMIT 1
);

-- Get current user's agent ID
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.get_user_agent_id()
RETURNS STRING
RETURN (
    SELECT agent_id
    FROM ${catalog_name}.access_control.user_role_mapping
    WHERE user_email = CURRENT_USER() AND is_active = TRUE
    LIMIT 1
);

-- Check if user is member of role
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.is_member(check_role STRING)
RETURNS BOOLEAN
RETURN ${catalog_name}.access_control.get_user_role() = check_role;

-- =====================================================
-- 3. RLS SECURE VIEWS - CUSTOMERS
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}_silver.customers.customers_secure AS
SELECT *
FROM ${catalog_name}_silver.customers.customers_dim
WHERE is_current = TRUE
AND (
    -- Executives: See all customers
    ${catalog_name}.access_control.get_user_role() = 'EXECUTIVE'
    
    -- Compliance officers: See all customers
    OR ${catalog_name}.access_control.get_user_role() = 'COMPLIANCE_OFFICER'
    
    -- Auditors: See all customers (read-only)
    OR ${catalog_name}.access_control.get_user_role() = 'AUDITOR'
    
    -- Agents: See only their assigned customers
    OR (${catalog_name}.access_control.get_user_role() = 'AGENT' 
        AND servicing_agent_id = ${catalog_name}.access_control.get_user_agent_id())
    
    -- Underwriters: See customers in their region
    OR (${catalog_name}.access_control.get_user_role() = 'UNDERWRITER'
        AND region = ${catalog_name}.access_control.get_user_region())
    
    -- Claims adjusters: See customers in their region
    OR (${catalog_name}.access_control.get_user_role() = 'CLAIMS_ADJUSTER'
        AND region = ${catalog_name}.access_control.get_user_region())
    
    -- Customer service: See customers in their branch
    OR (${catalog_name}.access_control.get_user_role() = 'CUSTOMER_SERVICE'
        AND branch_id = ${catalog_name}.access_control.get_user_branch())
    
    -- Fraud analysts: See flagged customers only
    OR (${catalog_name}.access_control.get_user_role() = 'FRAUD_ANALYST'
        AND fraud_flag = TRUE)
    
    -- Analysts: See aggregated/anonymized data only (handled separately)
    OR ${catalog_name}.access_control.get_user_role() = 'ANALYST'
);

-- =====================================================
-- 4. RLS SECURE VIEWS - POLICIES
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}_silver.policies.policies_secure AS
SELECT p.*
FROM ${catalog_name}_silver.policies.policies_fact p
WHERE (
    -- Executives: See all policies
    ${catalog_name}.access_control.get_user_role() = 'EXECUTIVE'
    
    -- Compliance officers: See all policies
    OR ${catalog_name}.access_control.get_user_role() = 'COMPLIANCE_OFFICER'
    
    -- Auditors: See all policies
    OR ${catalog_name}.access_control.get_user_role() = 'AUDITOR'
    
    -- Agents: See policies they sold or service
    OR (${catalog_name}.access_control.get_user_role() = 'AGENT'
        AND agent_id = ${catalog_name}.access_control.get_user_agent_id())
    
    -- Underwriters: See policies in their region
    OR (${catalog_name}.access_control.get_user_role() = 'UNDERWRITER'
        AND EXISTS (
            SELECT 1 FROM ${catalog_name}_silver.customers.customers_dim c
            WHERE c.customer_id = p.customer_id
              AND c.region = ${catalog_name}.access_control.get_user_region()
              AND c.is_current = TRUE
        ))
    
    -- Claims adjusters: See policies with claims in their region
    OR (${catalog_name}.access_control.get_user_role() = 'CLAIMS_ADJUSTER'
        AND EXISTS (
            SELECT 1 FROM ${catalog_name}_silver.claims.claims_fact cl
            JOIN ${catalog_name}_silver.customers.customers_dim c ON cl.customer_id = c.customer_id
            WHERE cl.policy_id = p.policy_id
              AND c.region = ${catalog_name}.access_control.get_user_region()
              AND c.is_current = TRUE
        ))
    
    -- Fraud analysts: See policies with fraud flags
    OR (${catalog_name}.access_control.get_user_role() = 'FRAUD_ANALYST'
        AND fraud_flag = TRUE)
);

-- =====================================================
-- 5. RLS SECURE VIEWS - CLAIMS
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}_silver.claims.claims_secure AS
SELECT cl.*
FROM ${catalog_name}_silver.claims.claims_fact cl
WHERE (
    -- Executives: See all claims
    ${catalog_name}.access_control.get_user_role() = 'EXECUTIVE'
    
    -- Compliance officers: See all claims
    OR ${catalog_name}.access_control.get_user_role() = 'COMPLIANCE_OFFICER'
    
    -- Auditors: See all claims
    OR ${catalog_name}.access_control.get_user_role() = 'AUDITOR'
    
    -- Claims adjusters: See claims assigned to them or in their region
    OR (${catalog_name}.access_control.get_user_role() = 'CLAIMS_ADJUSTER'
        AND (adjuster_id = CURRENT_USER()
             OR EXISTS (
                 SELECT 1 FROM ${catalog_name}_silver.customers.customers_dim c
                 WHERE c.customer_id = cl.customer_id
                   AND c.region = ${catalog_name}.access_control.get_user_region()
                   AND c.is_current = TRUE
             )))
    
    -- Agents: See claims for their customers
    OR (${catalog_name}.access_control.get_user_role() = 'AGENT'
        AND EXISTS (
            SELECT 1 FROM ${catalog_name}_silver.customers.customers_dim c
            WHERE c.customer_id = cl.customer_id
              AND c.servicing_agent_id = ${catalog_name}.access_control.get_user_agent_id()
              AND c.is_current = TRUE
        ))
    
    -- Fraud analysts: See suspicious claims
    OR (${catalog_name}.access_control.get_user_role() = 'FRAUD_ANALYST'
        AND (fraud_score >= 60 OR fraud_flag = TRUE))
    
    -- Underwriters: See claims in their region
    OR (${catalog_name}.access_control.get_user_role() = 'UNDERWRITER'
        AND EXISTS (
            SELECT 1 FROM ${catalog_name}_silver.customers.customers_dim c
            WHERE c.customer_id = cl.customer_id
              AND c.region = ${catalog_name}.access_control.get_user_region()
              AND c.is_current = TRUE
        ))
);

-- =====================================================
-- 6. RLS SECURE VIEWS - CUSTOMER 360
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}_gold.customer_analytics.customer_360_secure AS
SELECT *
FROM ${catalog_name}_gold.customer_analytics.customer_360
WHERE (
    -- Executives: See all customer 360 data
    ${catalog_name}.access_control.get_user_role() = 'EXECUTIVE'
    
    -- Compliance officers: See all
    OR ${catalog_name}.access_control.get_user_role() = 'COMPLIANCE_OFFICER'
    
    -- Agents: See their assigned customers
    OR (${catalog_name}.access_control.get_user_role() = 'AGENT'
        AND primary_agent_id = ${catalog_name}.access_control.get_user_agent_id())
    
    -- Regional managers: See customers in their region
    OR (${catalog_name}.access_control.get_user_role() IN ('UNDERWRITER', 'CLAIMS_ADJUSTER')
        AND customer_region = ${catalog_name}.access_control.get_user_region())
);

-- =====================================================
-- 7. RLS SECURE VIEWS - FRAUD DETECTION
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}_gold.fraud_analytics.fraud_detection_secure AS
SELECT *
FROM ${catalog_name}_gold.fraud_analytics.fraud_detection
WHERE (
    -- Executives: See all fraud data
    ${catalog_name}.access_control.get_user_role() = 'EXECUTIVE'
    
    -- Fraud analysts: See all fraud data
    OR ${catalog_name}.access_control.get_user_role() = 'FRAUD_ANALYST'
    
    -- Compliance officers: See all fraud data
    OR ${catalog_name}.access_control.get_user_role() = 'COMPLIANCE_OFFICER'
    
    -- Auditors: See all fraud data
    OR ${catalog_name}.access_control.get_user_role() = 'AUDITOR'
    
    -- Claims adjusters: See high and critical risk cases in their region
    OR (${catalog_name}.access_control.get_user_role() = 'CLAIMS_ADJUSTER'
        AND risk_category IN ('High', 'Critical')
        AND EXISTS (
            SELECT 1 FROM ${catalog_name}_silver.customers.customers_dim c
            WHERE c.customer_id = fraud_detection.customer_id
              AND c.region = ${catalog_name}.access_control.get_user_region()
              AND c.is_current = TRUE
        ))
);

-- =====================================================
-- 8. GRANT PERMISSIONS ON SECURE VIEWS
-- =====================================================

-- Revoke direct access to base tables (force use of secure views)
-- REVOKE ALL PRIVILEGES ON ${catalog_name}_silver.customers.customers_dim FROM PUBLIC;
-- REVOKE ALL PRIVILEGES ON ${catalog_name}_silver.policies.policies_fact FROM PUBLIC;
-- REVOKE ALL PRIVILEGES ON ${catalog_name}_silver.claims.claims_fact FROM PUBLIC;

-- Grant access to secure views
GRANT SELECT ON ${catalog_name}_silver.customers.customers_secure TO `all_users`;
GRANT SELECT ON ${catalog_name}_silver.policies.policies_secure TO `all_users`;
GRANT SELECT ON ${catalog_name}_silver.claims.claims_secure TO `all_users`;
GRANT SELECT ON ${catalog_name}_gold.customer_analytics.customer_360_secure TO `all_users`;
GRANT SELECT ON ${catalog_name}_gold.fraud_analytics.fraud_detection_secure TO `all_users`;

-- =====================================================
-- 9. RLS TESTING QUERIES
-- =====================================================

-- Test current user's role
SELECT ${catalog_name}.access_control.get_user_role() AS my_role;

-- Test current user's accessible customers
SELECT COUNT(*) AS accessible_customers
FROM ${catalog_name}_silver.customers.customers_secure;

-- Test current user's accessible policies
SELECT COUNT(*) AS accessible_policies
FROM ${catalog_name}_silver.policies.policies_secure;

-- Test current user's accessible claims
SELECT COUNT(*) AS accessible_claims
FROM ${catalog_name}_silver.claims.claims_secure;

-- =====================================================
-- 10. RLS AUDIT LOG
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.access_control.rls_access_log (
    access_id BIGINT GENERATED ALWAYS AS IDENTITY,
    access_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    user_id STRING NOT NULL,
    user_role STRING NOT NULL,
    view_name STRING NOT NULL,
    rows_returned BIGINT,
    filter_applied STRING COMMENT 'RLS filter criteria applied',
    session_id STRING,
    query_text STRING
)
USING DELTA
COMMENT 'Audit log for RLS-protected view access';

-- =====================================================
-- USAGE NOTES
-- =====================================================

-- 1. Always use secure views (e.g., customers_secure) instead of base tables
-- 2. RLS filters are enforced at query time based on CURRENT_USER()
-- 3. Executives and compliance officers have full access
-- 4. Regional access is enforced for underwriters and claims adjusters
-- 5. Agents see only their assigned customers
-- 6. Fraud analysts see only flagged/suspicious records

-- Example queries:
-- SELECT * FROM ${catalog_name}_silver.customers.customers_secure WHERE state = 'NY';
-- SELECT * FROM ${catalog_name}_silver.claims.claims_secure WHERE claim_status = 'Pending';

