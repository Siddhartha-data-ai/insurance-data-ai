-- =====================================================
-- INSURANCE DATA AI - COLUMN-LEVEL SECURITY (CLS)
-- Dynamic data masking based on user roles
-- =====================================================

-- =====================================================
-- 1. COLUMN MASKING FUNCTIONS (Reusable)
-- =====================================================

-- Mask SSN (show only last 4 digits)
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.mask_ssn_cls(ssn STRING, user_role STRING)
RETURNS STRING
RETURN CASE
    WHEN user_role IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'AUDITOR') THEN ssn
    WHEN user_role IN ('UNDERWRITER', 'FRAUD_ANALYST') THEN CONCAT('XXX-XX-', RIGHT(ssn, 4))
    ELSE 'XXX-XX-XXXX'
END;

-- Mask email address
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.mask_email_cls(email STRING, user_role STRING)
RETURNS STRING
RETURN CASE
    WHEN user_role IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'AGENT', 'CUSTOMER_SERVICE') THEN email
    WHEN user_role IN ('UNDERWRITER', 'CLAIMS_ADJUSTER') THEN CONCAT(LEFT(email, 3), '***', SUBSTRING(email, INSTR(email, '@')))
    ELSE CONCAT('***', SUBSTRING(email, INSTR(email, '@')))
END;

-- Mask phone number
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.mask_phone_cls(phone STRING, user_role STRING)
RETURNS STRING
RETURN CASE
    WHEN user_role IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'AGENT', 'CUSTOMER_SERVICE', 'CLAIMS_ADJUSTER') THEN phone
    WHEN user_role IN ('UNDERWRITER') THEN CONCAT('XXX-XXX-', RIGHT(REGEXP_REPLACE(phone, '[^0-9]', ''), 4))
    ELSE 'XXX-XXX-XXXX'
END;

-- Mask address
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.mask_address_cls(address STRING, user_role STRING)
RETURNS STRING
RETURN CASE
    WHEN user_role IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'AGENT', 'UNDERWRITER') THEN address
    WHEN user_role IN ('CLAIMS_ADJUSTER') THEN CONCAT(LEFT(address, 3), '***')
    ELSE '***'
END;

-- Mask date of birth (show only year)
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.mask_dob_cls(dob DATE, user_role STRING)
RETURNS STRING
RETURN CASE
    WHEN user_role IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'UNDERWRITER', 'AUDITOR') THEN CAST(dob AS STRING)
    WHEN user_role IN ('CLAIMS_ADJUSTER', 'AGENT') THEN CONCAT(YEAR(dob), '-XX-XX')
    ELSE 'XXXX-XX-XX'
END;

-- Mask financial amounts (premium, claim amounts)
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.mask_amount_cls(amount DECIMAL(15,2), user_role STRING)
RETURNS STRING
RETURN CASE
    WHEN user_role IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'UNDERWRITER', 'CLAIMS_ADJUSTER', 'AGENT', 'AUDITOR') THEN CAST(amount AS STRING)
    WHEN user_role IN ('ANALYST') THEN CAST(ROUND(amount, -3) AS STRING)  -- Round to nearest thousand
    ELSE '***'
END;

-- Mask policy/account numbers
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.mask_policy_number_cls(policy_num STRING, user_role STRING)
RETURNS STRING
RETURN CASE
    WHEN user_role IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'AGENT', 'UNDERWRITER', 'CLAIMS_ADJUSTER') THEN policy_num
    WHEN user_role IN ('AUDITOR') THEN CONCAT(REPEAT('*', LENGTH(policy_num) - 4), RIGHT(policy_num, 4))
    ELSE REPEAT('*', LENGTH(policy_num))
END;

-- Mask credit score
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.mask_credit_score_cls(credit_score INT, user_role STRING)
RETURNS STRING
RETURN CASE
    WHEN user_role IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'UNDERWRITER', 'AUDITOR') THEN CAST(credit_score AS STRING)
    WHEN user_role IN ('AGENT') THEN 
        CASE 
            WHEN credit_score >= 740 THEN 'Excellent'
            WHEN credit_score >= 670 THEN 'Good'
            WHEN credit_score >= 580 THEN 'Fair'
            ELSE 'Poor'
        END
    ELSE 'REDACTED'
END;

-- Mask fraud score
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.mask_fraud_score_cls(fraud_score DECIMAL(5,2), user_role STRING)
RETURNS STRING
RETURN CASE
    WHEN user_role IN ('EXECUTIVE', 'FRAUD_ANALYST', 'COMPLIANCE_OFFICER', 'AUDITOR') THEN CAST(fraud_score AS STRING)
    WHEN user_role IN ('CLAIMS_ADJUSTER') AND fraud_score >= 60 THEN CAST(fraud_score AS STRING)
    WHEN user_role IN ('CLAIMS_ADJUSTER') THEN 'Low Risk'
    ELSE 'REDACTED'
END;

-- Mask medical/health information
CREATE OR REPLACE FUNCTION ${catalog_name}.access_control.mask_medical_info_cls(medical_info STRING, user_role STRING)
RETURNS STRING
RETURN CASE
    WHEN user_role IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'CLAIMS_ADJUSTER', 'MEDICAL_REVIEWER') THEN medical_info
    ELSE 'MEDICAL_INFO_REDACTED'
END;

-- =====================================================
-- 2. CLS SECURE VIEWS - CUSTOMERS
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}_silver.customers.customers_cls_secure AS
SELECT
    customer_id,
    
    -- Name masking
    CASE 
        WHEN ${catalog_name}.access_control.get_user_role() IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'AGENT', 'UNDERWRITER', 'CLAIMS_ADJUSTER', 'CUSTOMER_SERVICE') 
        THEN customer_name
        ELSE CONCAT(LEFT(customer_name, 1), REPEAT('*', LENGTH(customer_name) - 1))
    END AS customer_name,
    
    -- SSN masking
    ${catalog_name}.access_control.mask_ssn_cls(ssn_last_4, ${catalog_name}.access_control.get_user_role()) AS ssn_last_4,
    
    -- Date of birth masking
    ${catalog_name}.access_control.mask_dob_cls(date_of_birth, ${catalog_name}.access_control.get_user_role()) AS date_of_birth,
    
    -- Email masking
    ${catalog_name}.access_control.mask_email_cls(email, ${catalog_name}.access_control.get_user_role()) AS email,
    
    -- Phone masking
    ${catalog_name}.access_control.mask_phone_cls(phone, ${catalog_name}.access_control.get_user_role()) AS phone,
    
    -- Address masking
    ${catalog_name}.access_control.mask_address_cls(address_line1, ${catalog_name}.access_control.get_user_role()) AS address_line1,
    ${catalog_name}.access_control.mask_address_cls(address_line2, ${catalog_name}.access_control.get_user_role()) AS address_line2,
    
    -- Non-sensitive fields (no masking)
    city,
    state,
    zip_code,
    
    -- Credit score masking
    ${catalog_name}.access_control.mask_credit_score_cls(credit_score, ${catalog_name}.access_control.get_user_role()) AS credit_score,
    
    -- Financial info masking
    CASE 
        WHEN ${catalog_name}.access_control.get_user_role() IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'UNDERWRITER', 'AUDITOR') 
        THEN annual_income
        ELSE NULL
    END AS annual_income,
    
    -- Non-sensitive metadata
    customer_segment,
    customer_status,
    created_date,
    updated_date,
    is_current,
    effective_date,
    end_date
    
FROM ${catalog_name}_silver.customers.customers_dim
WHERE is_current = TRUE;

-- =====================================================
-- 3. CLS SECURE VIEWS - POLICIES
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}_silver.policies.policies_cls_secure AS
SELECT
    -- Policy identifiers
    ${catalog_name}.access_control.mask_policy_number_cls(policy_id, ${catalog_name}.access_control.get_user_role()) AS policy_id,
    customer_id,
    
    -- Policy details
    policy_type,
    policy_status,
    
    -- Financial amounts with masking
    ${catalog_name}.access_control.mask_amount_cls(coverage_amount, ${catalog_name}.access_control.get_user_role()) AS coverage_amount,
    ${catalog_name}.access_control.mask_amount_cls(premium_amount, ${catalog_name}.access_control.get_user_role()) AS premium_amount,
    ${catalog_name}.access_control.mask_amount_cls(deductible_amount, ${catalog_name}.access_control.get_user_role()) AS deductible_amount,
    
    -- Dates (typically not masked)
    start_date,
    end_date,
    issue_date,
    renewal_date,
    
    -- Agent information
    agent_id,
    
    -- Risk information (restricted access)
    CASE 
        WHEN ${catalog_name}.access_control.get_user_role() IN ('EXECUTIVE', 'UNDERWRITER', 'FRAUD_ANALYST', 'AUDITOR') 
        THEN risk_score
        ELSE NULL
    END AS risk_score,
    
    -- Metadata
    created_date,
    updated_date
    
FROM ${catalog_name}_silver.policies.policies_fact;

-- =====================================================
-- 4. CLS SECURE VIEWS - CLAIMS
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}_silver.claims.claims_cls_secure AS
SELECT
    -- Claim identifiers
    claim_id,
    policy_id,
    customer_id,
    
    -- Claim details
    claim_type,
    claim_status,
    
    -- Financial amounts with masking
    ${catalog_name}.access_control.mask_amount_cls(claim_amount, ${catalog_name}.access_control.get_user_role()) AS claim_amount,
    ${catalog_name}.access_control.mask_amount_cls(approved_amount, ${catalog_name}.access_control.get_user_role()) AS approved_amount,
    ${catalog_name}.access_control.mask_amount_cls(paid_amount, ${catalog_name}.access_control.get_user_role()) AS paid_amount,
    
    -- Medical/health information (HIPAA protected)
    ${catalog_name}.access_control.mask_medical_info_cls(medical_diagnosis, ${catalog_name}.access_control.get_user_role()) AS medical_diagnosis,
    ${catalog_name}.access_control.mask_medical_info_cls(treatment_description, ${catalog_name}.access_control.get_user_role()) AS treatment_description,
    
    -- Incident description (may contain sensitive info)
    CASE 
        WHEN ${catalog_name}.access_control.get_user_role() IN ('EXECUTIVE', 'COMPLIANCE_OFFICER', 'CLAIMS_ADJUSTER', 'FRAUD_ANALYST', 'AUDITOR') 
        THEN incident_description
        ELSE 'RESTRICTED'
    END AS incident_description,
    
    -- Fraud information (restricted)
    ${catalog_name}.access_control.mask_fraud_score_cls(fraud_score, ${catalog_name}.access_control.get_user_role()) AS fraud_score,
    
    CASE 
        WHEN ${catalog_name}.access_control.get_user_role() IN ('EXECUTIVE', 'FRAUD_ANALYST', 'COMPLIANCE_OFFICER', 'AUDITOR') 
        THEN fraud_indicators
        ELSE NULL
    END AS fraud_indicators,
    
    -- Dates (typically not masked)
    claim_date,
    incident_date,
    filed_date,
    closed_date,
    
    -- Adjuster information
    adjuster_id,
    
    -- Metadata
    created_date,
    updated_date
    
FROM ${catalog_name}_silver.claims.claims_fact;

-- =====================================================
-- 5. CLS SECURE VIEWS - CUSTOMER 360 (Combined RLS + CLS)
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}_gold.customer_analytics.customer_360_cls_secure AS
SELECT
    customer_id,
    
    -- Masked PII
    ${catalog_name}.access_control.mask_email_cls(customer_email, ${catalog_name}.access_control.get_user_role()) AS customer_email,
    ${catalog_name}.access_control.mask_phone_cls(customer_phone, ${catalog_name}.access_control.get_user_role()) AS customer_phone,
    
    -- Non-sensitive metrics
    total_policies,
    active_policies,
    total_claims,
    
    -- Financial metrics (masked for analysts)
    ${catalog_name}.access_control.mask_amount_cls(total_premium_paid, ${catalog_name}.access_control.get_user_role()) AS total_premium_paid,
    ${catalog_name}.access_control.mask_amount_cls(total_claims_amount, ${catalog_name}.access_control.get_user_role()) AS total_claims_amount,
    ${catalog_name}.access_control.mask_amount_cls(customer_lifetime_value, ${catalog_name}.access_control.get_user_role()) AS customer_lifetime_value,
    
    -- Risk scores (restricted)
    CASE 
        WHEN ${catalog_name}.access_control.get_user_role() IN ('EXECUTIVE', 'UNDERWRITER', 'FRAUD_ANALYST', 'AUDITOR') 
        THEN churn_risk_score
        ELSE NULL
    END AS churn_risk_score,
    
    ${catalog_name}.access_control.mask_fraud_score_cls(fraud_risk_score, ${catalog_name}.access_control.get_user_role()) AS fraud_risk_score,
    
    -- Non-sensitive attributes
    customer_segment,
    customer_tenure_months,
    primary_agent_id,
    customer_region,
    last_interaction_date
    
FROM ${catalog_name}_gold.customer_analytics.customer_360
WHERE (
    -- Apply RLS filter
    ${catalog_name}.access_control.get_user_role() = 'EXECUTIVE'
    OR ${catalog_name}.access_control.get_user_role() = 'COMPLIANCE_OFFICER'
    OR (${catalog_name}.access_control.get_user_role() = 'AGENT'
        AND primary_agent_id = ${catalog_name}.access_control.get_user_agent_id())
    OR (${catalog_name}.access_control.get_user_role() IN ('UNDERWRITER', 'CLAIMS_ADJUSTER')
        AND customer_region = ${catalog_name}.access_control.get_user_region())
);

-- =====================================================
-- 6. CLS POLICY MATRIX
-- =====================================================

CREATE OR REPLACE VIEW ${catalog_name}.access_control.cls_policy_matrix AS
SELECT 'SSN' AS data_field, 'EXECUTIVE' AS role, 'FULL_ACCESS' AS access_level
UNION ALL SELECT 'SSN', 'COMPLIANCE_OFFICER', 'FULL_ACCESS'
UNION ALL SELECT 'SSN', 'UNDERWRITER', 'LAST_4_ONLY'
UNION ALL SELECT 'SSN', 'AGENT', 'MASKED'
UNION ALL SELECT 'SSN', 'ANALYST', 'NO_ACCESS'

UNION ALL SELECT 'EMAIL', 'EXECUTIVE', 'FULL_ACCESS'
UNION ALL SELECT 'EMAIL', 'AGENT', 'FULL_ACCESS'
UNION ALL SELECT 'EMAIL', 'CUSTOMER_SERVICE', 'FULL_ACCESS'
UNION ALL SELECT 'EMAIL', 'ANALYST', 'DOMAIN_ONLY'

UNION ALL SELECT 'PHONE', 'AGENT', 'FULL_ACCESS'
UNION ALL SELECT 'PHONE', 'CUSTOMER_SERVICE', 'FULL_ACCESS'
UNION ALL SELECT 'PHONE', 'CLAIMS_ADJUSTER', 'FULL_ACCESS'
UNION ALL SELECT 'PHONE', 'ANALYST', 'LAST_4_ONLY'

UNION ALL SELECT 'CLAIM_AMOUNT', 'EXECUTIVE', 'FULL_ACCESS'
UNION ALL SELECT 'CLAIM_AMOUNT', 'CLAIMS_ADJUSTER', 'FULL_ACCESS'
UNION ALL SELECT 'CLAIM_AMOUNT', 'ANALYST', 'ROUNDED'

UNION ALL SELECT 'CREDIT_SCORE', 'EXECUTIVE', 'FULL_ACCESS'
UNION ALL SELECT 'CREDIT_SCORE', 'UNDERWRITER', 'FULL_ACCESS'
UNION ALL SELECT 'CREDIT_SCORE', 'AGENT', 'RANGE_ONLY'
UNION ALL SELECT 'CREDIT_SCORE', 'ANALYST', 'NO_ACCESS'

UNION ALL SELECT 'FRAUD_SCORE', 'FRAUD_ANALYST', 'FULL_ACCESS'
UNION ALL SELECT 'FRAUD_SCORE', 'EXECUTIVE', 'FULL_ACCESS'
UNION ALL SELECT 'FRAUD_SCORE', 'CLAIMS_ADJUSTER', 'HIGH_RISK_ONLY'
UNION ALL SELECT 'FRAUD_SCORE', 'AGENT', 'NO_ACCESS'

UNION ALL SELECT 'MEDICAL_INFO', 'CLAIMS_ADJUSTER', 'FULL_ACCESS'
UNION ALL SELECT 'MEDICAL_INFO', 'MEDICAL_REVIEWER', 'FULL_ACCESS'
UNION ALL SELECT 'MEDICAL_INFO', 'COMPLIANCE_OFFICER', 'FULL_ACCESS'
UNION ALL SELECT 'MEDICAL_INFO', 'AGENT', 'NO_ACCESS'
UNION ALL SELECT 'MEDICAL_INFO', 'ANALYST', 'NO_ACCESS';

-- =====================================================
-- 7. GRANT PERMISSIONS ON CLS SECURE VIEWS
-- =====================================================

-- Grant SELECT on CLS-protected views to all users
GRANT SELECT ON ${catalog_name}_silver.customers.customers_cls_secure TO `all_users`;
GRANT SELECT ON ${catalog_name}_silver.policies.policies_cls_secure TO `all_users`;
GRANT SELECT ON ${catalog_name}_silver.claims.claims_cls_secure TO `all_users`;
GRANT SELECT ON ${catalog_name}_gold.customer_analytics.customer_360_cls_secure TO `all_users`;

-- =====================================================
-- 8. CLS TESTING & VALIDATION
-- =====================================================

-- Test CLS masking for current user
SELECT 
    'Current User' AS info_type,
    CURRENT_USER() AS value
UNION ALL
SELECT 
    'Current Role' AS info_type,
    ${catalog_name}.access_control.get_user_role() AS value;

-- Test SSN masking
SELECT ${catalog_name}.access_control.mask_ssn_cls('123-45-6789', ${catalog_name}.access_control.get_user_role()) AS masked_ssn;

-- Test email masking
SELECT ${catalog_name}.access_control.mask_email_cls('john.doe@example.com', ${catalog_name}.access_control.get_user_role()) AS masked_email;

-- Test sample customer data with CLS
SELECT 
    customer_id,
    customer_name,
    ssn_last_4,
    email,
    phone,
    credit_score
FROM ${catalog_name}_silver.customers.customers_cls_secure
LIMIT 5;

-- =====================================================
-- USAGE NOTES
-- =====================================================

-- 1. Always use CLS-protected views (*_cls_secure) for applications
-- 2. Masking is applied dynamically based on CURRENT_USER() role
-- 3. Executives and compliance officers have full access to all fields
-- 4. Sensitive fields (SSN, medical info) are heavily restricted
-- 5. Financial amounts are rounded for analysts
-- 6. Combine with RLS views for both row and column level protection

-- Example: Query with both RLS and CLS
-- SELECT * FROM ${catalog_name}_gold.customer_analytics.customer_360_cls_secure
-- WHERE customer_segment = 'Premium';

