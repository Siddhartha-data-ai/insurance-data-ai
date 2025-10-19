-- =====================================================
-- INSURANCE DATA AI - PII/PHI TAGGING SYSTEM
-- Automated PII/PHI Classification and Tagging
-- Unity Catalog Column-Level Tags
-- =====================================================

-- Create PII classification schema
CREATE SCHEMA IF NOT EXISTS ${catalog_name}.pii_classification
COMMENT 'PII/PHI classification, tagging, and discovery system';

-- =====================================================
-- 1. PII/PHI COLUMN INVENTORY
-- =====================================================

CREATE TABLE IF NOT EXISTS ${catalog_name}.pii_classification.pii_phi_inventory (
    inventory_id BIGINT GENERATED ALWAYS AS IDENTITY,
    catalog_name STRING NOT NULL,
    schema_name STRING NOT NULL,
    table_name STRING NOT NULL,
    column_name STRING NOT NULL,
    data_classification STRING NOT NULL COMMENT 'PUBLIC, INTERNAL, CONFIDENTIAL, RESTRICTED, PII, PHI',
    pii_category STRING COMMENT 'IDENTITY, CONTACT, FINANCIAL, DEMOGRAPHIC, HEALTH, BEHAVIORAL',
    sensitivity_level STRING NOT NULL COMMENT 'LOW, MEDIUM, HIGH, CRITICAL',
    regulatory_classification ARRAY<STRING> COMMENT 'GDPR_PERSONAL_DATA, GDPR_SPECIAL_CATEGORY, HIPAA_PHI, PCI_DSS, CCPA',
    masking_required BOOLEAN DEFAULT TRUE,
    masking_function STRING COMMENT 'Function to use for data masking',
    encryption_required BOOLEAN DEFAULT FALSE,
    access_restricted BOOLEAN DEFAULT TRUE,
    allowed_roles ARRAY<STRING> COMMENT 'Roles with unmasked access',
    discovery_method STRING COMMENT 'MANUAL, PATTERN_MATCH, ML_CLASSIFICATION',
    discovery_confidence DECIMAL(5,2) COMMENT 'Confidence score (0-100) for automated discovery',
    sample_values_hash STRING COMMENT 'Hash of sample values for validation',
    row_count BIGINT,
    distinct_count BIGINT,
    null_percentage DECIMAL(5,2),
    last_scanned_date TIMESTAMP,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP,
    tagged_by STRING DEFAULT CURRENT_USER()
)
USING DELTA
COMMENT 'Complete inventory of PII/PHI fields with classification metadata';

-- Pre-populate PII/PHI inventory for insurance tables
INSERT INTO ${catalog_name}.pii_classification.pii_phi_inventory
(catalog_name, schema_name, table_name, column_name, data_classification, pii_category, sensitivity_level, regulatory_classification, masking_function, allowed_roles)
VALUES
-- Customer PII
('${catalog_name}_bronze', 'customers', 'customers_raw', 'ssn', 'PII', 'IDENTITY', 'CRITICAL', ARRAY('GDPR_PERSONAL_DATA', 'HIPAA_PHI'), 'mask_ssn', ARRAY('compliance_officers', 'security_team')),
('${catalog_name}_bronze', 'customers', 'customers_raw', 'first_name', 'PII', 'IDENTITY', 'HIGH', ARRAY('GDPR_PERSONAL_DATA'), 'mask_name', ARRAY('agents', 'underwriters', 'claims_adjusters')),
('${catalog_name}_bronze', 'customers', 'customers_raw', 'last_name', 'PII', 'IDENTITY', 'HIGH', ARRAY('GDPR_PERSONAL_DATA'), 'mask_name', ARRAY('agents', 'underwriters', 'claims_adjusters')),
('${catalog_name}_bronze', 'customers', 'customers_raw', 'date_of_birth', 'PII', 'DEMOGRAPHIC', 'HIGH', ARRAY('GDPR_PERSONAL_DATA', 'HIPAA_PHI'), 'mask_dob', ARRAY('underwriters', 'actuaries')),
('${catalog_name}_bronze', 'customers', 'customers_raw', 'email', 'PII', 'CONTACT', 'MEDIUM', ARRAY('GDPR_PERSONAL_DATA'), 'mask_email', ARRAY('agents', 'customer_service')),
('${catalog_name}_bronze', 'customers', 'customers_raw', 'phone', 'PII', 'CONTACT', 'MEDIUM', ARRAY('GDPR_PERSONAL_DATA'), 'mask_phone', ARRAY('agents', 'customer_service')),
('${catalog_name}_bronze', 'customers', 'customers_raw', 'address_line1', 'PII', 'CONTACT', 'MEDIUM', ARRAY('GDPR_PERSONAL_DATA'), 'mask_address', ARRAY('agents', 'underwriters')),
('${catalog_name}_bronze', 'customers', 'customers_raw', 'address_line2', 'PII', 'CONTACT', 'MEDIUM', ARRAY('GDPR_PERSONAL_DATA'), 'mask_address', ARRAY('agents', 'underwriters')),
('${catalog_name}_bronze', 'customers', 'customers_raw', 'city', 'PII', 'CONTACT', 'LOW', ARRAY('GDPR_PERSONAL_DATA'), NULL, ARRAY('agents', 'underwriters', 'analysts')),
('${catalog_name}_bronze', 'customers', 'customers_raw', 'zip_code', 'PII', 'CONTACT', 'LOW', ARRAY('GDPR_PERSONAL_DATA'), NULL, ARRAY('agents', 'underwriters', 'analysts')),

-- Policy Financial PII
('${catalog_name}_bronze', 'policies', 'policies_raw', 'policy_number', 'PII', 'FINANCIAL', 'HIGH', ARRAY('GDPR_PERSONAL_DATA', 'HIPAA_PHI'), 'mask_policy_number', ARRAY('agents', 'underwriters')),
('${catalog_name}_bronze', 'policies', 'policies_raw', 'premium_amount', 'CONFIDENTIAL', 'FINANCIAL', 'MEDIUM', ARRAY('GDPR_PERSONAL_DATA'), NULL, ARRAY('agents', 'underwriters', 'finance_team')),
('${catalog_name}_bronze', 'policies', 'policies_raw', 'coverage_amount', 'CONFIDENTIAL', 'FINANCIAL', 'MEDIUM', ARRAY('GDPR_PERSONAL_DATA'), NULL, ARRAY('agents', 'underwriters')),
('${catalog_name}_bronze', 'policies', 'policies_raw', 'bank_account_number', 'PII', 'FINANCIAL', 'CRITICAL', ARRAY('GDPR_PERSONAL_DATA', 'PCI_DSS'), 'mask_account_number', ARRAY('finance_team', 'compliance_officers')),

-- Claims PHI/PII
('${catalog_name}_bronze', 'claims', 'claims_raw', 'claim_id', 'INTERNAL', 'IDENTITY', 'MEDIUM', ARRAY('GDPR_PERSONAL_DATA'), NULL, ARRAY('claims_adjusters', 'agents')),
('${catalog_name}_bronze', 'claims', 'claims_raw', 'medical_diagnosis', 'PHI', 'HEALTH', 'CRITICAL', ARRAY('HIPAA_PHI', 'GDPR_SPECIAL_CATEGORY'), 'mask_diagnosis', ARRAY('medical_reviewers', 'claims_adjusters')),
('${catalog_name}_bronze', 'claims', 'claims_raw', 'treatment_description', 'PHI', 'HEALTH', 'CRITICAL', ARRAY('HIPAA_PHI', 'GDPR_SPECIAL_CATEGORY'), 'mask_treatment', ARRAY('medical_reviewers', 'claims_adjusters')),
('${catalog_name}_bronze', 'claims', 'claims_raw', 'prescription_details', 'PHI', 'HEALTH', 'CRITICAL', ARRAY('HIPAA_PHI'), 'mask_prescription', ARRAY('medical_reviewers', 'pharmacists')),
('${catalog_name}_bronze', 'claims', 'claims_raw', 'claim_amount', 'CONFIDENTIAL', 'FINANCIAL', 'HIGH', ARRAY('GDPR_PERSONAL_DATA'), NULL, ARRAY('claims_adjusters', 'finance_team')),
('${catalog_name}_bronze', 'claims', 'claims_raw', 'incident_description', 'PII', 'BEHAVIORAL', 'MEDIUM', ARRAY('GDPR_PERSONAL_DATA'), NULL, ARRAY('claims_adjusters', 'fraud_investigators'));

-- =====================================================
-- 2. PII/PHI MASKING FUNCTIONS
-- =====================================================

-- Mask SSN (show only last 4 digits)
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.mask_ssn(ssn STRING)
RETURNS STRING
RETURN CONCAT('XXX-XX-', SUBSTRING(ssn, -4, 4));

-- Mask name (show only first initial)
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.mask_name(name STRING)
RETURNS STRING
RETURN CONCAT(SUBSTRING(name, 1, 1), REPEAT('*', LENGTH(name) - 1));

-- Mask email (show only domain)
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.mask_email(email STRING)
RETURNS STRING
RETURN CONCAT(REPEAT('*', INSTR(email, '@') - 1), SUBSTRING(email, INSTR(email, '@')));

-- Mask phone (show only last 4 digits)
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.mask_phone(phone STRING)
RETURNS STRING
RETURN CONCAT('XXX-XXX-', SUBSTRING(REGEXP_REPLACE(phone, '[^0-9]', ''), -4, 4));

-- Mask address (keep only city/state)
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.mask_address(address STRING)
RETURNS STRING
RETURN '****';

-- Mask date of birth (show only year)
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.mask_dob(dob DATE)
RETURNS STRING
RETURN CONCAT(YEAR(dob), '-XX-XX');

-- Mask policy/account numbers (show only last 4)
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.mask_account_number(account_num STRING)
RETURNS STRING
RETURN CONCAT(REPEAT('*', LENGTH(account_num) - 4), SUBSTRING(account_num, -4, 4));

-- Mask diagnosis codes (high-level category only)
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.mask_diagnosis(diagnosis STRING)
RETURNS STRING
RETURN 'MEDICAL_CONDITION';

-- Mask treatment details
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.mask_treatment(treatment STRING)
RETURNS STRING
RETURN 'TREATMENT_DETAILS_REDACTED';

-- Mask prescriptions
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.mask_prescription(prescription STRING)
RETURNS STRING
RETURN 'PRESCRIPTION_INFO';

-- =====================================================
-- 3. AUTOMATED PII DISCOVERY
-- =====================================================

CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.detect_pii_pattern(
    column_name STRING,
    sample_value STRING
)
RETURNS STRUCT<is_pii BOOLEAN, pii_type STRING, confidence DECIMAL(5,2)>
RETURN CASE
    -- SSN pattern: XXX-XX-XXXX
    WHEN REGEXP_LIKE(sample_value, '^\\d{3}-\\d{2}-\\d{4}$') THEN
        STRUCT(TRUE, 'SSN', 95.0)
    
    -- Email pattern
    WHEN REGEXP_LIKE(sample_value, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$') THEN
        STRUCT(TRUE, 'EMAIL', 90.0)
    
    -- Phone pattern: (XXX) XXX-XXXX or XXX-XXX-XXXX
    WHEN REGEXP_LIKE(sample_value, '^\\(?\\d{3}\\)?[-.\\s]?\\d{3}[-.\\s]?\\d{4}$') THEN
        STRUCT(TRUE, 'PHONE', 90.0)
    
    -- Credit card pattern: 16 digits
    WHEN REGEXP_LIKE(sample_value, '^\\d{4}[-.\\s]?\\d{4}[-.\\s]?\\d{4}[-.\\s]?\\d{4}$') THEN
        STRUCT(TRUE, 'CREDIT_CARD', 95.0)
    
    -- Date of birth (column name check)
    WHEN LOWER(column_name) LIKE '%birth%' OR LOWER(column_name) LIKE '%dob%' THEN
        STRUCT(TRUE, 'DATE_OF_BIRTH', 85.0)
    
    -- Address (column name check)
    WHEN LOWER(column_name) LIKE '%address%' OR LOWER(column_name) LIKE '%street%' THEN
        STRUCT(TRUE, 'ADDRESS', 80.0)
    
    -- Name (column name check)
    WHEN LOWER(column_name) LIKE '%name%' OR LOWER(column_name) IN ('first_name', 'last_name', 'full_name') THEN
        STRUCT(TRUE, 'NAME', 75.0)
    
    ELSE
        STRUCT(FALSE, 'NONE', 0.0)
END;

-- =====================================================
-- 4. PII CLASSIFICATION VIEWS
-- =====================================================

-- Summary by sensitivity level
CREATE OR REPLACE VIEW ${catalog_name}.pii_classification.pii_summary_by_sensitivity AS
SELECT
    sensitivity_level,
    COUNT(*) AS total_columns,
    COUNT(DISTINCT CONCAT(catalog_name, '.', schema_name, '.', table_name)) AS total_tables,
    SUM(CASE WHEN masking_required THEN 1 ELSE 0 END) AS columns_requiring_masking,
    SUM(CASE WHEN encryption_required THEN 1 ELSE 0 END) AS columns_requiring_encryption
FROM ${catalog_name}.pii_classification.pii_phi_inventory
GROUP BY sensitivity_level
ORDER BY 
    CASE sensitivity_level
        WHEN 'CRITICAL' THEN 1
        WHEN 'HIGH' THEN 2
        WHEN 'MEDIUM' THEN 3
        WHEN 'LOW' THEN 4
    END;

-- PII columns by table
CREATE OR REPLACE VIEW ${catalog_name}.pii_classification.pii_columns_by_table AS
SELECT
    CONCAT(catalog_name, '.', schema_name, '.', table_name) AS full_table_name,
    COUNT(*) AS pii_column_count,
    COLLECT_LIST(
        STRUCT(
            column_name,
            data_classification,
            sensitivity_level,
            masking_function
        )
    ) AS pii_columns
FROM ${catalog_name}.pii_classification.pii_phi_inventory
WHERE data_classification IN ('PII', 'PHI')
GROUP BY catalog_name, schema_name, table_name
ORDER BY pii_column_count DESC;

-- GDPR special category data (Article 9)
CREATE OR REPLACE VIEW ${catalog_name}.pii_classification.gdpr_special_category_data AS
SELECT
    CONCAT(catalog_name, '.', schema_name, '.', table_name) AS full_table_name,
    column_name,
    pii_category,
    sensitivity_level,
    masking_function,
    allowed_roles
FROM ${catalog_name}.pii_classification.pii_phi_inventory
WHERE ARRAY_CONTAINS(regulatory_classification, 'GDPR_SPECIAL_CATEGORY')
ORDER BY sensitivity_level, full_table_name;

-- =====================================================
-- 5. PII TAG APPLICATION (Unity Catalog)
-- =====================================================

-- Apply Unity Catalog tags to columns
-- Note: Run this after tables are created

-- Example: Tag SSN columns
-- ALTER TABLE ${catalog_name}_bronze.customers.customers_raw
-- ALTER COLUMN ssn SET TAGS ('pii_type' = 'SSN', 'sensitivity' = 'CRITICAL', 'masking_required' = 'true');

-- =====================================================
-- 6. PII AUDIT QUERIES
-- =====================================================

-- Check if a column is PII
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.is_pii_column(
    p_catalog STRING,
    p_schema STRING,
    p_table STRING,
    p_column STRING
)
RETURNS BOOLEAN
RETURN EXISTS(
    SELECT 1
    FROM ${catalog_name}.pii_classification.pii_phi_inventory
    WHERE catalog_name = p_catalog
      AND schema_name = p_schema
      AND table_name = p_table
      AND column_name = p_column
      AND data_classification IN ('PII', 'PHI')
);

-- Get masking function for a column
CREATE OR REPLACE FUNCTION ${catalog_name}.pii_classification.get_masking_function(
    p_catalog STRING,
    p_schema STRING,
    p_table STRING,
    p_column STRING
)
RETURNS STRING
RETURN (
    SELECT masking_function
    FROM ${catalog_name}.pii_classification.pii_phi_inventory
    WHERE catalog_name = p_catalog
      AND schema_name = p_schema
      AND table_name = p_table
      AND column_name = p_column
    LIMIT 1
);

-- =====================================================
-- PII STATISTICS
-- =====================================================

-- Total PII/PHI coverage
SELECT
    'Total PII/PHI Columns' AS metric,
    COUNT(*) AS value
FROM ${catalog_name}.pii_classification.pii_phi_inventory
WHERE data_classification IN ('PII', 'PHI')

UNION ALL

SELECT
    'Tables with PII/PHI' AS metric,
    COUNT(DISTINCT CONCAT(catalog_name, '.', schema_name, '.', table_name)) AS value
FROM ${catalog_name}.pii_classification.pii_phi_inventory
WHERE data_classification IN ('PII', 'PHI')

UNION ALL

SELECT
    'Critical Sensitivity Columns' AS metric,
    COUNT(*) AS value
FROM ${catalog_name}.pii_classification.pii_phi_inventory
WHERE sensitivity_level = 'CRITICAL'

UNION ALL

SELECT
    'Columns Requiring Masking' AS metric,
    COUNT(*) AS value
FROM ${catalog_name}.pii_classification.pii_phi_inventory
WHERE masking_required = TRUE;

-- =====================================================
-- GRANT PERMISSIONS
-- =====================================================

-- Data protection officers: Full access
GRANT ALL PRIVILEGES ON SCHEMA ${catalog_name}.pii_classification TO `data_protection_officers`;

-- Compliance team: Read access
GRANT SELECT ON SCHEMA ${catalog_name}.pii_classification TO `compliance_team`;

-- =====================================================
-- USAGE EXAMPLES
-- =====================================================

-- Check if a column is PII:
-- SELECT ${catalog_name}.pii_classification.is_pii_column('insurance_prod', 'customers', 'customers_dim', 'ssn');

-- Get masking function:
-- SELECT ${catalog_name}.pii_classification.get_masking_function('insurance_prod', 'customers', 'customers_dim', 'email');

-- View PII summary:
-- SELECT * FROM ${catalog_name}.pii_classification.pii_summary_by_sensitivity;

