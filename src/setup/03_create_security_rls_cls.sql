-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Row-Level Security (RLS) and Column-Level Security (CLS)
-- MAGIC Implement fine-grained access control using SQL UDFs and secure views

-- COMMAND ----------
-- Create widget for catalog parameter
CREATE WIDGET DROPDOWN catalog DEFAULT "insurance_dev_silver" CHOICES SELECT * FROM (VALUES ("insurance_dev_silver"), ("insurance_staging_silver"), ("insurance_prod_silver"));

-- COMMAND ----------
-- Use the selected catalog
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
-- Create schemas if they don't exist (if not already created by silver tables script)
CREATE SCHEMA IF NOT EXISTS customers;
CREATE SCHEMA IF NOT EXISTS policies;
CREATE SCHEMA IF NOT EXISTS claims;
CREATE SCHEMA IF NOT EXISTS agents;
CREATE SCHEMA IF NOT EXISTS payments;
CREATE SCHEMA IF NOT EXISTS master_data;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Security Helper Functions

-- COMMAND ----------
-- Function to get current user's role
CREATE OR REPLACE FUNCTION master_data.get_user_role()
RETURNS STRING
RETURN (
  CASE 
    WHEN IS_ACCOUNT_GROUP_MEMBER('executives') THEN 'EXECUTIVE'
    WHEN IS_ACCOUNT_GROUP_MEMBER('claims_managers') THEN 'CLAIMS_MANAGER'
    WHEN IS_ACCOUNT_GROUP_MEMBER('claims_adjusters') THEN 'CLAIMS_ADJUSTER'
    WHEN IS_ACCOUNT_GROUP_MEMBER('regional_managers') THEN 'REGIONAL_MANAGER'
    WHEN IS_ACCOUNT_GROUP_MEMBER('agents') THEN 'AGENT'
    WHEN IS_ACCOUNT_GROUP_MEMBER('underwriters') THEN 'UNDERWRITER'
    WHEN IS_ACCOUNT_GROUP_MEMBER('finance_team') THEN 'FINANCE'
    WHEN IS_ACCOUNT_GROUP_MEMBER('data_scientists') THEN 'DATA_SCIENTIST'
    WHEN IS_ACCOUNT_GROUP_MEMBER('business_analysts') THEN 'ANALYST'
    ELSE 'GUEST'
  END
);

-- COMMAND ----------
-- Function to get current user's region (from employee table)
CREATE OR REPLACE FUNCTION master_data.get_user_region()
RETURNS STRING
RETURN (
  SELECT region_code 
  FROM agents.agent_dim 
  WHERE email = CURRENT_USER() 
    AND is_active = TRUE 
  LIMIT 1
);

-- COMMAND ----------
-- Function to get current user's agent ID
CREATE OR REPLACE FUNCTION master_data.get_user_agent_id()
RETURNS STRING
RETURN (
  SELECT agent_id 
  FROM agents.agent_dim 
  WHERE email = CURRENT_USER() 
    AND is_active = TRUE 
  LIMIT 1
);

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Column-Level Security (CLS) Functions
-- MAGIC ### PII Masking Functions

-- COMMAND ----------
-- Mask SSN based on user role
CREATE OR REPLACE FUNCTION master_data.mask_ssn(ssn STRING)
RETURNS STRING
RETURN (
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN ssn
    WHEN 'CLAIMS_MANAGER' THEN ssn
    WHEN 'FINANCE' THEN ssn
    WHEN 'UNDERWRITER' THEN ssn
    ELSE CONCAT('XXX-XX-', RIGHT(ssn, 4))
  END
);

-- COMMAND ----------
-- Mask email based on user role  
CREATE OR REPLACE FUNCTION master_data.mask_email(email STRING)
RETURNS STRING
RETURN (
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN email
    WHEN 'CLAIMS_MANAGER' THEN email
    WHEN 'REGIONAL_MANAGER' THEN email
    WHEN 'AGENT' THEN email
    ELSE CONCAT(LEFT(email, 3), '***@', SPLIT(email, '@')[1])
  END
);

-- COMMAND ----------
-- Mask phone number
CREATE OR REPLACE FUNCTION master_data.mask_phone(phone STRING)
RETURNS STRING
RETURN (
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN phone
    WHEN 'CLAIMS_MANAGER' THEN phone
    WHEN 'AGENT' THEN phone
    ELSE CONCAT('XXX-XXX-', RIGHT(phone, 4))
  END
);

-- COMMAND ----------
-- Mask financial amounts (for junior analysts)
CREATE OR REPLACE FUNCTION master_data.mask_amount(amount DECIMAL(15,2))
RETURNS DECIMAL(15,2)
RETURN (
  CASE master_data.get_user_role()
    WHEN 'GUEST' THEN NULL
    WHEN 'ANALYST' THEN ROUND(amount, -3) -- Round to nearest thousand
    WHEN 'DATA_SCIENTIST' THEN ROUND(amount, -2) -- Round to nearest hundred
    ELSE amount
  END
);

-- COMMAND ----------
-- Redact sensitive claim description for specific roles
CREATE OR REPLACE FUNCTION master_data.redact_sensitive_text(text STRING, is_sensitive BOOLEAN)
RETURNS STRING
RETURN (
  CASE 
    WHEN is_sensitive = FALSE THEN text
    WHEN master_data.get_user_role() IN ('EXECUTIVE', 'CLAIMS_MANAGER', 'CLAIMS_ADJUSTER') THEN text
    ELSE '[REDACTED - SENSITIVE INFORMATION]'
  END
);

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Row-Level Security (RLS) Views
-- MAGIC ### Secure Customer View

-- COMMAND ----------
CREATE OR REPLACE VIEW customers.customer_secure AS
SELECT 
  customer_key,
  customer_id,
  first_name,
  last_name,
  full_name,
  date_of_birth,
  age_years,
  -- Apply column-level masking
  master_data.mask_ssn(ssn_masked) as ssn_masked,
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN ssn_encrypted
    WHEN 'UNDERWRITER' THEN ssn_encrypted
    ELSE NULL
  END as ssn_encrypted,
  master_data.mask_email(email) as email,
  email_domain,
  master_data.mask_phone(phone) as phone,
  address_line1,
  address_line2,
  city,
  state_code,
  state_name,
  zip_code,
  country,
  customer_type,
  -- Hide credit score from non-authorized roles
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN credit_score
    WHEN 'UNDERWRITER' THEN credit_score
    WHEN 'CLAIMS_MANAGER' THEN credit_score
    ELSE NULL
  END as credit_score,
  credit_tier,
  marital_status,
  occupation,
  -- Mask income for certain roles
  CASE master_data.get_user_role()
    WHEN 'GUEST' THEN NULL
    WHEN 'ANALYST' THEN NULL
    ELSE annual_income
  END as annual_income,
  income_bracket,
  communication_preference,
  preferred_language,
  marketing_opt_in,
  customer_segment,
  customer_since_date,
  customer_tenure_months,
  customer_status,
  assigned_agent_id,
  assigned_agent_name,
  assigned_region,
  risk_profile,
  lifetime_value,
  total_policies,
  total_claims,
  effective_start_date,
  effective_end_date,
  is_current,
  record_version,
  created_timestamp,
  updated_timestamp,
  data_quality_score,
  source_system
FROM customers.customer_dim
WHERE is_current = TRUE
  -- Row-level filtering based on role and region
  AND (
    master_data.get_user_role() = 'EXECUTIVE'
    OR master_data.get_user_role() = 'CLAIMS_MANAGER'
    OR master_data.get_user_role() = 'UNDERWRITER'
    OR master_data.get_user_role() = 'FINANCE'
    OR master_data.get_user_role() = 'DATA_SCIENTIST'
    OR (
      master_data.get_user_role() = 'REGIONAL_MANAGER' 
      AND assigned_region = master_data.get_user_region()
    )
    OR (
      master_data.get_user_role() = 'AGENT' 
      AND assigned_agent_id = master_data.get_user_agent_id()
    )
  );

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Secure Claims View

-- COMMAND ----------
CREATE OR REPLACE VIEW claims.claim_secure AS
SELECT 
  claim_key,
  claim_id,
  claim_number,
  policy_key,
  policy_id,
  customer_key,
  customer_id,
  claim_type,
  claim_category,
  loss_type,
  claim_status,
  claim_status_code,
  claim_sub_status,
  loss_date,
  report_date,
  fnol_date,
  closed_date,
  reopened_date,
  days_to_report,
  days_to_close,
  days_open,
  is_open,
  loss_location_address,
  loss_location_city,
  loss_location_state,
  loss_location_zip,
  loss_in_policy_state,
  distance_from_insured_miles,
  -- Apply financial masking
  master_data.mask_amount(claimed_amount) as claimed_amount,
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN claimed_amount_encrypted
    WHEN 'CLAIMS_MANAGER' THEN claimed_amount_encrypted
    WHEN 'FINANCE' THEN claimed_amount_encrypted
    ELSE NULL
  END as claimed_amount_encrypted,
  master_data.mask_amount(estimated_loss_amount) as estimated_loss_amount,
  master_data.mask_amount(reserved_amount) as reserved_amount,
  master_data.mask_amount(paid_amount) as paid_amount,
  deductible_amount,
  master_data.mask_amount(net_paid_amount) as net_paid_amount,
  recovery_amount,
  salvage_value,
  total_incurred,
  loss_ratio,
  severity_score,
  complexity_score,
  -- Mask claimant name for certain roles
  CASE master_data.get_user_role()
    WHEN 'ANALYST' THEN claimant_name_masked
    WHEN 'DATA_SCIENTIST' THEN claimant_name_masked
    ELSE claimant_name
  END as claimant_name,
  claimant_relationship,
  assigned_adjuster_id,
  assigned_adjuster_name,
  assigned_examiner_id,
  legal_representative,
  -- Fraud detection - sensitive to specific roles
  investigation_required,
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN siu_referral
    WHEN 'CLAIMS_MANAGER' THEN siu_referral
    WHEN 'CLAIMS_ADJUSTER' THEN siu_referral
    ELSE NULL
  END as siu_referral,
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN fraud_score
    WHEN 'CLAIMS_MANAGER' THEN fraud_score
    WHEN 'CLAIMS_ADJUSTER' THEN fraud_score
    ELSE NULL
  END as fraud_score,
  fraud_risk_category,
  police_report_filed,
  police_report_number,
  witness_count,
  repair_shop_id,
  repair_shop_name,
  repair_shop_rating,
  medical_provider_id,
  medical_provider_name,
  provider_estimate_amount,
  provider_in_network,
  litigation_status,
  attorney_involved,
  suit_date,
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN legal_expense
    WHEN 'CLAIMS_MANAGER' THEN legal_expense
    WHEN 'FINANCE' THEN legal_expense
    ELSE NULL
  END as legal_expense,
  workflow_stage,
  next_action,
  next_action_date,
  reopen_count,
  requires_approval,
  approval_level,
  approved_by,
  approved_date,
  denial_reason,
  denial_category,
  subrogation_potential,
  subrogation_amount,
  subrogation_status,
  payment_velocity,
  reserve_accuracy,
  customer_satisfaction_score,
  data_quality_score,
  validation_status,
  source_system,
  created_timestamp,
  updated_timestamp
FROM claims.claim_fact
WHERE 
  -- Row-level security based on role
  (
    master_data.get_user_role() = 'EXECUTIVE'
    OR master_data.get_user_role() = 'CLAIMS_MANAGER'
    OR master_data.get_user_role() = 'FINANCE'
    OR master_data.get_user_role() = 'DATA_SCIENTIST'
    OR (
      master_data.get_user_role() = 'CLAIMS_ADJUSTER' 
      AND assigned_adjuster_id = master_data.get_user_agent_id()
    )
    OR (
      master_data.get_user_role() = 'ANALYST'
      AND is_open = FALSE -- Analysts can only see closed claims
    )
  );

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Secure Policies View

-- COMMAND ----------
CREATE OR REPLACE VIEW policies.policy_secure AS
SELECT 
  policy_key,
  policy_id,
  policy_number,
  customer_key,
  customer_id,
  policy_type,
  product_line,
  product_code,
  product_name,
  policy_status,
  policy_status_code,
  effective_date,
  expiration_date,
  issue_date,
  application_date,
  bind_date,
  cancellation_date,
  cancellation_reason,
  cancellation_category,
  policy_term_months,
  days_in_force,
  renewal_count,
  is_renewal,
  parent_policy_id,
  -- Apply financial masking for premium amounts
  master_data.mask_amount(annual_premium) as annual_premium,
  master_data.mask_amount(earned_premium) as earned_premium,
  master_data.mask_amount(unearned_premium) as unearned_premium,
  payment_frequency,
  payment_method,
  premium_payment_status,
  days_overdue,
  coverage_amount,
  deductible_amount,
  coverage_start_date,
  coverage_end_date,
  writing_agent_id,
  writing_agent_name,
  servicing_agent_id,
  servicing_agent_name,
  agency_code,
  agency_name,
  territory_code,
  state_code,
  state_name,
  county,
  region_code,
  underwriter_id,
  underwriter_name,
  underwriting_tier,
  risk_class,
  risk_score,
  rating_factor,
  -- Commission data - restricted
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN commission_rate
    WHEN 'FINANCE' THEN commission_rate
    WHEN 'AGENT' THEN commission_rate
    ELSE NULL
  END as commission_rate,
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN commission_amount
    WHEN 'FINANCE' THEN commission_amount
    WHEN 'AGENT' THEN commission_amount
    ELSE NULL
  END as commission_amount,
  commission_paid,
  policy_value_score,
  retention_probability,
  cross_sell_opportunity,
  data_quality_score,
  validation_status,
  source_system,
  created_timestamp,
  updated_timestamp
FROM policies.policy_fact
WHERE 
  -- Row-level security based on role and region
  (
    master_data.get_user_role() = 'EXECUTIVE'
    OR master_data.get_user_role() = 'FINANCE'
    OR master_data.get_user_role() = 'UNDERWRITER'
    OR master_data.get_user_role() = 'DATA_SCIENTIST'
    OR (
      master_data.get_user_role() = 'REGIONAL_MANAGER' 
      AND region_code = master_data.get_user_region()
    )
    OR (
      master_data.get_user_role() = 'AGENT' 
      AND (servicing_agent_id = master_data.get_user_agent_id()
           OR writing_agent_id = master_data.get_user_agent_id())
    )
  );

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Secure Payments View

-- COMMAND ----------
CREATE OR REPLACE VIEW payments.payment_secure AS
SELECT 
  payment_key,
  payment_id,
  payment_reference_number,
  payment_type,
  payment_category,
  policy_key,
  policy_id,
  claim_key,
  claim_id,
  customer_key,
  customer_id,
  agent_key,
  agent_id,
  payee_id,
  -- Mask payee name for non-finance roles
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN payee_name
    WHEN 'FINANCE' THEN payee_name
    WHEN 'CLAIMS_MANAGER' THEN payee_name
    ELSE CONCAT(LEFT(payee_name, 1), '***')
  END as payee_name,
  payee_type,
  -- Mask all payment amounts for unauthorized users
  master_data.mask_amount(payment_amount) as payment_amount,
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN payment_amount_encrypted
    WHEN 'FINANCE' THEN payment_amount_encrypted
    ELSE NULL
  END as payment_amount_encrypted,
  payment_currency,
  payment_method,
  payment_status,
  payment_status_code,
  payment_date,
  payment_due_date,
  effective_date,
  cleared_date,
  posting_date,
  days_overdue,
  days_to_clear,
  -- Mask payment instrument details
  card_last_four,
  CASE master_data.get_user_role()
    WHEN 'FINANCE' THEN card_type
    ELSE NULL
  END as card_type,
  bank_name,
  account_last_four,
  check_number,
  transaction_id,
  authorization_code,
  processor_name,
  processor_transaction_id,
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN processor_fee_amount
    WHEN 'FINANCE' THEN processor_fee_amount
    ELSE NULL
  END as processor_fee_amount,
  processing_date,
  reconciliation_status,
  reconciliation_date,
  batch_id,
  billing_period_start,
  billing_period_end,
  premium_type,
  installment_number,
  total_installments,
  late_fee_amount,
  discount_amount,
  net_premium_amount,
  payment_category_claim,
  -- Completely hide tax ID except for finance
  CASE master_data.get_user_role()
    WHEN 'FINANCE' THEN payee_tax_id_masked
    ELSE NULL
  END as payee_tax_id_masked,
  form_1099_required,
  is_failed,
  is_reversed,
  failure_reason,
  failure_code,
  reversal_reason,
  reversed_payment_id,
  fraud_check_status,
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN fraud_risk_score
    WHEN 'FINANCE' THEN fraud_risk_score
    WHEN 'CLAIMS_MANAGER' THEN fraud_risk_score
    ELSE NULL
  END as fraud_risk_score,
  aml_check_status,
  source_system,
  created_timestamp,
  updated_timestamp
FROM payments.payment_fact
WHERE 
  -- Row-level security - only finance and executives see all payments
  (
    master_data.get_user_role() = 'EXECUTIVE'
    OR master_data.get_user_role() = 'FINANCE'
    OR master_data.get_user_role() = 'CLAIMS_MANAGER'
  );

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Secure Agents View

-- COMMAND ----------
CREATE OR REPLACE VIEW agents.agent_secure AS
SELECT 
  agent_key,
  agent_id,
  agent_code,
  first_name,
  last_name,
  full_name,
  -- Mask personal contact for certain roles
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN email
    WHEN 'REGIONAL_MANAGER' THEN email
    WHEN 'AGENT' THEN email
    ELSE CONCAT(LEFT(email, 3), '***@***')
  END as email,
  master_data.mask_phone(phone) as phone,
  agent_type,
  agent_status,
  hire_date,
  termination_date,
  termination_reason,
  tenure_months,
  is_active,
  license_number,
  license_state,
  license_effective_date,
  license_expiration_date,
  license_status,
  license_days_to_expiration,
  additional_state_licenses,
  licensed_states_count,
  agency_id,
  agency_name,
  agency_type,
  territory_code,
  region_code,
  region_name,
  district_code,
  office_location,
  reports_to_agent_id,
  reports_to_agent_name,
  manager_agent_key,
  hierarchy_level,
  hierarchy_path,
  auto_license,
  home_license,
  life_license,
  health_license,
  commercial_license,
  licensed_products_count,
  ytd_policies_written,
  ytd_premium_volume,
  -- Commission data visible only to specific roles
  CASE master_data.get_user_role()
    WHEN 'EXECUTIVE' THEN ytd_commission_earned
    WHEN 'FINANCE' THEN ytd_commission_earned
    WHEN 'AGENT' THEN 
      CASE 
        WHEN agent_id = master_data.get_user_agent_id() THEN ytd_commission_earned
        ELSE NULL
      END
    ELSE NULL
  END as ytd_commission_earned,
  retention_rate,
  customer_satisfaction_score,
  compliance_score,
  performance_tier,
  commission_tier,
  override_eligible,
  bonus_eligible,
  source_system,
  created_timestamp,
  updated_timestamp
FROM agents.agent_dim
WHERE 
  -- Row-level filtering based on hierarchy and role
  (
    master_data.get_user_role() = 'EXECUTIVE'
    OR master_data.get_user_role() = 'FINANCE'
    OR (
      master_data.get_user_role() = 'REGIONAL_MANAGER' 
      AND region_code = master_data.get_user_region()
    )
    OR (
      master_data.get_user_role() = 'AGENT' 
      AND (
        agent_id = master_data.get_user_agent_id()
        OR reports_to_agent_id = master_data.get_user_agent_id()
      )
    )
  );

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Grant Permissions on Secure Views
-- MAGIC 
-- MAGIC ### ⚠️ Important Note for Databricks Community Edition Users
-- MAGIC 
-- MAGIC **Databricks Community Edition (Free Edition) does NOT support:**
-- MAGIC - Account-level groups
-- MAGIC - Multi-user group-based access control
-- MAGIC - Enterprise Unity Catalog features
-- MAGIC 
-- MAGIC **For Community Edition:**
-- MAGIC - Only individual user grants work
-- MAGIC - Group grants are commented out below
-- MAGIC - The security views and functions still work for learning/demo purposes
-- MAGIC 
-- MAGIC **For Enterprise/Premium Edition:**
-- MAGIC - Uncomment group grants after creating account-level groups
-- MAGIC - Access account console at: https://accounts.cloud.databricks.com
-- MAGIC - Create groups under User Management → Groups

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Grant Permissions - Important Note
-- MAGIC 
-- MAGIC Unity Catalog requires **account-level groups** for GRANT statements.
-- MAGIC 
-- MAGIC **Option 1 (Current User Only):** Grant to yourself for testing
-- MAGIC ```sql
-- MAGIC GRANT SELECT ON VIEW customers.customer_secure TO `your.email@domain.com`;
-- MAGIC ```
-- MAGIC 
-- MAGIC **Option 2 (Production - Account Admin Required):** 
-- MAGIC Have your account admin create account-level groups in the Account Console, then grant:
-- MAGIC ```sql
-- MAGIC GRANT SELECT ON VIEW customers.customer_secure TO `business_analysts`;
-- MAGIC ```

-- COMMAND ----------
-- Grant permissions to current user for testing
-- Replace with your actual email address or use CURRENT_USER()

-- Get current user email
SELECT CURRENT_USER() as current_user_email;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Grant Permissions to Current User
-- MAGIC 
-- MAGIC ✅ **This works in Community Edition** - Individual user grants don't require account-level groups

-- COMMAND ----------
-- Grant SELECT on all secure views to current user
-- Individual user grants work in both Community and Enterprise editions

GRANT SELECT ON VIEW customers.customer_secure TO `siddhartha013@gmail.com`;
GRANT SELECT ON VIEW policies.policy_secure TO `siddhartha013@gmail.com`;
GRANT SELECT ON VIEW claims.claim_secure TO `siddhartha013@gmail.com`;
GRANT SELECT ON VIEW payments.payment_secure TO `siddhartha013@gmail.com`;
GRANT SELECT ON VIEW agents.agent_secure TO `siddhartha013@gmail.com`;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Grant Permissions to Groups (Enterprise/Premium Edition ONLY)
-- MAGIC 
-- MAGIC ❌ **NOT SUPPORTED in Databricks Community Edition (Free Edition)**
-- MAGIC 
-- MAGIC ✅ **For Enterprise/Premium Edition ONLY:**
-- MAGIC 
-- MAGIC **Prerequisites:**
-- MAGIC 1. Access to Account Console: https://accounts.cloud.databricks.com (Account Admin required)
-- MAGIC 2. Create account-level groups under User Management → Groups
-- MAGIC 3. Add users to appropriate groups
-- MAGIC 4. Uncomment the GRANT statements below
-- MAGIC 
-- MAGIC **Groups needed:**
-- MAGIC - `business_analysts`, `regional_managers`, `agents`
-- MAGIC - `claims_adjusters`, `claims_managers`, `finance_team`
-- MAGIC - `executives`, `underwriters`, `data_scientists`

-- COMMAND ----------
-- ❌ COMMENTED OUT - These require Enterprise/Premium Edition with account-level groups
-- Uncomment ONLY if you have Enterprise/Premium Edition and account-level groups created

-- GRANT SELECT ON VIEW customers.customer_secure TO `business_analysts`;
-- GRANT SELECT ON VIEW customers.customer_secure TO `regional_managers`;
-- GRANT SELECT ON VIEW customers.customer_secure TO `agents`;

-- GRANT SELECT ON VIEW policies.policy_secure TO `business_analysts`;
-- GRANT SELECT ON VIEW policies.policy_secure TO `regional_managers`;
-- GRANT SELECT ON VIEW policies.policy_secure TO `agents`;

-- GRANT SELECT ON VIEW claims.claim_secure TO `business_analysts`;
-- GRANT SELECT ON VIEW claims.claim_secure TO `claims_adjusters`;
-- GRANT SELECT ON VIEW claims.claim_secure TO `claims_managers`;

-- GRANT SELECT ON VIEW payments.payment_secure TO `finance_team`;
-- GRANT SELECT ON VIEW payments.payment_secure TO `executives`;

-- GRANT SELECT ON VIEW agents.agent_secure TO `regional_managers`;
-- GRANT SELECT ON VIEW agents.agent_secure TO `agents`;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ✅ **Security Views and Permissions Created Successfully**
-- MAGIC 
-- MAGIC **What was created:**
-- MAGIC - ✅ Security helper functions (RLS/CLS logic)
-- MAGIC - ✅ Column-level security (masking) functions
-- MAGIC - ✅ Secure views with row and column filtering
-- MAGIC - ✅ User-level permissions granted to `siddhartha013@gmail.com`
-- MAGIC - ✅ Audit log table for tracking data access
-- MAGIC 
-- MAGIC **Community Edition Note:**
-- MAGIC - Group-based permissions are commented out (not supported in free edition)
-- MAGIC - Security views work for single-user demos and learning
-- MAGIC - For production multi-user access, upgrade to Enterprise/Premium Edition

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Audit Log for Data Access
-- MAGIC Create table to track access to sensitive data

-- COMMAND ----------
-- Note: Removing DROP TABLE to preserve audit logs if table already exists
-- Uncomment if you need to recreate the table:
-- DROP TABLE IF EXISTS master_data.data_access_audit;

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS master_data.data_access_audit (
  audit_id STRING NOT NULL COMMENT 'Unique audit record ID - use uuid() when inserting',
  access_timestamp TIMESTAMP NOT NULL COMMENT 'Timestamp of data access - use CURRENT_TIMESTAMP() when inserting',
  access_date DATE NOT NULL COMMENT 'Date of access for partitioning - use CAST(CURRENT_TIMESTAMP() AS DATE)',
  user_email STRING COMMENT 'Email of user accessing data',
  user_role STRING COMMENT 'Role of user at time of access',
  accessed_table STRING COMMENT 'Name of table accessed',
  accessed_view STRING COMMENT 'Name of secure view accessed',
  query_text STRING COMMENT 'SQL query executed',
  row_count BIGINT COMMENT 'Number of rows returned',
  contained_pii BOOLEAN COMMENT 'Whether result contained PII data',
  contained_financial_data BOOLEAN COMMENT 'Whether result contained financial data',
  access_source STRING COMMENT 'Source of access (notebook, SQL editor, API, etc)',
  session_id STRING COMMENT 'Session identifier',
  ip_address STRING COMMENT 'IP address of user'
)
USING DELTA
PARTITIONED BY (access_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.logRetentionDuration' = '730 days',
  'quality' = 'audit'
)
COMMENT 'Audit log for tracking access to sensitive data';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### How to Insert Audit Records
-- MAGIC 
-- MAGIC When logging access to sensitive data, insert records like this:
-- MAGIC 
-- MAGIC ```sql
-- MAGIC INSERT INTO master_data.data_access_audit 
-- MAGIC VALUES (
-- MAGIC   uuid(),                                    -- audit_id
-- MAGIC   CURRENT_TIMESTAMP(),                       -- access_timestamp
-- MAGIC   CAST(CURRENT_TIMESTAMP() AS DATE),         -- access_date (for partitioning)
-- MAGIC   CURRENT_USER(),                            -- user_email
-- MAGIC   'ANALYST',                                 -- user_role
-- MAGIC   'customers.customer_dim',                  -- accessed_table
-- MAGIC   'customers.customer_secure',               -- accessed_view
-- MAGIC   'SELECT * FROM ...',                       -- query_text
-- MAGIC   100,                                       -- row_count
-- MAGIC   true,                                      -- contained_pii
-- MAGIC   false,                                     -- contained_financial_data
-- MAGIC   'SQL Editor',                              -- access_source
-- MAGIC   '12345',                                   -- session_id
-- MAGIC   '192.168.1.1'                              -- ip_address
-- MAGIC );
-- MAGIC ```

