-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Silver Layer - Table Creation with SCD Type 2
-- MAGIC Create validated, cleaned tables with business rules and history tracking

-- COMMAND ----------
-- Create widget for catalog parameter
CREATE WIDGET DROPDOWN catalog DEFAULT "insurance_dev_silver" CHOICES SELECT * FROM (VALUES ("insurance_dev_silver"), ("insurance_staging_silver"), ("insurance_prod_silver"));

-- COMMAND ----------
-- Use the selected catalog
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
-- Create schemas if they don't exist
CREATE SCHEMA IF NOT EXISTS customers;
CREATE SCHEMA IF NOT EXISTS policies;
CREATE SCHEMA IF NOT EXISTS claims;
CREATE SCHEMA IF NOT EXISTS agents;
CREATE SCHEMA IF NOT EXISTS payments;
CREATE SCHEMA IF NOT EXISTS underwriting;
CREATE SCHEMA IF NOT EXISTS providers;
CREATE SCHEMA IF NOT EXISTS master_data;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Customers Table (SCD Type 2)

-- COMMAND ----------
-- Drop table if it exists (to handle partial creation issues)
DROP TABLE IF EXISTS customers.customer_dim;

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS customers.customer_dim (
  customer_key BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
  customer_id STRING NOT NULL,
  first_name STRING NOT NULL,
  last_name STRING NOT NULL,
  full_name STRING GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)),
  date_of_birth DATE,
  age_years INT,
  ssn_masked STRING COMMENT 'Masked SSN: XXX-XX-1234',
  ssn_encrypted STRING COMMENT 'Encrypted full SSN',
  email STRING,
  email_domain STRING,
  phone STRING,
  phone_masked STRING,
  address_line1 STRING,
  address_line2 STRING,
  city STRING,
  state_code STRING NOT NULL,
  state_name STRING,
  zip_code STRING,
  country STRING DEFAULT 'USA',
  customer_type STRING,
  credit_score INT,
  credit_tier STRING COMMENT 'Excellent, Good, Fair, Poor',
  marital_status STRING,
  occupation STRING,
  annual_income DECIMAL(15,2),
  income_bracket STRING,
  communication_preference STRING,
  preferred_language STRING DEFAULT 'English',
  marketing_opt_in BOOLEAN DEFAULT FALSE,
  customer_segment STRING,
  customer_since_date DATE,
  customer_tenure_months INT,
  customer_status STRING DEFAULT 'Active',
  assigned_agent_id STRING,
  assigned_agent_name STRING,
  assigned_region STRING,
  risk_profile STRING,
  lifetime_value DECIMAL(15,2),
  total_policies INT DEFAULT 0,
  total_claims INT DEFAULT 0,
  -- SCD Type 2 columns
  effective_start_date DATE NOT NULL,
  effective_end_date DATE,
  is_current BOOLEAN NOT NULL DEFAULT TRUE,
  record_version INT DEFAULT 1,
  -- Audit columns
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  created_by STRING DEFAULT 'ETL_PROCESS',
  data_quality_score DECIMAL(3,2),
  data_quality_flags STRING,
  source_system STRING
)
USING DELTA
CLUSTER BY (customer_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.dataSkippingNumIndexedCols' = '10',
  'quality' = 'silver',
  'scd_type' = '2',
  'pii_data' = 'true',
  'domain' = 'customer'
)
COMMENT 'Validated customer dimension with SCD Type 2 history';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Policies Fact Table

-- COMMAND ----------
DROP TABLE IF EXISTS policies.policy_fact;

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS policies.policy_fact (
  policy_key BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
  policy_id STRING NOT NULL,
  policy_number STRING NOT NULL,
  customer_key BIGINT,
  customer_id STRING NOT NULL,
  policy_type STRING NOT NULL,
  product_line STRING NOT NULL,
  product_code STRING,
  product_name STRING,
  policy_status STRING NOT NULL,
  policy_status_code INT COMMENT '1=Active, 2=Lapsed, 3=Cancelled, 4=Pending',
  -- Key Dates
  effective_date DATE NOT NULL,
  expiration_date DATE NOT NULL,
  issue_date DATE,
  application_date DATE,
  bind_date DATE,
  cancellation_date DATE,
  cancellation_reason STRING,
  cancellation_category STRING,
  policy_term_months INT,
  days_in_force INT,
  renewal_count INT DEFAULT 0,
  is_renewal BOOLEAN DEFAULT FALSE,
  parent_policy_id STRING,
  -- Premium Information
  annual_premium DECIMAL(12,2) NOT NULL,
  earned_premium DECIMAL(12,2),
  unearned_premium DECIMAL(12,2),
  payment_frequency STRING,
  payment_method STRING,
  premium_payment_status STRING,
  days_overdue INT DEFAULT 0,
  -- Coverage Information
  coverage_amount DECIMAL(15,2),
  deductible_amount DECIMAL(10,2),
  coverage_start_date DATE,
  coverage_end_date DATE,
  -- Agent and Territory
  writing_agent_id STRING,
  writing_agent_name STRING,
  servicing_agent_id STRING,
  servicing_agent_name STRING,
  agency_code STRING,
  agency_name STRING,
  territory_code STRING,
  state_code STRING NOT NULL,
  state_name STRING,
  county STRING,
  region_code STRING,
  -- Underwriting
  underwriter_id STRING,
  underwriter_name STRING,
  underwriting_tier STRING,
  risk_class STRING,
  risk_score INT,
  rating_factor DECIMAL(5,4),
  -- Commission
  commission_rate DECIMAL(5,4),
  commission_amount DECIMAL(10,2),
  commission_paid BOOLEAN DEFAULT FALSE,
  -- Business Metrics
  policy_value_score DECIMAL(5,2),
  retention_probability DECIMAL(5,4),
  cross_sell_opportunity STRING,
  -- Data Quality
  data_quality_score DECIMAL(3,2),
  validation_status STRING,
  validation_errors STRING,
  -- Audit
  source_system STRING,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  batch_id STRING
)
USING DELTA
CLUSTER BY (policy_id, customer_id, policy_type)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.dataSkippingNumIndexedCols' = '15',
  'quality' = 'silver',
  'domain' = 'policy'
)
COMMENT 'Validated policy fact table with business metrics';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Claims Fact Table

-- COMMAND ----------
DROP TABLE IF EXISTS claims.claim_fact;

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS claims.claim_fact (
  claim_key BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
  claim_id STRING NOT NULL,
  claim_number STRING NOT NULL,
  policy_key BIGINT,
  policy_id STRING NOT NULL,
  customer_key BIGINT,
  customer_id STRING NOT NULL,
  -- Claim Classification
  claim_type STRING NOT NULL,
  claim_category STRING,
  loss_type STRING,
  claim_status STRING NOT NULL,
  claim_status_code INT,
  claim_sub_status STRING,
  -- Key Dates
  loss_date TIMESTAMP NOT NULL,
  report_date TIMESTAMP NOT NULL,
  fnol_date TIMESTAMP,
  closed_date TIMESTAMP,
  reopened_date TIMESTAMP,
  days_to_report INT,
  days_to_close INT,
  days_open INT,
  is_open BOOLEAN,
  -- Location
  loss_location_address STRING,
  loss_location_city STRING,
  loss_location_state STRING,
  loss_location_zip STRING,
  loss_in_policy_state BOOLEAN,
  distance_from_insured_miles DECIMAL(8,2),
  -- Financial (Encrypted for sensitive amounts)
  claimed_amount DECIMAL(15,2) NOT NULL,
  claimed_amount_encrypted STRING,
  estimated_loss_amount DECIMAL(15,2),
  reserved_amount DECIMAL(15,2),
  paid_amount DECIMAL(15,2) DEFAULT 0,
  deductible_amount DECIMAL(10,2),
  net_paid_amount DECIMAL(15,2),
  recovery_amount DECIMAL(12,2) DEFAULT 0,
  salvage_value DECIMAL(12,2) DEFAULT 0,
  total_incurred DECIMAL(15,2),
  -- Ratios and Metrics
  loss_ratio DECIMAL(5,4),
  severity_score INT COMMENT '1-100',
  complexity_score INT COMMENT '1-100',
  -- Parties Involved
  claimant_name STRING,
  claimant_name_masked STRING,
  claimant_relationship STRING,
  assigned_adjuster_id STRING,
  assigned_adjuster_name STRING,
  assigned_examiner_id STRING,
  legal_representative STRING,
  -- Investigation and Fraud
  investigation_required BOOLEAN DEFAULT FALSE,
  siu_referral BOOLEAN DEFAULT FALSE,
  siu_status STRING,
  fraud_score DECIMAL(5,2),
  fraud_risk_category STRING COMMENT 'Low, Medium, High, Critical',
  fraud_indicators STRING,
  fraud_indicators_count INT,
  police_report_filed BOOLEAN,
  police_report_number STRING,
  witness_count INT DEFAULT 0,
  -- Provider Network
  repair_shop_id STRING,
  repair_shop_name STRING,
  repair_shop_rating DECIMAL(3,2),
  medical_provider_id STRING,
  medical_provider_name STRING,
  provider_estimate_amount DECIMAL(12,2),
  provider_in_network BOOLEAN,
  -- Litigation
  litigation_status STRING,
  attorney_involved BOOLEAN DEFAULT FALSE,
  suit_date DATE,
  legal_expense DECIMAL(12,2),
  -- Workflow
  workflow_stage STRING,
  next_action STRING,
  next_action_date DATE,
  reopen_count INT DEFAULT 0,
  -- Approval
  requires_approval BOOLEAN DEFAULT FALSE,
  approval_level STRING,
  approved_by STRING,
  approved_date TIMESTAMP,
  denial_reason STRING,
  denial_category STRING,
  -- Subrogation
  subrogation_potential BOOLEAN DEFAULT FALSE,
  subrogation_amount DECIMAL(12,2),
  subrogation_status STRING,
  -- Business Metrics
  payment_velocity DECIMAL(10,2) COMMENT 'Average daily payment amount',
  reserve_accuracy DECIMAL(5,4),
  customer_satisfaction_score DECIMAL(3,2),
  -- Data Quality
  data_quality_score DECIMAL(3,2),
  validation_status STRING,
  -- Audit
  source_system STRING,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  batch_id STRING
)
USING DELTA
CLUSTER BY (claim_id, policy_id, claim_status)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.dataSkippingNumIndexedCols' = '20',
  'quality' = 'silver',
  'sensitive_data' = 'true',
  'domain' = 'claims'
)
COMMENT 'Validated claims fact table with fraud detection and financial metrics';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Agents Dimension

-- COMMAND ----------
DROP TABLE IF EXISTS agents.agent_dim;

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS agents.agent_dim (
  agent_key BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
  agent_id STRING NOT NULL,
  agent_code STRING NOT NULL,
  first_name STRING NOT NULL,
  last_name STRING NOT NULL,
  full_name STRING GENERATED ALWAYS AS (CONCAT(first_name, ' ', last_name)),
  email STRING,
  phone STRING,
  agent_type STRING,
  agent_status STRING DEFAULT 'Active',
  hire_date DATE,
  termination_date DATE,
  termination_reason STRING,
  tenure_months INT,
  is_active BOOLEAN,
  -- License Information
  license_number STRING,
  license_state STRING,
  license_effective_date DATE,
  license_expiration_date DATE,
  license_status STRING,
  license_days_to_expiration INT,
  additional_state_licenses STRING,
  licensed_states_count INT,
  -- Agency Information
  agency_id STRING,
  agency_name STRING,
  agency_type STRING,
  -- Territory and Hierarchy
  territory_code STRING,
  region_code STRING,
  region_name STRING,
  district_code STRING,
  office_location STRING,
  reports_to_agent_id STRING,
  reports_to_agent_name STRING,
  manager_agent_key BIGINT,
  hierarchy_level INT,
  hierarchy_path STRING COMMENT 'VP > Regional > Manager > Agent',
  -- Product Authorization
  auto_license BOOLEAN DEFAULT FALSE,
  home_license BOOLEAN DEFAULT FALSE,
  life_license BOOLEAN DEFAULT FALSE,
  health_license BOOLEAN DEFAULT FALSE,
  commercial_license BOOLEAN DEFAULT FALSE,
  licensed_products_count INT,
  -- Performance Metrics (Current)
  ytd_policies_written INT DEFAULT 0,
  ytd_premium_volume DECIMAL(15,2) DEFAULT 0,
  ytd_commission_earned DECIMAL(12,2) DEFAULT 0,
  retention_rate DECIMAL(5,2),
  customer_satisfaction_score DECIMAL(3,2),
  compliance_score DECIMAL(3,2),
  performance_tier STRING COMMENT 'Top Performer, Above Average, Average, Below Average',
  -- Commission Structure
  commission_tier STRING,
  override_eligible BOOLEAN DEFAULT FALSE,
  bonus_eligible BOOLEAN DEFAULT TRUE,
  -- Audit
  source_system STRING,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  batch_id STRING
)
USING DELTA
CLUSTER BY (agent_id, region_code)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'silver',
  'domain' = 'agent'
)
COMMENT 'Validated agent dimension with hierarchy and performance metrics';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Payments Fact Table

-- COMMAND ----------
-- Note: Using CREATE OR REPLACE to ensure clean recreation
-- COMMAND ----------
CREATE OR REPLACE TABLE payments.payment_fact (
  payment_key BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL PRIMARY KEY,
  payment_id STRING NOT NULL,
  payment_reference_number STRING NOT NULL,
  payment_type STRING NOT NULL,
  payment_category STRING,
  -- Foreign Keys
  policy_key BIGINT,
  policy_id STRING,
  claim_key BIGINT,
  claim_id STRING,
  customer_key BIGINT,
  customer_id STRING,
  agent_key BIGINT,
  agent_id STRING,
  payee_id STRING,
  payee_name STRING,
  payee_type STRING,
  -- Payment Details
  payment_amount DECIMAL(15,2) NOT NULL,
  payment_amount_encrypted STRING,
  payment_currency STRING DEFAULT 'USD',
  payment_method STRING,
  payment_status STRING NOT NULL,
  payment_status_code INT,
  -- Dates
  payment_date DATE NOT NULL,
  payment_due_date DATE,
  effective_date DATE,
  cleared_date DATE,
  posting_date DATE,
  days_overdue INT DEFAULT 0,
  days_to_clear INT,
  -- Payment Instrument (Masked)
  card_last_four STRING,
  card_type STRING,
  bank_name STRING,
  account_last_four STRING,
  check_number STRING,
  transaction_id STRING,
  authorization_code STRING,
  -- Processing
  processor_name STRING,
  processor_transaction_id STRING,
  processor_fee_amount DECIMAL(10,2),
  processing_date TIMESTAMP,
  -- Reconciliation
  reconciliation_status STRING,
  reconciliation_date DATE,
  batch_id STRING,
  -- Premium Payment Specific
  billing_period_start DATE,
  billing_period_end DATE,
  premium_type STRING,
  installment_number INT,
  total_installments INT,
  late_fee_amount DECIMAL(8,2) DEFAULT 0,
  discount_amount DECIMAL(8,2) DEFAULT 0,
  net_premium_amount DECIMAL(12,2),
  -- Claim Payment Specific
  payment_category_claim STRING,
  payee_tax_id_masked STRING,
  form_1099_required BOOLEAN DEFAULT FALSE,
  -- Failure/Reversal
  is_failed BOOLEAN DEFAULT FALSE,
  is_reversed BOOLEAN DEFAULT FALSE,
  failure_reason STRING,
  failure_code STRING,
  reversal_reason STRING,
  reversed_payment_id STRING,
  -- Risk Flags
  fraud_check_status STRING,
  fraud_risk_score DECIMAL(5,2),
  aml_check_status STRING COMMENT 'Anti-Money Laundering',
  -- Audit
  source_system STRING,
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (payment_id, policy_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.dataSkippingNumIndexedCols' = '15',
  'quality' = 'silver',
  'financial_data' = 'true',
  'domain' = 'payments'
)
COMMENT 'Validated payment transactions with reconciliation and fraud checks';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Liquid Clustering Applied
-- MAGIC
-- MAGIC All silver tables have been configured with **Liquid Clustering** in their CREATE TABLE statements.
-- MAGIC This provides automatic optimization and flexible clustering keys for better query performance.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Master Data Tables

-- COMMAND ----------
DROP TABLE IF EXISTS master_data.state_dim;
DROP TABLE IF EXISTS master_data.product_dim;
DROP TABLE IF EXISTS master_data.date_dim;

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS master_data.state_dim (
  state_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  state_code STRING NOT NULL,
  state_name STRING NOT NULL,
  region STRING NOT NULL,
  division STRING,
  territory_code STRING,
  population BIGINT,
  median_income DECIMAL(12,2),
  cost_of_living_index DECIMAL(5,2),
  insurance_market_size DECIMAL(15,2),
  regulatory_tier STRING COMMENT 'High, Medium, Low regulation',
  catastrophe_risk_level STRING,
  effective_date DATE DEFAULT CURRENT_DATE(),
  is_active BOOLEAN DEFAULT TRUE
)
USING DELTA
CLUSTER BY (state_code, region)
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'State dimension with demographic and market data';

CREATE TABLE IF NOT EXISTS master_data.product_dim (
  product_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  product_code STRING NOT NULL,
  product_name STRING NOT NULL,
  product_line STRING NOT NULL,
  product_type STRING NOT NULL,
  product_category STRING,
  description STRING,
  target_market STRING,
  available_in_states STRING,
  base_rate DECIMAL(10,2),
  min_coverage DECIMAL(15,2),
  max_coverage DECIMAL(15,2),
  min_deductible DECIMAL(10,2),
  max_deductible DECIMAL(10,2),
  term_options STRING,
  effective_date DATE,
  expiration_date DATE,
  is_active BOOLEAN DEFAULT TRUE
)
USING DELTA
CLUSTER BY (product_code, product_type, product_line)
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Insurance product catalog dimension';

CREATE TABLE IF NOT EXISTS master_data.date_dim (
  date_key INT PRIMARY KEY,
  date_value DATE NOT NULL,
  year INT NOT NULL,
  quarter INT NOT NULL,
  month INT NOT NULL,
  month_name STRING,
  day_of_month INT NOT NULL,
  day_of_week INT NOT NULL,
  day_name STRING,
  week_of_year INT,
  is_weekend BOOLEAN,
  is_holiday BOOLEAN,
  holiday_name STRING,
  fiscal_year INT,
  fiscal_quarter INT,
  fiscal_month INT
)
USING DELTA
CLUSTER BY (date_value, year, month)
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.enableChangeDataFeed' = 'true'
)
COMMENT 'Date dimension for time-based analysis';

-- COMMAND ----------
ANALYZE TABLE customers.customer_dim COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE policies.policy_fact COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE claims.claim_fact COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE agents.agent_dim COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE payments.payment_fact COMPUTE STATISTICS FOR ALL COLUMNS;

