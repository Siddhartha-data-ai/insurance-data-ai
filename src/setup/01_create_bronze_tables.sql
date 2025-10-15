-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Bronze Layer - Table Creation
-- MAGIC Create raw data tables in Bronze layer with proper partitioning and constraints

-- COMMAND ----------
-- Create widget for catalog parameter
CREATE WIDGET DROPDOWN catalog DEFAULT "insurance_dev_bronze" CHOICES SELECT * FROM (VALUES ("insurance_dev_bronze"), ("insurance_staging_bronze"), ("insurance_prod_bronze"));

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

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Customers Table

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS customers.customer_raw (
  customer_id STRING NOT NULL,
  first_name STRING NOT NULL,
  last_name STRING NOT NULL,
  date_of_birth DATE,
  ssn STRING COMMENT 'Social Security Number - PII',
  email STRING,
  phone STRING,
  address_line1 STRING,
  address_line2 STRING,
  city STRING,
  state_code STRING,
  zip_code STRING,
  country STRING DEFAULT 'USA',
  customer_type STRING COMMENT 'Individual, Family, Business',
  credit_score INT,
  marital_status STRING,
  occupation STRING,
  annual_income DECIMAL(15,2),
  communication_preference STRING,
  preferred_language STRING DEFAULT 'English',
  marketing_opt_in BOOLEAN DEFAULT FALSE,
  customer_segment STRING COMMENT 'Platinum, Gold, Silver, Bronze',
  customer_since_date DATE,
  customer_status STRING DEFAULT 'Active',
  assigned_agent_id STRING,
  assigned_region STRING,
  risk_profile STRING,
  source_system STRING DEFAULT 'CRM',
  source_record_id STRING,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  ingestion_date DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
  record_hash STRING,
  _metadata STRING COMMENT 'Raw JSON metadata from source'
)
USING DELTA
CLUSTER BY (customer_id, state_code, ingestion_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.dataSkippingNumIndexedCols' = '10',
  'delta.columnMapping.mode' = 'name',
  'quality' = 'bronze',
  'pii_data' = 'true',
  'domain' = 'customer'
)
COMMENT 'Raw customer data from CRM system';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Policies Table

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS policies.policy_raw (
  policy_id STRING NOT NULL,
  policy_number STRING NOT NULL,
  customer_id STRING NOT NULL,
  policy_type STRING NOT NULL COMMENT 'Auto, Home, Life, Health, Commercial',
  product_line STRING NOT NULL,
  product_code STRING,
  policy_status STRING NOT NULL COMMENT 'Active, Lapsed, Cancelled, Pending',
  effective_date DATE NOT NULL,
  expiration_date DATE NOT NULL,
  issue_date DATE,
  application_date DATE,
  bind_date DATE,
  cancellation_date DATE,
  cancellation_reason STRING,
  policy_term_months INT,
  renewal_count INT DEFAULT 0,
  is_renewal BOOLEAN DEFAULT FALSE,
  parent_policy_id STRING COMMENT 'For endorsements and renewals',
  -- Premium Information
  annual_premium DECIMAL(12,2) NOT NULL,
  payment_frequency STRING COMMENT 'Annual, Semi-Annual, Quarterly, Monthly',
  payment_method STRING,
  premium_payment_status STRING,
  -- Coverage Information
  coverage_amount DECIMAL(15,2),
  deductible_amount DECIMAL(10,2),
  coverage_start_date DATE,
  coverage_end_date DATE,
  -- Agent and Territory
  writing_agent_id STRING,
  servicing_agent_id STRING,
  agency_code STRING,
  territory_code STRING,
  state_code STRING NOT NULL,
  county STRING,
  -- Underwriting
  underwriter_id STRING,
  underwriting_tier STRING COMMENT 'Preferred, Standard, Non-Standard',
  risk_class STRING,
  rating_factor DECIMAL(5,4),
  -- Commission
  commission_rate DECIMAL(5,4),
  commission_amount DECIMAL(10,2),
  -- Source System
  source_system STRING DEFAULT 'PolicyAdmin',
  source_record_id STRING,
  source_last_modified_timestamp TIMESTAMP,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  ingestion_date DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
  record_hash STRING,
  _metadata STRING
)
USING DELTA
CLUSTER BY (policy_id, customer_id, policy_type, state_code)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.dataSkippingNumIndexedCols' = '15',
  'quality' = 'bronze',
  'domain' = 'policy'
)
COMMENT 'Raw policy data from policy administration system';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Claims Table

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS claims.claim_raw (
  claim_id STRING NOT NULL,
  claim_number STRING NOT NULL,
  policy_id STRING NOT NULL,
  customer_id STRING NOT NULL,
  -- Claim Details
  claim_type STRING NOT NULL COMMENT 'Property, Liability, Auto, Health, Life',
  claim_category STRING,
  loss_type STRING COMMENT 'Accident, Theft, Fire, Flood, Medical, Death',
  claim_status STRING NOT NULL COMMENT 'Reported, Under Investigation, Approved, Denied, Closed, Reopened',
  claim_sub_status STRING,
  -- Important Dates
  loss_date TIMESTAMP NOT NULL,
  report_date TIMESTAMP NOT NULL,
  fnol_date TIMESTAMP COMMENT 'First Notice of Loss',
  closed_date TIMESTAMP,
  reopened_date TIMESTAMP,
  -- Location
  loss_location_address STRING,
  loss_location_city STRING,
  loss_location_state STRING,
  loss_location_zip STRING,
  loss_location_coordinates STRING,
  -- Financial
  claimed_amount DECIMAL(15,2) NOT NULL,
  estimated_loss_amount DECIMAL(15,2),
  reserved_amount DECIMAL(15,2),
  paid_amount DECIMAL(15,2) DEFAULT 0,
  deductible_amount DECIMAL(10,2),
  net_paid_amount DECIMAL(15,2),
  recovery_amount DECIMAL(12,2) DEFAULT 0 COMMENT 'Subrogation recovery',
  salvage_value DECIMAL(12,2) DEFAULT 0,
  -- Parties Involved
  claimant_name STRING,
  claimant_relationship STRING COMMENT 'Self, Spouse, Child, Third Party',
  assigned_adjuster_id STRING,
  assigned_examiner_id STRING,
  legal_representative STRING,
  -- Investigation
  investigation_required BOOLEAN DEFAULT FALSE,
  siu_referral BOOLEAN DEFAULT FALSE COMMENT 'Special Investigation Unit',
  fraud_score DECIMAL(5,2) COMMENT '0-100 fraud probability score',
  fraud_indicators STRING,
  police_report_filed BOOLEAN,
  police_report_number STRING,
  witness_count INT DEFAULT 0,
  -- Provider Network (for auto/health claims)
  repair_shop_id STRING,
  medical_provider_id STRING,
  provider_estimate_amount DECIMAL(12,2),
  -- Litigation
  litigation_status STRING,
  attorney_involved BOOLEAN DEFAULT FALSE,
  suit_date DATE,
  -- Workflow
  workflow_stage STRING,
  next_action_date DATE,
  days_open INT,
  reopen_count INT DEFAULT 0,
  -- Approval
  requires_approval BOOLEAN DEFAULT FALSE,
  approval_level STRING,
  approved_by STRING,
  approved_date TIMESTAMP,
  denial_reason STRING,
  -- Notes and Description
  claim_description STRING,
  loss_description STRING,
  adjuster_notes STRING,
  -- Source System
  source_system STRING DEFAULT 'ClaimsManagement',
  source_record_id STRING,
  source_last_modified_timestamp TIMESTAMP,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  ingestion_date DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
  record_hash STRING,
  _metadata STRING
)
USING DELTA
CLUSTER BY (claim_id, policy_id, claim_status, claim_type)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.dataSkippingNumIndexedCols' = '20',
  'quality' = 'bronze',
  'sensitive_data' = 'true',
  'domain' = 'claims'
)
COMMENT 'Raw claims data from claims management system';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Agents Table

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS agents.agent_raw (
  agent_id STRING NOT NULL,
  agent_code STRING NOT NULL,
  first_name STRING NOT NULL,
  last_name STRING NOT NULL,
  email STRING,
  phone STRING,
  agent_type STRING COMMENT 'Captive, Independent, Broker, Managing General Agent',
  agent_status STRING DEFAULT 'Active' COMMENT 'Active, Inactive, Suspended, Terminated',
  hire_date DATE,
  termination_date DATE,
  termination_reason STRING,
  -- License Information
  license_number STRING,
  license_state STRING,
  license_effective_date DATE,
  license_expiration_date DATE,
  license_status STRING,
  additional_state_licenses STRING COMMENT 'Comma-separated list',
  -- Agency Information
  agency_id STRING,
  agency_name STRING,
  agency_type STRING,
  -- Territory and Hierarchy
  territory_code STRING,
  region_code STRING,
  district_code STRING,
  office_location STRING,
  reports_to_agent_id STRING COMMENT 'Manager/supervisor',
  hierarchy_level INT COMMENT '1=Agent, 2=Team Lead, 3=Manager, 4=Regional Director, 5=VP',
  -- Product Authorization
  auto_license BOOLEAN DEFAULT FALSE,
  home_license BOOLEAN DEFAULT FALSE,
  life_license BOOLEAN DEFAULT FALSE,
  health_license BOOLEAN DEFAULT FALSE,
  commercial_license BOOLEAN DEFAULT FALSE,
  -- Performance
  ytd_policies_written INT DEFAULT 0,
  ytd_premium_volume DECIMAL(15,2) DEFAULT 0,
  ytd_commission_earned DECIMAL(12,2) DEFAULT 0,
  retention_rate DECIMAL(5,2),
  customer_satisfaction_score DECIMAL(3,2),
  compliance_score DECIMAL(3,2),
  -- Commission Structure
  commission_tier STRING,
  override_eligible BOOLEAN DEFAULT FALSE,
  bonus_eligible BOOLEAN DEFAULT TRUE,
  -- Source System
  source_system STRING DEFAULT 'AgentPortal',
  source_record_id STRING,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  ingestion_date DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
  record_hash STRING,
  _metadata STRING
)
USING DELTA
CLUSTER BY (agent_id, region_code, agent_status)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'bronze',
  'domain' = 'agent'
)
COMMENT 'Raw agent and broker data from agent management system';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Payments Table

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS payments.payment_raw (
  payment_id STRING NOT NULL,
  payment_reference_number STRING NOT NULL,
  payment_type STRING NOT NULL COMMENT 'Premium Payment, Claim Payment, Refund, Commission',
  payment_category STRING,
  -- Related Entities
  policy_id STRING,
  claim_id STRING,
  customer_id STRING,
  agent_id STRING,
  payee_id STRING,
  payee_name STRING,
  payee_type STRING,
  -- Payment Details
  payment_amount DECIMAL(15,2) NOT NULL,
  payment_currency STRING DEFAULT 'USD',
  payment_method STRING COMMENT 'Credit Card, ACH, Check, Wire, Cash',
  payment_status STRING NOT NULL COMMENT 'Pending, Processed, Cleared, Failed, Reversed, Cancelled',
  payment_date DATE NOT NULL,
  payment_due_date DATE,
  effective_date DATE,
  cleared_date DATE,
  posting_date DATE,
  -- Payment Instrument
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
  premium_type STRING COMMENT 'New Business, Renewal, Endorsement, Reinstatement',
  installment_number INT,
  total_installments INT,
  late_fee_amount DECIMAL(8,2) DEFAULT 0,
  discount_amount DECIMAL(8,2) DEFAULT 0,
  -- Claim Payment Specific
  payment_category_claim STRING COMMENT 'Loss Payment, Medical Payment, Legal Expense, Adjustment',
  payee_tax_id STRING,
  form_1099_required BOOLEAN DEFAULT FALSE,
  -- Failure/Reversal
  failure_reason STRING,
  failure_code STRING,
  reversal_reason STRING,
  reversed_payment_id STRING,
  -- Source System
  source_system STRING DEFAULT 'PaymentGateway',
  source_record_id STRING,
  source_timestamp TIMESTAMP,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  ingestion_date DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
  record_hash STRING,
  _metadata STRING
)
USING DELTA
CLUSTER BY (payment_id, policy_id, claim_id, payment_date)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'delta.dataSkippingNumIndexedCols' = '15',
  'quality' = 'bronze',
  'financial_data' = 'true',
  'domain' = 'payments'
)
COMMENT 'Raw payment and transaction data from payment gateway';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Underwriting Table

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS underwriting.underwriting_raw (
  underwriting_id STRING NOT NULL,
  application_id STRING NOT NULL,
  policy_id STRING,
  customer_id STRING NOT NULL,
  -- Application Details
  application_date DATE NOT NULL,
  submission_date DATE,
  quote_date DATE,
  decision_date DATE,
  effective_date DATE,
  -- Product Information
  product_type STRING NOT NULL COMMENT 'Auto, Home, Life, Health, Commercial',
  product_code STRING,
  coverage_requested DECIMAL(15,2),
  term_requested INT,
  -- Underwriting Decision
  underwriting_status STRING NOT NULL COMMENT 'Pending, In Review, Approved, Declined, Referred',
  decision STRING COMMENT 'Accept, Decline, Refer to Senior UW, Counter Offer',
  decline_reason STRING,
  referral_reason STRING,
  underwriter_id STRING,
  underwriter_notes STRING,
  -- Risk Assessment
  risk_class STRING COMMENT 'Preferred Plus, Preferred, Standard, Substandard',
  risk_tier STRING,
  risk_score INT COMMENT '300-900 composite risk score',
  risk_factors STRING COMMENT 'JSON array of risk factors',
  credit_score INT,
  claims_history_score INT,
  driving_record_score INT COMMENT 'For auto insurance',
  property_condition_score INT COMMENT 'For home insurance',
  health_rating STRING COMMENT 'For life/health insurance',
  -- Pricing
  base_premium DECIMAL(12,2),
  risk_premium_adjustment DECIMAL(10,2),
  quoted_premium DECIMAL(12,2),
  final_premium DECIMAL(12,2),
  rating_factors STRING COMMENT 'JSON object of rating factors',
  discounts_applied STRING COMMENT 'Multi-policy, Safe driver, etc.',
  surcharges_applied STRING,
  -- Requirements and Conditions
  conditions_imposed STRING,
  exclusions STRING,
  endorsements_required STRING,
  additional_documents_required STRING,
  inspection_required BOOLEAN DEFAULT FALSE,
  inspection_completed BOOLEAN DEFAULT FALSE,
  medical_exam_required BOOLEAN DEFAULT FALSE,
  medical_exam_completed BOOLEAN DEFAULT FALSE,
  -- External Data Sources
  mvr_ordered BOOLEAN DEFAULT FALSE COMMENT 'Motor Vehicle Record',
  mvr_score INT,
  clue_report_ordered BOOLEAN DEFAULT FALSE COMMENT 'Comprehensive Loss Underwriting Exchange',
  clue_score INT,
  lexis_nexis_ordered BOOLEAN DEFAULT FALSE,
  credit_report_ordered BOOLEAN DEFAULT FALSE,
  -- Approval Workflow
  requires_senior_approval BOOLEAN DEFAULT FALSE,
  approved_by STRING,
  approval_level STRING,
  -- Source System
  source_system STRING DEFAULT 'UnderwritingSystem',
  source_record_id STRING,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  ingestion_date DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
  record_hash STRING,
  _metadata STRING
)
USING DELTA
CLUSTER BY (underwriting_id, customer_id, policy_id)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'bronze',
  'domain' = 'underwriting'
)
COMMENT 'Raw underwriting and risk assessment data';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Provider Network Table

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS providers.provider_raw (
  provider_id STRING NOT NULL,
  provider_code STRING NOT NULL,
  provider_name STRING NOT NULL,
  provider_type STRING NOT NULL COMMENT 'Auto Body Shop, Medical Provider, Legal Counsel, Public Adjuster, Towing Service',
  provider_category STRING,
  -- Contact Information
  contact_name STRING,
  phone STRING,
  email STRING,
  website STRING,
  -- Location
  address_line1 STRING,
  address_line2 STRING,
  city STRING,
  state_code STRING,
  zip_code STRING,
  county STRING,
  service_area_codes STRING COMMENT 'Comma-separated zip codes',
  service_radius_miles INT,
  -- Network Status
  network_status STRING DEFAULT 'Active' COMMENT 'Active, Inactive, Suspended, Pending Review',
  preferred_provider BOOLEAN DEFAULT FALSE,
  in_network BOOLEAN DEFAULT TRUE,
  network_tier STRING COMMENT 'Platinum, Gold, Silver',
  contract_start_date DATE,
  contract_end_date DATE,
  -- Credentials and Licensing
  license_number STRING,
  license_state STRING,
  license_expiration_date DATE,
  certifications STRING,
  specialties STRING,
  -- Financial
  payment_terms STRING,
  discount_rate DECIMAL(5,2),
  hourly_rate DECIMAL(8,2),
  average_claim_cost DECIMAL(10,2),
  ytd_payments DECIMAL(15,2) DEFAULT 0,
  -- Performance Metrics
  performance_rating DECIMAL(3,2) COMMENT '1.00 to 5.00',
  customer_satisfaction_score DECIMAL(3,2),
  quality_score DECIMAL(3,2),
  turnaround_time_days DECIMAL(5,2),
  total_claims_serviced INT DEFAULT 0,
  claims_last_12_months INT DEFAULT 0,
  complaint_count INT DEFAULT 0,
  recommendation_rate DECIMAL(5,2),
  -- Capacity
  capacity_status STRING COMMENT 'Available, Limited, Full',
  max_concurrent_claims INT,
  current_active_claims INT DEFAULT 0,
  -- Audit and Compliance
  last_audit_date DATE,
  audit_score DECIMAL(3,2),
  compliance_status STRING,
  background_check_date DATE,
  insurance_verified BOOLEAN DEFAULT FALSE,
  insurance_expiration_date DATE,
  -- Source System
  source_system STRING DEFAULT 'ProviderNetwork',
  source_record_id STRING,
  ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  ingestion_date DATE GENERATED ALWAYS AS (CAST(ingestion_timestamp AS DATE)),
  record_hash STRING,
  _metadata STRING
)
USING DELTA
CLUSTER BY (provider_id, state_code, provider_type, network_status)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'bronze',
  'domain' = 'providers'
)
COMMENT 'Raw provider network data (repair shops, medical providers, legal counsel)';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Reference Data Tables

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS customers.state_codes (
  state_code STRING NOT NULL PRIMARY KEY,
  state_name STRING NOT NULL,
  region STRING NOT NULL,
  territory_code STRING,
  population BIGINT,
  insurance_regulations STRING,
  min_coverage_requirements STRING
)
USING DELTA
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported'
)
COMMENT 'Reference table for US states';

CREATE TABLE IF NOT EXISTS policies.product_catalog (
  product_code STRING NOT NULL PRIMARY KEY,
  product_name STRING NOT NULL,
  product_line STRING NOT NULL,
  product_type STRING NOT NULL,
  description STRING,
  active BOOLEAN DEFAULT TRUE,
  available_in_states STRING,
  base_rate DECIMAL(10,2),
  effective_date DATE,
  expiration_date DATE
)
USING DELTA
TBLPROPERTIES (
  'delta.feature.allowColumnDefaults' = 'supported'
)
COMMENT 'Insurance product catalog';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Liquid Clustering Optimization
-- MAGIC
-- MAGIC All tables are configured with **Liquid Clustering** which provides:
-- MAGIC - **Automatic optimization** - No manual OPTIMIZE commands needed
-- MAGIC - **Flexible clustering keys** - Can be changed without rewriting data
-- MAGIC - **Better performance** - Auto-optimizes based on query patterns
-- MAGIC - **Lower maintenance** - Clustering happens automatically during writes
-- MAGIC
-- MAGIC Note: Liquid Clustering will automatically optimize data layout during writes and background maintenance.

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Analyze Tables for Statistics

-- COMMAND ----------
ANALYZE TABLE customers.customer_raw COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE policies.policy_raw COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE claims.claim_raw COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE agents.agent_raw COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE payments.payment_raw COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE underwriting.underwriting_raw COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE providers.provider_raw COMPUTE STATISTICS FOR ALL COLUMNS;

