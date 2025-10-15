-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Gold Layer - Business Analytics Tables
-- MAGIC Create aggregated, business-ready analytics tables

-- COMMAND ----------
-- Create widget for catalog parameter
CREATE WIDGET DROPDOWN catalog DEFAULT "insurance_dev_gold" CHOICES SELECT * FROM (VALUES ("insurance_dev_gold"), ("insurance_staging_gold"), ("insurance_prod_gold"));

-- COMMAND ----------
-- Use the selected catalog
USE CATALOG IDENTIFIER(:catalog);

-- COMMAND ----------
-- Create schemas if they don't exist
CREATE SCHEMA IF NOT EXISTS customer_analytics;
CREATE SCHEMA IF NOT EXISTS claims_analytics;
CREATE SCHEMA IF NOT EXISTS policy_analytics;
CREATE SCHEMA IF NOT EXISTS agent_analytics;
CREATE SCHEMA IF NOT EXISTS financial_analytics;
CREATE SCHEMA IF NOT EXISTS risk_analytics;
CREATE SCHEMA IF NOT EXISTS regulatory_reporting;
CREATE SCHEMA IF NOT EXISTS executive_dashboards;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Customer 360 View

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Clean up existing table completely

-- COMMAND ----------
-- Drop existing table if it exists (use DROP instead of DROP TABLE IF EXISTS to ensure clean removal)
DROP TABLE IF EXISTS customer_analytics.customer_360;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ### Create fresh customer_360 table with all columns and liquid clustering
-- MAGIC 
-- MAGIC **Important:** All columns must be defined BEFORE the CLUSTER BY clause

-- COMMAND ----------
-- Create customer_360 table with liquid clustering
-- Note: Changed CREATE TABLE IF NOT EXISTS to just CREATE TABLE for cleaner creation
CREATE TABLE customer_analytics.customer_360 (
  customer_360_key BIGINT NOT NULL COMMENT 'Unique key for customer 360 record',
  customer_key BIGINT NOT NULL,
  customer_id STRING NOT NULL,
  full_name STRING,
  -- Demographics
  age_years INT,
  age_bracket STRING,
  state_code STRING,
  region STRING,
  customer_segment STRING,
  customer_status STRING,
  customer_since_date DATE,
  tenure_years DECIMAL(5,2),
  -- Policy Summary
  total_policies INT,
  active_policies INT,
  lapsed_policies INT,
  cancelled_policies INT,
  auto_policies INT,
  home_policies INT,
  life_policies INT,
  health_policies INT,
  commercial_policies INT,
  -- Financial Metrics
  total_annual_premium DECIMAL(15,2),
  total_lifetime_premium DECIMAL(15,2),
  average_policy_premium DECIMAL(12,2),
  -- Claims History
  total_claims_count INT,
  open_claims_count INT,
  closed_claims_count INT,
  denied_claims_count INT,
  total_claims_amount DECIMAL(15,2),
  total_claims_paid DECIMAL(15,2),
  average_claim_amount DECIMAL(12,2),
  loss_ratio DECIMAL(5,4),
  last_claim_date DATE,
  days_since_last_claim INT,
  -- Risk and Underwriting
  overall_risk_score INT,
  risk_tier STRING,
  fraud_risk_score DECIMAL(5,2),
  credit_tier STRING,
  -- Engagement Metrics
  last_contact_date DATE,
  days_since_last_contact INT,
  contact_count_last_12m INT,
  complaints_count INT,
  satisfaction_score DECIMAL(3,2),
  nps_score INT COMMENT 'Net Promoter Score',
  -- Value Metrics
  customer_lifetime_value DECIMAL(15,2),
  predicted_lifetime_value DECIMAL(15,2),
  value_tier STRING COMMENT 'High Value, Medium Value, Low Value',
  profitability DECIMAL(15,2),
  -- Retention and Churn
  churn_risk_score DECIMAL(5,4),
  churn_risk_category STRING COMMENT 'High, Medium, Low',
  retention_probability DECIMAL(5,4),
  next_renewal_date DATE,
  days_to_renewal INT,
  renewal_likelihood DECIMAL(5,4),
  -- Cross-sell/Upsell
  cross_sell_score DECIMAL(5,2),
  recommended_products STRING,
  upsell_opportunity DECIMAL(12,2),
  -- Agent Relationship
  primary_agent_id STRING,
  primary_agent_name STRING,
  agent_relationship_years DECIMAL(5,2),
  agent_satisfaction_score DECIMAL(3,2),
  -- Payment Behavior
  payment_method_preference STRING,
  payment_frequency STRING,
  on_time_payment_rate DECIMAL(5,4),
  late_payments_count INT,
  total_late_fees DECIMAL(10,2),
  payment_risk_score DECIMAL(5,2),
  -- Timestamps
  snapshot_date DATE,
  created_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (customer_id, state_code, customer_segment)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'gold',
  'domain' = 'customer_analytics'
)
COMMENT 'Customer 360 comprehensive view with all customer metrics and predictions';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC **Note:** Liquid clustering uses only columns with computed stats (customer_id, state_code, customer_segment).
-- MAGIC Column `churn_risk_category` is beyond the stats computation limit and was removed from clustering.

-- COMMAND ----------
-- Verify table was created with all columns and clustering
DESCRIBE EXTENDED customer_analytics.customer_360;

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Claims Fraud Detection Analytics

-- COMMAND ----------
DROP TABLE IF EXISTS claims_analytics.claims_fraud_detection;

-- COMMAND ----------
CREATE TABLE claims_analytics.claims_fraud_detection (
  fraud_detection_key BIGINT NOT NULL,
  claim_key BIGINT NOT NULL,
  claim_id STRING NOT NULL,
  claim_number STRING,
  policy_id STRING,
  customer_id STRING,
  -- Claim Details
  claim_type STRING,
  loss_type STRING,
  claim_status STRING,
  loss_date TIMESTAMP,
  report_date TIMESTAMP,
  claimed_amount DECIMAL(15,2),
  paid_amount DECIMAL(15,2),
  -- Fraud Scores and Indicators
  overall_fraud_score DECIMAL(5,2) COMMENT '0-100 fraud probability',
  fraud_risk_category STRING COMMENT 'Critical, High, Medium, Low',
  ml_fraud_prediction DECIMAL(5,4),
  rule_based_score DECIMAL(5,2),
  behavioral_score DECIMAL(5,2),
  network_score DECIMAL(5,2),
  -- Fraud Indicators (Boolean Flags)
  late_reporting_flag BOOLEAN,
  duplicate_claim_flag BOOLEAN,
  excessive_amount_flag BOOLEAN,
  frequent_claimant_flag BOOLEAN,
  location_mismatch_flag BOOLEAN,
  provider_fraud_history_flag BOOLEAN,
  medical_billing_anomaly_flag BOOLEAN,
  staged_accident_indicator BOOLEAN,
  inconsistent_statement_flag BOOLEAN,
  suspicious_injury_pattern_flag BOOLEAN,
  total_fraud_indicators INT,
  -- Customer Fraud History
  customer_total_claims INT,
  customer_siu_referrals INT,
  customer_denied_claims INT,
  customer_fraud_score DECIMAL(5,2),
  -- Provider Fraud Risk
  provider_id STRING,
  provider_name STRING,
  provider_fraud_history INT,
  provider_fraud_score DECIMAL(5,2),
  -- Network Analysis
  related_claims_count INT COMMENT 'Claims in same network',
  shared_provider_suspicious_count INT,
  shared_attorney_suspicious_count INT,
  -- Financial Anomalies
  claim_to_premium_ratio DECIMAL(5,4),
  amount_vs_similar_claims_percentile DECIMAL(5,2),
  settlement_speed_percentile DECIMAL(5,2),
  -- Investigation Status
  siu_referral_flag BOOLEAN,
  siu_referral_date DATE,
  investigation_status STRING,
  investigation_result STRING,
  investigator_id STRING,
  -- Actions and Outcomes
  recommended_action STRING COMMENT 'Approve, Deny, Investigate, Refer to SIU',
  action_priority STRING COMMENT 'Urgent, High, Medium, Low',
  estimated_exposure DECIMAL(15,2),
  potential_recovery DECIMAL(12,2),
  -- Model Metadata
  model_version STRING,
  model_confidence DECIMAL(5,4),
  model_explanation STRING COMMENT 'Top contributing factors',
  -- Timestamps
  analysis_date DATE,
  created_timestamp TIMESTAMP,
  updated_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (claim_id, fraud_risk_category)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'gold',
  'domain' = 'claims_analytics'
)
COMMENT 'Claims fraud detection analytics with ML scores and indicators - Simplified for Community Edition';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Policy Performance Analytics

-- COMMAND ----------
DROP TABLE IF EXISTS policy_analytics.policy_performance;

-- COMMAND ----------
CREATE TABLE policy_analytics.policy_performance (
  performance_key BIGINT NOT NULL,
  -- Time Dimension
  report_date DATE NOT NULL,
  year INT,
  quarter INT,
  month INT,
  -- Policy Segmentation
  policy_type STRING,
  product_line STRING,
  state_code STRING,
  region STRING,
  agent_id STRING,
  agency_code STRING,
  -- Volume Metrics
  total_policies INT DEFAULT 0,
  new_business_count INT DEFAULT 0,
  renewal_count INT DEFAULT 0,
  cancelled_count INT DEFAULT 0,
  lapsed_count INT DEFAULT 0,
  active_policies INT DEFAULT 0,
  -- Premium Metrics
  total_premium DECIMAL(15,2) DEFAULT 0,
  new_business_premium DECIMAL(15,2) DEFAULT 0,
  renewal_premium DECIMAL(15,2) DEFAULT 0,
  earned_premium DECIMAL(15,2) DEFAULT 0,
  written_premium DECIMAL(15,2) DEFAULT 0,
  average_premium DECIMAL(12,2),
  premium_growth_rate DECIMAL(5,4),
  -- Claims Metrics
  claims_count INT DEFAULT 0,
  claims_frequency DECIMAL(5,4) COMMENT 'Claims per 100 policies',
  total_incurred DECIMAL(15,2) DEFAULT 0,
  total_paid DECIMAL(15,2) DEFAULT 0,
  average_claim_severity DECIMAL(12,2),
  -- Loss Ratios
  loss_ratio DECIMAL(5,4),
  loss_ratio_incurred DECIMAL(5,4),
  loss_ratio_paid DECIMAL(5,4),
  combined_ratio DECIMAL(5,4),
  -- Retention Metrics
  retention_rate DECIMAL(5,4),
  cancellation_rate DECIMAL(5,4),
  lapse_rate DECIMAL(5,4),
  renewal_rate DECIMAL(5,4),
  -- Underwriting Quality
  average_risk_score INT,
  preferred_tier_percentage DECIMAL(5,4),
  standard_tier_percentage DECIMAL(5,4),
  substandard_tier_percentage DECIMAL(5,4),
  -- Profitability
  underwriting_profit DECIMAL(15,2),
  profit_margin DECIMAL(5,4),
  roi DECIMAL(5,4),
  -- Customer Metrics
  unique_customers INT,
  multi_policy_customers INT,
  average_policies_per_customer DECIMAL(5,2),
  customer_lifetime_value DECIMAL(15,2),
  -- Payment Metrics
  on_time_payment_rate DECIMAL(5,4),
  overdue_premium DECIMAL(15,2),
  collection_rate DECIMAL(5,4),
  -- Commission Metrics
  total_commission DECIMAL(15,2),
  commission_rate_avg DECIMAL(5,4),
  -- Quality Metrics
  quote_to_bind_ratio DECIMAL(5,4),
  application_approval_rate DECIMAL(5,4),
  customer_satisfaction_avg DECIMAL(3,2),
  complaint_rate DECIMAL(5,4),
  -- Timestamps
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (report_date, policy_type, region)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'gold',
  'domain' = 'policy_analytics'
)
COMMENT 'Policy performance metrics and KPIs by various dimensions';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Agent Performance Scorecard

-- COMMAND ----------
DROP TABLE IF EXISTS agent_analytics.agent_performance_scorecard;

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS agent_analytics.agent_performance_scorecard (
  scorecard_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  -- Time and Agent
  report_date DATE NOT NULL,
  year INT,
  quarter INT,
  month INT,
  agent_key BIGINT NOT NULL,
  agent_id STRING NOT NULL,
  agent_name STRING,
  agent_type STRING,
  region_code STRING,
  territory_code STRING,
  hierarchy_level INT,
  manager_agent_id STRING,
  -- Production Metrics
  new_policies_written INT DEFAULT 0,
  renewals_serviced INT DEFAULT 0,
  total_policies INT DEFAULT 0,
  policies_in_force INT DEFAULT 0,
  cancelled_policies INT DEFAULT 0,
  -- Premium Production
  new_business_premium DECIMAL(15,2) DEFAULT 0,
  renewal_premium DECIMAL(15,2) DEFAULT 0,
  total_premium_written DECIMAL(15,2) DEFAULT 0,
  premium_growth_rate DECIMAL(5,4),
  premium_per_policy DECIMAL(12,2),
  -- Product Mix
  auto_premium DECIMAL(12,2) DEFAULT 0,
  home_premium DECIMAL(12,2) DEFAULT 0,
  life_premium DECIMAL(12,2) DEFAULT 0,
  health_premium DECIMAL(12,2) DEFAULT 0,
  commercial_premium DECIMAL(12,2) DEFAULT 0,
  product_diversification_score DECIMAL(3,2),
  -- Commission and Earnings
  total_commission_earned DECIMAL(12,2) DEFAULT 0,
  commission_rate_avg DECIMAL(5,4),
  bonus_earned DECIMAL(10,2) DEFAULT 0,
  override_commission DECIMAL(10,2) DEFAULT 0,
  total_earnings DECIMAL(15,2),
  -- Retention Metrics
  retention_rate DECIMAL(5,4),
  persistency_rate DECIMAL(5,4),
  cancellation_rate DECIMAL(5,4),
  lapse_rate DECIMAL(5,4),
  -- Customer Metrics
  active_customers INT DEFAULT 0,
  new_customers_acquired INT DEFAULT 0,
  lost_customers INT DEFAULT 0,
  multi_policy_customers INT DEFAULT 0,
  cross_sell_ratio DECIMAL(5,4),
  customer_lifetime_value_avg DECIMAL(12,2),
  -- Quality Metrics
  quote_to_bind_ratio DECIMAL(5,4),
  average_policy_value DECIMAL(12,2),
  underwriting_quality_score DECIMAL(3,2),
  claims_frequency DECIMAL(5,4),
  loss_ratio DECIMAL(5,4),
  -- Service Metrics
  customer_satisfaction_score DECIMAL(3,2),
  nps_score INT,
  complaint_count INT DEFAULT 0,
  complaint_rate DECIMAL(5,4),
  response_time_hours DECIMAL(6,2),
  issue_resolution_rate DECIMAL(5,4),
  -- Activity Metrics
  customer_interactions INT DEFAULT 0,
  quotes_generated INT DEFAULT 0,
  applications_submitted INT DEFAULT 0,
  policies_endorsed INT DEFAULT 0,
  claims_assisted INT DEFAULT 0,
  -- Compliance and Training
  compliance_score DECIMAL(3,2),
  training_hours_completed DECIMAL(6,2),
  license_status STRING,
  continuing_education_credits INT DEFAULT 0,
  policy_violations INT DEFAULT 0,
  -- Performance Rankings
  rank_in_region INT,
  rank_in_company INT,
  percentile_performance DECIMAL(5,2),
  -- Performance Tier
  performance_tier STRING COMMENT 'Top Performer, Above Average, Average, Below Average, Underperforming',
  performance_trend STRING COMMENT 'Improving, Stable, Declining',
  -- Goals and Targets
  premium_goal DECIMAL(15,2),
  premium_achievement_rate DECIMAL(5,4),
  policy_count_goal INT,
  policy_achievement_rate DECIMAL(5,4),
  retention_goal DECIMAL(5,4),
  retention_achievement_rate DECIMAL(5,4),
  overall_goal_achievement DECIMAL(5,4),
  -- Recognition
  award_qualified BOOLEAN DEFAULT FALSE,
  award_tier STRING,
  recognition_points INT DEFAULT 0,
  -- Timestamps
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (agent_id, region_code)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'gold',
  'domain' = 'agent_analytics'
)
COMMENT 'Agent performance scorecard with comprehensive KPIs and rankings';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Financial Performance Summary

-- COMMAND ----------
DROP TABLE IF EXISTS financial_analytics.financial_summary;

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS financial_analytics.financial_summary (
  financial_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  -- Time Dimension
  report_date DATE NOT NULL,
  fiscal_year INT,
  fiscal_quarter INT,
  fiscal_month INT,
  -- Segmentation
  business_segment STRING COMMENT 'Personal Lines, Commercial Lines, Life & Health',
  product_line STRING,
  state_code STRING,
  region STRING,
  -- Premium Revenue
  direct_written_premium DECIMAL(18,2) DEFAULT 0,
  earned_premium DECIMAL(18,2) DEFAULT 0,
  unearned_premium DECIMAL(18,2) DEFAULT 0,
  ceded_premium DECIMAL(18,2) DEFAULT 0 COMMENT 'Reinsurance',
  net_written_premium DECIMAL(18,2),
  net_earned_premium DECIMAL(18,2),
  premium_growth_rate DECIMAL(5,4),
  -- Claims and Losses
  incurred_losses DECIMAL(18,2) DEFAULT 0,
  paid_losses DECIMAL(18,2) DEFAULT 0,
  loss_reserves DECIMAL(18,2) DEFAULT 0,
  ibnr_reserves DECIMAL(18,2) DEFAULT 0 COMMENT 'Incurred But Not Reported',
  loss_adjustment_expenses DECIMAL(15,2) DEFAULT 0,
  total_incurred DECIMAL(18,2),
  -- Loss Ratios
  loss_ratio_earned DECIMAL(5,4),
  loss_ratio_written DECIMAL(5,4),
  loss_ratio_paid DECIMAL(5,4),
  -- Expenses
  commission_expense DECIMAL(15,2) DEFAULT 0,
  underwriting_expense DECIMAL(15,2) DEFAULT 0,
  operating_expense DECIMAL(15,2) DEFAULT 0,
  general_admin_expense DECIMAL(15,2) DEFAULT 0,
  total_expenses DECIMAL(18,2),
  expense_ratio DECIMAL(5,4),
  -- Combined Ratio
  combined_ratio DECIMAL(5,4),
  combined_ratio_target DECIMAL(5,4),
  combined_ratio_variance DECIMAL(5,4),
  -- Underwriting Profit/Loss
  underwriting_gain_loss DECIMAL(18,2),
  underwriting_profit_margin DECIMAL(5,4),
  -- Investment Income
  investment_income DECIMAL(15,2) DEFAULT 0,
  realized_gains_losses DECIMAL(15,2) DEFAULT 0,
  unrealized_gains_losses DECIMAL(15,2) DEFAULT 0,
  investment_yield DECIMAL(5,4),
  -- Net Income
  operating_income DECIMAL(18,2),
  net_income_before_tax DECIMAL(18,2),
  income_tax DECIMAL(15,2),
  net_income DECIMAL(18,2),
  net_margin DECIMAL(5,4),
  -- Return Metrics
  roe DECIMAL(5,4) COMMENT 'Return on Equity',
  roa DECIMAL(5,4) COMMENT 'Return on Assets',
  roic DECIMAL(5,4) COMMENT 'Return on Invested Capital',
  -- Capital and Surplus
  policyholders_surplus DECIMAL(18,2),
  total_assets DECIMAL(20,2),
  total_liabilities DECIMAL(20,2),
  -- Ratios and Metrics
  premium_to_surplus_ratio DECIMAL(5,4),
  reserve_to_surplus_ratio DECIMAL(5,4),
  leverage_ratio DECIMAL(5,4),
  -- Reinsurance
  ceded_percentage DECIMAL(5,4),
  retention_rate DECIMAL(5,4),
  reinsurance_recoverable DECIMAL(15,2),
  -- Cash Flow
  operating_cash_flow DECIMAL(18,2),
  investing_cash_flow DECIMAL(15,2),
  financing_cash_flow DECIMAL(15,2),
  free_cash_flow DECIMAL(18,2),
  -- Risk Metrics
  rbc_ratio DECIMAL(5,4) COMMENT 'Risk-Based Capital Ratio',
  capital_adequacy_ratio DECIMAL(5,4),
  -- Timestamps
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  updated_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (report_date, business_segment, region)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact' = 'true',
  'quality' = 'gold',
  'financial_data' = 'true',
  'domain' = 'financial_analytics'
)
COMMENT 'Financial performance summary with P&L, balance sheet, and key ratios';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Regulatory Reporting Tables

-- COMMAND ----------
DROP TABLE IF EXISTS regulatory_reporting.state_regulatory_report;

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS regulatory_reporting.state_regulatory_report (
  report_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  report_date DATE NOT NULL,
  report_period STRING COMMENT 'Q1-2024, Annual-2024',
  report_type STRING COMMENT 'Quarterly, Annual',
  state_code STRING NOT NULL,
  filing_deadline DATE,
  filing_status STRING COMMENT 'Draft, Submitted, Approved, Rejected',
  -- Policy Data
  policies_in_force INT,
  new_policies_written INT,
  cancelled_policies INT,
  total_direct_premium DECIMAL(15,2),
  total_earned_premium DECIMAL(15,2),
  -- Claims Data
  claims_reported INT,
  claims_closed INT,
  claims_paid_amount DECIMAL(15,2),
  claims_denied INT,
  -- Loss Ratios
  loss_ratio DECIMAL(5,4),
  combined_ratio DECIMAL(5,4),
  -- Market Share
  state_market_share DECIMAL(5,4),
  rank_in_state INT,
  -- Compliance Metrics
  complaint_count INT,
  complaint_ratio DECIMAL(5,4),
  regulatory_violations INT,
  fines_penalties DECIMAL(12,2) DEFAULT 0,
  -- Solvency Metrics
  surplus DECIMAL(18,2),
  rbc_ratio DECIMAL(5,4),
  -- Agent Licensing
  licensed_agents_count INT,
  license_violations INT,
  -- Timestamps
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  submitted_timestamp TIMESTAMP,
  approved_timestamp TIMESTAMP
)
USING DELTA
CLUSTER BY (report_date, state_code, report_type)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality' = 'gold',
  'domain' = 'regulatory_reporting'
)
COMMENT 'State regulatory reporting data for compliance';

-- COMMAND ----------
-- MAGIC %md
-- MAGIC ## Executive Dashboard Metrics

-- COMMAND ----------
DROP TABLE IF EXISTS executive_dashboards.executive_kpi_summary;

-- COMMAND ----------
CREATE TABLE IF NOT EXISTS executive_dashboards.executive_kpi_summary (
  kpi_key BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  report_date DATE NOT NULL,
  metric_category STRING COMMENT 'Financial, Operational, Customer, Risk',
  -- Top-Level Financials
  total_revenue DECIMAL(18,2),
  net_income DECIMAL(18,2),
  operating_margin DECIMAL(5,4),
  roe DECIMAL(5,4),
  -- Growth Metrics
  revenue_growth_yoy DECIMAL(5,4),
  premium_growth_yoy DECIMAL(5,4),
  customer_growth_yoy DECIMAL(5,4),
  policy_growth_yoy DECIMAL(5,4),
  -- Profitability
  combined_ratio DECIMAL(5,4),
  loss_ratio DECIMAL(5,4),
  expense_ratio DECIMAL(5,4),
  profit_per_policy DECIMAL(10,2),
  -- Customer Metrics
  total_customers BIGINT,
  active_policies BIGINT,
  customer_lifetime_value DECIMAL(12,2),
  customer_acquisition_cost DECIMAL(8,2),
  customer_retention_rate DECIMAL(5,4),
  nps_score DECIMAL(5,2),
  -- Market Position
  market_share DECIMAL(5,4),
  market_rank INT,
  brand_value DECIMAL(18,2),
  -- Risk Metrics
  rbc_ratio DECIMAL(5,4),
  capital_adequacy DECIMAL(5,4),
  claims_frequency DECIMAL(5,4),
  fraud_rate DECIMAL(5,4),
  -- Operational Efficiency
  claims_processing_days DECIMAL(5,2),
  policy_issuance_days DECIMAL(5,2),
  customer_service_response_time DECIMAL(6,2),
  straight_through_processing_rate DECIMAL(5,4),
  -- Employee Metrics
  total_agents INT,
  agent_productivity DECIMAL(12,2),
  agent_retention_rate DECIMAL(5,4),
  agent_satisfaction_score DECIMAL(3,2),
  -- Digital Metrics
  online_quote_rate DECIMAL(5,4),
  mobile_app_adoption DECIMAL(5,4),
  digital_payment_rate DECIMAL(5,4),
  self_service_rate DECIMAL(5,4),
  -- Strategic Initiatives
  innovation_investment DECIMAL(15,2),
  digital_transformation_progress DECIMAL(5,4),
  sustainability_score DECIMAL(3,2),
  -- Timestamps
  created_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
CLUSTER BY (report_date, metric_category)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.feature.allowColumnDefaults' = 'supported',
  'delta.autoOptimize.optimizeWrite' = 'true',
  'quality' = 'gold',
  'domain' = 'executive_dashboards'
)
COMMENT 'Executive KPI summary for C-level dashboards';

-- COMMAND ----------
ANALYZE TABLE customer_analytics.customer_360 COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE claims_analytics.claims_fraud_detection COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE policy_analytics.policy_performance COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE agent_analytics.agent_performance_scorecard COMPUTE STATISTICS FOR ALL COLUMNS;
ANALYZE TABLE financial_analytics.financial_summary COMPUTE STATISTICS FOR ALL COLUMNS;

