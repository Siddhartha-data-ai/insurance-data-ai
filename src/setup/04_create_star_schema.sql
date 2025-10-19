-- =====================================================
-- INSURANCE DATA AI - STAR SCHEMA
-- Dimensional Modeling for Analytics and BI
-- Optimized for OLAP queries and reporting
-- =====================================================

-- =====================================================
-- DIMENSION TABLES
-- =====================================================

-- 1. CUSTOMER DIMENSION (Type 2 SCD)
CREATE TABLE IF NOT EXISTS ${catalog_name}_gold.analytics.dim_customer (
    customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    customer_id STRING NOT NULL,  -- Natural key
    customer_name STRING NOT NULL,
    date_of_birth DATE,
    age INT,
    age_group STRING,  -- '18-25', '26-35', '36-45', '46-55', '56-65', '65+'
    gender STRING,
    marital_status STRING,
    email STRING,
    phone STRING,
    address_line1 STRING,
    address_line2 STRING,
    city STRING,
    state STRING,
    zip_code STRING,
    region STRING,  -- Northeast, Southeast, Midwest, West
    credit_score INT,
    credit_score_band STRING,  -- Excellent, Good, Fair, Poor
    annual_income DECIMAL(15,2),
    income_bracket STRING,  -- <50K, 50-100K, 100-200K, >200K
    employment_status STRING,
    occupation STRING,
    customer_segment STRING,  -- Platinum, Gold, Silver, Regular
    customer_status STRING,  -- Active, Inactive, Suspended
    kyc_status STRING,  -- Verified, Pending, Incomplete
    risk_profile STRING,  -- Low, Medium, High, Very High
    
    -- SCD Type 2 fields
    effective_date DATE NOT NULL,
    end_date DATE,
    is_current BOOLEAN DEFAULT TRUE,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP,
    source_system STRING DEFAULT 'INSURANCE_CORE'
)
USING DELTA
PARTITIONED BY (region)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Customer dimension with SCD Type 2 for historical tracking';

-- 2. POLICY DIMENSION
CREATE TABLE IF NOT EXISTS ${catalog_name}_gold.analytics.dim_policy (
    policy_sk BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    policy_id STRING NOT NULL,  -- Natural key
    policy_number STRING,
    policy_type STRING NOT NULL,  -- Auto, Home, Life, Health, Commercial
    policy_sub_type STRING,  -- Comprehensive, Liability, Term Life, etc.
    policy_status STRING NOT NULL,  -- Active, Lapsed, Cancelled, Expired
    product_name STRING,
    product_code STRING,
    coverage_type STRING,  -- Individual, Family, Group
    payment_frequency STRING,  -- Monthly, Quarterly, Semi-Annual, Annual
    auto_renewal BOOLEAN DEFAULT FALSE,
    is_bundled BOOLEAN DEFAULT FALSE,  -- Part of multi-policy bundle
    bundle_id STRING,
    underwriting_company STRING,
    distribution_channel STRING,  -- Direct, Agent, Broker, Online
    
    -- Policy attributes
    term_years INT,
    is_renewable BOOLEAN,
    has_riders BOOLEAN,  -- Additional coverage riders
    deductible_type STRING,  -- Flat, Percentage
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP,
    source_system STRING DEFAULT 'INSURANCE_CORE'
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Policy dimension for policy attributes';

-- 3. AGENT DIMENSION
CREATE TABLE IF NOT EXISTS ${catalog_name}_gold.analytics.dim_agent (
    agent_sk BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    agent_id STRING NOT NULL,  -- Natural key
    agent_name STRING NOT NULL,
    agent_email STRING,
    agent_phone STRING,
    agent_status STRING,  -- Active, Inactive, Terminated
    license_number STRING,
    license_state STRING,
    license_expiration_date DATE,
    hire_date DATE,
    years_of_experience INT,
    specialization ARRAY<STRING>,  -- Auto, Home, Life, etc.
    performance_tier STRING,  -- Top Performer, High Performer, Standard
    branch_id STRING,
    branch_name STRING,
    region STRING,
    manager_id STRING,
    manager_name STRING,
    commission_rate DECIMAL(5,2),
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP
)
USING DELTA
COMMENT 'Agent/broker dimension';

-- 4. DATE DIMENSION
CREATE TABLE IF NOT EXISTS ${catalog_name}_gold.analytics.dim_date (
    date_sk INT NOT NULL,  -- Surrogate key (YYYYMMDD format)
    date_value DATE NOT NULL UNIQUE,
    year INT NOT NULL,
    quarter INT NOT NULL,
    quarter_name STRING,  -- Q1, Q2, Q3, Q4
    month INT NOT NULL,
    month_name STRING,  -- January, February, etc.
    month_short_name STRING,  -- Jan, Feb, etc.
    week_of_year INT,
    day_of_month INT,
    day_of_week INT,  -- 1=Sunday, 7=Saturday
    day_of_week_name STRING,  -- Sunday, Monday, etc.
    day_of_week_short STRING,  -- Sun, Mon, etc.
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    holiday_name STRING,
    fiscal_year INT,
    fiscal_quarter INT,
    fiscal_month INT,
    is_month_end BOOLEAN,
    is_quarter_end BOOLEAN,
    is_year_end BOOLEAN,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Date dimension for time-based analysis';

-- 5. TIME DIMENSION
CREATE TABLE IF NOT EXISTS ${catalog_name}_gold.analytics.dim_time (
    time_sk INT NOT NULL,  -- Surrogate key (HHMMSS format)
    time_value TIME NOT NULL UNIQUE,
    hour INT NOT NULL,
    minute INT NOT NULL,
    second INT NOT NULL,
    hour_12 INT,  -- 12-hour format
    am_pm STRING,  -- AM/PM
    hour_name STRING,  -- 12:00 AM, 1:00 AM, etc.
    minute_of_day INT,  -- 0-1439
    second_of_day INT,  -- 0-86399
    time_period STRING,  -- Morning, Afternoon, Evening, Night
    is_business_hours BOOLEAN,  -- 9 AM - 5 PM
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
USING DELTA
COMMENT 'Time dimension for intraday analysis';

-- 6. CLAIM TYPE DIMENSION
CREATE TABLE IF NOT EXISTS ${catalog_name}_gold.analytics.dim_claim_type (
    claim_type_sk BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    claim_type STRING NOT NULL UNIQUE,  -- Natural key
    claim_category STRING NOT NULL,  -- Auto, Property, Health, Liability
    claim_sub_category STRING,
    severity_level STRING,  -- Minor, Moderate, Major, Catastrophic
    typical_processing_days INT,
    requires_investigation BOOLEAN DEFAULT FALSE,
    requires_adjuster BOOLEAN DEFAULT TRUE,
    average_claim_amount DECIMAL(15,2),
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP
)
USING DELTA
COMMENT 'Claim type dimension for claim classification';

-- 7. GEOGRAPHY DIMENSION
CREATE TABLE IF NOT EXISTS ${catalog_name}_gold.analytics.dim_geography (
    geography_sk BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    zip_code STRING NOT NULL,
    city STRING NOT NULL,
    county STRING,
    state STRING NOT NULL,
    state_code STRING NOT NULL,
    region STRING NOT NULL,  -- Northeast, Southeast, Midwest, West
    country STRING DEFAULT 'USA',
    country_code STRING DEFAULT 'US',
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6),
    population INT,
    median_income DECIMAL(15,2),
    urban_rural STRING,  -- Urban, Suburban, Rural
    risk_zone STRING,  -- Flood, Hurricane, Earthquake, etc.
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP
)
USING DELTA
COMMENT 'Geography dimension for location-based analysis';

-- =====================================================
-- FACT TABLES
-- =====================================================

-- 1. POLICY FACT TABLE (Periodic Snapshot)
CREATE TABLE IF NOT EXISTS ${catalog_name}_gold.analytics.fact_policy (
    policy_sk BIGINT NOT NULL,  -- FK to dim_policy
    customer_sk BIGINT NOT NULL,  -- FK to dim_customer
    agent_sk BIGINT,  -- FK to dim_agent
    issue_date_sk INT NOT NULL,  -- FK to dim_date
    start_date_sk INT NOT NULL,  -- FK to dim_date
    end_date_sk INT,  -- FK to dim_date
    geography_sk BIGINT,  -- FK to dim_geography
    
    -- Degenerate dimensions (facts that are also dimensions)
    policy_number STRING NOT NULL,
    
    -- Metrics
    coverage_amount DECIMAL(15,2),
    premium_amount DECIMAL(15,2),
    deductible_amount DECIMAL(15,2),
    annual_premium DECIMAL(15,2),
    lifetime_premium DECIMAL(15,2),
    policy_count INT DEFAULT 1,
    
    -- Calculated fields
    policy_age_days INT,
    days_to_renewal INT,
    premium_per_1000_coverage DECIMAL(10,2),
    
    -- Flags
    is_active BOOLEAN,
    is_lapsed BOOLEAN,
    is_renewed BOOLEAN,
    has_claims BOOLEAN,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP,
    batch_id STRING,
    
    -- Constraints
    CONSTRAINT fk_policy FOREIGN KEY (policy_sk) REFERENCES ${catalog_name}_gold.analytics.dim_policy(policy_sk),
    CONSTRAINT fk_customer FOREIGN KEY (customer_sk) REFERENCES ${catalog_name}_gold.analytics.dim_customer(customer_sk),
    CONSTRAINT fk_agent FOREIGN KEY (agent_sk) REFERENCES ${catalog_name}_gold.analytics.dim_agent(agent_sk),
    CONSTRAINT fk_issue_date FOREIGN KEY (issue_date_sk) REFERENCES ${catalog_name}_gold.analytics.dim_date(date_sk),
    CONSTRAINT fk_start_date FOREIGN KEY (start_date_sk) REFERENCES ${catalog_name}_gold.analytics.dim_date(date_sk)
)
USING DELTA
PARTITIONED BY (start_date_sk)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Policy fact table - periodic snapshot of all policies';

-- 2. CLAIM FACT TABLE (Transaction Fact)
CREATE TABLE IF NOT EXISTS ${catalog_name}_gold.analytics.fact_claim (
    claim_sk BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    policy_sk BIGINT NOT NULL,  -- FK to dim_policy
    customer_sk BIGINT NOT NULL,  -- FK to dim_customer
    agent_sk BIGINT,  -- FK to dim_agent
    claim_type_sk BIGINT NOT NULL,  -- FK to dim_claim_type
    claim_date_sk INT NOT NULL,  -- FK to dim_date
    claim_time_sk INT,  -- FK to dim_time
    incident_date_sk INT NOT NULL,  -- FK to dim_date
    filed_date_sk INT,  -- FK to dim_date
    closed_date_sk INT,  -- FK to dim_date
    geography_sk BIGINT,  -- FK to dim_geography
    
    -- Degenerate dimensions
    claim_number STRING NOT NULL UNIQUE,
    claim_status STRING NOT NULL,
    
    -- Metrics
    claim_amount DECIMAL(15,2) NOT NULL,
    approved_amount DECIMAL(15,2),
    paid_amount DECIMAL(15,2),
    deductible_applied DECIMAL(15,2),
    reserve_amount DECIMAL(15,2),
    
    -- Counts
    claim_count INT DEFAULT 1,
    
    -- Calculated fields
    days_to_close INT,
    days_from_incident_to_filed INT,
    approval_rate DECIMAL(5,2),  -- approved_amount / claim_amount
    payment_rate DECIMAL(5,2),  -- paid_amount / approved_amount
    
    -- Flags
    is_approved BOOLEAN DEFAULT FALSE,
    is_paid BOOLEAN DEFAULT FALSE,
    is_closed BOOLEAN DEFAULT FALSE,
    is_denied BOOLEAN DEFAULT FALSE,
    fraud_flag BOOLEAN DEFAULT FALSE,
    subrogation_flag BOOLEAN DEFAULT FALSE,
    
    -- Scores
    fraud_score DECIMAL(5,2),
    complexity_score INT,  -- 1-10
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP,
    batch_id STRING,
    
    -- Constraints
    CONSTRAINT fk_claim_policy FOREIGN KEY (policy_sk) REFERENCES ${catalog_name}_gold.analytics.dim_policy(policy_sk),
    CONSTRAINT fk_claim_customer FOREIGN KEY (customer_sk) REFERENCES ${catalog_name}_gold.analytics.dim_customer(customer_sk),
    CONSTRAINT fk_claim_type FOREIGN KEY (claim_type_sk) REFERENCES ${catalog_name}_gold.analytics.dim_claim_type(claim_type_sk),
    CONSTRAINT fk_claim_date FOREIGN KEY (claim_date_sk) REFERENCES ${catalog_name}_gold.analytics.dim_date(date_sk),
    CONSTRAINT fk_incident_date FOREIGN KEY (incident_date_sk) REFERENCES ${catalog_name}_gold.analytics.dim_date(date_sk)
)
USING DELTA
PARTITIONED BY (claim_date_sk)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)
COMMENT 'Claim fact table - transaction grain';

-- 3. PREMIUM PAYMENT FACT TABLE (Accumulating Snapshot)
CREATE TABLE IF NOT EXISTS ${catalog_name}_gold.analytics.fact_premium_payment (
    payment_sk BIGINT GENERATED ALWAYS AS IDENTITY,  -- Surrogate key
    policy_sk BIGINT NOT NULL,  -- FK to dim_policy
    customer_sk BIGINT NOT NULL,  -- FK to dim_customer
    due_date_sk INT NOT NULL,  -- FK to dim_date
    payment_date_sk INT,  -- FK to dim_date
    geography_sk BIGINT,  -- FK to dim_geography
    
    -- Degenerate dimensions
    payment_id STRING NOT NULL UNIQUE,
    payment_method STRING,  -- Check, Credit Card, ACH, Cash
    payment_status STRING NOT NULL,  -- Paid, Pending, Overdue, Failed
    
    -- Metrics
    premium_due DECIMAL(15,2) NOT NULL,
    premium_paid DECIMAL(15,2),
    discount_amount DECIMAL(15,2) DEFAULT 0,
    late_fee DECIMAL(15,2) DEFAULT 0,
    net_payment DECIMAL(15,2),
    
    -- Counts
    payment_count INT DEFAULT 1,
    
    -- Calculated fields
    days_overdue INT,
    days_to_payment INT,  -- How long it took to pay after due date
    
    -- Flags
    is_paid BOOLEAN DEFAULT FALSE,
    is_overdue BOOLEAN DEFAULT FALSE,
    is_partial_payment BOOLEAN DEFAULT FALSE,
    auto_pay_enrolled BOOLEAN DEFAULT FALSE,
    
    -- Metadata
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_date TIMESTAMP,
    batch_id STRING,
    
    -- Constraints
    CONSTRAINT fk_payment_policy FOREIGN KEY (policy_sk) REFERENCES ${catalog_name}_gold.analytics.dim_policy(policy_sk),
    CONSTRAINT fk_payment_customer FOREIGN KEY (customer_sk) REFERENCES ${catalog_name}_gold.analytics.dim_customer(customer_sk),
    CONSTRAINT fk_due_date FOREIGN KEY (due_date_sk) REFERENCES ${catalog_name}_gold.analytics.dim_date(date_sk)
)
USING DELTA
PARTITIONED BY (due_date_sk)
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true'
)
COMMENT 'Premium payment fact table - accumulating snapshot';

-- =====================================================
-- POPULATE DATE DIMENSION (10 years: 2020-2030)
-- =====================================================

INSERT INTO ${catalog_name}_gold.analytics.dim_date
SELECT
    CAST(DATE_FORMAT(date_value, 'yyyyMMdd') AS INT) AS date_sk,
    date_value,
    YEAR(date_value) AS year,
    QUARTER(date_value) AS quarter,
    CONCAT('Q', QUARTER(date_value)) AS quarter_name,
    MONTH(date_value) AS month,
    DATE_FORMAT(date_value, 'MMMM') AS month_name,
    DATE_FORMAT(date_value, 'MMM') AS month_short_name,
    WEEKOFYEAR(date_value) AS week_of_year,
    DAY(date_value) AS day_of_month,
    DAYOFWEEK(date_value) AS day_of_week,
    DATE_FORMAT(date_value, 'EEEE') AS day_of_week_name,
    DATE_FORMAT(date_value, 'E') AS day_of_week_short,
    CASE WHEN DAYOFWEEK(date_value) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
    FALSE AS is_holiday,  -- Populate holidays separately
    NULL AS holiday_name,
    YEAR(date_value) AS fiscal_year,  -- Adjust if fiscal year differs
    QUARTER(date_value) AS fiscal_quarter,
    MONTH(date_value) AS fiscal_month,
    CASE WHEN date_value = LAST_DAY(date_value) THEN TRUE ELSE FALSE END AS is_month_end,
    CASE WHEN MONTH(date_value) IN (3, 6, 9, 12) AND date_value = LAST_DAY(date_value) THEN TRUE ELSE FALSE END AS is_quarter_end,
    CASE WHEN MONTH(date_value) = 12 AND DAY(date_value) = 31 THEN TRUE ELSE FALSE END AS is_year_end,
    CURRENT_TIMESTAMP() AS created_date
FROM (
    SELECT EXPLODE(SEQUENCE(TO_DATE('2020-01-01'), TO_DATE('2030-12-31'), INTERVAL 1 DAY)) AS date_value
);

-- =====================================================
-- INDEXES AND OPTIMIZATION
-- =====================================================

-- Enable Z-Ordering on fact tables for query performance
ALTER TABLE ${catalog_name}_gold.analytics.fact_policy ZORDER BY (customer_sk, agent_sk, policy_sk);
ALTER TABLE ${catalog_name}_gold.analytics.fact_claim ZORDER BY (customer_sk, policy_sk, claim_type_sk);
ALTER TABLE ${catalog_name}_gold.analytics.fact_premium_payment ZORDER BY (customer_sk, policy_sk);

-- =====================================================
-- GRANT PERMISSIONS
-- =====================================================

GRANT SELECT ON SCHEMA ${catalog_name}_gold.analytics TO `all_users`;
GRANT SELECT ON ${catalog_name}_gold.analytics.dim_customer TO `business_analysts`;
GRANT SELECT ON ${catalog_name}_gold.analytics.fact_policy TO `business_analysts`;
GRANT SELECT ON ${catalog_name}_gold.analytics.fact_claim TO `business_analysts`;

-- =====================================================
-- USAGE NOTES
-- =====================================================

-- Star schema is optimized for OLAP queries and BI tools
-- Fact tables contain metrics and foreign keys to dimensions
-- Dimension tables contain descriptive attributes
-- Date dimension enables powerful time-based analysis
-- Use surrogate keys (SK) for joins, not natural keys
-- Apply RLS/CLS security on top of star schema views

-- Example queries:
-- SELECT d.year, d.quarter, SUM(f.claim_amount)
-- FROM fact_claim f
-- JOIN dim_date d ON f.claim_date_sk = d.date_sk
-- GROUP BY d.year, d.quarter;

