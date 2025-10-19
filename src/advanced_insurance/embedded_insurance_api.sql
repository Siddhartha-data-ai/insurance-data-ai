-- Insurance 4.0: Embedded Insurance API Platform
-- API-first insurance distribution through partners (e-commerce, ride-sharing, travel)

CREATE SCHEMA IF NOT EXISTS insurance_catalog.embedded_insurance;

-- Partner Registry
CREATE TABLE IF NOT EXISTS insurance_catalog.embedded_insurance.partner_registry (
    partner_id STRING NOT NULL,
    partner_name STRING,
    partner_type STRING, -- ECOMMERCE, RIDESHARE, TRAVEL, RENTAL, EVENT
    api_key_hash STRING,
    commission_rate DECIMAL(5,4),
    is_active BOOLEAN,
    activation_date TIMESTAMP
) USING DELTA;

-- Embedded Policies (micro-coverage)
CREATE TABLE IF NOT EXISTS insurance_catalog.embedded_insurance.embedded_policies (
    policy_id STRING NOT NULL,
    partner_id STRING,
    customer_id STRING,
    coverage_type STRING, -- PURCHASE_PROTECTION, RIDE_INSURANCE, TRIP_CANCEL, etc
    coverage_amount DECIMAL(10,2),
    premium_amount DECIMAL(10,2),
    policy_start TIMESTAMP,
    policy_end TIMESTAMP,
    partner_transaction_id STRING,
    status STRING,
    created_at TIMESTAMP
) USING DELTA
PARTITIONED BY (partner_id);

SELECT '✅ Embedded Insurance API Platform created!' as status;
