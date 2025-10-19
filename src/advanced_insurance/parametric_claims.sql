-- Insurance 4.0: Parametric Claims - Instant Settlements
-- Trigger-based automatic payouts (weather, flight delays, IoT events)

CREATE SCHEMA IF NOT EXISTS insurance_catalog.parametric;

-- Parametric Policy Definitions
CREATE TABLE IF NOT EXISTS insurance_catalog.parametric.parametric_policies (
    policy_id STRING NOT NULL,
    customer_id STRING,
    trigger_type STRING, -- HURRICANE, FLOOD, FLIGHT_DELAY, EARTHQUAKE, CROP_RAIN
    trigger_definition STRING, -- JSON with trigger rules
    payout_amount DECIMAL(10,2),
    coverage_limit DECIMAL(10,2),
    premium DECIMAL(10,2),
    policy_start DATE,
    policy_end DATE
) USING DELTA;

-- Automated Payouts
CREATE TABLE IF NOT EXISTS insurance_catalog.parametric.parametric_payouts (
    payout_id STRING NOT NULL,
    policy_id STRING,
    trigger_event_id STRING,
    trigger_timestamp TIMESTAMP,
    payout_amount DECIMAL(10,2),
    auto_approved BOOLEAN,
    payout_status STRING,
    settlement_timestamp TIMESTAMP
) USING DELTA;

SELECT '✅ Parametric Claims system created - <24hr settlements!' as status;
