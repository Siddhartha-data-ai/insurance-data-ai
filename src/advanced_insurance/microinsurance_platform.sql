-- Insurance 4.0: Microinsurance Platform
-- On-demand, bite-sized insurance for gig economy and underserved markets

CREATE SCHEMA IF NOT EXISTS insurance_catalog.microinsurance;

-- Micro Policy Catalog
CREATE TABLE IF NOT EXISTS insurance_catalog.microinsurance.micro_policy_catalog (
    product_id STRING NOT NULL,
    product_name STRING,
    product_type STRING, -- DEVICE, TRIP, EVENT, GIG_SHIFT, PET, RENTAL
    coverage_amount DECIMAL(10,2),
    price_per_unit DECIMAL(6,2), -- per day, per trip, per shift
    billing_unit STRING, -- DAILY, PER_TRIP, PER_SHIFT, PER_EVENT
    min_term_days INT,
    max_term_days INT,
    instant_activation BOOLEAN
) USING DELTA;

-- Micro Policies (Active)
CREATE TABLE IF NOT EXISTS insurance_catalog.microinsurance.micro_policies (
    micro_policy_id STRING NOT NULL,
    customer_id STRING,
    product_id STRING,
    activation_timestamp TIMESTAMP,
    expiration_timestamp TIMESTAMP,
    total_cost DECIMAL(10,2),
    payment_method STRING, -- MOBILE_MONEY, CRYPTO, CARD, AUTO_DEDUCT
    status STRING,
    cancellation_timestamp TIMESTAMP
) USING DELTA
PARTITIONED BY (product_id);

-- Pre-populate catalog
INSERT INTO insurance_catalog.microinsurance.micro_policy_catalog VALUES
('PHONE_PROTECT', 'Phone Protection', 'DEVICE', 500.00, 1.00, 'DAILY', 1, 30, true),
('TRIP_INS', 'Trip Insurance', 'TRIP', 1000.00, 5.00, 'PER_TRIP', 1, 1, true),
('PET_EMERG', 'Pet Emergency', 'PET', 500.00, 2.00, 'DAILY', 1, 30, true),
('GIG_LIABILITY', 'Gig Worker Liability', 'GIG_SHIFT', 10000.00, 3.00, 'PER_SHIFT', 1, 1, true),
('EVENT_CANCEL', 'Event Cancellation', 'EVENT', 500.00, 10.00, 'PER_EVENT', 1, 1, true),
('RENTAL_PROTECT', 'Renters Protection', 'RENTAL', 2500.00, 1.00, 'DAILY', 1, 365, true);

SELECT '✅ Microinsurance Platform created - On-demand insurance!' as status;
