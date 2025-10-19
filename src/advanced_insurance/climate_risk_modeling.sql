-- Insurance 4.0: Climate Risk Modeling
-- Environmental risk assessment and climate-aware pricing

CREATE SCHEMA IF NOT EXISTS insurance_catalog.climate_risk;

-- Property Climate Risk Scores
CREATE TABLE IF NOT EXISTS insurance_catalog.climate_risk.property_climate_scores (
    property_id STRING NOT NULL,
    address STRING,
    zip_code STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    
    -- Risk Scores (0-100)
    flood_risk_score INT,
    wildfire_risk_score INT,
    hurricane_risk_score INT,
    earthquake_risk_score INT,
    tornado_risk_score INT,
    
    -- Climate Change Projections
    flood_risk_2030 INT,
    flood_risk_2050 INT,
    wildfire_risk_2030 INT,
    wildfire_risk_2050 INT,
    
    -- Overall Climate Risk
    composite_risk_score INT,
    risk_tier STRING,
    premium_adjustment_pct DECIMAL(5,2),
    
    -- Data Sources
    fema_flood_zone STRING,
    noaa_hurricane_zone STRING,
    usgs_earthquake_zone STRING,
    
    last_updated TIMESTAMP
) USING DELTA
PARTITIONED BY (zip_code);

SELECT '✅ Climate Risk Modeling system created!' as status;
