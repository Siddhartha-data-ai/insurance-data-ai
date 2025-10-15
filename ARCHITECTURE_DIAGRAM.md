# 🏗️ AI-Powered Insurance Analytics - System Architecture

## 📊 Complete System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        INSURANCE ANALYTICS PLATFORM                      │
│                     AI-Powered Predictions & Dashboard                  │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 1: DATA GENERATION (Bronze - Raw Data)                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  📝 generate_customers_data.py  →  customers.customer_raw               │
│     • 50,000 synthetic customers                                        │
│     • Demographics, credit scores, segments                             │
│                                                                          │
│  📝 generate_policies_data.py   →  policies.policy_raw                  │
│     • 75,000 policies (1.5 per customer)                                │
│     • Auto, Home, Life, Health, Commercial                              │
│                                                                          │
│  📝 generate_claims_data.py     →  claims.claim_raw                     │
│     • 25,000 claims                                                     │
│     • Multiple claim types with fraud indicators                        │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ Transform
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 2: DATA TRANSFORMATION (Silver - Cleaned & Standardized)        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  🔄 transform_bronze_to_silver.py                                       │
│     │                                                                    │
│     ├──→  customers.customer_dim (SCD Type 2)                          │
│     │     • Cleaned customer records                                    │
│     │     • Historical change tracking                                  │
│     │                                                                    │
│     ├──→  policies.policy_dim                                          │
│     │     • Standardized policy data                                    │
│     │     • Business key: policy_number                                 │
│     │                                                                    │
│     ├──→  claims.claim_fact                                            │
│     │     • Validated claims                                            │
│     │     • Calculated metrics                                          │
│     │                                                                    │
│     └──→  agents.agent_dim (Reference)                                 │
│           • Agent master data                                           │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                      ┌─────────────┴──────────────┐
                      │                            │
                      ▼                            ▼
┌────────────────────────────────┐  ┌────────────────────────────────┐
│  LAYER 3A: ANALYTICS (Gold)   │  │  LAYER 3B: ML PREDICTIONS      │
│  Business-Ready Tables         │  │  AI-Powered Insights           │
├────────────────────────────────┤  ├────────────────────────────────┤
│                                │  │                                │
│  📊 build_customer_360.py      │  │  🤖 predict_customer_churn.py  │
│     └→ customer_360            │  │     └→ customer_churn_risk     │
│        • 360° customer view    │  │        • Churn probability     │
│        • Cross-functional data │  │        • Risk categories       │
│                                │  │        • Retention actions     │
│  📊 build_fraud_detection.py   │  │                                │
│     └→ claims_fraud_detection  │  │  🚨 predict_fraud_enhanced.py  │
│        • Rule-based fraud flags│  │     └→ fraud_alerts            │
│                                │  │        • ML fraud scores       │
│  📊 Other gold tables:         │  │        • Investigation priority│
│     • policy_performance       │  │        • Risk indicators       │
│     • agent_performance        │  │                                │
│     • financial_summary        │  │  📈 forecast_claims.py         │
│     • regulatory_reports       │  │     └→ claim_forecast          │
│                                │  │        • Daily predictions     │
│                                │  │        • Confidence intervals  │
│                                │  │        • By claim type         │
│                                │  │                                │
│                                │  │  💰 optimize_premiums.py       │
│                                │  │     └→ premium_optimization    │
│                                │  │        • Pricing recommendations│
│                                │  │        • Revenue impact        │
│                                │  │        • Implementation priority│
└────────────────────────────────┘  └────────────────────────────────┘
                                                  │
                                                  │ Query
                                                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  LAYER 4: DATABRICKS SQL DASHBOARD                                     │
│  Interactive Visualizations & KPIs                                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  📊 Tab 1: Executive Overview                                           │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  [High Risk]  [Premium@Risk]  [Fraud Cases]  [Potential Fraud] │    │
│  │    1,247         $4.2M            89            $1.8M          │    │
│  │                                                                 │    │
│  │  [30d Claims]  [30d Amount]  [Hi-Pri Pricing]  [Revenue Opp]  │    │
│  │     3,845        $38.5M           452             +$680K       │    │
│  │                                                                 │    │
│  │  [Churn Distribution Pie]  [Fraud Cases Bar]  [Forecast Line] │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  📊 Tab 2: Customer Churn Analysis                                      │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  [Top 20 At-Risk Customers Table]                              │    │
│  │  Customer | Segment | Churn% | Premium | Action                │    │
│  │                                                                 │    │
│  │  [Churn by Segment Stacked Bar]  [Premium@Risk by Segment]    │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  🚨 Tab 3: Fraud Detection                                              │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  [Critical Fraud Cases Table]                                   │    │
│  │  Claim | Type | Score | Amount | Risk Flags                    │    │
│  │                                                                 │    │
│  │  [Fraud by Type Stacked]  [Fraud Alert Summary]                │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  📈 Tab 4: Claim Forecasting                                            │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  [14-Day Daily Forecast Line Chart with Confidence Bands]      │    │
│  │                                                                 │    │
│  │  [30-Day Forecast by Claim Type Bar Chart]                     │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  💰 Tab 5: Premium Optimization                                         │
│  ┌────────────────────────────────────────────────────────────────┐    │
│  │  [High Priority Opportunities Table]                            │    │
│  │  Policy | Current | Recommended | Change | Impact               │    │
│  │                                                                 │    │
│  │  [Optimization Summary]  [Revenue Impact by Policy Type]       │    │
│  └────────────────────────────────────────────────────────────────┘    │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🔄 Data Flow Details

### End-to-End Pipeline

```
RAW DATA → CLEANED DATA → ANALYTICS → PREDICTIONS → DASHBOARD → ACTIONS

 Bronze         Silver         Gold      Predictions    SQL         Business
  (Raw)      (Standardized)  (Business)   (ML/AI)    Queries      Decisions
```

### Execution Flow

```
1️⃣  Data Generation (One-time or periodic)
    ↓
    generate_customers_data
    generate_policies_data
    generate_claims_data
    ↓
    ⏱️  Time: 5-10 minutes
    📊 Output: ~150,000 raw records

2️⃣  Data Transformation (Daily/Weekly)
    ↓
    transform_bronze_to_silver
    ↓
    ⏱️  Time: 5-10 minutes
    📊 Output: Cleaned dimension & fact tables

3️⃣  Analytics Layer (Daily/Weekly)
    ↓
    build_customer_360
    build_fraud_detection
    ↓
    ⏱️  Time: 5-10 minutes
    📊 Output: Business-ready gold tables

4️⃣  ML Predictions (Daily or on-demand)
    ↓
    run_all_predictions (master)
      ├─ predict_customer_churn
      ├─ predict_fraud_enhanced
      ├─ forecast_claims
      └─ optimize_premiums
    ↓
    ⏱️  Time: 15-30 minutes
    📊 Output: 4 prediction tables

5️⃣  Dashboard (Real-time query)
    ↓
    12 SQL queries
    20+ visualizations
    5 tabs
    ↓
    ⏱️  Load time: < 5 seconds
    📊 Output: Interactive insights
```

---

## 🧠 ML Model Architecture

### Model 1: Churn Prediction

```
┌─────────────────────────────────────────────────┐
│  INPUT FEATURES (24)                            │
├─────────────────────────────────────────────────┤
│  • Customer: Age, Tenure, Credit, Segment       │
│  • Policies: Count, Premiums, Age, Diversity    │
│  • Claims: Count, Amounts, Fraud Score          │
│  • Risk: Loss ratios, Engagement metrics        │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  PREPROCESSING                                  │
├─────────────────────────────────────────────────┤
│  1. Feature Engineering                         │
│  2. Vector Assembly                             │
│  3. Standard Scaling                            │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  RANDOM FOREST CLASSIFIER                       │
├─────────────────────────────────────────────────┤
│  • 100 trees                                    │
│  • Max depth: 10                                │
│  • Binary classification (churn/no churn)       │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  OUTPUT                                         │
├─────────────────────────────────────────────────┤
│  • Churn Probability: 0-100%                    │
│  • Risk Category: High/Medium/Low               │
│  • Recommended Action                           │
└─────────────────────────────────────────────────┘
```

### Model 2: Fraud Detection

```
┌─────────────────────────────────────────────────┐
│  INPUT FEATURES (19)                            │
├─────────────────────────────────────────────────┤
│  • Claim: Amount, Type, Timing, Location        │
│  • Customer: History, Frequency, Profile        │
│  • Network: Adjuster patterns                   │
│  • Rules: Existing fraud_score                  │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  RANDOM FOREST CLASSIFIER                       │
├─────────────────────────────────────────────────┤
│  • 100 trees                                    │
│  • Max depth: 10                                │
│  • Outputs: ML fraud probability                │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  ENSEMBLE COMBINATION                           │
├─────────────────────────────────────────────────┤
│  Combined = (ML Score + Rule Score) / 2         │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  OUTPUT                                         │
├─────────────────────────────────────────────────┤
│  • ML Fraud Score: 0-100                        │
│  • Combined Score: 0-100                        │
│  • Risk Category: Critical/High/Medium/Low      │
│  • Investigation Priority: 1-4                  │
└─────────────────────────────────────────────────┘
```

### Model 3: Claim Forecasting

```
┌─────────────────────────────────────────────────┐
│  HISTORICAL DATA                                │
├─────────────────────────────────────────────────┤
│  • Daily claim counts by type                   │
│  • Date range: All available history            │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  TIME SERIES DECOMPOSITION                      │
├─────────────────────────────────────────────────┤
│  1. Calculate baseline by day-of-week           │
│  2. Calculate seasonal patterns by month        │
│  3. Identify trend (30-day rolling)             │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  FORECAST GENERATION                            │
├─────────────────────────────────────────────────┤
│  For each future date (1-90 days):              │
│  Prediction = Baseline × DoW Factor ×           │
│               Month Factor + Trend              │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  CONFIDENCE INTERVALS                           │
├─────────────────────────────────────────────────┤
│  95% CI = Prediction ± 1.96 × Std Dev           │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  OUTPUT                                         │
├─────────────────────────────────────────────────┤
│  • Daily predictions (1-90 days)                │
│  • By claim type + aggregate                    │
│  • Confidence intervals                         │
└─────────────────────────────────────────────────┘
```

### Model 4: Premium Optimization

```
┌─────────────────────────────────────────────────┐
│  INPUT DATA                                     │
├─────────────────────────────────────────────────┤
│  • Policy: Current premium, type, state         │
│  • Risk: Loss ratio, risk score                 │
│  • Market: Benchmarks by segment                │
│  • Customer: CLV, churn risk                    │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  MULTI-FACTOR OPTIMIZATION                      │
├─────────────────────────────────────────────────┤
│  1. Loss Ratio Adjustment (-5% to +15%)         │
│  2. Risk Adjustment (-2% to +10%)               │
│  3. Market Adjustment (-8% to +5%)              │
│  4. Retention Adjustment (-7% to 0%)            │
│                                                 │
│  Combined = Π(all adjustments)                  │
│  Capped at ±25%                                 │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│  OUTPUT                                         │
├─────────────────────────────────────────────────┤
│  • Recommended Premium                          │
│  • Change Amount & Percent                      │
│  • Revenue Impact                               │
│  • Implementation Priority                      │
│  • Rationale                                    │
└─────────────────────────────────────────────────┘
```

---

## 📁 File Structure

```
insurance-data-ai/
│
├── src/
│   ├── setup/
│   │   ├── 00_create_catalog.sql
│   │   ├── 01_create_bronze_tables.sql
│   │   ├── 02_create_silver_tables.sql
│   │   ├── 03_create_security_rls_cls.sql
│   │   └── 04_create_gold_tables.sql
│   │
│   ├── bronze/
│   │   ├── generate_customers_data.py
│   │   ├── generate_policies_data.py
│   │   └── generate_claims_data.py
│   │
│   ├── transformations/
│   │   └── transform_bronze_to_silver.py
│   │
│   ├── gold/
│   │   ├── build_customer_360.py
│   │   └── build_fraud_detection.py
│   │
│   ├── ml/  ⭐ NEW
│   │   ├── predict_customer_churn.py
│   │   ├── predict_fraud_enhanced.py
│   │   ├── forecast_claims.py
│   │   ├── optimize_premiums.py
│   │   └── run_all_predictions.py  (Master)
│   │
│   └── analytics/
│       ├── data_quality_validation.py
│       └── pipeline_completion_report.py
│
├── databricks.yml
├── README.md
├── PROJECT_SUMMARY.md
│
└── 📚 NEW DOCUMENTATION
    ├── DASHBOARD_SETUP_GUIDE.md
    ├── ML_PREDICTIONS_QUICKSTART.md
    ├── AI_DASHBOARD_DELIVERY_SUMMARY.md
    └── ARCHITECTURE_DIAGRAM.md (this file)
```

---

## 🔐 Security Architecture

```
┌─────────────────────────────────────────────────┐
│  UNITY CATALOG                                  │
├─────────────────────────────────────────────────┤
│  insurance_dev_bronze                           │
│  insurance_dev_silver                           │
│  insurance_dev_gold                             │
│    ├── customer_analytics                       │
│    ├── claims_analytics                         │
│    ├── policy_analytics                         │
│    └── predictions  ⭐                          │
│         ├── customer_churn_risk                 │
│         ├── fraud_alerts                        │
│         ├── claim_forecast                      │
│         └── premium_optimization                │
└─────────────────────────────────────────────────┘

Note: Community Edition limitations apply
• No account-level groups
• Individual user permissions only
```

---

## ⚡ Performance Optimization

### Table Optimization

```
ALL PREDICTION TABLES use:
  ✅ Delta Lake format
  ✅ Parquet compression
  ✅ Liquid clustering (on key columns)
  ✅ OPTIMIZE on write
```

### Query Optimization

```
DASHBOARD SQL QUERIES use:
  ✅ Predicate pushdown
  ✅ Column pruning
  ✅ Aggregations at source
  ✅ Minimal data transfer
```

---

## 🎯 Success Metrics

### Technical Metrics

```
✅ Model Training: < 10 min per model
✅ Prediction Generation: < 30 min total
✅ Dashboard Load Time: < 5 seconds
✅ Table Sizes: < 5 GB total
✅ Query Performance: < 2 seconds per query
```

### Business Metrics

```
✅ Churn Prediction Accuracy: > 75%
✅ Fraud Detection Precision: > 70%
✅ Forecast Accuracy (MAPE): < 15%
✅ Premium Optimization ROI: > 5%
```

---

## 🚀 Deployment Options

### Community Edition (Current)
- ✅ Manual execution
- ✅ On-demand predictions
- ✅ Dashboard refresh on page load
- ⚠️  No scheduled jobs
- ⚠️  No auto-refresh

### Standard/Premium Edition (Upgrade)
- ✅ Scheduled job execution
- ✅ Automatic daily predictions
- ✅ Dashboard auto-refresh
- ✅ Email/Slack alerts
- ✅ Workflow orchestration

---

## 📊 Data Lineage

```
customer_raw ─┐
              ├──→ customer_dim ─┐
              │                  ├──→ customer_360 ─┐
policy_raw ───┼──→ policy_dim ───┤                  ├──→ customer_churn_risk
              │                  │                  │
claim_raw ────┼──→ claim_fact ───┼──→ fraud_det ────┼──→ fraud_alerts
              │                  │                  │
              └──→ agent_dim ────┘                  ├──→ claim_forecast
                                                    │
                                                    └──→ premium_optimization
```

---

## 🎓 Technology Stack

```
┌─────────────────────────────────────────────────┐
│  COMPUTE                                        │
│  • Apache Spark 3.4+                            │
│  • Databricks Runtime 13.0+                     │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│  STORAGE                                        │
│  • Delta Lake                                   │
│  • Unity Catalog                                │
│  • Parquet + Snappy Compression                 │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│  ML/AI                                          │
│  • PySpark MLlib (Random Forest)                │
│  • Pandas (Time Series)                         │
│  • NumPy (Numerical Computing)                  │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│  VISUALIZATION                                  │
│  • Databricks SQL                               │
│  • Native Charts & Dashboards                   │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│  LANGUAGES                                      │
│  • Python (PySpark)                             │
│  • SQL (Databricks SQL)                         │
└─────────────────────────────────────────────────┘
```

---

## 🌟 Key Differentiators

### vs Traditional BI

```
Traditional BI          →  Our Solution
────────────────────────────────────────────────
Historical only         →  Predictive insights
Manual analysis         →  AI-powered recommendations
Static reports          →  Interactive dashboards
Reactive                →  Proactive
Backward-looking        →  Forward-looking
```

### vs External Tools

```
External ML Platform    →  Our Solution
────────────────────────────────────────────────
Separate system         →  Integrated in Databricks
Export/Import required  →  Native data access
Additional cost         →  Community Edition compatible
Complex integration     →  Seamless workflow
Data security risks     →  Data stays in platform
```

---

**🎉 Complete, production-ready AI analytics platform!**

