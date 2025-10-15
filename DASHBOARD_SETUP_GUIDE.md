# 📊 Insurance Analytics Dashboard - Setup Guide

## Overview

This guide helps you create an **AI-Powered Insurance Analytics Dashboard** in Databricks SQL that displays predictions from all 4 ML models:
- 🔴 Customer Churn Risk
- 🚨 Enhanced Fraud Detection
- 📈 Claim Volume Forecasting
- 💰 Premium Optimization

---

## 📋 Prerequisites

Before creating the dashboard, you need to:

1. ✅ **Run all 4 ML prediction notebooks** (see "Running Predictions" section below)
2. ✅ **Verify prediction tables exist** in `insurance_dev_gold.predictions` schema
3. ✅ **Have access to Databricks SQL** (SQL Workspace)

---

## 🚀 Step 1: Run All ML Predictions

### Option A: Run Manually (One by One)

In your Databricks workspace, run these notebooks in order:

```
1. /Workspace/Shared/insurance-analytics/ml/predict_customer_churn
   ⏱️  Takes: 5-10 minutes
   📊 Creates: insurance_dev_gold.predictions.customer_churn_risk

2. /Workspace/Shared/insurance-analytics/ml/predict_fraud_enhanced
   ⏱️  Takes: 3-5 minutes
   📊 Creates: insurance_dev_gold.predictions.fraud_alerts

3. /Workspace/Shared/insurance-analytics/ml/forecast_claims
   ⏱️  Takes: 2-4 minutes
   📊 Creates: insurance_dev_gold.predictions.claim_forecast

4. /Workspace/Shared/insurance-analytics/ml/optimize_premiums
   ⏱️  Takes: 4-8 minutes
   📊 Creates: insurance_dev_gold.predictions.premium_optimization
```

**Total Time: ~15-30 minutes**

### Option B: Use the Master Orchestrator

Run this single notebook that executes all 4 models:
```
/Workspace/Shared/insurance-analytics/ml/run_all_predictions
```

---

## 🏗️ Step 2: Create Databricks SQL Dashboard

### 2.1 Navigate to SQL Workspace

1. In Databricks, click **SQL** in the left sidebar
2. Click **Dashboards** → **Create Dashboard**
3. Name it: **Insurance Analytics - AI Predictions**

---

### 2.2 Create SQL Queries

Create the following queries in Databricks SQL. For each query:
1. Click **+ Create** → **Query**
2. Paste the SQL
3. Click **Run**
4. Click **Save** and give it a name

---

#### Query 1: Executive KPI Summary

**Name:** `exec_kpis`

```sql
-- Executive KPI Summary
SELECT
  -- Churn Metrics
  (SELECT COUNT(*) 
   FROM insurance_dev_gold.predictions.customer_churn_risk 
   WHERE churn_risk_category = 'High Risk') AS high_risk_customers,
  
  (SELECT SUM(total_annual_premium) 
   FROM insurance_dev_gold.predictions.customer_churn_risk 
   WHERE churn_risk_category = 'High Risk') AS premium_at_risk,
  
  -- Fraud Metrics
  (SELECT COUNT(*) 
   FROM insurance_dev_gold.predictions.fraud_alerts 
   WHERE fraud_risk_category IN ('Critical', 'High')) AS critical_fraud_cases,
  
  (SELECT SUM(estimated_fraud_amount) 
   FROM insurance_dev_gold.predictions.fraud_alerts 
   WHERE fraud_risk_category IN ('Critical', 'High')) AS potential_fraud_amount,
  
  -- Forecasting Metrics (Next 30 days)
  (SELECT SUM(predicted_claim_count) 
   FROM insurance_dev_gold.predictions.claim_forecast 
   WHERE claim_type = 'ALL_TYPES' AND days_ahead <= 30) AS forecast_30d_claims,
  
  (SELECT SUM(predicted_total_amount) 
   FROM insurance_dev_gold.predictions.claim_forecast 
   WHERE claim_type = 'ALL_TYPES' AND days_ahead <= 30) AS forecast_30d_amount,
  
  -- Premium Optimization
  (SELECT COUNT(*) 
   FROM insurance_dev_gold.predictions.premium_optimization 
   WHERE implementation_priority = 'High') AS high_priority_pricing,
  
  (SELECT SUM(annual_revenue_impact) 
   FROM insurance_dev_gold.predictions.premium_optimization 
   WHERE implementation_priority = 'High') AS revenue_opportunity
```

---

#### Query 2: Churn Risk Distribution

**Name:** `churn_distribution`

```sql
-- Churn Risk Distribution
SELECT 
  churn_risk_category,
  COUNT(*) AS customer_count,
  ROUND(AVG(churn_probability), 2) AS avg_churn_prob,
  SUM(total_annual_premium) AS premium_at_risk
FROM insurance_dev_gold.predictions.customer_churn_risk
GROUP BY churn_risk_category
ORDER BY 
  CASE churn_risk_category
    WHEN 'High Risk' THEN 1
    WHEN 'Medium Risk' THEN 2
    WHEN 'Low Risk' THEN 3
  END
```

---

#### Query 3: Top Churn Risk Customers

**Name:** `top_churn_customers`

```sql
-- Top 20 Customers at Risk of Churning
SELECT 
  customer_id,
  customer_segment,
  state_code,
  ROUND(churn_probability, 1) AS churn_prob_pct,
  total_annual_premium,
  active_policies,
  total_claims,
  recommended_action
FROM insurance_dev_gold.predictions.customer_churn_risk
WHERE churn_risk_category = 'High Risk'
ORDER BY churn_probability DESC
LIMIT 20
```

---

#### Query 4: Churn by Segment

**Name:** `churn_by_segment`

```sql
-- Churn Risk by Customer Segment
SELECT 
  customer_segment,
  churn_risk_category,
  COUNT(*) AS customers,
  ROUND(AVG(churn_probability), 1) AS avg_churn_prob,
  SUM(total_annual_premium) AS segment_premium
FROM insurance_dev_gold.predictions.customer_churn_risk
GROUP BY customer_segment, churn_risk_category
ORDER BY customer_segment, 
  CASE churn_risk_category
    WHEN 'High Risk' THEN 1
    WHEN 'Medium Risk' THEN 2
    WHEN 'Low Risk' THEN 3
  END
```

---

#### Query 5: Fraud Alert Summary

**Name:** `fraud_summary`

```sql
-- Fraud Alert Summary by Risk Category
SELECT 
  fraud_risk_category,
  investigation_priority,
  COUNT(*) AS claim_count,
  ROUND(AVG(combined_fraud_score), 1) AS avg_fraud_score,
  SUM(estimated_fraud_amount) AS total_potential_fraud,
  SUM(claimed_amount) AS total_claimed
FROM insurance_dev_gold.predictions.fraud_alerts
GROUP BY fraud_risk_category, investigation_priority
ORDER BY investigation_priority
```

---

#### Query 6: Critical Fraud Cases

**Name:** `critical_fraud_cases`

```sql
-- Critical Fraud Cases Requiring Immediate Investigation
SELECT 
  claim_id,
  claim_number,
  customer_id,
  claim_type,
  ROUND(combined_fraud_score, 1) AS fraud_score,
  claimed_amount,
  estimated_fraud_amount,
  recommended_action,
  CASE 
    WHEN excessive_claim = 1 THEN 'Excessive Amount, '
    ELSE ''
  END ||
  CASE 
    WHEN late_reporting = 1 THEN 'Late Reporting, '
    ELSE ''
  END ||
  CASE 
    WHEN location_risk = 1 THEN 'Location Mismatch, '
    ELSE ''
  END ||
  CASE 
    WHEN frequent_claimant = 1 THEN 'Frequent Claimant'
    ELSE ''
  END AS risk_flags
FROM insurance_dev_gold.predictions.fraud_alerts
WHERE fraud_risk_category IN ('Critical', 'High')
ORDER BY combined_fraud_score DESC
LIMIT 25
```

---

#### Query 7: Claim Forecast - Next 30 Days

**Name:** `claim_forecast_30d`

```sql
-- Claim Forecast for Next 30 Days by Type
SELECT 
  claim_type,
  SUM(predicted_claim_count) AS expected_claims_30d,
  SUM(predicted_total_amount) AS expected_amount_30d,
  ROUND(AVG(predicted_claim_count), 1) AS avg_daily_claims
FROM insurance_dev_gold.predictions.claim_forecast
WHERE days_ahead <= 30 AND claim_type != 'ALL_TYPES'
GROUP BY claim_type
ORDER BY expected_claims_30d DESC
```

---

#### Query 8: Daily Claim Forecast (Next 14 Days)

**Name:** `daily_forecast_14d`

```sql
-- Daily Claim Forecast for Next 14 Days
SELECT 
  forecast_date,
  predicted_claim_count AS predicted_claims,
  confidence_lower_95 AS lower_bound,
  confidence_upper_95 AS upper_bound,
  ROUND(predicted_total_amount, 0) AS predicted_amount
FROM insurance_dev_gold.predictions.claim_forecast
WHERE claim_type = 'ALL_TYPES' AND days_ahead <= 14
ORDER BY forecast_date
```

---

#### Query 9: Premium Optimization Summary

**Name:** `premium_opt_summary`

```sql
-- Premium Optimization Summary
SELECT 
  recommendation_category,
  implementation_priority,
  COUNT(*) AS policy_count,
  ROUND(AVG(premium_change_percent), 1) AS avg_change_pct,
  SUM(annual_revenue_impact) AS total_revenue_impact
FROM insurance_dev_gold.predictions.premium_optimization
GROUP BY recommendation_category, implementation_priority
ORDER BY 
  CASE recommendation_category
    WHEN 'Increase' THEN 1
    WHEN 'Maintain' THEN 2
    WHEN 'Decrease' THEN 3
  END,
  CASE implementation_priority
    WHEN 'High' THEN 1
    WHEN 'Medium' THEN 2
    WHEN 'Low' THEN 3
  END
```

---

#### Query 10: High Priority Pricing Opportunities

**Name:** `high_priority_pricing`

```sql
-- High Priority Premium Optimization Opportunities
SELECT 
  policy_id,
  policy_number,
  policy_type,
  state_code,
  annual_premium AS current_premium,
  recommended_premium,
  ROUND(premium_change_percent, 1) AS change_pct,
  annual_revenue_impact,
  ROUND(loss_ratio, 2) AS loss_ratio,
  ROUND(risk_score, 0) AS risk_score,
  recommendation_category,
  rationale
FROM insurance_dev_gold.predictions.premium_optimization
WHERE implementation_priority = 'High'
ORDER BY ABS(annual_revenue_impact) DESC
LIMIT 30
```

---

#### Query 11: Fraud by Claim Type

**Name:** `fraud_by_claim_type`

```sql
-- Fraud Risk by Claim Type
SELECT 
  claim_type,
  fraud_risk_category,
  COUNT(*) AS claim_count,
  ROUND(AVG(combined_fraud_score), 1) AS avg_fraud_score,
  SUM(claimed_amount) AS total_claimed,
  SUM(estimated_fraud_amount) AS estimated_fraud
FROM insurance_dev_gold.predictions.fraud_alerts
GROUP BY claim_type, fraud_risk_category
ORDER BY claim_type, 
  CASE fraud_risk_category
    WHEN 'Critical' THEN 1
    WHEN 'High' THEN 2
    WHEN 'Medium' THEN 3
    WHEN 'Low' THEN 4
  END
```

---

#### Query 12: Revenue Impact by Policy Type

**Name:** `revenue_by_policy_type`

```sql
-- Revenue Impact by Policy Type
SELECT 
  policy_type,
  recommendation_category,
  COUNT(*) AS policies,
  SUM(annual_revenue_impact) AS total_impact,
  ROUND(AVG(premium_change_percent), 1) AS avg_change_pct
FROM insurance_dev_gold.predictions.premium_optimization
GROUP BY policy_type, recommendation_category
ORDER BY policy_type, 
  CASE recommendation_category
    WHEN 'Increase' THEN 1
    WHEN 'Maintain' THEN 2
    WHEN 'Decrease' THEN 3
  END
```

---

## 🎨 Step 3: Build Dashboard Visualizations

### Tab 1: Executive Overview

**Add these visualizations:**

1. **KPI Cards** (Counter visualization)
   - Query: `exec_kpis`
   - Create 8 counter cards:
     - High Risk Customers
     - Premium at Risk
     - Critical Fraud Cases
     - Potential Fraud Amount
     - 30-Day Claim Forecast
     - 30-Day Forecast Amount
     - High Priority Pricing
     - Revenue Opportunity

2. **Churn Risk Distribution** (Pie Chart)
   - Query: `churn_distribution`
   - X-axis: `churn_risk_category`
   - Y-axis: `customer_count`

3. **Fraud Cases by Priority** (Bar Chart)
   - Query: `fraud_summary`
   - X-axis: `fraud_risk_category`
   - Y-axis: `claim_count`

4. **30-Day Claim Forecast** (Bar Chart)
   - Query: `claim_forecast_30d`
   - X-axis: `claim_type`
   - Y-axis: `expected_claims_30d`

---

### Tab 2: Customer Churn

1. **Top Churn Risk Customers** (Table)
   - Query: `top_churn_customers`

2. **Churn by Segment** (Stacked Bar Chart)
   - Query: `churn_by_segment`
   - X-axis: `customer_segment`
   - Y-axis: `customers`
   - Group by: `churn_risk_category`

3. **Premium at Risk by Segment** (Bar Chart)
   - Query: `churn_by_segment`
   - X-axis: `customer_segment`
   - Y-axis: `segment_premium`
   - Filter: `churn_risk_category = 'High Risk'`

---

### Tab 3: Fraud Detection

1. **Critical Fraud Cases** (Table)
   - Query: `critical_fraud_cases`

2. **Fraud by Claim Type** (Stacked Bar Chart)
   - Query: `fraud_by_claim_type`
   - X-axis: `claim_type`
   - Y-axis: `claim_count`
   - Group by: `fraud_risk_category`

3. **Fraud Alert Summary** (Table)
   - Query: `fraud_summary`

---

### Tab 4: Claim Forecasting

1. **14-Day Daily Forecast** (Line Chart)
   - Query: `daily_forecast_14d`
   - X-axis: `forecast_date`
   - Y-axis: `predicted_claims`, `lower_bound`, `upper_bound` (3 lines)

2. **30-Day Forecast by Type** (Bar Chart)
   - Query: `claim_forecast_30d`
   - X-axis: `claim_type`
   - Y-axis: `expected_claims_30d`

---

### Tab 5: Premium Optimization

1. **High Priority Opportunities** (Table)
   - Query: `high_priority_pricing`

2. **Optimization Summary** (Stacked Bar Chart)
   - Query: `premium_opt_summary`
   - X-axis: `recommendation_category`
   - Y-axis: `policy_count`
   - Group by: `implementation_priority`

3. **Revenue Impact by Policy Type** (Bar Chart)
   - Query: `revenue_by_policy_type`
   - X-axis: `policy_type`
   - Y-axis: `total_impact`
   - Group by: `recommendation_category`

---

## 🔄 Step 4: Set Up Auto-Refresh

### For Community Edition (Manual Refresh):
- Dashboard shows latest data from prediction tables
- To update predictions: Re-run ML notebooks
- To refresh dashboard: Click refresh icon in dashboard

### For Standard/Premium Edition:
1. Go to **Data** → **SQL Warehouses**
2. Start/Create a SQL Warehouse
3. In Dashboard settings:
   - Enable **Auto-refresh**
   - Set refresh interval (e.g., every 1 hour)
4. Schedule ML notebooks to run daily:
   - Go to **Workflows** → **Create Job**
   - Add all 4 ML notebooks as tasks
   - Set schedule: Daily at 6:00 AM

---

## 📊 Step 5: Access Your Dashboard

1. Go to **SQL** → **Dashboards**
2. Click on **Insurance Analytics - AI Predictions**
3. Click **Full Screen** for best viewing experience
4. Share with team members using **Share** button

---

## 🎯 Dashboard Usage Tips

### Daily Workflow:
1. **Morning (8 AM):** Run all ML predictions (or check if auto-scheduled)
2. **Review Dashboard:**
   - Check Executive Overview for alerts
   - Review High Risk Churn customers
   - Investigate Critical Fraud cases
   - Verify forecasts align with targets
   - Review pricing opportunities
3. **Take Action:**
   - Export high-risk customer lists
   - Assign fraud cases to investigators
   - Adjust staffing based on forecast
   - Implement pricing changes

### Weekly Review:
- Compare actual vs forecasted claims
- Track churn prediction accuracy
- Review fraud detection hit rate
- Assess revenue impact of pricing changes

---

## 🐛 Troubleshooting

### Dashboard shows "No data"
- **Cause:** ML predictions haven't been run yet
- **Fix:** Run all 4 ML notebooks first

### Query fails with "Table not found"
- **Cause:** Prediction table missing
- **Fix:** Run corresponding ML notebook

### Old data showing
- **Cause:** Predictions not updated
- **Fix:** Re-run ML notebooks

### Dashboard loads slowly
- **Cause:** Large prediction tables
- **Fix:** Add filters (e.g., top 100 rows) or use SQL Warehouse

---

## 📚 Next Steps

1. ✅ **Validate Predictions:** Compare ML predictions with actual outcomes
2. ✅ **Tune Models:** Adjust thresholds based on business needs
3. ✅ **Create Alerts:** Set up email notifications for critical cases
4. ✅ **Export Data:** Integrate with CRM/ERP systems
5. ✅ **Monitor Performance:** Track model accuracy over time

---

## 🎓 Model Information

### Churn Prediction Model
- **Algorithm:** Random Forest Classifier
- **Features:** 24 customer, policy, and claims features
- **Output:** Churn probability (0-100%), risk category, recommended action

### Fraud Detection Model
- **Algorithm:** Random Forest Classifier
- **Features:** 19 fraud indicators (claim patterns, customer history, network analysis)
- **Output:** ML fraud score, combined score, investigation priority

### Claim Forecasting Model
- **Algorithm:** Time Series Decomposition
- **Features:** Historical patterns, seasonality, day-of-week effects
- **Output:** Daily forecasts, 95% confidence intervals

### Premium Optimization Model
- **Algorithm:** Multi-factor optimization
- **Features:** Loss ratio, risk score, market position, CLV, churn risk
- **Output:** Recommended premium, revenue impact, implementation priority

---

## 📞 Support

For questions or issues:
1. Check notebook outputs for error messages
2. Verify data exists in prediction tables
3. Review SQL query syntax
4. Ensure you have proper access permissions

---

**🎉 Congratulations! Your AI-Powered Insurance Analytics Dashboard is ready!**

