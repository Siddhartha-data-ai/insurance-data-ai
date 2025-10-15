# 🚀 ML Predictions & Dashboard - Quick Start Guide

## ⚡ TL;DR - Get Started in 3 Steps

1. **Run Predictions** (15-30 min): Execute `/Workspace/Shared/insurance-analytics/ml/run_all_predictions`
2. **Create Dashboard**: Follow `DASHBOARD_SETUP_GUIDE.md` to build visualizations
3. **Review Results**: Open dashboard and take action on insights

---

## 📁 What Was Created

### 🤖 ML Prediction Notebooks (in `/Workspace/Shared/insurance-analytics/ml/`)

| Notebook | Purpose | Runtime | Output Table |
|----------|---------|---------|--------------|
| `run_all_predictions` | **Master orchestrator** - runs all models | 15-30 min | All tables below |
| `predict_customer_churn` | Identifies customers at risk of canceling | 5-10 min | `predictions.customer_churn_risk` |
| `predict_fraud_enhanced` | Detects suspicious claims | 3-5 min | `predictions.fraud_alerts` |
| `forecast_claims` | Predicts claim volumes | 2-4 min | `predictions.claim_forecast` |
| `optimize_premiums` | Recommends optimal pricing | 4-8 min | `predictions.premium_optimization` |

### 📊 Prediction Tables (in `insurance_dev_gold.predictions`)

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `customer_churn_risk` | Churn probability per customer | `churn_probability`, `churn_risk_category`, `recommended_action` |
| `fraud_alerts` | Fraud scores for claims | `combined_fraud_score`, `fraud_risk_category`, `investigation_priority` |
| `claim_forecast` | Daily claim forecasts | `forecast_date`, `predicted_claim_count`, `confidence_lower_95`, `confidence_upper_95` |
| `premium_optimization` | Pricing recommendations | `recommended_premium`, `premium_change_percent`, `annual_revenue_impact` |

---

## 🎯 How to Use

### First Time Setup

```
Step 1: Run all predictions
   → Go to: /Workspace/Shared/insurance-analytics/ml/run_all_predictions
   → Click: "Run All"
   → Wait: ~15-30 minutes
   → Result: 4 prediction tables created

Step 2: Create dashboard
   → Open: DASHBOARD_SETUP_GUIDE.md (in project root)
   → Follow: Step-by-step instructions
   → Create: 12 SQL queries + visualizations
   → Result: Interactive dashboard ready

Step 3: Review predictions
   → Open: Databricks SQL → Dashboards → Insurance Analytics
   → Review: KPIs and insights
   → Export: High-priority cases for action
```

### Daily Workflow (Community Edition)

```
Morning (30 minutes total):
   1. Run predictions (15-30 min)
      → /Workspace/Shared/insurance-analytics/ml/run_all_predictions
   
   2. Open dashboard (instant)
      → SQL → Dashboards → Insurance Analytics
   
   3. Review insights (5-10 min)
      → Check high-risk customers
      → Review fraud alerts
      → Verify forecasts
      → Identify pricing opportunities
   
   4. Export & action (5 min)
      → Export high-priority cases
      → Send to relevant teams
```

### Weekly Tasks

- Compare forecast vs actual claims
- Review churn prediction accuracy
- Track fraud detection success rate
- Measure revenue impact of pricing changes
- Retrain models if needed

---

## 📊 What Each Model Tells You

### 🔴 Customer Churn Prediction

**Question:** Which customers will cancel in the next 30 days?

**Insights:**
- Churn probability (0-100%) for each customer
- Risk category: High / Medium / Low
- Recommended retention action

**Use Case:**
- Proactively contact high-risk customers
- Offer retention incentives
- Prevent revenue loss

**Example Output:**
```
Customer ID: C123456
Churn Probability: 85%
Risk: High Risk
Action: Immediate Retention Campaign
Premium at Risk: $5,200/year
```

---

### 🚨 Enhanced Fraud Detection

**Question:** Which claims are likely fraudulent?

**Insights:**
- ML fraud score (0-100)
- Combined fraud score (ML + rules)
- Investigation priority (1-4)
- Risk flags (excessive amount, late reporting, etc.)

**Use Case:**
- Prioritize claims for investigation
- Prevent fraudulent payouts
- Optimize investigation resources

**Example Output:**
```
Claim ID: CLM789012
Fraud Score: 92
Risk: Critical
Priority: 1 (Immediate)
Est. Fraud Amount: $45,000
Flags: Excessive Amount, Location Mismatch, Frequent Claimant
```

---

### 📈 Claim Volume Forecasting

**Question:** How many claims should we expect?

**Insights:**
- Daily claim count predictions
- 95% confidence intervals
- Forecasts by claim type
- 30/60/90 day horizons

**Use Case:**
- Staff planning
- Budget allocation
- Capacity management
- Early warning for spikes

**Example Output:**
```
Date: 2025-11-15
Predicted Claims: 127
Lower Bound: 98
Upper Bound: 156
Expected Amount: $1,245,000
```

---

### 💰 Premium Optimization

**Question:** Are we pricing policies optimally?

**Insights:**
- Recommended premium per policy
- Price change amount & percent
- Revenue impact estimate
- Implementation priority

**Use Case:**
- Increase prices on high-risk policies
- Offer discounts to retain valuable customers
- Maximize profitability
- Stay competitive

**Example Output:**
```
Policy ID: POL456789
Current Premium: $2,400
Recommended: $2,760 (+15%)
Revenue Impact: +$342/year
Priority: High
Rationale: Loss ratio: 0.82, Risk: 68
```

---

## 🎨 Dashboard Preview

### Executive Overview Tab
```
┌─────────────────────────────────────────────────────────┐
│  HIGH-RISK CUSTOMERS    │  PREMIUM AT RISK              │
│        1,247            │    $4.2M                      │
├─────────────────────────┼───────────────────────────────┤
│  CRITICAL FRAUD CASES   │  POTENTIAL FRAUD              │
│         89              │    $1.8M                      │
├─────────────────────────┼───────────────────────────────┤
│  30-DAY CLAIM FORECAST  │  FORECAST AMOUNT              │
│        3,845            │    $38.5M                     │
├─────────────────────────┼───────────────────────────────┤
│  HIGH-PRIORITY PRICING  │  REVENUE OPPORTUNITY          │
│        452              │    +$680K/year                │
└─────────────────────────┴───────────────────────────────┘

[Churn Risk Pie Chart]  [Fraud Cases Bar Chart]  [Forecast Line Chart]
```

---

## ⚙️ Customization

### Change Prediction Parameters

Edit widgets in each notebook:

```python
# Churn Prediction
churn_window_days = "30"  # Options: 30, 60, 90

# Claim Forecasting
forecast_days = "90"  # Options: 30, 60, 90

# Catalogs
silver_catalog = "insurance_dev_silver"
gold_catalog = "insurance_dev_gold"
```

### Adjust Risk Thresholds

In notebooks, modify risk category logic:

```python
# Example: Churn risk thresholds
when(col("churn_probability") >= 70, "High Risk")    # Was: 70
.when(col("churn_probability") >= 40, "Medium Risk") # Was: 40
.otherwise("Low Risk")
```

### Filter Dashboard Data

Add filters to SQL queries:

```sql
-- Example: Only show specific states
WHERE state_code IN ('CA', 'NY', 'TX')

-- Example: Only recent predictions
WHERE prediction_date >= CURRENT_DATE - INTERVAL 7 DAYS

-- Example: Minimum threshold
WHERE churn_probability >= 50
```

---

## 🐛 Troubleshooting

### Issue: Notebook fails with "Table not found"

**Cause:** Required source tables missing

**Fix:**
```
1. Run bronze data generation notebooks:
   - generate_customers_data
   - generate_policies_data
   - generate_claims_data

2. Run silver transformation:
   - transform_bronze_to_silver

3. Run gold layer:
   - build_customer_360
   - build_fraud_detection
```

---

### Issue: Model performance is poor

**Cause:** Insufficient or unrealistic training data

**Fix:**
```
1. Generate more training data (increase row counts)
2. Ensure data has realistic patterns
3. Adjust model hyperparameters
4. Add more features
5. Collect real historical data
```

---

### Issue: Predictions seem incorrect

**Cause:** Model needs tuning or data quality issues

**Fix:**
```
1. Review feature engineering logic
2. Check for data quality issues
3. Validate model metrics (AUC, F1)
4. Compare predictions with actual outcomes
5. Retrain with updated data
```

---

### Issue: Dashboard loads slowly

**Cause:** Large prediction tables

**Fix:**
```
1. Add LIMIT clauses to queries
2. Use date filters
3. Create aggregated summary tables
4. Use SQL Warehouse (Standard/Premium)
5. Optimize Delta tables (OPTIMIZE, ZORDER)
```

---

## 📈 Model Performance Metrics

### Churn Prediction
- **Target Metric:** AUC-ROC > 0.75
- **Acceptable:** Precision > 0.65, Recall > 0.60
- **Validation:** Compare predicted vs actual churn monthly

### Fraud Detection
- **Target Metric:** AUC-ROC > 0.80
- **Acceptable:** Precision > 0.70 (minimize false positives)
- **Validation:** Track investigation outcomes

### Claim Forecasting
- **Target Metric:** MAPE < 15% (Mean Absolute Percentage Error)
- **Acceptable:** Actuals within 95% confidence interval
- **Validation:** Compare daily forecast vs actual

### Premium Optimization
- **Target Metric:** Revenue impact > 5% uplift
- **Acceptable:** Retention rate maintained above 90%
- **Validation:** A/B test pricing changes

---

## 🔄 Automation (Standard/Premium Edition Only)

### Schedule Daily Predictions

```
1. Go to: Workflows → Create Job
2. Add task: run_all_predictions notebook
3. Schedule: Daily at 6:00 AM
4. Notifications: Email on failure
```

### Auto-Refresh Dashboard

```
1. Start SQL Warehouse
2. Dashboard Settings → Enable Auto-refresh
3. Set interval: Every 1 hour
```

---

## 📞 Key Questions & Answers

**Q: How often should I run predictions?**
A: Daily for most use cases. Weekly if data changes slowly.

**Q: Can I run only one model?**
A: Yes! Run individual notebooks instead of `run_all_predictions`.

**Q: How accurate are the predictions?**
A: On synthetic data: 70-85% accuracy. On real data: Requires validation and tuning.

**Q: Can I export predictions?**
A: Yes! Use SQL queries or download from dashboard tables.

**Q: What if I have millions of records?**
A: Models scale well. May need to optimize (sampling, parallelization, etc.).

**Q: Can I use different ML algorithms?**
A: Yes! Notebooks use PySpark MLlib - swap RandomForest for GBT, Logistic Regression, etc.

---

## 🎓 Learning Resources

### Understanding the Models

- **Random Forest:** Ensemble of decision trees, good for tabular data
- **Time Series:** Decomposition of trend, seasonality, and residuals
- **Feature Engineering:** Creating predictive variables from raw data
- **Evaluation Metrics:** AUC-ROC, Precision, Recall, F1, MAPE

### Databricks Resources

- [Databricks SQL Dashboards](https://docs.databricks.com/sql/user/dashboards/index.html)
- [MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [Delta Lake Optimization](https://docs.databricks.com/delta/optimize.html)

---

## 🎉 Success Criteria

Your ML prediction system is successful when:

✅ All 4 models run without errors
✅ Prediction tables contain reasonable data
✅ Dashboard displays insights clearly
✅ Business teams take action on predictions
✅ Predictions improve business outcomes
✅ Models are retrained regularly with new data

---

## 🚀 Next Level Enhancements

Once basic system is working, consider:

1. **Advanced ML:** Try XGBoost, Neural Networks, AutoML
2. **Feature Store:** Centralize feature engineering
3. **Model Versioning:** Track model versions with MLflow
4. **A/B Testing:** Validate prediction impact
5. **Real-Time:** Stream processing for instant predictions
6. **Explainability:** SHAP values for model interpretability
7. **Monitoring:** Track model drift and performance decay
8. **Integration:** Export to CRM, ERP, or external systems

---

## 📚 File Locations

```
Project Root: /Users/kanikamondal/Databricks/insurance-data-ai/

Notebooks (local):
  └─ src/ml/
      ├─ predict_customer_churn.py
      ├─ predict_fraud_enhanced.py
      ├─ forecast_claims.py
      ├─ optimize_premiums.py
      └─ run_all_predictions.py

Notebooks (Databricks):
  └─ /Workspace/Shared/insurance-analytics/ml/
      ├─ predict_customer_churn
      ├─ predict_fraud_enhanced
      ├─ forecast_claims
      ├─ optimize_premiums
      └─ run_all_predictions

Documentation:
  ├─ DASHBOARD_SETUP_GUIDE.md (detailed dashboard guide)
  └─ ML_PREDICTIONS_QUICKSTART.md (this file)
```

---

## ✅ Your Next Steps

1. **NOW:** Run `/Workspace/Shared/insurance-analytics/ml/run_all_predictions`
2. **THEN:** Follow `DASHBOARD_SETUP_GUIDE.md` to create visualizations
3. **FINALLY:** Review insights and share with your team!

---

**🎊 You now have a complete AI-powered insurance analytics platform!**

Questions? Issues? Review error messages in notebook outputs or check troubleshooting section above.

