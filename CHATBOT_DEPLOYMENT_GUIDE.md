# 🤖 Insurance Analytics AI Chatbot - Deployment Guide

## 🎯 **What You're Deploying**

A **Streamlit-powered AI chatbot** that:
- ✅ Answers questions in natural language
- ✅ Auto-generates charts and visualizations  
- ✅ Connects directly to your Databricks prediction tables
- ✅ Provides conversational insights
- ✅ Includes Quick Actions for common queries

---

## 📋 **Prerequisites**

Before deploying, ensure:
1. ✅ All 4 ML prediction notebooks have run successfully
2. ✅ All prediction tables exist in `insurance_dev_gold.predictions`
3. ✅ You have access to Databricks workspace
4. ✅ Your cluster is running

---

## 🚀 **Deployment Methods**

### **Method 1: Run in Databricks Notebook (Recommended)**

This is the **easiest** way to run your chatbot on Databricks!

#### **Step 1: Upload Chatbot File**

Upload the chatbot to your Databricks workspace:

```bash
databricks workspace import \
  /Users/kanikamondal/Databricks/insurance-data-ai/src/chatbot/insurance_chatbot.py \
  /Workspace/Shared/insurance-analytics/chatbot/insurance_chatbot \
  -l PYTHON -o
```

#### **Step 2: Create a New Notebook**

1. In Databricks, go to **Workspace** → **Shared** → **insurance-analytics**
2. Create new folder: **chatbot**
3. Create new **Python notebook**: `launch_chatbot`

#### **Step 3: Add This Code to the Notebook**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Insurance Analytics AI Chatbot Launcher

# COMMAND ----------
# Install required packages
%pip install streamlit plotly

# COMMAND ----------
# Import the chatbot app
import sys
sys.path.append('/Workspace/Shared/insurance-analytics/chatbot')

# Import and run
from insurance_chatbot import main

# COMMAND ----------
# Launch the chatbot
main()
```

#### **Step 4: Run the Notebook**

1. Attach to your cluster
2. Click **"Run All"**
3. The chatbot will launch in the notebook output!

**🎉 Done! Your chatbot is now running!**

---

### **Method 2: Databricks Apps (For Premium Workspaces)**

If you have Databricks Standard/Premium, you can deploy as a proper app:

#### **Step 1: Create App Configuration**

Create `app.yaml`:

```yaml
command: ["streamlit", "run", "insurance_chatbot.py", "--server.port=8080"]
resources:
  - name: chatbot-files
    path: /Workspace/Shared/insurance-analytics/chatbot
```

#### **Step 2: Deploy Using Databricks CLI**

```bash
databricks apps create insurance-analytics-chatbot \
  --source-path /Workspace/Shared/insurance-analytics/chatbot
```

#### **Step 3: Access Your App**

The chatbot will be available at:
```
https://<your-databricks-workspace>/apps/insurance-analytics-chatbot
```

---

### **Method 3: Local Testing (Before Deploying)**

Test locally on your machine before deploying:

#### **Step 1: Install Dependencies**

```bash
cd /Users/kanikamondal/Databricks/insurance-data-ai/src/chatbot
pip install -r requirements.txt
```

#### **Step 2: Set Databricks Connection**

Create `.streamlit/secrets.toml`:

```toml
[databricks]
host = "https://your-workspace.cloud.databricks.com"
token = "your-personal-access-token"
```

#### **Step 3: Run Locally**

```bash
streamlit run insurance_chatbot.py
```

The chatbot will open in your browser at `http://localhost:8501`

---

## 🎨 **Using the Chatbot**

### **Quick Actions (Sidebar)**

Click any button to instantly:
- 📊 See executive summary
- 🔴 View high-risk customers
- 🚨 Check fraud alerts
- 📈 Get 30-day forecast
- 💰 See pricing opportunities

### **Natural Language Queries**

Type questions like:

```
"Show me customers at risk"
"What are our fraud alerts?"
"Forecast next week's claims"
"Which policies should we reprice?"
"Show me top 20 high-risk customers"
"Tell me about critical fraud cases"
"Compare churn by customer segment"
"Summarize everything"
```

### **Understanding the Response**

Each response includes:
1. **📝 Text Summary** - AI-generated insights
2. **📊 Visualization** - Auto-generated chart (when relevant)
3. **📋 Data Table** - Detailed data you can scroll through

---

## 💡 **Example Conversations**

### **Example 1: Customer Churn**

**You:** "Show me high-risk customers"

**Bot:** *[Shows metrics + pie chart + table]*
```
📊 Found 1,247 customers at risk of churning.

• High Risk: 1,247 customers
• Premium at Risk: $4,234,567.00
• Avg Churn Probability: 78.5%

The table below shows detailed customer information. 
These customers need immediate retention efforts.
```

---

### **Example 2: Fraud Detection**

**You:** "What are our critical fraud cases?"

**Bot:** *[Shows metrics + bar chart + table]*
```
🚨 Found 89 suspicious claims requiring investigation.

• Critical Cases: 89
• Estimated Fraud Amount: $1,823,456.00
• Avg Fraud Score: 85.2/100

These cases should be reviewed by your SIU team immediately.
```

---

### **Example 3: Forecasting**

**You:** "Forecast claims for next week"

**Bot:** *[Shows metrics + line chart with confidence intervals + table]*
```
📈 Claim forecast for next 7 days:

• Expected Claims: 899
• Expected Amount: $8,950,123.00
• Daily Average: 128 claims/day

Use this forecast for staffing and resource planning.
```

---

### **Example 4: Executive Summary**

**You:** "Show me the executive summary"

**Bot:** *[Shows 4 KPI cards with metrics]*
```
📊 Executive Summary - AI Predictions

Here's your complete analytics overview:

[KPI Cards Display:]
🔴 High Risk Customers: 1,247 ($4.2M at risk)
🚨 Critical Fraud Cases: 89 ($1.8M potential fraud)
📈 30-Day Forecast: 3,845 claims ($38.5M)
💰 Revenue Opportunity: 452 policies (+$680K/year)
```

---

## 🔧 **Customization**

### **Change Catalog Names**

If you're using different catalog names, update line 20 in `insurance_chatbot.py`:

```python
# Change these to match your catalogs
BRONZE_CATALOG = "insurance_dev_bronze"
SILVER_CATALOG = "insurance_dev_silver"
GOLD_CATALOG = "insurance_dev_gold"
```

### **Add More Intents**

To teach the bot new question types, edit the `parse_user_intent()` function:

```python
intents = {
    'churn': ['churn', 'cancel', 'leaving'],
    'fraud': ['fraud', 'suspicious'],
    'your_new_intent': ['keyword1', 'keyword2'],  # Add here
}
```

### **Customize Colors**

Change the color scheme in the CSS section (lines 30-60):

```python
# Primary color
background-color: #FF3621;  # Change to your brand color
```

---

## 🐛 **Troubleshooting**

### **Issue: "Failed to connect to Spark"**

**Solution:** Make sure you're running on a Databricks cluster with Spark available.

```python
# Test Spark connection
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.sql("SELECT 1").show()  # Should work
```

---

### **Issue: "Table not found"**

**Solution:** Verify prediction tables exist:

```sql
SHOW TABLES IN insurance_dev_gold.predictions;
```

Should show:
- `customer_churn_risk`
- `fraud_alerts`
- `claim_forecast`
- `premium_optimization`

---

### **Issue: Chatbot is slow**

**Solution:** The first query is always slower. Subsequent queries are faster. To improve:

1. **Use caching:** Already built-in with `@st.cache_resource`
2. **Limit data:** Use TOP N queries (already implemented)
3. **Optimize tables:** Run `OPTIMIZE` on prediction tables

```sql
OPTIMIZE insurance_dev_gold.predictions.customer_churn_risk;
OPTIMIZE insurance_dev_gold.predictions.fraud_alerts;
OPTIMIZE insurance_dev_gold.predictions.claim_forecast;
OPTIMIZE insurance_dev_gold.predictions.premium_optimization;
```

---

### **Issue: Charts not displaying**

**Solution:** Make sure Plotly is installed:

```python
%pip install plotly
```

---

## 📊 **Features Included**

### ✅ **Current Features (Standard)**

- Natural language query understanding
- Auto-generated SQL based on intent
- Interactive Plotly visualizations
- Conversational chat interface
- Quick Action buttons
- KPI cards
- Data tables with sorting/filtering
- Context-aware responses
- 8 different query types:
  - Customer churn analysis
  - Fraud detection alerts
  - Claim forecasting
  - Premium optimization
  - Executive summary
  - Comparison queries
  - Detail drill-downs
  - Export recommendations

### 🚀 **Future Enhancements (Optional)**

Want to add more features? Here are ideas:

- **Voice Input:** Use speech recognition
- **Export to PDF:** Generate downloadable reports
- **Email Integration:** Send summaries via email
- **Scheduled Reports:** Daily/weekly automated insights
- **Advanced NLP:** Use OpenAI/Claude for smarter responses
- **Multi-language:** Support other languages
- **Mobile App:** Deploy as mobile-friendly PWA

---

## 🎯 **Next Steps**

1. ✅ **Deploy the chatbot** using Method 1 (easiest)
2. ✅ **Test with Quick Actions** to verify it works
3. ✅ **Try natural language queries** to explore features
4. ✅ **Share with your team** for feedback
5. ✅ **Customize** as needed for your use case

---

## 📞 **Need Help?**

If you encounter issues:

1. **Check the logs** in your notebook output
2. **Verify data exists** in prediction tables
3. **Test SQL queries** directly in SQL Editor first
4. **Review error messages** - they usually point to the issue

---

## 🎉 **Congratulations!**

You now have an **AI-powered conversational analytics chatbot** that:
- Understands natural language
- Generates insights automatically
- Creates visualizations on demand
- Makes your data accessible to everyone

**Much cooler than a static dashboard!** 🚀

---

**Ready to deploy? Follow Method 1 above to get started!** 💪

