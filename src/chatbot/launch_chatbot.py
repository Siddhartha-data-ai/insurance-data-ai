# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Insurance Analytics AI Chatbot Launcher
# MAGIC
# MAGIC **This notebook launches your AI-powered analytics chatbot!**
# MAGIC
# MAGIC **Features:**
# MAGIC - Natural language queries
# MAGIC - Auto-generated visualizations
# MAGIC - Quick Actions for common insights
# MAGIC - Conversational interface
# MAGIC
# MAGIC **Just click "Run All" to start!**

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Install Required Packages

# COMMAND ----------
print("📦 Installing required packages...")
# %pip install streamlit plotly --quiet  # Databricks magic command - run in notebook
print("✅ Packages installed successfully!")
print("🔄 Restarting Python kernel to use new packages...")

# COMMAND ----------
# Restart Python to use the newly installed packages
dbutils.library.restartPython()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Launch the Chatbot
# MAGIC
# MAGIC The chatbot will start in the cell below. You can interact with it directly!

# COMMAND ----------
print("🚀 Launching Insurance Analytics AI Chatbot...")
print("=" * 70)
print()

import os

# Import the main chatbot app
import sys

# Get the directory of this notebook
notebook_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
sys.path.insert(0, notebook_path)

# Launch the chatbot
exec(open("insurance_chatbot.py").read())  # nosec B102 - Safe: Databricks notebook launcher with hardcoded path

# COMMAND ----------
# MAGIC %md
# MAGIC ## 💡 How to Use the Chatbot
# MAGIC
# MAGIC ### Quick Actions (Sidebar)
# MAGIC Click any button to instantly get insights:
# MAGIC - 📊 Executive Summary
# MAGIC - 🔴 High Risk Customers
# MAGIC - 🚨 Critical Fraud Cases
# MAGIC - 📈 30-Day Forecast
# MAGIC - 💰 Pricing Opportunities
# MAGIC
# MAGIC ### Natural Language Queries
# MAGIC Type questions like:
# MAGIC - "Show me customers at risk"
# MAGIC - "What are our fraud alerts?"
# MAGIC - "Forecast next week's claims"
# MAGIC - "Which policies should we reprice?"
# MAGIC - "Show me top 20 high-risk customers"
# MAGIC
# MAGIC ### Understanding Responses
# MAGIC Each response includes:
# MAGIC 1. **Text Summary** - AI-generated insights
# MAGIC 2. **Visualization** - Auto-generated chart
# MAGIC 3. **Data Table** - Detailed data

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔄 Troubleshooting
# MAGIC
# MAGIC **If the chatbot doesn't start:**
# MAGIC 1. Make sure all prediction tables exist:
# MAGIC    ```sql
# MAGIC    SHOW TABLES IN insurance_dev_gold.predictions;
# MAGIC    ```
# MAGIC 2. Verify your cluster is running
# MAGIC 3. Re-run all cells above
# MAGIC
# MAGIC **If queries fail:**
# MAGIC - Check that ML prediction notebooks have completed successfully
# MAGIC - Verify table names match your catalogs
# MAGIC
# MAGIC **Need help?** Check the `CHATBOT_DEPLOYMENT_GUIDE.md` in your project root!
