# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Insurance Analytics AI Chatbot
# MAGIC
# MAGIC **Databricks-Native Version - Multi-Environment Support**
# MAGIC
# MAGIC Ask questions in natural language and get instant insights with interactive visualizations!
# MAGIC
# MAGIC **Features:**
# MAGIC - 🌍 **Multi-environment support** (dev, staging, prod)
# MAGIC - Natural language understanding
# MAGIC - Auto-generated Plotly charts
# MAGIC - Interactive data tables
# MAGIC - Quick Action buttons
# MAGIC - Question suggestions

# COMMAND ----------
# MAGIC %md
# MAGIC ## ⚙️ Environment Configuration

# COMMAND ----------
# Create catalog selection widget
dbutils.widgets.dropdown(
    "gold_catalog",
    "insurance_dev_gold",
    ["insurance_dev_gold", "insurance_staging_gold", "insurance_prod_gold"],
    "📊 Environment Catalog",
)

# Get catalog value
gold_catalog = dbutils.widgets.get("gold_catalog")
silver_catalog = gold_catalog.replace("_gold", "_silver")  # Auto-derive silver catalog

# Determine environment from catalog name
if "dev" in gold_catalog:
    environment = "dev"
elif "staging" in gold_catalog:
    environment = "staging"
elif "prod" in gold_catalog:
    environment = "prod"
else:
    environment = "unknown"

print(f"✅ Environment: {environment.upper()}")
print(f"📊 Gold Catalog: {gold_catalog}")
print(f"📁 Silver Catalog: {silver_catalog} (auto-derived)")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🎯 Quick Actions - Click to Get Instant Insights

# COMMAND ----------
# Create Quick Action buttons using widgets
dbutils.widgets.dropdown(
    "quick_action",
    "None",
    [
        "None",
        "📊 Executive Summary",
        "🔴 High Risk Customers",
        "🚨 Critical Fraud Cases",
        "📈 30-Day Forecast",
        "💰 Pricing Opportunities",
    ],
    "Quick Actions",
)

dbutils.widgets.text("question", "", "💬 Or Ask Your Question Here:")

print("✅ Chatbot interface ready!")
print("👆 Use Quick Actions dropdown or type your question above")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 💡 Suggested Questions
# MAGIC
# MAGIC **Try asking:**
# MAGIC - "Show me the executive summary"
# MAGIC - "Who are my high-risk customers?"
# MAGIC - "Show me top 10 customers at risk"
# MAGIC - "What fraud cases need investigation?"
# MAGIC - "Forecast claims for next week"
# MAGIC - "Which policies should we reprice?"
# MAGIC - "Compare churn risk by state"
# MAGIC - "Show me fraud alerts"
# MAGIC
# MAGIC **Advanced queries:**
# MAGIC - "Show me top 20 high-risk customers in California"
# MAGIC - "Forecast claims for next 30 days"
# MAGIC - "Compare churn by customer segment"

# COMMAND ----------
# Import required libraries
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from IPython.display import display, HTML
import re

print("📦 Libraries loaded successfully!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🧠 Natural Language Processing Engine


# COMMAND ----------
def parse_user_intent(user_input):
    """
    Parse user input to understand intent and extract parameters
    Returns: (intent, params)
    """
    if not user_input or user_input == "None":
        return None, {}

    user_input_lower = user_input.lower()

    # Intent patterns
    intents = {
        "summary": ["summary", "overview", "dashboard", "executive", "kpi", "all metrics"],
        "churn": ["churn", "cancel", "leaving", "retention", "at risk", "high risk", "customers"],
        "fraud": ["fraud", "suspicious", "investigation", "siu", "alert", "fraudulent"],
        "forecast": ["forecast", "predict", "future", "expect", "upcoming", "next"],
        "pricing": ["price", "premium", "pricing", "optimize", "revenue", "reprice"],
    }

    detected_intent = None
    for intent, keywords in intents.items():
        if any(keyword in user_input_lower for keyword in keywords):
            detected_intent = intent
            break

    # Extract parameters
    params = {"limit": 10, "state": None, "segment": None, "risk_level": None, "days": 30}

    # Extract limit
    limit_match = re.search(r"top (\d+)|first (\d+)|show (\d+)", user_input_lower)
    if limit_match:
        params["limit"] = int(next(g for g in limit_match.groups() if g is not None))

    # Extract days
    if "week" in user_input_lower:
        params["days"] = 7
    elif "month" in user_input_lower:
        params["days"] = 30
    elif "quarter" in user_input_lower:
        params["days"] = 90
    else:
        days_match = re.search(r"(\d+)\s*days?", user_input_lower)
        if days_match:
            params["days"] = int(days_match.group(1))

    # Extract state
    states = ["california", "ca", "new york", "ny", "texas", "tx", "florida", "fl"]
    for state in states:
        if state in user_input_lower:
            params["state"] = state.upper()[:2]
            break

    # Extract risk level
    if "high risk" in user_input_lower or "critical" in user_input_lower:
        params["risk_level"] = "high"
    elif "medium risk" in user_input_lower:
        params["risk_level"] = "medium"

    return detected_intent, params


print("✅ NLP engine loaded!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 Visualization Engine


# COMMAND ----------
def create_kpi_cards(row):
    """Create HTML KPI cards"""
    # Handle None/NULL values with default of 0
    high_risk_customers = int(row["high_risk_customers"] or 0)
    premium_at_risk = float(row["premium_at_risk"] or 0)
    critical_fraud_cases = int(row["critical_fraud_cases"] or 0)
    potential_fraud_amount = float(row["potential_fraud_amount"] or 0)
    forecast_30d_claims = int(row["forecast_30d_claims"] or 0)
    forecast_30d_amount = float(row["forecast_30d_amount"] or 0)
    high_priority_pricing = int(row["high_priority_pricing"] or 0)
    revenue_opportunity = float(row["revenue_opportunity"] or 0)

    html = f"""
    <style>
        .kpi-container {{
            display: flex;
            flex-wrap: wrap;
            gap: 20px;
            margin: 20px 0;
        }}
        .kpi-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            flex: 1;
            min-width: 200px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .kpi-card h3 {{
            margin: 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .kpi-card .value {{
            font-size: 32px;
            font-weight: bold;
            margin: 10px 0;
        }}
        .kpi-card .subtitle {{
            font-size: 12px;
            opacity: 0.8;
        }}
    </style>
    <div class="kpi-container">
        <div class="kpi-card" style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);">
            <h3>🔴 High Risk Customers</h3>
            <div class="value">{high_risk_customers:,}</div>
            <div class="subtitle">${premium_at_risk:,.0f} at risk</div>
        </div>
        <div class="kpi-card" style="background: linear-gradient(135deg, #fa709a 0%, #fee140 100%);">
            <h3>🚨 Critical Fraud Cases</h3>
            <div class="value">{critical_fraud_cases:,}</div>
            <div class="subtitle">${potential_fraud_amount:,.0f} potential</div>
        </div>
        <div class="kpi-card" style="background: linear-gradient(135deg, #a8edea 0%, #fed6e3 100%);">
            <h3>📈 30-Day Forecast</h3>
            <div class="value">{forecast_30d_claims:,}</div>
            <div class="subtitle">${forecast_30d_amount:,.0f} expected</div>
        </div>
        <div class="kpi-card" style="background: linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%);">
            <h3>💰 Revenue Opportunity</h3>
            <div class="value">{high_priority_pricing:,}</div>
            <div class="subtitle">+${revenue_opportunity:,.0f}/year</div>
        </div>
    </div>
    """
    return html


def create_response_html(title, message, insights=None):
    """Create formatted response HTML"""
    insights_html = ""
    if insights:
        insights_html = "<ul>" + "".join([f"<li>{insight}</li>" for insight in insights]) + "</ul>"

    html = f"""
    <div style="background: #f8f9fa; padding: 20px; border-radius: 10px; border-left: 4px solid #667eea; margin: 10px 0;">
        <h3 style="margin-top: 0; color: #667eea;">🤖 {title}</h3>
        <p style="font-size: 16px; line-height: 1.6;">{message}</p>
        {insights_html}
    </div>
    """
    return html


def create_suggestions_html(suggestions):
    """Create suggested questions HTML"""
    buttons_html = "".join(
        [
            f'<li style="margin: 10px 0; padding: 10px; background: #e8f4f8; border-radius: 5px; cursor: pointer;">💡 {s}</li>'
            for s in suggestions
        ]
    )

    html = f"""
    <div style="margin: 20px 0; padding: 15px; background: white; border-radius: 10px; border: 1px solid #ddd;">
        <h4 style="margin-top: 0; color: #667eea;">💡 Related Questions:</h4>
        <ul style="list-style: none; padding: 0;">
            {buttons_html}
        </ul>
    </div>
    """
    return html


print("✅ Visualization engine loaded!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔍 Query Generator


# COMMAND ----------
def generate_sql_query(intent, params, gold_catalog, silver_catalog):
    """Generate SQL based on intent and parameters - Environment aware"""

    if intent == "summary":
        return f"""
        SELECT
            (SELECT COUNT(*) FROM {gold_catalog}.predictions.customer_churn_risk 
             WHERE churn_risk_category = 'High Risk') AS high_risk_customers,
            (SELECT SUM(total_annual_premium) FROM {gold_catalog}.predictions.customer_churn_risk 
             WHERE churn_risk_category = 'High Risk') AS premium_at_risk,
            (SELECT COUNT(*) FROM {gold_catalog}.predictions.fraud_alerts 
             WHERE fraud_risk_category IN ('Critical', 'High')) AS critical_fraud_cases,
            (SELECT SUM(estimated_fraud_amount) FROM {gold_catalog}.predictions.fraud_alerts 
             WHERE fraud_risk_category IN ('Critical', 'High')) AS potential_fraud_amount,
            (SELECT SUM(predicted_claim_count) FROM {gold_catalog}.predictions.claim_forecast 
             WHERE claim_type = 'ALL_TYPES' AND days_ahead <= 30) AS forecast_30d_claims,
            (SELECT SUM(predicted_total_amount) FROM {gold_catalog}.predictions.claim_forecast 
             WHERE claim_type = 'ALL_TYPES' AND days_ahead <= 30) AS forecast_30d_amount,
            (SELECT COUNT(*) FROM {gold_catalog}.predictions.premium_optimization 
             WHERE implementation_priority = 'High') AS high_priority_pricing,
            (SELECT SUM(annual_revenue_impact) FROM {gold_catalog}.predictions.premium_optimization 
             WHERE implementation_priority = 'High') AS revenue_opportunity
        """

    elif intent == "churn":
        limit = params.get("limit", 10)
        query = f"""
        SELECT 
            customer_id,
            customer_segment,
            state_code,
            ROUND(churn_probability, 1) AS churn_prob,
            total_annual_premium,
            active_policies,
            total_claims,
            churn_risk_category,
            recommended_action
        FROM {gold_catalog}.predictions.customer_churn_risk
        """

        if params.get("risk_level") == "high":
            query += " WHERE churn_risk_category = 'High Risk'"
        if params.get("state"):
            query += f" {'WHERE' if 'WHERE' not in query else 'AND'} state_code = '{params['state']}'"

        query += f" ORDER BY churn_probability DESC LIMIT {limit}"
        return query

    elif intent == "fraud":
        limit = params.get("limit", 10)
        query = f"""
        SELECT 
            claim_id,
            claim_number,
            claim_type,
            ROUND(combined_fraud_score, 1) AS fraud_score,
            claimed_amount,
            estimated_fraud_amount,
            fraud_risk_category,
            recommended_action
        FROM {gold_catalog}.predictions.fraud_alerts
        WHERE fraud_risk_category IN ('Critical', 'High')
        ORDER BY combined_fraud_score DESC
        LIMIT {limit}
        """
        return query

    elif intent == "forecast":
        days = params.get("days", 30)
        query = f"""
        SELECT 
            forecast_date,
            claim_type,
            ROUND(predicted_claim_count, 0) AS predicted_claims,
            ROUND(predicted_total_amount, 0) AS predicted_amount,
            ROUND(confidence_lower_95, 0) AS lower_bound,
            ROUND(confidence_upper_95, 0) AS upper_bound
        FROM {gold_catalog}.predictions.claim_forecast
        WHERE days_ahead <= {days}
        ORDER BY forecast_date, claim_type
        """
        return query

    elif intent == "pricing":
        limit = params.get("limit", 10)
        query = f"""
        SELECT 
            policy_id,
            policy_type,
            state_code,
            annual_premium AS current_premium,
            recommended_premium,
            ROUND(premium_change_percent, 1) AS change_pct,
            annual_revenue_impact,
            recommendation_category,
            rationale
        FROM {gold_catalog}.predictions.premium_optimization
        WHERE implementation_priority = 'High'
        ORDER BY ABS(annual_revenue_impact) DESC
        LIMIT {limit}
        """
        return query

    return None


print("✅ Query generator loaded!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🎨 Chart Generator


# COMMAND ----------
def create_charts(intent, df):
    """Create appropriate charts based on intent"""

    if df is None or df.empty:
        return None

    if intent == "churn" and "churn_risk_category" in df.columns:
        # Pie chart for risk distribution
        risk_counts = df["churn_risk_category"].value_counts()
        fig = px.pie(
            values=risk_counts.values,
            names=risk_counts.index,
            title="Churn Risk Distribution",
            color_discrete_sequence=["#f5576c", "#ffa600", "#90EE90"],
        )
        fig.update_layout(height=400)
        return fig

    elif intent == "fraud" and "claim_type" in df.columns:
        # Bar chart for fraud by type
        fig = px.bar(
            df,
            x="claim_type",
            y="fraud_score",
            color="fraud_risk_category",
            title="Fraud Scores by Claim Type",
            color_discrete_map={"Critical": "#FF0000", "High": "#FF6600"},
        )
        fig.update_layout(height=400)
        return fig

    elif intent == "forecast":
        # Line chart with confidence intervals
        df_all = df[df["claim_type"] == "ALL_TYPES"] if "claim_type" in df.columns else df

        if not df_all.empty and "forecast_date" in df_all.columns:
            fig = go.Figure()

            # Predicted line
            fig.add_trace(
                go.Scatter(
                    x=df_all["forecast_date"],
                    y=df_all["predicted_claims"],
                    mode="lines+markers",
                    name="Predicted Claims",
                    line=dict(color="#667eea", width=3),
                )
            )

            # Confidence interval
            if "upper_bound" in df_all.columns:
                fig.add_trace(
                    go.Scatter(
                        x=df_all["forecast_date"],
                        y=df_all["upper_bound"],
                        mode="lines",
                        line=dict(width=0),
                        showlegend=False,
                    )
                )

                fig.add_trace(
                    go.Scatter(
                        x=df_all["forecast_date"],
                        y=df_all["lower_bound"],
                        mode="lines",
                        name="95% Confidence",
                        line=dict(width=0),
                        fillcolor="rgba(102, 126, 234, 0.2)",
                        fill="tonexty",
                    )
                )

            fig.update_layout(
                title="Claim Volume Forecast",
                xaxis_title="Date",
                yaxis_title="Predicted Claims",
                hovermode="x unified",
                height=400,
            )
            return fig

    elif intent == "pricing" and "recommendation_category" in df.columns:
        # Bar chart for pricing recommendations
        category_counts = df["recommendation_category"].value_counts()
        fig = px.bar(
            x=category_counts.index,
            y=category_counts.values,
            title="Premium Recommendations by Category",
            labels={"x": "Recommendation", "y": "Count"},
            color=category_counts.index,
            color_discrete_map={"Increase": "#f5576c", "Decrease": "#90EE90", "Maintain": "#ffa600"},
        )
        fig.update_layout(height=400)
        return fig

    return None


print("✅ Chart generator loaded!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🤖 Response Generator


# COMMAND ----------
def generate_response(user_input, gold_catalog, silver_catalog, environment):
    """Generate complete response with text, charts, and data - Environment aware"""

    # Parse intent
    intent, params = parse_user_intent(user_input)

    if intent is None:
        return {
            "html": create_response_html(
                "I'm not sure what you're asking",
                "Try asking about customer churn, fraud detection, forecasts, or pricing optimization.",
            ),
            "suggestions": [
                "Show me the executive summary",
                "Who are my high-risk customers?",
                "What fraud cases need investigation?",
                "Forecast claims for next week",
            ],
        }

    # Generate SQL with environment-specific catalogs
    sql_query = generate_sql_query(intent, params, gold_catalog, silver_catalog)

    if sql_query is None:
        return {
            "html": create_response_html("Query Error", "Could not generate query for your request."),
            "suggestions": [],
        }

    # Execute query
    try:
        df = spark.sql(sql_query).toPandas()
    except Exception as e:
        return {"html": create_response_html("Query Error", f"Error executing query: {str(e)}"), "suggestions": []}

    # Generate response based on intent
    if intent == "summary":
        row = df.iloc[0]
        return {
            "html": create_response_html("Executive Summary", "Here's your complete AI-powered analytics overview:")
            + create_kpi_cards(row),
            "suggestions": [
                "Show me high-risk customers in detail",
                "What are the critical fraud cases?",
                "Show me the 30-day forecast breakdown",
                "Which policies need immediate repricing?",
            ],
        }

    elif intent == "churn":
        high_risk = len(df[df["churn_risk_category"] == "High Risk"]) if "churn_risk_category" in df.columns else 0
        total_premium = df["total_annual_premium"].sum()
        avg_prob = df["churn_prob"].mean()

        insights = [
            f"<strong>High Risk:</strong> {high_risk:,} customers",
            f"<strong>Premium at Risk:</strong> ${total_premium:,.2f}",
            f"<strong>Avg Churn Probability:</strong> {avg_prob:.1f}%",
            f"<strong>Action:</strong> Immediate retention campaigns needed",
        ]

        return {
            "html": create_response_html(
                f"Found {len(df):,} Customers at Risk",
                "These customers need immediate attention to prevent cancellation.",
                insights,
            ),
            "data": df,
            "chart": create_charts(intent, df),
            "suggestions": [
                f"Show me top {min(20, len(df))} at highest risk",
                "Compare churn risk by state",
                "Compare churn risk by customer segment",
                "Show me their policy details",
            ],
        }

    elif intent == "fraud":
        critical = len(df[df["fraud_risk_category"] == "Critical"]) if "fraud_risk_category" in df.columns else 0
        total_fraud = df["estimated_fraud_amount"].sum()
        avg_score = df["fraud_score"].mean()

        insights = [
            f"<strong>Critical Cases:</strong> {critical:,} requiring immediate action",
            f"<strong>Estimated Fraud:</strong> ${total_fraud:,.2f}",
            f"<strong>Avg Fraud Score:</strong> {avg_score:.1f}/100",
            f"<strong>Action:</strong> Route to SIU for investigation",
        ]

        return {
            "html": create_response_html(
                f"Found {len(df):,} Suspicious Claims",
                "These cases require investigation to prevent fraudulent payouts.",
                insights,
            ),
            "data": df,
            "chart": create_charts(intent, df),
            "suggestions": [
                "Show me fraud patterns by claim type",
                "Compare fraud scores across regions",
                "Show me cases with location mismatches",
                "Export investigation list",
            ],
        }

    elif intent == "forecast":
        days = params.get("days", 30)
        df_all = df[df["claim_type"] == "ALL_TYPES"] if "claim_type" in df.columns else df
        total_claims = df_all["predicted_claims"].sum() if not df_all.empty else 0
        total_amount = df_all["predicted_amount"].sum() if not df_all.empty else 0
        daily_avg = total_claims / days if days > 0 else 0

        insights = [
            f"<strong>Expected Claims:</strong> {int(total_claims):,} over {days} days",
            f"<strong>Expected Amount:</strong> ${total_amount:,.2f}",
            f"<strong>Daily Average:</strong> {int(daily_avg):,} claims/day",
            f"<strong>Action:</strong> Use for staffing and resource planning",
        ]

        return {
            "html": create_response_html(
                f"{days}-Day Claim Forecast", "Predictive analytics for upcoming claim volumes.", insights
            ),
            "data": df_all if not df_all.empty else df,
            "chart": create_charts(intent, df),
            "suggestions": [
                "Forecast for next 7 days",
                "Forecast for next 90 days",
                "Break down forecast by claim type",
                "Compare forecast with historical actuals",
            ],
        }

    elif intent == "pricing":
        total_impact = df["annual_revenue_impact"].sum()
        increases = len(df[df["recommendation_category"] == "Increase"])
        decreases = len(df[df["recommendation_category"] == "Decrease"])

        insights = [
            f"<strong>High Priority Policies:</strong> {len(df):,}",
            f"<strong>Revenue Opportunity:</strong> ${total_impact:,.2f}/year",
            f"<strong>Price Increases:</strong> {increases:,} policies",
            f"<strong>Price Decreases:</strong> {decreases:,} policies (retention)",
        ]

        return {
            "html": create_response_html(
                f"{len(df):,} Premium Optimization Opportunities",
                "Recommended pricing changes to maximize profitability while retaining customers.",
                insights,
            ),
            "data": df,
            "chart": create_charts(intent, df),
            "suggestions": [
                "Show me all pricing recommendations",
                "Focus on price increase opportunities",
                "Show retention-focused price decreases",
                "Compare recommendations by policy type",
            ],
        }

    return {"html": create_response_html("Results", f"Found {len(df):,} results"), "data": df}


print("✅ Response generator loaded!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🚀 Process User Input

# COMMAND ----------
# Get user input from widgets
quick_action = dbutils.widgets.get("quick_action")
question = dbutils.widgets.get("question")

# Get environment configuration (reload in case user changed it)
gold_catalog = dbutils.widgets.get("gold_catalog")
silver_catalog = gold_catalog.replace("_gold", "_silver")  # Auto-derive

# Determine environment from catalog name
if "dev" in gold_catalog:
    environment = "dev"
elif "staging" in gold_catalog:
    environment = "staging"
elif "prod" in gold_catalog:
    environment = "prod"
else:
    environment = "unknown"

# Determine what to process
user_input = None

if quick_action and quick_action != "None":
    # Map quick actions to questions
    action_mapping = {
        "📊 Executive Summary": "Show me the executive summary",
        "🔴 High Risk Customers": "Show me top 10 high-risk customers",
        "🚨 Critical Fraud Cases": "Show me critical fraud cases",
        "📈 30-Day Forecast": "Forecast claims for next 30 days",
        "💰 Pricing Opportunities": "Show me high priority pricing recommendations",
    }
    user_input = action_mapping.get(quick_action)
elif question and question.strip():
    user_input = question

if user_input:
    print(f"🔍 Processing: {user_input}")
    print(f"🌍 Environment: {environment.upper()}")
    print("=" * 70)

    # Generate response with environment-specific catalogs
    response = generate_response(user_input, gold_catalog, silver_catalog, environment)

    # Display HTML response
    displayHTML(response["html"])

    # Display chart if available
    if "chart" in response and response["chart"] is not None:
        response["chart"].show()

    # Display data table if available
    if "data" in response and response["data"] is not None:
        print(f"\n📋 Detailed Data ({len(response['data'])} rows):")
        display(response["data"])

    # Display suggestions
    if "suggestions" in response and response["suggestions"]:
        displayHTML(create_suggestions_html(response["suggestions"]))

    print("\n" + "=" * 70)
    print("✅ Response complete!")
    print("\n💡 Tip: Change the question or quick action above and re-run this cell!")

else:
    displayHTML(
        f"""
    <div style="text-align: center; padding: 40px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border-radius: 15px; margin: 20px 0;">
        <h1 style="margin: 0;">🤖 Welcome to Insurance Analytics AI</h1>
        <p style="font-size: 18px; margin: 20px 0;">Your intelligent assistant for data-driven insights</p>
        <div style="background: rgba(255,255,255,0.1); padding: 15px; border-radius: 8px; margin: 15px 0; font-size: 14px;">
            <strong>🌍 Environment:</strong> {environment.upper()}<br/>
            <strong>📊 Catalog:</strong> {gold_catalog}
        </div>
        <div style="background: rgba(255,255,255,0.2); padding: 20px; border-radius: 10px; margin: 20px 0;">
            <h3>👆 Get Started:</h3>
            <p>1. Select a <strong>Quick Action</strong> from the dropdown, OR</p>
            <p>2. Type your question in the text box</p>
            <p>3. Re-run this cell to see results!</p>
        </div>
        <p style="font-size: 12px; opacity: 0.8;">💡 Tip: Change environment using the catalog dropdown at the top</p>
    </div>
    """
    )

    displayHTML(
        """
    <div style="display: flex; gap: 20px; margin: 20px 0;">
        <div style="flex: 1; background: #f8f9fa; padding: 20px; border-radius: 10px;">
            <h3 style="color: #667eea;">📊 I can help with:</h3>
            <ul style="line-height: 2;">
                <li>Customer churn predictions</li>
                <li>Fraud detection alerts</li>
                <li>Claim volume forecasts</li>
                <li>Premium optimization</li>
                <li>Comparative analytics</li>
            </ul>
        </div>
        <div style="flex: 1; background: #f8f9fa; padding: 20px; border-radius: 10px;">
            <h3 style="color: #667eea;">💡 Example questions:</h3>
            <ul style="line-height: 2;">
                <li>"Show me high-risk customers"</li>
                <li>"What fraud cases need review?"</li>
                <li>"Forecast next week's claims"</li>
                <li>"Top 20 customers at risk"</li>
                <li>"Pricing recommendations"</li>
            </ul>
        </div>
    </div>
    """
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📚 Tips & Tricks
# MAGIC
# MAGIC **Using Quick Actions:**
# MAGIC - Select an option from the dropdown
# MAGIC - Re-run the cell above
# MAGIC - Get instant insights!
# MAGIC
# MAGIC **Asking Questions:**
# MAGIC - Type naturally: "Show me...", "What are...", "Forecast..."
# MAGIC - Add specifics: "top 20", "next week", "in California"
# MAGIC - Mix and match: "Show me top 10 high-risk customers in Texas"
# MAGIC
# MAGIC **Understanding Results:**
# MAGIC - **HTML Summary**: Key insights and metrics
# MAGIC - **Chart**: Visual representation
# MAGIC - **Data Table**: Detailed information (sortable)
# MAGIC - **Suggestions**: Related questions to explore
# MAGIC
# MAGIC **Pro Tips:**
# MAGIC - Start with "Executive Summary" to see everything
# MAGIC - Use suggested questions for deeper exploration
# MAGIC - Charts are interactive - hover for details
# MAGIC - Tables are sortable - click column headers
# MAGIC
# MAGIC **Questions? Just ask!** 🤖
