"""
Insurance Analytics AI Chatbot
Built with Streamlit + LangChain + Databricks

Features:
- Natural language queries
- Auto-generated visualizations
- Conversational insights
- Quick Actions for common queries
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import re

# Configure Streamlit page
st.set_page_config(
    page_title="Insurance Analytics AI",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better UI
st.markdown("""
    <style>
    .main {
        padding: 1rem;
    }
    .stButton>button {
        width: 100%;
        border-radius: 20px;
        height: 3em;
        background-color: #FF3621;
        color: white;
    }
    .chat-message {
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        display: flex;
        flex-direction: column;
    }
    .user-message {
        background-color: #E8F4F8;
        align-items: flex-end;
    }
    .bot-message {
        background-color: #F0F0F0;
        align-items: flex-start;
    }
    .metric-card {
        background-color: #F8F9FA;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #FF3621;
    }
    </style>
    """, unsafe_allow_html=True)

# Initialize session state
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'conversation_context' not in st.session_state:
    st.session_state.conversation_context = []

# =============================================================================
# DATABASE CONNECTION
# =============================================================================

@st.cache_resource
def get_spark():
    """Get or create Spark session for Databricks"""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
        return spark
    except Exception as e:
        st.error(f"Failed to connect to Spark: {str(e)}")
        return None

def query_databricks(sql_query):
    """Execute SQL query on Databricks and return pandas DataFrame"""
    try:
        spark = get_spark()
        if spark is None:
            return None
        
        df = spark.sql(sql_query).toPandas()
        return df
    except Exception as e:
        st.error(f"Query failed: {str(e)}")
        return None

# =============================================================================
# NATURAL LANGUAGE PROCESSING
# =============================================================================

def parse_user_intent(user_input):
    """
    Parse user input to understand intent and extract parameters
    Returns: (intent, params)
    """
    user_input_lower = user_input.lower()
    
    # Intent patterns
    intents = {
        'churn': ['churn', 'cancel', 'leaving', 'retention', 'at risk', 'high risk'],
        'fraud': ['fraud', 'suspicious', 'investigation', 'siu', 'fraudulent'],
        'forecast': ['forecast', 'predict', 'future', 'expect', 'upcoming', 'next'],
        'pricing': ['price', 'premium', 'pricing', 'optimize', 'revenue', 'increase premium'],
        'summary': ['summary', 'overview', 'dashboard', 'kpi', 'metrics', 'all'],
        'compare': ['compare', 'comparison', 'versus', 'vs', 'between'],
        'detail': ['detail', 'explain', 'why', 'tell me more', 'breakdown'],
        'export': ['export', 'download', 'save', 'send', 'email']
    }
    
    detected_intent = None
    for intent, keywords in intents.items():
        if any(keyword in user_input_lower for keyword in keywords):
            detected_intent = intent
            break
    
    # Extract parameters
    params = {
        'limit': 10,  # default
        'state': None,
        'segment': None,
        'risk_level': None,
        'days': 30  # default forecast period
    }
    
    # Extract limit
    limit_match = re.search(r'top (\d+)|first (\d+)|show (\d+)', user_input_lower)
    if limit_match:
        params['limit'] = int(next(g for g in limit_match.groups() if g is not None))
    
    # Extract days for forecast
    days_match = re.search(r'(\d+) days?|next week|next month', user_input_lower)
    if days_match:
        if 'week' in user_input_lower:
            params['days'] = 7
        elif 'month' in user_input_lower:
            params['days'] = 30
        else:
            params['days'] = int(days_match.group(1))
    
    # Extract risk level
    if 'high risk' in user_input_lower or 'critical' in user_input_lower:
        params['risk_level'] = 'high'
    elif 'medium risk' in user_input_lower:
        params['risk_level'] = 'medium'
    elif 'low risk' in user_input_lower:
        params['risk_level'] = 'low'
    
    return detected_intent, params

# =============================================================================
# QUERY GENERATORS
# =============================================================================

def generate_churn_query(params):
    """Generate SQL for churn-related queries"""
    limit = params.get('limit', 10)
    risk_level = params.get('risk_level')
    
    base_query = """
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
    FROM insurance_dev_gold.predictions.customer_churn_risk
    """
    
    if risk_level == 'high':
        base_query += " WHERE churn_risk_category = 'High Risk'"
    elif risk_level == 'medium':
        base_query += " WHERE churn_risk_category = 'Medium Risk'"
    elif risk_level == 'low':
        base_query += " WHERE churn_risk_category = 'Low Risk'"
    
    base_query += f" ORDER BY churn_probability DESC LIMIT {limit}"
    
    return base_query

def generate_fraud_query(params):
    """Generate SQL for fraud-related queries"""
    limit = params.get('limit', 10)
    risk_level = params.get('risk_level')
    
    base_query = """
    SELECT 
        claim_id,
        claim_number,
        claim_type,
        ROUND(combined_fraud_score, 1) AS fraud_score,
        claimed_amount,
        estimated_fraud_amount,
        fraud_risk_category,
        investigation_priority,
        recommended_action
    FROM insurance_dev_gold.predictions.fraud_alerts
    """
    
    if risk_level == 'high':
        base_query += " WHERE fraud_risk_category IN ('Critical', 'High')"
    
    base_query += f" ORDER BY combined_fraud_score DESC LIMIT {limit}"
    
    return base_query

def generate_forecast_query(params):
    """Generate SQL for forecast-related queries"""
    days = params.get('days', 30)
    
    query = f"""
    SELECT 
        forecast_date,
        claim_type,
        ROUND(predicted_claim_count, 0) AS predicted_claims,
        ROUND(predicted_total_amount, 0) AS predicted_amount,
        ROUND(confidence_lower_95, 0) AS lower_bound,
        ROUND(confidence_upper_95, 0) AS upper_bound
    FROM insurance_dev_gold.predictions.claim_forecast
    WHERE days_ahead <= {days}
    ORDER BY forecast_date, claim_type
    """
    
    return query

def generate_pricing_query(params):
    """Generate SQL for pricing optimization queries"""
    limit = params.get('limit', 10)
    
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
        implementation_priority,
        rationale
    FROM insurance_dev_gold.predictions.premium_optimization
    WHERE implementation_priority = 'High'
    ORDER BY ABS(annual_revenue_impact) DESC
    LIMIT {limit}
    """
    
    return query

def generate_summary_query():
    """Generate SQL for executive summary"""
    query = """
    SELECT
        (SELECT COUNT(*) FROM insurance_dev_gold.predictions.customer_churn_risk 
         WHERE churn_risk_category = 'High Risk') AS high_risk_customers,
        (SELECT SUM(total_annual_premium) FROM insurance_dev_gold.predictions.customer_churn_risk 
         WHERE churn_risk_category = 'High Risk') AS premium_at_risk,
        (SELECT COUNT(*) FROM insurance_dev_gold.predictions.fraud_alerts 
         WHERE fraud_risk_category IN ('Critical', 'High')) AS critical_fraud_cases,
        (SELECT SUM(estimated_fraud_amount) FROM insurance_dev_gold.predictions.fraud_alerts 
         WHERE fraud_risk_category IN ('Critical', 'High')) AS potential_fraud_amount,
        (SELECT SUM(predicted_claim_count) FROM insurance_dev_gold.predictions.claim_forecast 
         WHERE claim_type = 'ALL_TYPES' AND days_ahead <= 30) AS forecast_30d_claims,
        (SELECT SUM(predicted_total_amount) FROM insurance_dev_gold.predictions.claim_forecast 
         WHERE claim_type = 'ALL_TYPES' AND days_ahead <= 30) AS forecast_30d_amount,
        (SELECT COUNT(*) FROM insurance_dev_gold.predictions.premium_optimization 
         WHERE implementation_priority = 'High') AS high_priority_pricing,
        (SELECT SUM(annual_revenue_impact) FROM insurance_dev_gold.predictions.premium_optimization 
         WHERE implementation_priority = 'High') AS revenue_opportunity
    """
    
    return query

# =============================================================================
# VISUALIZATION GENERATORS
# =============================================================================

def create_churn_chart(df):
    """Create visualization for churn data"""
    if df is None or df.empty:
        return None
    
    # Risk distribution
    if 'churn_risk_category' in df.columns:
        risk_counts = df['churn_risk_category'].value_counts()
        fig = px.pie(
            values=risk_counts.values,
            names=risk_counts.index,
            title='Churn Risk Distribution',
            color_discrete_sequence=['#FF3621', '#FFA500', '#90EE90']
        )
        return fig
    
    return None

def create_fraud_chart(df):
    """Create visualization for fraud data"""
    if df is None or df.empty:
        return None
    
    # Fraud by type
    if 'claim_type' in df.columns and 'fraud_score' in df.columns:
        fig = px.bar(
            df,
            x='claim_type',
            y='fraud_score',
            title='Fraud Scores by Claim Type',
            color='fraud_risk_category',
            color_discrete_map={'Critical': '#FF0000', 'High': '#FF6600', 'Medium': '#FFA500', 'Low': '#90EE90'}
        )
        return fig
    
    return None

def create_forecast_chart(df):
    """Create visualization for forecast data"""
    if df is None or df.empty:
        return None
    
    # Filter for ALL_TYPES for main trend
    if 'claim_type' in df.columns:
        df_all = df[df['claim_type'] == 'ALL_TYPES'].copy()
    else:
        df_all = df.copy()
    
    if not df_all.empty and 'forecast_date' in df_all.columns:
        fig = go.Figure()
        
        # Add prediction line
        fig.add_trace(go.Scatter(
            x=df_all['forecast_date'],
            y=df_all['predicted_claims'],
            mode='lines+markers',
            name='Predicted Claims',
            line=dict(color='#FF3621', width=2)
        ))
        
        # Add confidence interval
        if 'upper_bound' in df_all.columns:
            fig.add_trace(go.Scatter(
                x=df_all['forecast_date'],
                y=df_all['upper_bound'],
                mode='lines',
                name='Upper Bound',
                line=dict(width=0),
                showlegend=False
            ))
            
            fig.add_trace(go.Scatter(
                x=df_all['forecast_date'],
                y=df_all['lower_bound'],
                mode='lines',
                name='Confidence Interval',
                line=dict(width=0),
                fillcolor='rgba(255, 54, 33, 0.2)',
                fill='tonexty'
            ))
        
        fig.update_layout(
            title='Claim Volume Forecast',
            xaxis_title='Date',
            yaxis_title='Predicted Claims',
            hovermode='x unified'
        )
        
        return fig
    
    return None

def create_kpi_cards(df):
    """Create KPI cards from summary data"""
    if df is None or df.empty:
        return None
    
    row = df.iloc[0]
    
    cols = st.columns(4)
    
    with cols[0]:
        st.metric(
            label="🔴 High Risk Customers",
            value=f"{int(row['high_risk_customers']):,}",
            delta=f"${row['premium_at_risk']:,.0f} at risk"
        )
    
    with cols[1]:
        st.metric(
            label="🚨 Critical Fraud Cases",
            value=f"{int(row['critical_fraud_cases']):,}",
            delta=f"${row['potential_fraud_amount']:,.0f} potential fraud"
        )
    
    with cols[2]:
        st.metric(
            label="📈 30-Day Forecast",
            value=f"{int(row['forecast_30d_claims']):,} claims",
            delta=f"${row['forecast_30d_amount']:,.0f}"
        )
    
    with cols[3]:
        st.metric(
            label="💰 Revenue Opportunity",
            value=f"{int(row['high_priority_pricing']):,} policies",
            delta=f"+${row['revenue_opportunity']:,.0f}/year"
        )

# =============================================================================
# RESPONSE GENERATOR
# =============================================================================

def generate_response(user_input):
    """Generate bot response based on user input"""
    intent, params = parse_user_intent(user_input)
    
    if intent is None:
        return {
            'text': "I'm not sure what you're asking about. Try asking about:\n- Customer churn risk\n- Fraud detection\n- Claim forecasts\n- Premium optimization\n- Overall summary",
            'data': None,
            'chart': None
        }
    
    # Generate appropriate query
    if intent == 'churn':
        sql_query = generate_churn_query(params)
        df = query_databricks(sql_query)
        chart = create_churn_chart(df)
        
        if df is not None and not df.empty:
            high_risk = len(df[df['churn_risk_category'] == 'High Risk']) if 'churn_risk_category' in df.columns else 0
            total_premium = df['total_annual_premium'].sum()
            text = f"📊 Found {len(df):,} customers at risk of churning.\n\n"
            text += f"• **High Risk:** {high_risk:,} customers\n"
            text += f"• **Premium at Risk:** ${total_premium:,.2f}\n"
            text += f"• **Avg Churn Probability:** {df['churn_prob'].mean():.1f}%\n\n"
            text += "The table below shows detailed customer information. These customers need immediate retention efforts."
        else:
            text = "No churn data available."
            df = None
            chart = None
    
    elif intent == 'fraud':
        sql_query = generate_fraud_query(params)
        df = query_databricks(sql_query)
        chart = create_fraud_chart(df)
        
        if df is not None and not df.empty:
            critical = len(df[df['fraud_risk_category'] == 'Critical']) if 'fraud_risk_category' in df.columns else 0
            total_fraud = df['estimated_fraud_amount'].sum()
            text = f"🚨 Found {len(df):,} suspicious claims requiring investigation.\n\n"
            text += f"• **Critical Cases:** {critical:,}\n"
            text += f"• **Estimated Fraud Amount:** ${total_fraud:,.2f}\n"
            text += f"• **Avg Fraud Score:** {df['fraud_score'].mean():.1f}/100\n\n"
            text += "These cases should be reviewed by your SIU team immediately."
        else:
            text = "No fraud alerts available."
            df = None
            chart = None
    
    elif intent == 'forecast':
        sql_query = generate_forecast_query(params)
        df = query_databricks(sql_query)
        chart = create_forecast_chart(df)
        
        if df is not None and not df.empty:
            days = params.get('days', 30)
            total_predicted = df[df['claim_type'] == 'ALL_TYPES']['predicted_claims'].sum() if 'claim_type' in df.columns else df['predicted_claims'].sum()
            total_amount = df[df['claim_type'] == 'ALL_TYPES']['predicted_amount'].sum() if 'claim_type' in df.columns else df['predicted_amount'].sum()
            text = f"📈 Claim forecast for next {days} days:\n\n"
            text += f"• **Expected Claims:** {int(total_predicted):,}\n"
            text += f"• **Expected Amount:** ${total_amount:,.2f}\n"
            text += f"• **Daily Average:** {int(total_predicted/days):,} claims/day\n\n"
            text += "Use this forecast for staffing and resource planning."
        else:
            text = "No forecast data available."
            df = None
            chart = None
    
    elif intent == 'pricing':
        sql_query = generate_pricing_query(params)
        df = query_databricks(sql_query)
        chart = None
        
        if df is not None and not df.empty:
            total_impact = df['annual_revenue_impact'].sum()
            increases = len(df[df['recommendation_category'] == 'Increase'])
            decreases = len(df[df['recommendation_category'] == 'Decrease'])
            text = f"💰 Premium optimization recommendations:\n\n"
            text += f"• **High Priority Policies:** {len(df):,}\n"
            text += f"• **Revenue Opportunity:** ${total_impact:,.2f}/year\n"
            text += f"• **Price Increases:** {increases:,} | **Decreases:** {decreases:,}\n\n"
            text += "These pricing changes are recommended based on risk and market analysis."
        else:
            text = "No pricing recommendations available."
            df = None
    
    elif intent == 'summary':
        sql_query = generate_summary_query()
        df = query_databricks(sql_query)
        chart = None
        
        if df is not None and not df.empty:
            text = "📊 **Executive Summary - AI Predictions**\n\nHere's your complete analytics overview:"
            create_kpi_cards(df)
        else:
            text = "Summary data not available."
            df = None
    
    else:
        text = f"I understand you're asking about {intent}, but I'm still learning how to answer that question."
        df = None
        chart = None
    
    return {
        'text': text,
        'data': df,
        'chart': chart
    }

# =============================================================================
# CHAT INTERFACE
# =============================================================================

def display_message(role, content):
    """Display a chat message"""
    if role == "user":
        st.markdown(f"""
        <div class="chat-message user-message">
            <b>You:</b> {content}
        </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown(f"""
        <div class="chat-message bot-message">
            <b>🤖 AI Assistant:</b><br>{content}
        </div>
        """, unsafe_allow_html=True)

# =============================================================================
# MAIN APP
# =============================================================================

def main():
    # Header
    st.title("🤖 Insurance Analytics AI Assistant")
    st.markdown("Ask me anything about customer churn, fraud detection, forecasts, or pricing!")
    
    # Sidebar with Quick Actions
    with st.sidebar:
        st.header("⚡ Quick Actions")
        
        if st.button("📊 Executive Summary"):
            st.session_state.messages.append({"role": "user", "content": "Show me the executive summary"})
        
        if st.button("🔴 High Risk Customers"):
            st.session_state.messages.append({"role": "user", "content": "Show me top 10 high-risk customers"})
        
        if st.button("🚨 Critical Fraud Cases"):
            st.session_state.messages.append({"role": "user", "content": "Show me critical fraud cases"})
        
        if st.button("📈 30-Day Forecast"):
            st.session_state.messages.append({"role": "user", "content": "Forecast claims for next 30 days"})
        
        if st.button("💰 Pricing Opportunities"):
            st.session_state.messages.append({"role": "user", "content": "Show me high priority pricing recommendations"})
        
        st.markdown("---")
        st.markdown("### 💡 Example Questions")
        st.markdown("""
        - "Show me customers at risk"
        - "What are our fraud alerts?"
        - "Forecast next week's claims"
        - "Which policies should we reprice?"
        - "Compare churn by state"
        """)
        
        st.markdown("---")
        if st.button("🗑️ Clear Chat"):
            st.session_state.messages = []
            st.rerun()
    
    # Display chat history
    for message in st.session_state.messages:
        display_message(message["role"], message["content"])
        
        # Display data and charts if available
        if message["role"] == "assistant" and "data" in message:
            if message.get("chart") is not None:
                st.plotly_chart(message["chart"], use_container_width=True)
            
            if message.get("data") is not None and not message["data"].empty:
                st.dataframe(message["data"], use_container_width=True)
    
    # Chat input
    user_input = st.chat_input("Ask me anything...")
    
    if user_input:
        # Add user message
        st.session_state.messages.append({"role": "user", "content": user_input})
        
        # Generate response
        with st.spinner("🤔 Thinking..."):
            response = generate_response(user_input)
        
        # Add assistant message
        assistant_message = {
            "role": "assistant",
            "content": response['text'],
            "data": response['data'],
            "chart": response['chart']
        }
        st.session_state.messages.append(assistant_message)
        
        # Rerun to display new messages
        st.rerun()

if __name__ == "__main__":
    main()

