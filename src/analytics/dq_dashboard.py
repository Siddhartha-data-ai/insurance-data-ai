# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Data Quality Monitoring Dashboard
# MAGIC 
# MAGIC **Real-Time Interactive Dashboard for Data Quality**
# MAGIC 
# MAGIC This Streamlit dashboard provides:
# MAGIC - Real-time quality metrics across Bronze, Silver, Gold layers
# MAGIC - Interactive visualizations and trend analysis
# MAGIC - Alert system for quality threshold violations
# MAGIC - Automated quality scoring and recommendations
# MAGIC 
# MAGIC **Deployment:** Streamlit app accessible via browser

# COMMAND ----------
# MAGIC %pip install streamlit plotly altair --quiet

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
# Save the Streamlit app code
streamlit_app_code = '''
import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, lit, current_timestamp, max as spark_max, min as spark_min, sum as spark_sum
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import time

# Page configuration
st.set_page_config(
    page_title="Data Quality Monitoring Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1F77B4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .alert-critical {
        background-color: #ff4444;
        color: white;
        padding: 1rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
    .alert-warning {
        background-color: #ffaa00;
        color: white;
        padding: 1rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
    .alert-success {
        background-color: #00C851;
        color: white;
        padding: 1rem;
        border-radius: 5px;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Initialize Spark session
@st.cache_resource
def get_spark():
    return SparkSession.builder.getOrCreate()

spark = get_spark()

# Sidebar configuration
st.sidebar.title("⚙️ Configuration")
environment = st.sidebar.selectbox(
    "Environment",
    ["dev", "staging", "prod"],
    index=0
)

catalog_prefix = f"insurance_{environment}"
bronze_catalog = f"{catalog_prefix}_bronze"
silver_catalog = f"{catalog_prefix}_silver"
gold_catalog = f"{catalog_prefix}_gold"

st.sidebar.info(f"""
**Selected Environment:** {environment.upper()}
- Bronze: `{bronze_catalog}`
- Silver: `{silver_catalog}`
- Gold: `{gold_catalog}`
""")

# Quality thresholds
st.sidebar.subheader("📏 Quality Thresholds")
null_threshold = st.sidebar.slider("Max Null Rate (%)", 0, 20, 5) / 100
duplicate_threshold = st.sidebar.slider("Max Duplicate Rate (%)", 0, 10, 1) / 100
freshness_hours = st.sidebar.number_input("Max Data Age (hours)", 1, 168, 48)

auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=False)

# Main header
st.markdown('<div class="main-header">📊 Data Quality Monitoring Dashboard</div>', unsafe_allow_html=True)
st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Create tabs
tab1, tab2, tab3, tab4 = st.tabs(["🎯 Overview", "🔍 Detailed Analysis", "📈 Trends", "🚨 Alerts"])

# ----------------------------
# Helper Functions
# ----------------------------

@st.cache_data(ttl=30)
def get_table_list(catalog, schema):
    """Get list of tables in a schema"""
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").toPandas()
        return tables['tableName'].tolist()
    except Exception as e:
        st.error(f"Error accessing {catalog}.{schema}: {str(e)}")
        return []

@st.cache_data(ttl=30)
def check_table_quality(catalog, schema, table_name, layer):
    """Comprehensive quality check for a table"""
    try:
        full_table = f"{catalog}.{schema}.{table_name}"
        df = spark.table(full_table)
        
        # Basic metrics
        total_rows = df.count()
        total_columns = len(df.columns)
        
        if total_rows == 0:
            return {
                'layer': layer,
                'table': table_name,
                'total_rows': 0,
                'total_columns': total_columns,
                'null_rate': 0,
                'duplicate_rate': 0,
                'quality_score': 0,
                'status': 'WARNING',
                'message': 'Table is empty'
            }
        
        # Null rate calculation
        null_counts = []
        for column in df.columns:
            try:
                null_count = df.filter(col(column).isNull()).count()
                null_counts.append(null_count)
            except:
                pass
        
        avg_null_rate = sum(null_counts) / (total_rows * total_columns) if null_counts else 0
        
        # Duplicate rate (approximate using first few columns)
        key_columns = df.columns[:min(3, len(df.columns))]
        distinct_rows = df.select(key_columns).distinct().count()
        duplicate_rate = (total_rows - distinct_rows) / total_rows if total_rows > 0 else 0
        
        # Freshness check
        timestamp_cols = [c for c in df.columns if 'timestamp' in c.lower() or 'date' in c.lower()]
        hours_old = None
        if timestamp_cols:
            try:
                latest = df.select(spark_max(col(timestamp_cols[0]))).collect()[0][0]
                if latest:
                    hours_old = (datetime.now() - latest).total_seconds() / 3600
            except:
                pass
        
        # Quality scoring
        score = 100
        status = 'PASS'
        issues = []
        
        if avg_null_rate > null_threshold:
            score -= 30
            status = 'FAIL'
            issues.append(f"High null rate: {avg_null_rate:.2%}")
        
        if duplicate_rate > duplicate_threshold:
            score -= 25
            status = 'FAIL'
            issues.append(f"High duplicate rate: {duplicate_rate:.2%}")
        
        if hours_old and hours_old > freshness_hours:
            score -= 20
            status = 'FAIL' if status != 'FAIL' else status
            issues.append(f"Stale data: {hours_old:.1f} hours old")
        
        if total_rows < 100:
            score -= 15
            issues.append(f"Low row count: {total_rows}")
        
        message = "; ".join(issues) if issues else "All checks passed"
        
        return {
            'layer': layer,
            'table': table_name,
            'total_rows': total_rows,
            'total_columns': total_columns,
            'null_rate': round(avg_null_rate, 4),
            'duplicate_rate': round(duplicate_rate, 4),
            'hours_old': round(hours_old, 2) if hours_old else None,
            'quality_score': max(0, score),
            'status': status,
            'message': message
        }
    except Exception as e:
        return {
            'layer': layer,
            'table': table_name,
            'total_rows': 0,
            'total_columns': 0,
            'null_rate': 0,
            'duplicate_rate': 0,
            'quality_score': 0,
            'status': 'ERROR',
            'message': str(e)
        }

# ----------------------------
# TAB 1: Overview
# ----------------------------
with tab1:
    st.header("🎯 Quality Overview")
    
    # Collect quality data for all layers
    all_quality_data = []
    
    with st.spinner("Analyzing Bronze layer..."):
        bronze_tables = get_table_list(bronze_catalog, 'customers')
        for table in bronze_tables[:5]:  # Limit to 5 tables for performance
            quality_data = check_table_quality(bronze_catalog, 'customers', table, 'Bronze')
            all_quality_data.append(quality_data)
    
    with st.spinner("Analyzing Silver layer..."):
        silver_schemas = ['customers', 'policies', 'claims', 'agents']
        for schema in silver_schemas:
            try:
                silver_tables = get_table_list(silver_catalog, schema)
                for table in silver_tables[:3]:
                    quality_data = check_table_quality(silver_catalog, schema, table, 'Silver')
                    all_quality_data.append(quality_data)
            except:
                pass
    
    with st.spinner("Analyzing Gold layer..."):
        gold_schemas = ['analytics', 'predictions']
        for schema in gold_schemas:
            try:
                gold_tables = get_table_list(gold_catalog, schema)
                for table in gold_tables[:3]:
                    quality_data = check_table_quality(gold_catalog, schema, table, 'Gold')
                    all_quality_data.append(quality_data)
            except:
                pass
    
    if all_quality_data:
        df_quality = pd.DataFrame(all_quality_data)
        
        # Summary metrics
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            total_tables = len(df_quality)
            st.metric("📊 Total Tables", total_tables)
        
        with col2:
            passed = len(df_quality[df_quality['status'] == 'PASS'])
            st.metric("✅ Passed", passed, delta=f"{passed/total_tables*100:.1f}%")
        
        with col3:
            failed = len(df_quality[df_quality['status'] == 'FAIL'])
            st.metric("❌ Failed", failed, delta=f"-{failed/total_tables*100:.1f}%", delta_color="inverse")
        
        with col4:
            avg_score = df_quality['quality_score'].mean()
            st.metric("🎯 Avg Quality Score", f"{avg_score:.1f}/100")
        
        with col5:
            total_rows = df_quality['total_rows'].sum()
            st.metric("📝 Total Rows", f"{total_rows:,}")
        
        # Quality distribution by layer
        st.subheader("📊 Quality Distribution by Layer")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Quality score by layer
            fig_score = px.box(
                df_quality,
                x='layer',
                y='quality_score',
                color='layer',
                title='Quality Score Distribution by Layer',
                color_discrete_map={'Bronze': '#FF6B6B', 'Silver': '#4ECDC4', 'Gold': '#FFD93D'}
            )
            st.plotly_chart(fig_score, use_container_width=True)
        
        with col2:
            # Status by layer
            status_counts = df_quality.groupby(['layer', 'status']).size().reset_index(name='count')
            fig_status = px.bar(
                status_counts,
                x='layer',
                y='count',
                color='status',
                title='Quality Status by Layer',
                color_discrete_map={'PASS': '#00C851', 'FAIL': '#ff4444', 'WARNING': '#ffaa00', 'ERROR': '#666666'},
                barmode='stack'
            )
            st.plotly_chart(fig_status, use_container_width=True)
        
        # Top issues
        st.subheader("🔍 Tables Requiring Attention")
        failed_tables = df_quality[df_quality['status'] == 'FAIL'].sort_values('quality_score')
        if not failed_tables.empty:
            st.dataframe(
                failed_tables[['layer', 'table', 'quality_score', 'null_rate', 'duplicate_rate', 'message']],
                use_container_width=True,
                hide_index=True
            )
        else:
            st.success("✅ All tables passed quality checks!")
    else:
        st.warning("No quality data available. Please check catalog configuration.")

# ----------------------------
# TAB 2: Detailed Analysis
# ----------------------------
with tab2:
    st.header("🔍 Detailed Quality Analysis")
    
    if all_quality_data:
        df_quality = pd.DataFrame(all_quality_data)
        
        # Filter controls
        col1, col2 = st.columns(2)
        with col1:
            selected_layer = st.multiselect(
                "Select Layers",
                options=df_quality['layer'].unique(),
                default=df_quality['layer'].unique()
            )
        with col2:
            selected_status = st.multiselect(
                "Select Status",
                options=df_quality['status'].unique(),
                default=df_quality['status'].unique()
            )
        
        filtered_df = df_quality[
            (df_quality['layer'].isin(selected_layer)) &
            (df_quality['status'].isin(selected_status))
        ]
        
        # Detailed table
        st.subheader("📋 Complete Quality Report")
        st.dataframe(
            filtered_df.sort_values('quality_score'),
            use_container_width=True,
            hide_index=True
        )
        
        # Quality metrics heatmap
        st.subheader("🔥 Quality Metrics Heatmap")
        
        heatmap_data = filtered_df.pivot_table(
            values='quality_score',
            index='table',
            columns='layer',
            aggfunc='first'
        )
        
        fig_heatmap = px.imshow(
            heatmap_data,
            color_continuous_scale='RdYlGn',
            aspect='auto',
            title='Quality Score Heatmap (0=Poor, 100=Excellent)',
            labels=dict(color="Quality Score")
        )
        st.plotly_chart(fig_heatmap, use_container_width=True)

# ----------------------------
# TAB 3: Trends
# ----------------------------
with tab3:
    st.header("📈 Quality Trends")
    
    if all_quality_data:
        df_quality = pd.DataFrame(all_quality_data)
        
        # Simulate historical data (in production, this would come from a tracking table)
        st.info("📌 Trend analysis requires historical quality data. Configure automated daily quality checks to populate trends.")
        
        # Current snapshot as baseline
        col1, col2 = st.columns(2)
        
        with col1:
            # Average quality score by layer
            layer_scores = df_quality.groupby('layer')['quality_score'].mean().reset_index()
            fig_layer_trend = px.line(
                layer_scores,
                x='layer',
                y='quality_score',
                markers=True,
                title='Average Quality Score by Layer (Current)',
                labels={'quality_score': 'Quality Score'}
            )
            fig_layer_trend.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="Threshold")
            st.plotly_chart(fig_layer_trend, use_container_width=True)
        
        with col2:
            # Null rate distribution
            fig_null = px.histogram(
                df_quality,
                x='null_rate',
                nbins=20,
                title='Null Rate Distribution',
                labels={'null_rate': 'Null Rate'},
                color_discrete_sequence=['#4ECDC4']
            )
            fig_null.add_vline(x=null_threshold, line_dash="dash", line_color="red", annotation_text="Threshold")
            st.plotly_chart(fig_null, use_container_width=True)
        
        # Row count trends
        st.subheader("📊 Data Volume by Layer")
        row_counts = df_quality.groupby('layer')['total_rows'].sum().reset_index()
        fig_volume = px.bar(
            row_counts,
            x='layer',
            y='total_rows',
            title='Total Rows by Layer',
            color='layer',
            color_discrete_map={'Bronze': '#FF6B6B', 'Silver': '#4ECDC4', 'Gold': '#FFD93D'}
        )
        st.plotly_chart(fig_volume, use_container_width=True)

# ----------------------------
# TAB 4: Alerts
# ----------------------------
with tab4:
    st.header("🚨 Quality Alerts")
    
    if all_quality_data:
        df_quality = pd.DataFrame(all_quality_data)
        
        # Critical alerts (FAIL status)
        critical = df_quality[df_quality['status'] == 'FAIL']
        if not critical.empty:
            st.markdown(f'<div class="alert-critical">🚨 <b>CRITICAL:</b> {len(critical)} tables failed quality checks!</div>', unsafe_allow_html=True)
            for _, row in critical.iterrows():
                st.error(f"**{row['layer']} - {row['table']}**: {row['message']} (Score: {row['quality_score']}/100)")
        
        # Warnings
        warnings = df_quality[df_quality['status'] == 'WARNING']
        if not warnings.empty:
            st.markdown(f'<div class="alert-warning">⚠️ <b>WARNING:</b> {len(warnings)} tables have warnings</div>', unsafe_allow_html=True)
            for _, row in warnings.iterrows():
                st.warning(f"**{row['layer']} - {row['table']}**: {row['message']}")
        
        # Success
        passed = df_quality[df_quality['status'] == 'PASS']
        if not passed.empty:
            st.markdown(f'<div class="alert-success">✅ <b>SUCCESS:</b> {len(passed)} tables passed all checks</div>', unsafe_allow_html=True)
        
        # Recommendations
        st.subheader("💡 Recommendations")
        
        if not critical.empty:
            st.markdown("### High Priority Actions")
            for _, row in critical.iterrows():
                if row['null_rate'] > null_threshold:
                    st.markdown(f"- 🔧 **{row['table']}**: Investigate and fix null values in source data")
                if row['duplicate_rate'] > duplicate_threshold:
                    st.markdown(f"- 🔧 **{row['table']}**: Remove duplicate records using DISTINCT or deduplication logic")
                if row['hours_old'] and row['hours_old'] > freshness_hours:
                    st.markdown(f"- 🔧 **{row['table']}**: Schedule more frequent data refreshes")
        else:
            st.success("✅ No critical issues detected. All tables meet quality standards!")

# Auto-refresh logic
if auto_refresh:
    time.sleep(30)
    st.rerun()
'''

# Write the app to a file
with open('/tmp/dq_dashboard_app.py', 'w') as f:
    f.write(streamlit_app_code)

print("✅ Data Quality Dashboard app created at /tmp/dq_dashboard_app.py")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🚀 Launch Dashboard
# MAGIC 
# MAGIC **Option 1: Run Locally**
# MAGIC ```bash
# MAGIC streamlit run /tmp/dq_dashboard_app.py --server.port 8502
# MAGIC ```
# MAGIC 
# MAGIC **Option 2: Deploy in Databricks**
# MAGIC - Upload this notebook to your Databricks workspace
# MAGIC - Run all cells
# MAGIC - Access via Databricks Apps or port forwarding

# COMMAND ----------
# Create a launcher script
launcher_script = '''#!/bin/bash

echo "🚀 Starting Data Quality Monitoring Dashboard..."
echo "================================================="

# Check if Streamlit is installed
if ! command -v streamlit &> /dev/null; then
    echo "❌ Streamlit not found. Installing..."
    pip install streamlit plotly altair
fi

# Launch the dashboard
echo "✅ Launching dashboard on port 8502..."
echo "📊 Access dashboard at: http://localhost:8502"
echo ""

streamlit run /tmp/dq_dashboard_app.py --server.port 8502 --server.address 0.0.0.0
'''

with open('/tmp/launch_dq_dashboard.sh', 'w') as f:
    f.write(launcher_script)

print("✅ Launcher script created at /tmp/launch_dq_dashboard.sh")
print("\nTo launch the dashboard, run:")
print("bash /tmp/launch_dq_dashboard.sh")

