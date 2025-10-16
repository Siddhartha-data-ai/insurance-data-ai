#!/bin/bash

# ============================================================================
# Data Quality Monitoring Dashboard Launcher
# ============================================================================

echo "🚀 Starting Data Quality Monitoring Dashboard..."
echo "================================================================="

# Navigate to project root
cd /Users/kanikamondal/Databricks/insurance-data-ai || exit 1

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.8+."
    exit 1
fi

# Check if Streamlit is installed
if ! command -v streamlit &> /dev/null; then
    echo "⚠️  Streamlit not found. Installing dependencies..."
    pip3 install -r src/analytics/requirements_dashboard.txt
fi

# Extract the Streamlit app from the notebook
echo "📝 Extracting dashboard app..."

# Create temporary app file
python3 << 'EOF'
import re

# Read the notebook
with open('src/analytics/dq_dashboard.py', 'r') as f:
    content = f.read()

# Extract the Streamlit app code
match = re.search(r"streamlit_app_code = '''(.*?)'''", content, re.DOTALL)
if match:
    app_code = match.group(1)
    
    # Write to temp file
    with open('/tmp/dq_dashboard_app.py', 'w') as f:
        f.write(app_code)
    
    print("✅ Dashboard app extracted successfully")
else:
    print("❌ Could not extract app code")
    exit(1)
EOF

if [ $? -ne 0 ]; then
    echo "❌ Failed to extract dashboard app"
    exit 1
fi

# Launch the dashboard
echo ""
echo "================================================================="
echo "✅ Launching Data Quality Monitoring Dashboard..."
echo "================================================================="
echo ""
echo "📊 Dashboard will be available at:"
echo "   http://localhost:8502"
echo ""
echo "🔑 Features:"
echo "   • Real-time quality metrics across all layers"
echo "   • Interactive visualizations"
echo "   • Quality alerts and recommendations"
echo "   • Auto-refresh support"
echo ""
echo "⏹️  To stop the dashboard, press Ctrl+C"
echo ""
echo "================================================================="

streamlit run /tmp/dq_dashboard_app.py --server.port 8502 --server.address 0.0.0.0

