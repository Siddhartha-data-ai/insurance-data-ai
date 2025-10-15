#!/bin/bash

# =============================================================================
# DLT Pipeline Deployment Script - Method 3 (Databricks Asset Bundles)
# =============================================================================

set -e  # Exit on error

echo "======================================================================"
echo "🚀 DEPLOYING DLT PIPELINE WITH DATABRICKS ASSET BUNDLES"
echo "======================================================================"
echo ""

# Color codes
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Navigate to project directory
cd "$(dirname "$0")"
PROJECT_DIR=$(pwd)
echo "📁 Project directory: $PROJECT_DIR"
echo ""

# Step 1: Check if new Databricks CLI is installed
echo "======================================================================"
echo "STEP 1: Checking Databricks CLI Installation"
echo "======================================================================"

if ! command -v databricks &> /dev/null; then
    echo -e "${RED}❌ Databricks CLI not found!${NC}"
    echo ""
    echo "Please install it using ONE of these methods:"
    echo ""
    echo "Option 1 (Homebrew):"
    echo "  brew tap databricks/tap"
    echo "  brew install databricks"
    echo ""
    echo "Option 2 (Direct install):"
    echo "  curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh"
    echo ""
    exit 1
fi

CLI_VERSION=$(databricks --version 2>&1 | head -1)
echo -e "${GREEN}✅ Found: $CLI_VERSION${NC}"

# Check if it's the new CLI (version 0.200+)
if [[ ! "$CLI_VERSION" =~ "version 0.2" ]] && [[ ! "$CLI_VERSION" =~ "version 0.3" ]]; then
    echo -e "${YELLOW}⚠️  Warning: You might have the old Databricks CLI${NC}"
    echo "For Asset Bundles, you need the NEW CLI (version 0.200+)"
fi
echo ""

# Step 2: Check authentication
echo "======================================================================"
echo "STEP 2: Checking Databricks Authentication"
echo "======================================================================"

if databricks auth profiles &> /dev/null; then
    echo -e "${GREEN}✅ Authenticated with Databricks${NC}"
else
    echo -e "${YELLOW}⚠️  Not authenticated yet${NC}"
    echo ""
    echo "Please run: databricks auth login"
    echo ""
    read -p "Press Enter after you've authenticated..."
fi
echo ""

# Step 3: Validate bundle configuration
echo "======================================================================"
echo "STEP 3: Validating Bundle Configuration"
echo "======================================================================"

echo "Using simplified configuration for deployment..."
if databricks bundle validate -c databricks_simplified.yml -t dev; then
    echo -e "${GREEN}✅ Configuration is valid!${NC}"
else
    echo -e "${RED}❌ Configuration validation failed${NC}"
    echo ""
    echo "Please check your databricks_simplified.yml file"
    exit 1
fi
echo ""

# Step 4: Deploy the bundle
echo "======================================================================"
echo "STEP 4: Deploying Bundle to Databricks"
echo "======================================================================"

echo "This will:"
echo "  • Upload all 5 DLT notebooks to Databricks workspace"
echo "  • Create the DLT pipeline: insurance_dev_bronze_to_silver_pipeline"
echo ""

read -p "Continue with deployment? (yes/no): " CONFIRM

if [[ "$CONFIRM" != "yes" ]]; then
    echo "Deployment cancelled."
    exit 0
fi

echo ""
echo "Deploying..."
if databricks bundle deploy -c databricks_simplified.yml -t dev; then
    echo -e "${GREEN}✅ Deployment successful!${NC}"
else
    echo -e "${RED}❌ Deployment failed${NC}"
    exit 1
fi
echo ""

# Step 5: Get pipeline information
echo "======================================================================"
echo "STEP 5: Pipeline Information"
echo "======================================================================"

echo "Fetching pipeline details..."
PIPELINE_ID=$(databricks pipelines list --output json 2>/dev/null | grep -o '"pipeline_id":"[^"]*' | grep insurance | cut -d'"' -f4 | head -1)

if [ -n "$PIPELINE_ID" ]; then
    echo -e "${GREEN}✅ Pipeline Created!${NC}"
    echo ""
    echo "Pipeline ID: $PIPELINE_ID"
    echo "Pipeline Name: insurance_dev_bronze_to_silver_pipeline"
    echo ""
else
    echo -e "${YELLOW}⚠️  Could not retrieve pipeline ID automatically${NC}"
    echo "Please check Databricks UI: Workflows → Delta Live Tables"
fi
echo ""

# Step 6: Instructions for running
echo "======================================================================"
echo "🎉 DEPLOYMENT COMPLETE!"
echo "======================================================================"
echo ""
echo "NEXT STEPS:"
echo ""
echo "1️⃣  View your pipeline in Databricks UI:"
echo "    • Go to: Workflows → Delta Live Tables"
echo "    • Find: insurance_dev_bronze_to_silver_pipeline"
echo ""
echo "2️⃣  Start the pipeline:"
echo "    • Click on the pipeline name"
echo "    • Click the [Start] button"
echo ""
echo "    OR run from command line:"
if [ -n "$PIPELINE_ID" ]; then
    echo "    databricks pipelines start --pipeline-id $PIPELINE_ID"
fi
echo ""
echo "3️⃣  Monitor progress:"
echo "    • Graph tab: See visual data flow"
echo "    • Tables tab: Check row counts"
echo "    • Event Log: View execution details"
echo ""
echo "4️⃣  Verify results (after pipeline completes):"
echo "    • Go to: Data Explorer → Catalogs"
echo "    • Check: insurance_dev_silver"
echo "    • Verify tables: customer_dim, policy_fact, claim_fact, etc."
echo ""
echo "======================================================================"
echo ""

# Save pipeline ID to file
if [ -n "$PIPELINE_ID" ]; then
    echo "$PIPELINE_ID" > .pipeline_id
    echo "Pipeline ID saved to .pipeline_id file"
fi

echo -e "${GREEN}🚀 Ready to run your DLT pipeline!${NC}"
echo ""

