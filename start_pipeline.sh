#!/bin/bash

# Start the DLT Pipeline
PIPELINE_ID=$(cat .pipeline_id)

echo "======================================================================"
echo "🚀 STARTING DLT PIPELINE"
echo "======================================================================"
echo ""
echo "Pipeline ID: $PIPELINE_ID"
echo "Pipeline Name: insurance_dev_bronze_to_silver_pipeline"
echo ""
echo "Starting pipeline..."
echo ""

databricks pipelines start-update --pipeline-id $PIPELINE_ID

echo ""
echo "======================================================================"
echo "✅ PIPELINE STARTED!"
echo "======================================================================"
echo ""
echo "Monitor progress:"
echo "  • Databricks UI: Workflows → Delta Live Tables"
echo "  • Command line: databricks pipelines get --pipeline-id $PIPELINE_ID"
echo ""
echo "Expected runtime: 15-25 minutes"
echo ""

