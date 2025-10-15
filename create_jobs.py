#!/usr/bin/env python3
"""
🚀 Databricks Jobs Creation Script
Automatically creates and configures all pipeline jobs with proper dependencies
"""

import requests
import json
import os
from typing import Dict, List, Optional

# ============================================================================
# CONFIGURATION - Update these values
# ============================================================================

DATABRICKS_HOST = "https://dbc-1e4d394d-ca62.cloud.databricks.com"  # Your workspace URL
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "YOUR_TOKEN_HERE")  # Set via environment or replace

# Email for alerts
ALERT_EMAIL = "siddhartha013@gmail.com"

# Environment to create jobs for
ENVIRONMENT = "dev"  # dev, staging, or prod

# Workspace paths
WORKSPACE_BASE = "/Workspace/Shared/insurance-analytics"

# Catalog names
BRONZE_CATALOG = f"insurance_{ENVIRONMENT}_bronze"
SILVER_CATALOG = f"insurance_{ENVIRONMENT}_silver"
GOLD_CATALOG = f"insurance_{ENVIRONMENT}_gold"

# Use serverless compute (modern Databricks workspaces)
USE_SERVERLESS = True

# Cluster configuration (only used if serverless is not available)
CLUSTER_CONFIG = {
    "spark_version": "13.3.x-scala2.12",  # Latest LTS
    "node_type_id": "i3.xlarge",  # Adjust to your available node types
    "num_workers": 2,
    "spark_conf": {
        "spark.databricks.delta.preview.enabled": "true"
    }
}

# Schedule configuration
SCHEDULE_CRON = "0 0 6 * * ?"  # Daily at 6 AM (Quartz cron: sec min hour day month dayofweek)
SCHEDULE_TIMEZONE = "America/New_York"  # Adjust to your timezone

# ============================================================================
# API Helper Functions
# ============================================================================

class DatabricksJobsAPI:
    def __init__(self, host: str, token: str):
        self.host = host.rstrip('/')
        self.token = token
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
    
    def create_job(self, job_config: Dict) -> Dict:
        """Create a job in Databricks"""
        url = f"{self.host}/api/2.1/jobs/create"
        response = requests.post(url, headers=self.headers, json=job_config)
        if response.status_code != 200:
            print(f"   Error Response: {response.text}")
        response.raise_for_status()
        return response.json()
    
    def list_jobs(self, name_filter: Optional[str] = None) -> List[Dict]:
        """List existing jobs"""
        url = f"{self.host}/api/2.1/jobs/list"
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        jobs = response.json().get('jobs', [])
        
        if name_filter:
            jobs = [j for j in jobs if name_filter in j.get('settings', {}).get('name', '')]
        
        return jobs
    
    def delete_job(self, job_id: int):
        """Delete a job"""
        url = f"{self.host}/api/2.1/jobs/delete"
        response = requests.post(url, headers=self.headers, json={"job_id": job_id})
        response.raise_for_status()

# ============================================================================
# Helper Functions
# ============================================================================

def get_compute_config():
    """Get compute configuration based on USE_SERVERLESS setting"""
    if USE_SERVERLESS:
        # Serverless doesn't need cluster config
        return {}
    else:
        # Traditional cluster
        return {"new_cluster": CLUSTER_CONFIG}

# ============================================================================
# Job Definitions
# ============================================================================

def get_bronze_job_config() -> Dict:
    """Bronze layer: Data generation"""
    return {
        "name": f"[{ENVIRONMENT.upper()}] 1_Bronze_Data_Generation",
        "tags": {
            "environment": ENVIRONMENT,
            "layer": "bronze",
            "project": "insurance-analytics"
        },
        "email_notifications": {
            "on_failure": [ALERT_EMAIL],
            "on_success": [],
            "no_alert_for_skipped_runs": True
        },
        "webhook_notifications": {},
        "timeout_seconds": 3600,  # 1 hour timeout
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "generate_customers",
                "description": "Generate customer data",
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/bronze/generate_customers_data",
                    "base_parameters": {
                        "catalog": BRONZE_CATALOG
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 900,
                "email_notifications": {}
            },
            {
                "task_key": "generate_policies",
                "description": "Generate policy data",
                "depends_on": [{"task_key": "generate_customers"}],
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/bronze/generate_policies_data",
                    "base_parameters": {
                        "catalog": BRONZE_CATALOG
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 900,
                "email_notifications": {}
            },
            {
                "task_key": "generate_claims",
                "description": "Generate claims data",
                "depends_on": [{"task_key": "generate_policies"}],
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/bronze/generate_claims_data",
                    "base_parameters": {
                        "catalog": BRONZE_CATALOG
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 900,
                "email_notifications": {}
            }
        ],
        "schedule": {
            "quartz_cron_expression": SCHEDULE_CRON,
            "timezone_id": SCHEDULE_TIMEZONE,
            "pause_status": "PAUSED"  # Start paused, you can unpause after verification
        },
        "format": "MULTI_TASK"
    }

def get_silver_job_config() -> Dict:
    """Silver layer: Data transformation"""
    return {
        "name": f"[{ENVIRONMENT.upper()}] 2_Silver_Transformation",
        "tags": {
            "environment": ENVIRONMENT,
            "layer": "silver",
            "project": "insurance-analytics"
        },
        "email_notifications": {
            "on_failure": [ALERT_EMAIL],
            "on_success": [],
            "no_alert_for_skipped_runs": True
        },
        "timeout_seconds": 3600,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "transform_bronze_to_silver",
                "description": "Transform bronze data to silver with SCD Type 2",
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/transformations/transform_bronze_to_silver",
                    "base_parameters": {
                        "bronze_catalog": BRONZE_CATALOG,
                        "silver_catalog": SILVER_CATALOG
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 1800,
                "email_notifications": {}
            }
        ],
        "schedule": {
            "quartz_cron_expression": SCHEDULE_CRON,
            "timezone_id": SCHEDULE_TIMEZONE,
            "pause_status": "PAUSED"
        },
        "format": "MULTI_TASK"
    }

def get_gold_job_config() -> Dict:
    """Gold layer: Analytics tables"""
    return {
        "name": f"[{ENVIRONMENT.upper()}] 3_Gold_Analytics",
        "tags": {
            "environment": ENVIRONMENT,
            "layer": "gold",
            "project": "insurance-analytics"
        },
        "email_notifications": {
            "on_failure": [ALERT_EMAIL],
            "on_success": [],
            "no_alert_for_skipped_runs": True
        },
        "timeout_seconds": 3600,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "build_customer_360",
                "description": "Build customer 360 view",
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/gold/build_customer_360",
                    "base_parameters": {
                        "silver_catalog": SILVER_CATALOG,
                        "gold_catalog": GOLD_CATALOG
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 1200,
                "email_notifications": {}
            },
            {
                "task_key": "build_fraud_detection",
                "description": "Build fraud detection analytics",
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/gold/build_fraud_detection",
                    "base_parameters": {
                        "silver_catalog": SILVER_CATALOG,
                        "gold_catalog": GOLD_CATALOG
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 1200,
                "email_notifications": {}
            }
        ],
        "schedule": {
            "quartz_cron_expression": SCHEDULE_CRON,
            "timezone_id": SCHEDULE_TIMEZONE,
            "pause_status": "PAUSED"
        },
        "format": "MULTI_TASK"
    }

def get_ml_predictions_job_config() -> Dict:
    """ML layer: Prediction models"""
    return {
        "name": f"[{ENVIRONMENT.upper()}] 4_ML_Predictions",
        "tags": {
            "environment": ENVIRONMENT,
            "layer": "ml",
            "project": "insurance-analytics"
        },
        "email_notifications": {
            "on_failure": [ALERT_EMAIL],
            "on_success": [],
            "no_alert_for_skipped_runs": True
        },
        "timeout_seconds": 7200,  # 2 hours for ML jobs
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "predict_churn",
                "description": "Predict customer churn risk",
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/ml/predict_customer_churn_sklearn",
                    "base_parameters": {
                        "silver_catalog": SILVER_CATALOG,
                        "gold_catalog": GOLD_CATALOG
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 1800,
                "email_notifications": {}
            },
            {
                "task_key": "predict_fraud",
                "description": "Predict fraud risk",
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/ml/predict_fraud_enhanced_sklearn",
                    "base_parameters": {
                        "silver_catalog": SILVER_CATALOG,
                        "gold_catalog": GOLD_CATALOG
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 1800,
                "email_notifications": {}
            },
            {
                "task_key": "forecast_claims",
                "description": "Forecast future claims",
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/ml/forecast_claims",
                    "base_parameters": {
                        "silver_catalog": SILVER_CATALOG,
                        "gold_catalog": GOLD_CATALOG
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 1800,
                "email_notifications": {}
            },
            {
                "task_key": "optimize_premiums",
                "description": "Optimize premium recommendations",
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/ml/optimize_premiums_sklearn",
                    "base_parameters": {
                        "silver_catalog": SILVER_CATALOG,
                        "gold_catalog": GOLD_CATALOG
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 1800,
                "email_notifications": {}
            }
        ],
        "schedule": {
            "quartz_cron_expression": SCHEDULE_CRON,
            "timezone_id": SCHEDULE_TIMEZONE,
            "pause_status": "PAUSED"
        },
        "format": "MULTI_TASK"
    }

def get_dq_monitoring_job_config() -> Dict:
    """Data Quality Monitoring"""
    return {
        "name": f"[{ENVIRONMENT.upper()}] 5_Data_Quality_Monitoring",
        "tags": {
            "environment": ENVIRONMENT,
            "layer": "monitoring",
            "project": "insurance-analytics"
        },
        "email_notifications": {
            "on_failure": [ALERT_EMAIL],
            "on_success": [],  # Could enable for DQ failures
            "no_alert_for_skipped_runs": True
        },
        "timeout_seconds": 1800,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "data_quality_checks",
                "description": "Run data quality monitoring",
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/analytics/data_quality_monitoring",
                    "base_parameters": {
                        "environment_catalog": GOLD_CATALOG
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 1200,
                "email_notifications": {}
            }
        ],
        "schedule": {
            "quartz_cron_expression": SCHEDULE_CRON,
            "timezone_id": SCHEDULE_TIMEZONE,
            "pause_status": "PAUSED"
        },
        "format": "MULTI_TASK"
    }

def get_pipeline_monitoring_job_config() -> Dict:
    """Pipeline Monitoring Dashboard"""
    return {
        "name": f"[{ENVIRONMENT.upper()}] 6_Pipeline_Monitoring",
        "tags": {
            "environment": ENVIRONMENT,
            "layer": "monitoring",
            "project": "insurance-analytics"
        },
        "email_notifications": {
            "on_failure": [ALERT_EMAIL],
            "on_success": [],
            "no_alert_for_skipped_runs": True
        },
        "timeout_seconds": 900,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "pipeline_monitoring",
                "description": "Generate pipeline monitoring report",
                "notebook_task": {
                    "notebook_path": f"{WORKSPACE_BASE}/analytics/pipeline_monitoring_dashboard",
                    "base_parameters": {
                        "environment": ENVIRONMENT
                    }
                },
                **get_compute_config(),
                "timeout_seconds": 600,
                "email_notifications": {}
            }
        ],
        "schedule": {
            "quartz_cron_expression": "0 0 7 * * ?",  # Run after pipeline at 7 AM
            "timezone_id": SCHEDULE_TIMEZONE,
            "pause_status": "PAUSED"
        },
        "format": "MULTI_TASK"
    }

# ============================================================================
# Main Execution
# ============================================================================

def main():
    print("=" * 70)
    print("🚀 DATABRICKS JOBS CREATION SCRIPT")
    print("=" * 70)
    print(f"\n📊 Environment: {ENVIRONMENT.upper()}")
    print(f"🌐 Databricks Host: {DATABRICKS_HOST}")
    print(f"📧 Alert Email: {ALERT_EMAIL}")
    print(f"⏰ Schedule: {SCHEDULE_CRON} ({SCHEDULE_TIMEZONE})")
    print()
    
    # Validate token
    if DATABRICKS_TOKEN == "YOUR_TOKEN_HERE" or not DATABRICKS_TOKEN:
        print("❌ ERROR: DATABRICKS_TOKEN not set!")
        print("\nPlease set your token:")
        print("  export DATABRICKS_TOKEN='your-token-here'")
        print("\nOr edit the script and replace YOUR_TOKEN_HERE")
        print("\nTo get your token:")
        print("  1. Go to Databricks workspace")
        print("  2. Click user icon (top right)")
        print("  3. User Settings → Access Tokens")
        print("  4. Generate New Token")
        return 1
    
    # Initialize API client
    api = DatabricksJobsAPI(DATABRICKS_HOST, DATABRICKS_TOKEN)
    
    # Check for existing jobs
    print("🔍 Checking for existing jobs...")
    existing_jobs = api.list_jobs(name_filter=f"[{ENVIRONMENT.upper()}]")
    
    if existing_jobs:
        print(f"\n⚠️  Found {len(existing_jobs)} existing jobs for {ENVIRONMENT.upper()}:")
        for job in existing_jobs:
            print(f"   • {job['settings']['name']} (ID: {job['job_id']})")
        
        response = input("\n❓ Delete existing jobs and recreate? (yes/no): ").strip().lower()
        if response == 'yes':
            print("\n🗑️  Deleting existing jobs...")
            for job in existing_jobs:
                print(f"   Deleting: {job['settings']['name']}")
                api.delete_job(job['job_id'])
            print("✅ Existing jobs deleted")
        else:
            print("\n❌ Aborted. Existing jobs will not be modified.")
            return 0
    
    # Job configurations
    jobs_to_create = [
        ("Bronze Data Generation", get_bronze_job_config()),
        ("Silver Transformation", get_silver_job_config()),
        ("Gold Analytics", get_gold_job_config()),
        ("ML Predictions", get_ml_predictions_job_config()),
        ("Data Quality Monitoring", get_dq_monitoring_job_config()),
        ("Pipeline Monitoring", get_pipeline_monitoring_job_config()),
    ]
    
    # Create jobs
    print("\n" + "=" * 70)
    print("📦 CREATING JOBS")
    print("=" * 70)
    
    created_jobs = []
    for job_name, job_config in jobs_to_create:
        try:
            print(f"\n🔨 Creating: {job_name}...")
            result = api.create_job(job_config)
            job_id = result['job_id']
            created_jobs.append((job_name, job_id))
            print(f"✅ Created successfully! Job ID: {job_id}")
        except Exception as e:
            print(f"❌ Failed to create {job_name}")
            print(f"   Error: {str(e)}")
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"\n✅ Successfully created {len(created_jobs)}/{len(jobs_to_create)} jobs:")
    print()
    
    for job_name, job_id in created_jobs:
        job_url = f"{DATABRICKS_HOST}/#job/{job_id}"
        print(f"   ✓ {job_name}")
        print(f"     Job ID: {job_id}")
        print(f"     URL: {job_url}")
        print()
    
    print("=" * 70)
    print("🎉 JOBS CREATED SUCCESSFULLY!")
    print("=" * 70)
    print()
    print("📝 NEXT STEPS:")
    print()
    print("1. Review jobs in Databricks UI:")
    print(f"   {DATABRICKS_HOST}/#job/list")
    print()
    print("2. All jobs start PAUSED. To enable scheduling:")
    print("   • Go to each job")
    print("   • Click 'Schedule' tab")
    print("   • Click 'Resume' to activate schedule")
    print()
    print("3. Test run each job manually first:")
    print("   • Click job name")
    print("   • Click 'Run now'")
    print("   • Verify successful execution")
    print()
    print("4. Monitor execution:")
    print("   • Jobs & Pipelines → Job Runs")
    print("   • Check email for failure alerts")
    print()
    print("5. Recommended execution order for first run:")
    print("   1. Bronze Data Generation")
    print("   2. Silver Transformation")
    print("   3. Gold Analytics")
    print("   4. ML Predictions")
    print("   5. Data Quality Monitoring")
    print("   6. Pipeline Monitoring")
    print()
    print("=" * 70)
    print("✨ Happy Data Engineering! ✨")
    print("=" * 70)
    
    return 0

if __name__ == "__main__":
    exit(main())

