"""
Pytest Configuration and Shared Fixtures
"""

import os
from datetime import datetime, timedelta
from decimal import Decimal

import pytest


# Test Data Fixtures
@pytest.fixture
def sample_customer_data():
    """Sample customer data for testing"""
    return {
        "customer_id": "CUST001",
        "first_name": "John",
        "last_name": "Doe",
        "email": "john.doe@example.com",
        "phone": "555-123-4567",
        "date_of_birth": "1980-01-01",
        "ssn": "123-45-6789",
        "address": "123 Main St",
        "city": "New York",
        "state": "NY",
        "zip_code": "10001",
        "created_date": datetime.now(),
    }


@pytest.fixture
def sample_policy_data():
    """Sample policy data for testing"""
    return {
        "policy_id": "POL001",
        "customer_id": "CUST001",
        "policy_type": "Auto",
        "policy_status": "Active",
        "coverage_amount": Decimal("500000.00"),
        "premium_amount": Decimal("1200.00"),
        "start_date": datetime.now(),
        "end_date": datetime.now() + timedelta(days=365),
        "agent_id": "AGT001",
    }


@pytest.fixture
def sample_claim_data():
    """Sample claim data for testing"""
    return {
        "claim_id": "CLM001",
        "policy_id": "POL001",
        "customer_id": "CUST001",
        "claim_type": "Auto Collision",
        "claim_amount": Decimal("15000.00"),
        "claim_date": datetime.now(),
        "claim_status": "Pending",
        "incident_description": "Minor collision at intersection",
        "incident_date": datetime.now() - timedelta(days=2),
    }


@pytest.fixture
def sample_fraud_indicators():
    """Sample fraud indicators for testing"""
    return {
        "claim_id": "CLM001",
        "excessive_claim_amount": True,
        "short_policy_tenure": False,
        "multiple_recent_claims": False,
        "suspicious_timing": True,
        "conflicting_statements": False,
        "missing_documentation": True,
        "prior_fraud_history": False,
        "unusual_claim_pattern": True,
    }


# Spark Session Fixture (for integration tests)
@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing"""
    try:
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.appName("InsuranceTests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.default.parallelism", "2")
            .getOrCreate()
        )
        yield spark
        spark.stop()
    except ImportError:
        pytest.skip("PySpark not available")


# Data Quality Thresholds
@pytest.fixture
def data_quality_thresholds():
    """Data quality thresholds for validation"""
    return {
        "min_completeness": 0.95,  # 95% of records must be complete
        "max_null_percentage": 0.05,  # Max 5% nulls allowed
        "min_uniqueness": 0.99,  # 99% unique for key columns
        "max_duplicate_percentage": 0.01,  # Max 1% duplicates
        "min_accuracy": 0.98,  # 98% accuracy for business rules
    }


# Mock Environment Variables
@pytest.fixture(autouse=True)
def mock_env_vars(monkeypatch):
    """Set mock environment variables for tests"""
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.databricks.com")
    monkeypatch.setenv("DATABRICKS_TOKEN", "test_token")
    monkeypatch.setenv("CATALOG_NAME", "insurance_test")
    monkeypatch.setenv("ENVIRONMENT", "test")
