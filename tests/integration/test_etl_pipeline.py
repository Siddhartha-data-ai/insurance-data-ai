"""
Integration Tests for Insurance ETL Pipeline
Tests: 16-18
"""

import pytest
from decimal import Decimal
from datetime import datetime


@pytest.mark.integration
class TestInsuranceETLPipeline:
    """Test end-to-end ETL pipeline integration"""

    def test_bronze_to_silver_customer_transformation(self, spark_session):
        """Test 16: Transform bronze customer data to silver layer"""
        # Create sample bronze data
        bronze_data = [
            ("CUST001", "john doe", "JOHN.DOE@EMAIL.COM", "5551234567", "1980-01-01"),
            ("CUST002", "jane smith", "jane.smith@email.com", "555-987-6543", "1990-05-15"),
        ]

        bronze_df = spark_session.createDataFrame(bronze_data, ["customer_id", "name", "email", "phone", "dob"])

        # Apply silver transformation logic
        from pyspark.sql.functions import col, lower, trim, regexp_replace

        silver_df = bronze_df.select(
            col("customer_id"),
            trim(col("name")).alias("customer_name"),
            lower(trim(col("email"))).alias("email"),
            regexp_replace(col("phone"), "[^0-9]", "").alias("phone_clean"),
            col("dob").alias("date_of_birth"),
        )

        result = silver_df.collect()

        # Assertions
        assert len(result) == 2, "Should have 2 customers"
        assert result[0]["email"] == "john.doe@email.com", "Email should be lowercase"
        assert result[0]["phone_clean"] == "5551234567", "Phone should be cleaned"
        assert result[1]["phone_clean"] == "5559876543", "Phone should remove hyphens"

    def test_claims_fraud_enrichment_pipeline(self, spark_session):
        """Test 17: Enrich claims with fraud scores"""
        # Sample claims data
        claims_data = [
            ("CLM001", "POL001", Decimal("50000.00"), "2024-01-15", 10),  # 10 days after policy
            ("CLM002", "POL002", Decimal("5000.00"), "2024-06-20", 180),  # 180 days after
        ]

        claims_df = spark_session.createDataFrame(
            claims_data, ["claim_id", "policy_id", "claim_amount", "claim_date", "policy_age_days"]
        )

        # Calculate fraud indicators
        from pyspark.sql.functions import when

        enriched_df = claims_df.withColumn(
            "short_tenure_flag", when(col("policy_age_days") < 30, True).otherwise(False)
        ).withColumn("high_amount_flag", when(col("claim_amount") > 25000, True).otherwise(False))

        result = enriched_df.collect()

        # Assertions
        assert result[0]["short_tenure_flag"] is True, "Should flag short tenure"
        assert result[0]["high_amount_flag"] is True, "Should flag high amount"
        assert result[1]["short_tenure_flag"] is False, "Should not flag normal tenure"
        assert result[1]["high_amount_flag"] is False, "Should not flag normal amount"

    def test_gold_layer_customer_360_aggregation(self, spark_session):
        """Test 18: Create Customer 360 aggregation"""
        # Sample customer data with policies and claims
        customer_360_data = [
            ("CUST001", 3, Decimal("3600.00"), 2, Decimal("25000.00"), 720),  # 3 policies, 2 claims
            ("CUST002", 1, Decimal("1200.00"), 0, Decimal("0.00"), 650),  # 1 policy, 0 claims
        ]

        df = spark_session.createDataFrame(
            customer_360_data,
            [
                "customer_id",
                "total_policies",
                "total_premium",
                "total_claims",
                "total_claim_amount",
                "credit_score",
            ],
        )

        # Calculate customer metrics
        from pyspark.sql.functions import round as spark_round

        customer_360_df = df.withColumn(
            "claims_ratio", spark_round(col("total_claims") / col("total_policies"), 2)
        ).withColumn(
            "loss_ratio", spark_round(col("total_claim_amount") / (col("total_premium") * 10), 2)
        )  # Assuming 10-year horizon

        result = customer_360_df.collect()

        # Assertions
        assert result[0]["claims_ratio"] == pytest.approx(0.67, 0.01), "Claims ratio should be ~0.67"
        assert result[1]["claims_ratio"] == 0.0, "Claims ratio should be 0 for no claims"
        assert result[0]["total_policies"] == 3, "Should have 3 policies"
