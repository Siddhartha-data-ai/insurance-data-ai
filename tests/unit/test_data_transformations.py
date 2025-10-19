"""
Unit Tests for Data Transformation Logic
Tests: 11-15
"""
import pytest
from decimal import Decimal
from datetime import datetime, timedelta


class TestPolicyTransformations:
    """Test policy data transformation logic"""

    def test_premium_calculation(self):
        """Test 11: Calculate annual premium based on risk factors"""
        base_premium = Decimal("1000.00")
        risk_multiplier = Decimal("1.2")  # 20% risk increase
        discount = Decimal("0.1")  # 10% discount

        final_premium = base_premium * risk_multiplier * (1 - discount)
        expected = Decimal("1080.00")

        assert final_premium == expected, "Premium calculation should be accurate"

    def test_policy_status_derivation(self):
        """Test 12: Derive policy status based on dates and payments"""
        current_date = datetime.now()
        policy_end_date = current_date + timedelta(days=30)
        last_payment_date = current_date - timedelta(days=60)

        # Policy is active but payment overdue
        is_active = policy_end_date > current_date
        is_payment_overdue = (current_date - last_payment_date).days > 45

        assert is_active is True, "Policy should be active"
        assert is_payment_overdue is True, "Payment should be overdue"

    def test_coverage_amount_standardization(self):
        """Test 13: Standardize coverage amounts to thousands"""
        raw_amounts = [
            Decimal("123456.78"),
            Decimal("500000.00"),
            Decimal("999999.99"),
        ]

        standardized = [int(amount / 1000) * 1000 for amount in raw_amounts]

        assert standardized[0] == 123000, "Should round down to nearest thousand"
        assert standardized[1] == 500000, "Should remain same"
        assert standardized[2] == 999000, "Should round down"

    def test_policy_age_calculation(self, sample_policy_data):
        """Test 14: Calculate policy age in days"""
        start_date = datetime.now() - timedelta(days=180)
        current_date = datetime.now()

        policy_age_days = (current_date - start_date).days

        assert policy_age_days == 180, "Policy age should be 180 days"
        assert policy_age_days >= 0, "Policy age cannot be negative"

    def test_customer_name_standardization(self, sample_customer_data):
        """Test 15: Standardize customer name format"""
        first_name = sample_customer_data["first_name"].strip().title()
        last_name = sample_customer_data["last_name"].strip().title()
        full_name = f"{first_name} {last_name}"

        assert full_name == "John Doe", "Name should be properly formatted"
        assert " " in full_name, "Full name should contain space"

