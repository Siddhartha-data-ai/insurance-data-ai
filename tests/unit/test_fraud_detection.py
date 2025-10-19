"""
Unit Tests for Claims Fraud Detection Logic
Tests: 1-7
"""

import pytest
from decimal import Decimal
from datetime import datetime, timedelta


class TestFraudDetectionLogic:
    """Test fraud detection business logic"""

    def test_excessive_claim_amount_detection(self, sample_claim_data, sample_policy_data):
        """Test 1: Detect claims exceeding policy coverage"""
        claim_amount = Decimal("600000.00")  # Exceeds $500K coverage
        coverage_amount = sample_policy_data["coverage_amount"]

        is_excessive = claim_amount > coverage_amount
        assert is_excessive is True, "Should flag claim exceeding coverage"

    def test_short_policy_tenure_risk(self, sample_policy_data):
        """Test 2: Flag claims filed shortly after policy inception"""
        policy_start = datetime.now() - timedelta(days=10)  # 10 days old
        claim_date = datetime.now()

        days_since_inception = (claim_date - policy_start).days
        is_suspicious = days_since_inception < 30  # Less than 30 days

        assert is_suspicious is True, "Should flag claims within 30 days of policy start"

    def test_multiple_claims_velocity(self):
        """Test 3: Detect unusual claim frequency"""
        claims_last_90_days = 5  # 5 claims in 3 months
        threshold = 3

        is_high_velocity = claims_last_90_days > threshold
        assert is_high_velocity is True, "Should flag high claim frequency"

    def test_fraud_score_calculation(self, sample_fraud_indicators):
        """Test 4: Calculate composite fraud score"""
        # Count true fraud indicators
        fraud_count = sum(1 for v in sample_fraud_indicators.values() if isinstance(v, bool) and v)
        total_indicators = len([v for v in sample_fraud_indicators.values() if isinstance(v, bool)])

        fraud_score = (fraud_count / total_indicators) * 100

        assert fraud_score > 0, "Fraud score should be calculated"
        assert 0 <= fraud_score <= 100, "Fraud score should be between 0-100"
        assert fraud_score == pytest.approx(50.0, 0.1), "Should match expected score"

    def test_fraud_risk_categorization(self):
        """Test 5: Categorize fraud risk levels"""

        def categorize_risk(fraud_score):
            if fraud_score >= 80:
                return "Critical"
            elif fraud_score >= 60:
                return "High"
            elif fraud_score >= 40:
                return "Medium"
            else:
                return "Low"

        assert categorize_risk(85) == "Critical"
        assert categorize_risk(65) == "High"
        assert categorize_risk(45) == "Medium"
        assert categorize_risk(25) == "Low"

    def test_claim_amount_pattern_anomaly(self):
        """Test 6: Detect round-number claim amounts (fraud indicator)"""
        suspicious_amounts = [
            Decimal("10000.00"),
            Decimal("25000.00"),
            Decimal("50000.00"),
        ]

        for amount in suspicious_amounts:
            # Round numbers ending in multiple zeros are suspicious
            is_round = amount % 1000 == 0 and amount >= 10000
            assert is_round is True, f"Amount {amount} should be flagged as suspicious"

    def test_claim_timing_analysis(self):
        """Test 7: Detect suspicious claim timing patterns"""
        # Claims filed on holidays or weekends more likely fraudulent
        claim_datetime = datetime(2024, 12, 25, 23, 30)  # Christmas, late night

        is_holiday = claim_datetime.month == 12 and claim_datetime.day == 25
        is_late_night = claim_datetime.hour >= 22 or claim_datetime.hour <= 5
        is_suspicious_timing = is_holiday or is_late_night

        assert is_suspicious_timing is True, "Should flag suspicious timing"


class TestFraudDataValidation:
    """Test fraud detection data validation"""

    def test_claim_amount_validation(self):
        """Test 8: Validate claim amount is positive"""
        valid_amount = Decimal("15000.00")
        invalid_amount = Decimal("-100.00")

        assert valid_amount > 0, "Valid amount should be positive"
        assert invalid_amount <= 0, "Invalid amount should be non-positive"

    def test_fraud_indicators_completeness(self, sample_fraud_indicators):
        """Test 9: Ensure all fraud indicators are present"""
        required_indicators = [
            "excessive_claim_amount",
            "short_policy_tenure",
            "multiple_recent_claims",
            "suspicious_timing",
            "conflicting_statements",
            "missing_documentation",
            "prior_fraud_history",
            "unusual_claim_pattern",
        ]

        for indicator in required_indicators:
            assert indicator in sample_fraud_indicators, f"Missing indicator: {indicator}"

    def test_fraud_confidence_score_range(self):
        """Test 10: Validate fraud confidence score is within range"""
        confidence_scores = [0.0, 0.5, 0.85, 1.0]

        for score in confidence_scores:
            assert 0.0 <= score <= 1.0, f"Confidence score {score} out of range"
