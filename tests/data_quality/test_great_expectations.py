"""
Data Quality Tests using Great Expectations Patterns
Tests: 19-20
"""
import pytest
from decimal import Decimal


@pytest.mark.data_quality
class TestCustomerDataQuality:
    """Test customer data quality expectations"""

    def test_customer_data_completeness(self, sample_customer_data, data_quality_thresholds):
        """Test 19: Validate customer data completeness"""
        # Check for null/missing values
        required_fields = [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "date_of_birth",
        ]

        null_count = sum(1 for field in required_fields if not sample_customer_data.get(field))
        completeness_rate = (len(required_fields) - null_count) / len(required_fields)

        assert completeness_rate >= data_quality_thresholds["min_completeness"], (
            f"Completeness rate {completeness_rate:.2%} below threshold "
            f"{data_quality_thresholds['min_completeness']:.2%}"
        )

        # Validate email format
        email = sample_customer_data["email"]
        assert "@" in email and "." in email, "Email should be valid format"

        # Validate phone format (digits only after cleaning)
        phone = sample_customer_data["phone"].replace("-", "").replace(" ", "")
        assert len(phone) == 10 or len(phone) == 11, "Phone should have 10-11 digits"

    def test_claims_data_integrity(self, sample_claim_data, sample_policy_data):
        """Test 20: Validate claims data integrity and business rules"""
        # Test referential integrity
        assert (
            sample_claim_data["policy_id"] == sample_policy_data["policy_id"]
        ), "Claim must reference valid policy"

        assert (
            sample_claim_data["customer_id"] == sample_policy_data["customer_id"]
        ), "Claim customer must match policy customer"

        # Test business rules
        claim_amount = sample_claim_data["claim_amount"]
        coverage_amount = sample_policy_data["coverage_amount"]

        assert claim_amount > 0, "Claim amount must be positive"
        assert claim_amount <= coverage_amount * Decimal("1.5"), (
            "Claim amount should not exceed 150% of coverage " "(allows for additional expenses)"
        )

        # Test date logic
        claim_date = sample_claim_data["claim_date"]
        incident_date = sample_claim_data["incident_date"]
        policy_start = sample_policy_data["start_date"]

        assert incident_date <= claim_date, "Incident date must be before or equal to claim date"
        assert incident_date >= policy_start, "Incident must occur after policy start"

        # Test status values
        valid_statuses = ["Pending", "Approved", "Denied", "Under Review", "Paid"]
        assert sample_claim_data["claim_status"] in valid_statuses, "Claim status must be valid"

