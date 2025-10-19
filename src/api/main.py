"""
REST API for Insurance Data AI Platform using FastAPI

Provides:
- Quote generation and retrieval
- Policy management
- Claims submission and tracking
- Customer data access
- ML model predictions (fraud, churn, premium optimization)
"""

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, EmailStr
from typing import Optional, List
from datetime import datetime, date
from decimal import Decimal
import os

# Initialize FastAPI app
app = FastAPI(
    title="Insurance Data AI API",
    description="Enterprise REST API for insurance analytics and ML predictions",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc"
)

security = HTTPBearer()

# =====================================================
# Pydantic Models
# =====================================================

class Customer(BaseModel):
    customer_id: str
    full_name: str
    email: EmailStr
    phone: Optional[str] = None
    credit_score: Optional[int] = Field(None, ge=300, le=850)
    customer_segment: Optional[str] = None
    created_at: datetime

class QuoteRequest(BaseModel):
    customer_id: str
    policy_type: str = Field(..., description="AUTO, HOME, LIFE, HEALTH")
    coverage_amount: float = Field(..., gt=0)
    deductible: float = Field(..., gt=0)
    vehicle_year: Optional[int] = None
    vehicle_make: Optional[str] = None
    vehicle_model: Optional[str] = None
    home_value: Optional[float] = None
    zip_code: str
    requested_start_date: date

class QuoteResponse(BaseModel):
    quote_id: str
    customer_id: str
    policy_type: str
    annual_premium: float
    monthly_premium: float
    coverage_amount: float
    deductible: float
    discount_applied: float
    risk_tier: str
    quote_valid_until: datetime
    created_at: datetime

class Policy(BaseModel):
    policy_id: str
    policy_number: str
    customer_id: str
    policy_type: str
    policy_status: str
    annual_premium: float
    coverage_amount: float
    start_date: date
    end_date: date
    is_auto_renew: bool

class ClaimSubmission(BaseModel):
    policy_id: str
    claim_type: str
    incident_date: date
    claim_description: str
    claimed_amount: float = Field(..., gt=0)
    location_city: Optional[str] = None
    location_state: Optional[str] = None
    injury_count: int = Field(0, ge=0)
    has_police_report: bool = False
    witness_count: int = Field(0, ge=0)

class ClaimResponse(BaseModel):
    claim_id: str
    claim_number: str
    policy_id: str
    claim_status: str
    claimed_amount: float
    estimated_payout: Optional[float] = None
    assigned_adjuster: Optional[str] = None
    estimated_resolution_days: Optional[int] = None
    created_at: datetime

class FraudPredictionRequest(BaseModel):
    claim_id: str
    claimed_amount: float
    claim_type: str
    days_since_policy_start: int
    previous_claims_count: int = Field(0, ge=0)
    injury_count: int = Field(0, ge=0)

class FraudPredictionResponse(BaseModel):
    claim_id: str
    fraud_score: float = Field(..., ge=0, le=100)
    fraud_risk_category: str
    is_high_risk: bool
    risk_factors: List[str]
    recommended_action: str

class PremiumOptimizationRequest(BaseModel):
    policy_id: str
    customer_id: str
    current_premium: float
    policy_type: str
    claims_history_count: int = 0
    customer_tenure_years: float = 0
    credit_score: Optional[int] = None

class PremiumOptimizationResponse(BaseModel):
    policy_id: str
    current_premium: float
    recommended_premium: float
    premium_change_percent: float
    rationale: str
    estimated_retention_impact: str

# =====================================================
# Auth Dependency
# =====================================================

async def verify_token(credentials: HTTPAuthorizationCredentials = Depends(security)):
    """Verify API token"""
    token = credentials.credentials
    # In production, verify against actual auth system
    if token != os.getenv("API_SECRET_KEY", "dev-secret-key"):
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    return token

# =====================================================
# Health Check
# =====================================================

@app.get("/health", tags=["Health"])
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "version": "1.0.0",
        "service": "Insurance Data AI API"
    }

# =====================================================
# Customer Endpoints
# =====================================================

@app.get("/api/v1/customers/{customer_id}", response_model=Customer, tags=["Customers"])
async def get_customer(
    customer_id: str,
    token: str = Depends(verify_token)
):
    """Get customer by ID"""
    # In production, query from Databricks
    return {
        "customer_id": customer_id,
        "full_name": "Jane Doe",
        "email": "jane@example.com",
        "phone": "555-0100",
        "credit_score": 780,
        "customer_segment": "PREMIUM",
        "created_at": datetime.now()
    }

@app.get("/api/v1/customers/{customer_id}/policies", response_model=List[Policy], tags=["Customers"])
async def get_customer_policies(
    customer_id: str,
    status: Optional[str] = None,
    token: str = Depends(verify_token)
):
    """Get all policies for a customer"""
    # In production, query from Databricks
    return []

# =====================================================
# Quote Endpoints
# =====================================================

@app.post("/api/v1/quotes", response_model=QuoteResponse, tags=["Quotes"])
async def create_quote(
    request: QuoteRequest,
    token: str = Depends(verify_token)
):
    """Generate insurance quote"""
    # Calculate premium (simplified)
    base_premium = request.coverage_amount * 0.02  # 2% of coverage
    
    # Apply risk factors
    risk_multiplier = 1.0
    if request.policy_type == "AUTO" and request.vehicle_year and request.vehicle_year < 2010:
        risk_multiplier *= 1.2
    
    annual_premium = base_premium * risk_multiplier
    discount = 0.10 if annual_premium > 2000 else 0.05
    final_premium = annual_premium * (1 - discount)
    
    return {
        "quote_id": f"QTE-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        "customer_id": request.customer_id,
        "policy_type": request.policy_type,
        "annual_premium": round(final_premium, 2),
        "monthly_premium": round(final_premium / 12, 2),
        "coverage_amount": request.coverage_amount,
        "deductible": request.deductible,
        "discount_applied": discount,
        "risk_tier": "STANDARD",
        "quote_valid_until": datetime.now(),
        "created_at": datetime.now()
    }

@app.get("/api/v1/quotes/{quote_id}", response_model=QuoteResponse, tags=["Quotes"])
async def get_quote(
    quote_id: str,
    token: str = Depends(verify_token)
):
    """Retrieve quote by ID"""
    # In production, query from Databricks
    raise HTTPException(status_code=404, detail="Quote not found")

# =====================================================
# Policy Endpoints
# =====================================================

@app.get("/api/v1/policies/{policy_id}", response_model=Policy, tags=["Policies"])
async def get_policy(
    policy_id: str,
    token: str = Depends(verify_token)
):
    """Get policy by ID"""
    # In production, query from Databricks
    return {
        "policy_id": policy_id,
        "policy_number": f"POL-{policy_id}",
        "customer_id": "CUST-12345",
        "policy_type": "AUTO",
        "policy_status": "ACTIVE",
        "annual_premium": 1200.00,
        "coverage_amount": 100000.00,
        "start_date": date.today(),
        "end_date": date.today(),
        "is_auto_renew": True
    }

@app.put("/api/v1/policies/{policy_id}/status", tags=["Policies"])
async def update_policy_status(
    policy_id: str,
    new_status: str,
    token: str = Depends(verify_token)
):
    """Update policy status"""
    valid_statuses = ["ACTIVE", "SUSPENDED", "CANCELLED", "EXPIRED"]
    if new_status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    return {
        "policy_id": policy_id,
        "new_status": new_status,
        "updated_at": datetime.now()
    }

# =====================================================
# Claims Endpoints
# =====================================================

@app.post("/api/v1/claims", response_model=ClaimResponse, tags=["Claims"])
async def submit_claim(
    claim: ClaimSubmission,
    token: str = Depends(verify_token)
):
    """Submit new insurance claim"""
    claim_id = f"CLM-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    claim_number = f"CLAIM-{claim_id}"
    
    # Auto-assign severity and adjuster (simplified)
    if claim.claimed_amount > 25000 or claim.injury_count > 0:
        assigned_adjuster = "SENIOR_ADJUSTER"
        estimated_days = 21
    elif claim.claimed_amount > 10000:
        assigned_adjuster = "EXPERIENCED_ADJUSTER"
        estimated_days = 14
    else:
        assigned_adjuster = "STANDARD_ADJUSTER"
        estimated_days = 7
    
    return {
        "claim_id": claim_id,
        "claim_number": claim_number,
        "policy_id": claim.policy_id,
        "claim_status": "PENDING_REVIEW",
        "claimed_amount": claim.claimed_amount,
        "estimated_payout": claim.claimed_amount * 0.85,  # Estimated after deductible
        "assigned_adjuster": assigned_adjuster,
        "estimated_resolution_days": estimated_days,
        "created_at": datetime.now()
    }

@app.get("/api/v1/claims/{claim_id}", response_model=ClaimResponse, tags=["Claims"])
async def get_claim(
    claim_id: str,
    token: str = Depends(verify_token)
):
    """Get claim by ID"""
    # In production, query from Databricks
    raise HTTPException(status_code=404, detail="Claim not found")

@app.get("/api/v1/claims/policy/{policy_id}", response_model=List[ClaimResponse], tags=["Claims"])
async def get_policy_claims(
    policy_id: str,
    status: Optional[str] = None,
    token: str = Depends(verify_token)
):
    """Get all claims for a policy"""
    # In production, query from Databricks
    return []

# =====================================================
# ML Model Endpoints - Fraud Detection
# =====================================================

@app.post("/api/v1/ml/fraud/predict", response_model=FraudPredictionResponse, tags=["ML Models"])
async def predict_fraud(
    request: FraudPredictionRequest,
    token: str = Depends(verify_token)
):
    """Predict fraud probability for a claim"""
    # Calculate fraud score (simplified)
    fraud_score = 10.0
    risk_factors = []
    
    # High claim amount
    if request.claimed_amount > 50000:
        fraud_score += 30
        risk_factors.append("High claim amount (>$50K)")
    elif request.claimed_amount > 25000:
        fraud_score += 20
        risk_factors.append("Elevated claim amount (>$25K)")
    
    # New policy
    if request.days_since_policy_start < 90:
        fraud_score += 25
        risk_factors.append("Recent policy inception (<90 days)")
    
    # Claim history
    if request.previous_claims_count > 3:
        fraud_score += 20
        risk_factors.append(f"Multiple previous claims ({request.previous_claims_count})")
    
    # Injury claims
    if request.injury_count > 2:
        fraud_score += 15
        risk_factors.append(f"Multiple injuries ({request.injury_count})")
    
    fraud_score = min(fraud_score, 100.0)
    is_high_risk = fraud_score > 70
    
    if fraud_score >= 80:
        risk_category = "CRITICAL"
        action = "IMMEDIATE_INVESTIGATION"
    elif fraud_score >= 60:
        risk_category = "HIGH"
        action = "DETAILED_REVIEW"
    elif fraud_score >= 40:
        risk_category = "MEDIUM"
        action = "STANDARD_VERIFICATION"
    else:
        risk_category = "LOW"
        action = "STANDARD_PROCESSING"
    
    return {
        "claim_id": request.claim_id,
        "fraud_score": round(fraud_score, 2),
        "fraud_risk_category": risk_category,
        "is_high_risk": is_high_risk,
        "risk_factors": risk_factors,
        "recommended_action": action
    }

# =====================================================
# ML Model Endpoints - Premium Optimization
# =====================================================

@app.post("/api/v1/ml/premium/optimize", response_model=PremiumOptimizationResponse, tags=["ML Models"])
async def optimize_premium(
    request: PremiumOptimizationRequest,
    token: str = Depends(verify_token)
):
    """Optimize premium for a policy"""
    current = request.current_premium
    
    # Calculate optimization factors
    adjustment = 1.0
    rationale_parts = []
    
    # Tenure discount
    if request.customer_tenure_years > 5:
        adjustment *= 0.90
        rationale_parts.append("5+ year loyalty discount")
    elif request.customer_tenure_years > 3:
        adjustment *= 0.95
        rationale_parts.append("3+ year loyalty discount")
    
    # Claims history penalty
    if request.claims_history_count > 2:
        adjustment *= 1.15
        rationale_parts.append("Multiple claims surcharge")
    elif request.claims_history_count == 0:
        adjustment *= 0.90
        rationale_parts.append("Claim-free discount")
    
    # Credit score discount
    if request.credit_score and request.credit_score > 750:
        adjustment *= 0.92
        rationale_parts.append("Excellent credit discount")
    
    recommended = current * adjustment
    change_percent = ((recommended - current) / current) * 100
    
    if abs(change_percent) < 5:
        retention_impact = "NEUTRAL"
    elif change_percent < 0:
        retention_impact = "POSITIVE"
    else:
        retention_impact = "AT_RISK"
    
    return {
        "policy_id": request.policy_id,
        "current_premium": round(current, 2),
        "recommended_premium": round(recommended, 2),
        "premium_change_percent": round(change_percent, 2),
        "rationale": "; ".join(rationale_parts) if rationale_parts else "No adjustments recommended",
        "estimated_retention_impact": retention_impact
    }

# =====================================================
# Metrics Endpoints
# =====================================================

@app.get("/api/v1/metrics/claims", tags=["Metrics"])
async def get_claims_metrics(
    days: int = 30,
    token: str = Depends(verify_token)
):
    """Get claims metrics"""
    return {
        "period_days": days,
        "total_claims": 4250,
        "total_claimed_amount": 12500000.00,
        "total_paid_amount": 9800000.00,
        "avg_claim_amount": 2941.18,
        "avg_processing_days": 12.5,
        "fraud_detection_rate": 0.08,
        "approval_rate": 0.87
    }

@app.get("/api/v1/metrics/policies", tags=["Metrics"])
async def get_policy_metrics(
    policy_type: Optional[str] = None,
    token: str = Depends(verify_token)
):
    """Get policy metrics"""
    return {
        "policy_type": policy_type or "ALL",
        "active_policies": 125000,
        "total_premium_value": 185000000.00,
        "avg_premium": 1480.00,
        "retention_rate": 0.92,
        "new_policies_this_month": 3200
    }

# =====================================================
# Run Server
# =====================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

