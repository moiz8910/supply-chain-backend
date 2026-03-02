from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(prefix="/api/anomaly", tags=["anomaly"])

class ApproveRequest(BaseModel):
    alternative_id: str

@router.get("/current")
def get_current_anomaly():
    return {
        "id": "AN-2026-001",
        "title": "ISO Tanker Shortage Detected",
        "description": "Critical shortage in ISO tanker availability detected due to an operational suspension at a major regional tank cleaning facility. Predicted to impact out-bound logistics for the next 14 days.",
        "severity": "high",
        "affected_products": [
            "Ethyl acetate",
            "Acetic anhydride",
            "Acetaldehyde",
            "Fuel-grade ethanol",
            "Butyl Acetate"
        ],
        "impacted_orders_count": 42,
        "estimated_value_at_risk": "$1.25M"
    }

@router.get("/impact")
def get_anomaly_impact():
    return [
        {
            "kpi": "OTIF",
            "current": "92.4%",
            "predicted": "81.0%",
            "delta": "-11.4%",
            "status": "critical"
        },
        {
            "kpi": "Order Lead Time",
            "current": "4.2 Days",
            "predicted": "8.5 Days",
            "delta": "+4.3 Days",
            "status": "critical"
        },
        {
            "kpi": "Freight Cost / Unit",
            "current": "$12.50",
            "predicted": "$14.80",
            "delta": "+18.4%",
            "status": "warning"
        },
        {
             "kpi": "Inventory Turnover (FG)",
             "current": "12.5",
             "predicted": "11.2",
             "delta": "-10.4%",
             "status": "warning"
        }
    ]

@router.get("/alternatives")
def get_alternatives():
    return [
        {
            "id": "alt_1",
            "title": "Pre-book / Secure Capacity",
            "description": "Pay a premium to LSPs to secure guaranteed ISO tanker capacity immediately.",
            "cost_impact": "+$120K (Premium)",
            "kpi_impact": "Maintains OTIF at >90%, avoids Lead Time delays.",
            "tradeoff": "High immediate cost, protects critical customer relationships."
        },
        {
            "id": "alt_2",
            "title": "Prioritize Critical Orders",
            "description": "Allocate available tankers only to Tier 1 customers. Delay Tier 2/3.",
            "cost_impact": "Minimal direct cost",
            "kpi_impact": "Overall OTIF drops to 85%. SLA penalties estimated at $45K.",
            "tradeoff": "Lowest upfront cost, but risks long-term satisfaction for non-critical accounts."
        },
        {
            "id": "alt_3",
            "title": "Re-route via Alternate Port",
            "description": "Divert shipments to Port B where ISO tankers are generally more available.",
            "cost_impact": "+$85K (Extra transit)",
            "kpi_impact": "Adds 2 days to Lead Time. OTIF drops to 88%.",
            "tradeoff": "Moderate cost, slight delays, highly dependent on Port B's actual real-time capacity."
        },
        {
            "id": "alt_4",
            "title": "Dispatch Earlier",
            "description": "Push all available inventory out now before the shortage fully hits the market.",
            "cost_impact": "+$60K (Storage at destination)",
            "kpi_impact": "Maintains OTIF. Decreases Inventory Turnover temporarily.",
            "tradeoff": "Requires immediate plant action and coordination with destination warehouses."
        }
    ]

@router.post("/approve")
def approve_alternative(req: ApproveRequest):
    # Simulate processing the approval
    return {
        "status": "approved",
        "message": f"Alternative '{req.alternative_id}' authorized successfully.",
        "actions_triggered": [
            "Communicated predicted delays to affected Tier 2/3 customers.",
            "Issued POs to LSPs (Maersk, DHL) to secure priority ISO tankers.",
            "Updated shipping instructions with carriers.",
            "Adjusted plant loading schedule for accelerated dispatch."
        ]
    }
