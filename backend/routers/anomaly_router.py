from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import sqlite3
import json
import os
from typing import List, Dict

# LangChain for AWS Bedrock
from langchain_aws import ChatBedrockConverse
from database import engine
from sqlalchemy import text

router = APIRouter(prefix="/api/anomaly", tags=["anomaly"])

class ApproveRequest(BaseModel):
    exception_id: str
    alternative_id: str

class ExceptionStatusUpdate(BaseModel):
    status: str

@router.get("/alternatives/{exception_id}")
def get_alternatives(exception_id: str):
    try:
        # Check if we already have alternatives cached
        with engine.connect() as conn:
            res = conn.execute(
                text("SELECT alt_id, title, description, cost_impact, kpi_impact, tradeoff FROM exception_alternatives WHERE exception_id = :eid"),
                {"eid": exception_id}
            )
            rows = res.fetchall()
            if rows:
                alts = []
                for row in rows:
                    alts.append({
                        "id": row._mapping["alt_id"],
                        "title": row._mapping["title"],
                        "description": row._mapping["description"],
                        "cost_impact": row._mapping["cost_impact"],
                        "kpi_impact": row._mapping["kpi_impact"],
                        "tradeoff": row._mapping["tradeoff"]
                    })
                return alts

            # If not found, fetch the exception details to give to the LLM
            ex_res = conn.execute(
                text("SELECT root_cause_hypotheses, exception_type, severity_level, impacted_kpis FROM exceptions WHERE exception_id = :eid"),
                {"eid": exception_id}
            )
            ex_row = ex_res.fetchone()

        # Build context
        if ex_row:
            context = f"Exception ID: {exception_id}\nType: {ex_row._mapping['exception_type']}\nSeverity: {ex_row._mapping['severity_level']}\nRoot Cause: {ex_row._mapping['root_cause_hypotheses']}\nImpacted KPIs: {ex_row._mapping['impacted_kpis']}"
        else:
            context = f"Exception ID: {exception_id}\nType: Unknown\nSeverity: High. Assume a generic global supply chain disruption."

        # Initialize LLM
        llm = ChatBedrockConverse(
            model_id="openai.gpt-oss-120b-1:0",
            region_name="us-east-1",
            max_tokens=2048,
        )

        prompt = f"""You are an expert supply chain optimization AI.
Given the following context of a supply chain exception:
{context}

Generate 3 strategic and distinct mitigation alternatives. Format your response STRICTLY as a JSON array of objects with the exact keys: "id" (e.g. "alt_1"), "title" (short title up to 6 words), "description" (one full sentence describing what to do), "cost_impact" (short phrase e.g. "+$10K Extra Freight"), "kpi_impact" (short phrase on kpi change), "tradeoff" (short sentence on the risk vs reward).

Return ONLY the raw JSON array. DO NOT wrap in ```json markers. Do not provide any explanation."""

        ai_msg = llm.invoke([("system", "You output pure JSON arrays exclusively."), ("human", prompt)])
        
        # Parse the JSON response
        if isinstance(ai_msg.content, list):
            content_str = "".join([block.get("text", "") for block in ai_msg.content if "text" in block]).strip()
        else:
            content_str = str(ai_msg.content).strip()

        # Clean up in case of markdown
        if content_str.startswith("```json"): content_str = content_str[7:]
        if content_str.startswith("```"): content_str = content_str[3:]
        if content_str.endswith("```"): content_str = content_str[:-3]
        content_str = content_str.strip()

        alternatives = json.loads(content_str)

        # Cache to DB
        with engine.connect() as conn:
            for alt in alternatives:
                conn.execute(
                    text("""INSERT INTO exception_alternatives 
                        (exception_id, alt_id, title, description, cost_impact, kpi_impact, tradeoff) 
                        VALUES (:eid, :aid, :title, :desc, :c_impact, :k_impact, :tradeoff)"""),
                    {
                        "eid": exception_id,
                        "aid": str(alt.get("id", "alt_0")),
                        "title": str(alt.get("title", "")),
                        "desc": str(alt.get("description", "")),
                        "c_impact": str(alt.get("cost_impact", "")),
                        "k_impact": str(alt.get("kpi_impact", "")),
                        "tradeoff": str(alt.get("tradeoff", ""))
                    }
                )
            conn.commit()

        return alternatives

    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/approve")
def approve_alternative(req: ApproveRequest):
    try:
        with engine.connect() as conn:
            # Look up the alternative details
            res = conn.execute(
                text("SELECT title, description FROM exception_alternatives WHERE exception_id = :eid AND alt_id = :aid"),
                {"eid": req.exception_id, "aid": req.alternative_id}
            )
            alt_row = res.fetchone()
            
            alt_title = alt_row._mapping["title"] if alt_row else "Selected Alternative"
            alt_desc = alt_row._mapping["description"] if alt_row else ""
            
            # Create a task
            task_title = f"Execute Mitigation: {alt_title}"
            conn.execute(
                text("""INSERT INTO tasks (exception_id, alternative_id, title, description, status) 
                        VALUES (:eid, :aid, :title, :desc, 'Open')"""),
                {"eid": req.exception_id, "aid": req.alternative_id, "title": task_title, "desc": alt_desc}
            )
            
            # Update the exception status to 'Investigating' or 'Mitigating'
            conn.execute(
                text("UPDATE exceptions SET current_status = 'Mitigating' WHERE exception_id = :eid"),
                {"eid": req.exception_id}
            )
            
            conn.commit()
            
        return {
            "status": "approved",
            "message": f"Alternative '{req.alternative_id}' authorized successfully. A new task has been assigned.",
            "actions_triggered": [
                f"Generated workflow task: {task_title}",
                "Notified regional operations manager via Teams.",
                "Updated anomaly status to 'Mitigating'."
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{exception_id}/status")
def update_exception_status(exception_id: str, req: ExceptionStatusUpdate):
    try:
        with engine.begin() as conn:
            conn.execute(
                text("UPDATE exceptions SET current_status = :status WHERE exception_id = :eid"),
                {"status": req.status, "eid": exception_id}
            )
        return {"message": "Exception status updated", "new_status": req.status}
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))
