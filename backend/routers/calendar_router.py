from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from database import engine
from sqlalchemy import text
from datetime import datetime, timedelta
import json

router = APIRouter(prefix="/api/calendar", tags=["calendar"])


class NewCalendarEvent(BaseModel):
    category: str
    title: str
    date: str
    startHour: int
    duration: float
    owner: Optional[str] = ""
    participants: Optional[List[str]] = []
    decisions: Optional[str] = ""
    agenda: Optional[str] = ""
    attachments: Optional[List[str]] = []
    linkedData: Optional[List[str]] = []


def fmt_date(dt_str):
    if not dt_str:
        return None
    try:
        return str(dt_str)[:10]
    except Exception:
        return None


def fmt_hour(dt_str):
    if not dt_str:
        return 9
    try:
        return int(str(dt_str)[11:13]) or 9
    except Exception:
        return 9


# ── POST: Create a new user-defined calendar event ────────────────────────────
@router.post("/events", status_code=201)
def create_calendar_event(payload: NewCalendarEvent):
    with engine.connect() as conn:
        result = conn.execute(text("""
            INSERT INTO calendar_events
                (category, title, date, start_hour, duration, owner,
                 participants, decisions, agenda, attachments, linked_data)
            VALUES
                (:category, :title, :date, :start_hour, :duration, :owner,
                 :participants, :decisions, :agenda, :attachments, :linked_data)
        """), {
            "category": payload.category,
            "title": payload.title,
            "date": payload.date,
            "start_hour": payload.startHour,
            "duration": payload.duration,
            "owner": payload.owner,
            "participants": json.dumps(payload.participants),
            "decisions": payload.decisions,
            "agenda": payload.agenda,
            "attachments": json.dumps(payload.attachments),
            "linked_data": json.dumps(payload.linkedData),
        })
        conn.commit()
        new_id = result.lastrowid
    return {"success": True, "event_id": new_id}


# ── GET: All calendar events (DB-sourced + user-created) ──────────────────────
@router.get("/events")
def get_calendar_events():
    events = []
    event_id = 1

    with engine.connect() as conn:

        # 1. Logistics: Shipment ETD / ETA
        result = conn.execute(text("""
            SELECT shipment_id, actual_start_date, actual_end_date,
                   mode, shipment_type, to_customer_id, from_supplier_id
            FROM shipments
            WHERE actual_start_date IS NOT NULL
              AND actual_start_date >= date('now', '-60 days')
              AND actual_start_date <= date('now', '+90 days')
            ORDER BY actual_start_date DESC LIMIT 40
        """))
        for row in result.mappings():
            sid = row["shipment_id"]
            etd_date = fmt_date(row["actual_start_date"])
            eta_date = fmt_date(row["actual_end_date"])
            direction = "Inbound" if row["from_supplier_id"] else "Outbound"
            mode = row["mode"] or row["shipment_type"] or "Truck"
            if etd_date:
                events.append({"id": event_id, "category": "logistics",
                    "title": f"Shipment {sid} ETD ({direction})", "date": etd_date,
                    "startHour": fmt_hour(row["actual_start_date"]) or 8, "duration": 1,
                    "owner": "Logistics Team", "participants": ["Logistics Team", "Warehouse"],
                    "decisions": f"Confirm departure of {sid} via {mode}.",
                    "agenda": f"ETD confirmation for {direction.lower()} shipment via {mode}. Verify docs.",
                    "attachments": [], "linkedData": [f"Shipment {sid}", f"Mode: {mode}"]})
                event_id += 1
            if eta_date and eta_date != etd_date:
                events.append({"id": event_id, "category": "logistics",
                    "title": f"Shipment {sid} ETA ({direction})", "date": eta_date,
                    "startHour": fmt_hour(row["actual_end_date"]) or 14, "duration": 1,
                    "owner": "Logistics Team", "participants": ["Logistics Team", "Warehouse"],
                    "decisions": f"Confirm dock slot for {sid}.",
                    "agenda": "ETA monitoring and dock slot booking.",
                    "attachments": [], "linkedData": [f"Shipment {sid}"]})
                event_id += 1

        # 2. Plant: Production runs
        result = conn.execute(text("""
            SELECT pr.production_run_id, pr.sku_id, pr.line_id,
                   pr.start_datetime, pr.end_datetime, pr.quantity_produced,
                   ss.sku_name, ss.product_family
            FROM production_runs pr
            LEFT JOIN sales_sku ss ON pr.sku_id = ss.sku_code
            WHERE pr.start_datetime IS NOT NULL
              AND pr.start_datetime >= date('now', '-60 days')
              AND pr.start_datetime <= date('now', '+90 days')
            ORDER BY pr.start_datetime DESC LIMIT 40
        """))
        for row in result.mappings():
            start_date = fmt_date(row["start_datetime"])
            sku = row["sku_name"] or row["sku_id"] or "SKU"
            line = row["line_id"] or "Line"
            fam = row["product_family"] or ""
            try:
                start_dt = datetime.fromisoformat(str(row["start_datetime"]))
                end_dt = datetime.fromisoformat(str(row["end_datetime"])) if row["end_datetime"] else start_dt + timedelta(hours=4)
                duration = min(max(round((end_dt - start_dt).total_seconds() / 3600, 1), 1), 12)
            except Exception:
                duration = 4
            if start_date:
                events.append({"id": event_id, "category": "plant",
                    "title": f"Campaign: {line} \u2013 {fam or sku}", "date": start_date,
                    "startHour": fmt_hour(row["start_datetime"]) or 7, "duration": duration,
                    "owner": "Plant Operations", "participants": ["Plant Operations", "Quality", "Maintenance"],
                    "decisions": f"Approve production run for {sku}.",
                    "agenda": f"Production run for {sku} on {line}. Qty: {row['quantity_produced']}.",
                    "attachments": ["Changeover_SOP.pdf"],
                    "linkedData": [f"Run {row['production_run_id']}", f"SKU: {sku}", f"Line: {line}"]})
                event_id += 1

        # 3. Planning: MRP plan milestones
        result = conn.execute(text("""
            SELECT pp.production_plan_id, pp.sku_id, pp.line_id,
                   pp.plan_start_date, pp.planned_quantity,
                   ss.sku_name, ss.product_family
            FROM production_plan pp
            LEFT JOIN sales_sku ss ON pp.sku_id = ss.sku_code
            WHERE pp.plan_start_date IS NOT NULL
              AND pp.plan_start_date >= date('now', '-30 days')
              AND pp.plan_start_date <= date('now', '+90 days')
            ORDER BY pp.plan_start_date DESC LIMIT 30
        """))
        for row in result.mappings():
            plan_date = fmt_date(row["plan_start_date"])
            sku = row["sku_name"] or row["sku_id"] or "SKU"
            fam = row["product_family"] or ""
            if plan_date:
                events.append({"id": event_id, "category": "planning",
                    "title": f"MRP Plan Release \u2013 {fam or sku}", "date": plan_date,
                    "startHour": 9, "duration": 1,
                    "owner": "Planning Team", "participants": ["Planning Team", "Production", "Procurement"],
                    "decisions": f"Approve MRP release for {sku}. Qty: {row['planned_quantity']}.",
                    "agenda": f"Review demand signals for {sku}, confirm RM, release MRP.",
                    "attachments": ["MRP_Output.xlsx"],
                    "linkedData": [f"Plan {row['production_plan_id']}", f"SKU: {sku}"]})
                event_id += 1

        # 4. Governance: Active exception reviews
        result = conn.execute(text("""
            SELECT exception_id, exception_type, severity_level,
                   current_status, impacted_entities, alerted_stakeholders
            FROM exceptions
            WHERE current_status NOT IN ('Resolved', 'Closed')
            ORDER BY exception_id DESC LIMIT 20
        """))
        for i, row in enumerate(result.mappings()):
            review_date = (datetime.now() + timedelta(days=(i % 7) - 3)).strftime("%Y-%m-%d")
            exc_type = row["exception_type"] or "Exception"
            severity = row["severity_level"] or "Medium"
            status = row["current_status"] or "Open"
            owner = str(row["alerted_stakeholders"] or "Supply Chain Manager").split(",")[0].strip()
            events.append({"id": event_id, "category": "governance",
                "title": f"Exception Review: {exc_type}", "date": review_date,
                "startHour": 10 + (i % 4), "duration": 1,
                "owner": owner, "participants": [owner, "Supply Chain Team"],
                "decisions": f"Determine mitigation for {severity} exception. Status: {status}.",
                "agenda": f"Root cause and alternatives review. Impacted: {str(row['impacted_entities'] or '')[:80]}.",
                "attachments": [], "linkedData": [f"Exception {row['exception_id']}", f"Type: {exc_type}"]})
            event_id += 1

        # 5. Planning: Pending tasks
        result = conn.execute(text("""
            SELECT task_id, title, description, status, exception_id
            FROM tasks
            WHERE status NOT IN ('Completed', 'Rejected', 'Resolved')
            ORDER BY task_id DESC LIMIT 20
        """))
        for i, row in enumerate(result.mappings()):
            task_date = (datetime.now() + timedelta(days=i % 5)).strftime("%Y-%m-%d")
            events.append({"id": event_id, "category": "planning",
                "title": f"Task: {str(row['title'] or 'Pending Task')[:50]}", "date": task_date,
                "startHour": 14 + (i % 3), "duration": 1,
                "owner": "Task Owner", "participants": ["Task Owner", "Supply Chain Team"],
                "decisions": f"Execute task. Status: {row['status']}.",
                "agenda": str(row["description"] or "Review and action pending task.")[:200],
                "attachments": [], "linkedData": [f"Task #{row['task_id']}"]})
            event_id += 1

        # 6. User-created events
        result = conn.execute(text("""
            SELECT event_id, category, title, date, start_hour, duration,
                   owner, participants, decisions, agenda, attachments, linked_data
            FROM calendar_events ORDER BY date ASC
        """))
        for row in result.mappings():
            try:
                parts = json.loads(row["participants"] or "[]")
            except Exception:
                parts = []
            try:
                attaches = json.loads(row["attachments"] or "[]")
            except Exception:
                attaches = []
            try:
                linked = json.loads(row["linked_data"] or "[]")
            except Exception:
                linked = []
            events.append({
                "id": f"user-{row['event_id']}",
                "category": row["category"],
                "title": row["title"],
                "date": row["date"],
                "startHour": row["start_hour"],
                "duration": row["duration"],
                "owner": row["owner"] or "",
                "participants": parts,
                "decisions": row["decisions"] or "",
                "agenda": row["agenda"] or "",
                "attachments": attaches,
                "linkedData": linked,
                "isUserCreated": True,
            })

    return events
