from fastapi import APIRouter, HTTPException
from database import engine
from sqlalchemy import text
from typing import List, Dict
from pydantic import BaseModel

class TaskUpdate(BaseModel):
    status: str

router = APIRouter(prefix="/api/tasks", tags=["tasks"])

@router.get("")
def get_tasks():
    try:
        with engine.connect() as conn:
            res = conn.execute(text("SELECT task_id, exception_id, alternative_id, title, description, status, created_at FROM tasks ORDER BY created_at DESC"))
            rows = res.fetchall()
            
            tasks = []
            for row in rows:
                tasks.append({
                    "id": f"TSKAI-{row._mapping['task_id']}",
                    "exception_id": row._mapping["exception_id"],
                    "alternative_id": row._mapping.get("alternative_id", ""),
                    "title": row._mapping["title"],
                    "description": row._mapping["description"],
                    "status": row._mapping["status"],
                    "created_at": row._mapping["created_at"],
                    "owner": "Me",
                    "priority": "High",
                    "due": "Today",
                    "type": "AI Mitigation"
                })
            return tasks
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@router.put("/{task_id}")
def update_task(task_id: int, req: TaskUpdate):
    try:
        with engine.connect() as conn:
            conn.execute(
                text("UPDATE tasks SET status = :status WHERE task_id = :tid"),
                {"status": req.status, "tid": task_id}
            )
            conn.commit()
        return {"message": "Task updated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
