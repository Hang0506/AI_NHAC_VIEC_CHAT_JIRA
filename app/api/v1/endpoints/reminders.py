from fastapi import APIRouter, Depends
from typing import List

from services.reminder_service import ReminderService
from app.dependencies import get_reminder_service, get_db_session
from db.repositories.reminder_repo import ReminderRepository
from schemas.reminder import ReminderOut

router = APIRouter()


@router.post("/sync")
async def manual_sync(service: ReminderService = Depends(get_reminder_service)) -> dict:
    count = await service.sync_from_jira()
    return {"synced": count}


@router.post("/run")
async def run_reminders(service: ReminderService = Depends(get_reminder_service)) -> dict:
    sent = await service.send_due_reminders()
    return {"sent": sent}


@router.get("/", response_model=List[ReminderOut])
async def list_reminders(limit: int = 100, offset: int = 0, session=Depends(get_db_session)):
    repo = ReminderRepository(session)
    items = repo.list_all(limit=limit, offset=offset)
    return items
