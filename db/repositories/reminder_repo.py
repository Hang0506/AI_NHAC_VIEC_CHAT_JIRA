from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import select

from db.models.reminder import Reminder


class ReminderRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def create(self, employee_id: int, issue_key: str, message: str, due_at: datetime) -> Reminder:
        reminder = Reminder(employee_id=employee_id, jira_issue_key=issue_key, message=message, due_at=due_at)
        self.session.add(reminder)
        return reminder

    def due_unsent(self, now: datetime) -> list[Reminder]:
        stmt = select(Reminder).where(Reminder.sent.is_(False), Reminder.due_at <= now)
        return list(self.session.scalars(stmt))

    def mark_sent(self, reminder: Reminder) -> None:
        reminder.sent = True
        reminder.sent_at = datetime.utcnow()

    def list_all(self, limit: int = 100, offset: int = 0) -> list[Reminder]:
        stmt = select(Reminder).order_by(Reminder.due_at.desc()).limit(limit).offset(offset)
        return list(self.session.scalars(stmt))
