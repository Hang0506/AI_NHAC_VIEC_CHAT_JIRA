from sqlalchemy.orm import Session
from sqlalchemy import select

from db.models.log import Log


class LogRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, level: str, source: str, message: str, email: str | None = None, task_key: str | None = None) -> Log:
        log = Log(level=level, source=source, message=message, email=email, task_key=task_key)
        self.session.add(log)
        return log

    def list_by_source(self, source: str, limit: int = 1000) -> list[Log]:
        stmt = (
            select(Log)
            .where(Log.source == source)
            .order_by(Log.created_at.desc())
            .limit(limit)
        )
        return list(self.session.scalars(stmt))

    def list_by_email_and_task(self, source: str, email: str | None = None, task_key: str | None = None, limit: int = 1000) -> list[Log]:
        """Query logs by source, email, and/or task_key."""
        stmt = select(Log).where(Log.source == source)
        if email:
            stmt = stmt.where(Log.email == email)
        if task_key:
            stmt = stmt.where(Log.task_key == task_key)
        stmt = stmt.order_by(Log.created_at.desc()).limit(limit)
        return list(self.session.scalars(stmt))
