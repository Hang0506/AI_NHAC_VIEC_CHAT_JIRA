from sqlalchemy.orm import Session
from sqlalchemy import select

from db.models.log import Log


class LogRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, level: str, source: str, message: str) -> Log:
        log = Log(level=level, source=source, message=message)
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
