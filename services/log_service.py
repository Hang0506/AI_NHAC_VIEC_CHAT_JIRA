from __future__ import annotations
from typing import Optional

from sqlalchemy.orm import Session

from db.session import get_session
from db.repositories.log_repo import LogRepository
from db.models.log import Log


def create_log(level: str, source: str, message: str, session: Optional[Session] = None) -> Log:
    """Create a log record. If session is not provided, a new one is created."""
    if session is not None:
        return LogRepository(session).add(level=level, source=source, message=message)

    with get_session() as _session:
        return LogRepository(_session).add(level=level, source=source, message=message)


