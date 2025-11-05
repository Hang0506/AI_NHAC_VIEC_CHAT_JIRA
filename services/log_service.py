from __future__ import annotations
from typing import Optional, List

from sqlalchemy.orm import Session

from db.session import get_session
from db.repositories.log_repo import LogRepository
from db.models.log import Log


def create_log(level: str, source: str, message: str, email: str | None = None, task_key: str | None = None, rule_type: str | None = None, session: Optional[Session] = None) -> Log:
    """Create a log record. If session is not provided, a new one is created."""
    if session is not None:
        return LogRepository(session).add(level=level, source=source, message=message, email=email, task_key=task_key, rule_type=rule_type)

    with get_session() as _session:
        return LogRepository(_session).add(level=level, source=source, message=message, email=email, task_key=task_key, rule_type=rule_type)


def create_logs_batch(records: List[dict], session: Optional[Session] = None) -> List[Log]:
    """Batch create multiple log records from a list of dicts.
    
    Args:
        records: List of dicts with keys: level, source, message, email, task_key, rule_type
        session: Optional session. If not provided, a new one is created.
    
    Returns:
        List of created Log objects.
    """
    logs = [
        Log(
            level=r.get("level", "INFO"),
            source=r.get("source", "unknown"),
            message=r.get("message", ""),
            email=r.get("email"),
            task_key=r.get("task_key"),
            rule_type=r.get("rule_type"),
        )
        for r in records
    ]
    
    if session is not None:
        repo = LogRepository(session)
        result = repo.batch_add(logs)
        session.commit()
        return result
    
    with get_session() as _session:
        repo = LogRepository(_session)
        result = repo.batch_add(logs)
        _session.commit()
        return result


