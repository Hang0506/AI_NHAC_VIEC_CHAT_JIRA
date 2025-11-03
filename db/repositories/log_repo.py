from sqlalchemy.orm import Session

from db.models.log import Log


class LogRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def add(self, level: str, source: str, message: str) -> Log:
        log = Log(level=level, source=source, message=message)
        self.session.add(log)
        return log
