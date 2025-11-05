from datetime import datetime
from pydantic import BaseModel


class LogOut(BaseModel):
    id: int
    level: str
    source: str
    message: str
    created_at: datetime
    email: str | None = None
    task_key: str | None = None
    rule_type: str | None = None

    class Config:
        from_attributes = True


class LogIn(BaseModel):
    level: str
    source: str
    message: str
    email: str | None = None
    task_key: str | None = None
    rule_type: str | None = None