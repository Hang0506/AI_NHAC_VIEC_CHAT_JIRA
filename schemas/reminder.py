from datetime import datetime
from pydantic import BaseModel


class ReminderBase(BaseModel):
    employee_id: int
    jira_issue_key: str
    message: str
    due_at: datetime


class ReminderOut(ReminderBase):
    id: int
    sent: bool
    sent_at: datetime | None

    class Config:
        from_attributes = True
