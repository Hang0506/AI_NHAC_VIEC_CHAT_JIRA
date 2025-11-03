from datetime import datetime
from pydantic import BaseModel


class LogOut(BaseModel):
    id: int
    level: str
    source: str
    message: str
    created_at: datetime

    class Config:
        from_attributes = True
