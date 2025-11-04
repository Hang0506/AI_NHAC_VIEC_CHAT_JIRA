from __future__ import annotations
from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class EmployeeIn(BaseModel):
    username: str
    display_name: str = ""
    email: str = ""
    group_id: Optional[str] = None
    created_by: Optional[str] = None


class EmployeeOut(BaseModel):
    id: int
    username: str
    display_name: str
    email: str
    group_id: Optional[str] = None
    created_at: Optional[datetime] = None
    created_by: Optional[str] = None

    class Config:
        from_attributes = True

