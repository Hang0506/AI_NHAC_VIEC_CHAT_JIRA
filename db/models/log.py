from datetime import datetime
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer, DateTime, Text

from db.base import Base


class Log(Base):
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    level: Mapped[str] = mapped_column(String(20), index=True)
    source: Mapped[str] = mapped_column(String(100), index=True)
    message: Mapped[str] = mapped_column(Text())
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, index=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    task_key: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
