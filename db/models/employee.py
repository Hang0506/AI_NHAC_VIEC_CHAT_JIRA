from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String, Integer, DateTime

from db.base import Base

# Timezone Việt Nam
VN_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")

def get_vn_now() -> datetime:
    """Lấy datetime hiện tại theo giờ Việt Nam."""
    return datetime.now(VN_TIMEZONE)


class Employee(Base):
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    username: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(200))
    email: Mapped[str] = mapped_column(String(200), index=True)
    group_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=get_vn_now, index=True)
    created_by: Mapped[str | None] = mapped_column(String(100), nullable=True)

    reminders: Mapped[list["Reminder"]] = relationship("Reminder", back_populates="employee")
