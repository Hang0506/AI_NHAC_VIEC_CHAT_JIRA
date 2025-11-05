from datetime import datetime
from zoneinfo import ZoneInfo
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy import String, Integer, DateTime, Text

from db.base import Base

# Timezone Việt Nam
VN_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")

def get_vn_now() -> datetime:
    """Lấy datetime hiện tại theo giờ Việt Nam."""
    return datetime.now(VN_TIMEZONE)


class Log(Base):
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    level: Mapped[str] = mapped_column(String(20), index=True)
    source: Mapped[str] = mapped_column(String(100), index=True)
    message: Mapped[str] = mapped_column(Text())
    created_at: Mapped[datetime] = mapped_column(DateTime, default=get_vn_now, index=True)
    email: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    task_key: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
    # New: store rule type explicitly for fast filtering (e.g., reminder rules)
    rule_type: Mapped[str | None] = mapped_column(String(100), nullable=True, index=True)
