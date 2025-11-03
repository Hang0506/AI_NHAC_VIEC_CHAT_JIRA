from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import String, Integer

from db.base import Base


class Employee(Base):
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    username: Mapped[str] = mapped_column(String(100), unique=True, index=True)
    display_name: Mapped[str] = mapped_column(String(200))
    email: Mapped[str] = mapped_column(String(200), index=True)

    reminders: Mapped[list["Reminder"]] = relationship("Reminder", back_populates="employee")
