from sqlalchemy.orm import Session
from datetime import datetime
from sqlalchemy import select

from db.models.employee import Employee


class EmployeeRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def get_by_id(self, employee_id: int) -> Employee | None:
        stmt = select(Employee).where(Employee.id == employee_id)
        return self.session.scalars(stmt).first()

    def find_by_username(self, username: str) -> Employee | None:
        stmt = select(Employee).where(Employee.username == username)
        return self.session.scalars(stmt).first()

    def list(self, limit: int = 100, offset: int = 0, group_id: str | None = None) -> list[Employee]:
        stmt = select(Employee)
        if group_id is not None:
            stmt = stmt.where(Employee.group_id == group_id)
        stmt = stmt.order_by(Employee.id.desc()).limit(limit).offset(offset)
        return list(self.session.scalars(stmt))

    def upsert_by_username(
        self,
        username: str,
        display_name: str,
        email: str,
        group_id: str | None = None,
        created_by: str | None = None,
    ) -> Employee:
        stmt = select(Employee).where(Employee.username == username)
        employee = self.session.scalars(stmt).first()
        if employee is None:
            employee = Employee(
                username=username,
                display_name=display_name,
                email=email,
                group_id=group_id,
                created_by=created_by,
                created_at=datetime.utcnow(),
            )
            self.session.add(employee)
        else:
            employee.display_name = display_name
            employee.email = email
            if group_id is not None:
                employee.group_id = group_id
            if created_by is not None:
                employee.created_by = created_by
            # Backfill created_at if legacy rows were missing it
            if getattr(employee, "created_at", None) is None:
                employee.created_at = datetime.utcnow()
        # Ensure database-generated fields (id, created_at) are available before returning
        self.session.flush()
        self.session.refresh(employee)
        # Rare safeguard if id/created_at still not loaded
        if employee.id is None or getattr(employee, "created_at", None) is None:
            self.session.flush()
            self.session.refresh(employee)
        return employee
