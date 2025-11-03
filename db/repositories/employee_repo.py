from sqlalchemy.orm import Session
from sqlalchemy import select

from db.models.employee import Employee


class EmployeeRepository:
    def __init__(self, session: Session) -> None:
        self.session = session

    def upsert_by_username(self, username: str, display_name: str, email: str) -> Employee:
        stmt = select(Employee).where(Employee.username == username)
        employee = self.session.scalars(stmt).first()
        if employee is None:
            employee = Employee(username=username, display_name=display_name, email=email)
            self.session.add(employee)
        else:
            employee.display_name = display_name
            employee.email = email
        return employee
