from pydantic import BaseModel


class EmployeeBase(BaseModel):
    username: str
    display_name: str
    email: str


class EmployeeOut(EmployeeBase):
    id: int

    class Config:
        from_attributes = True
