from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session
from loguru import logger

from app.dependencies import get_db_session
from schemas.log import LogIn, LogOut
from services.log_service import create_log as create_log_service
from services.employee_service import import_employees_from_excel, import_employees_from_excel_stream
from schemas.employee import EmployeeIn, EmployeeOut
from db.repositories.employee_repo import EmployeeRepository


router = APIRouter()


@router.post("/logs", response_model=LogOut)
def create_log(payload: LogIn, session: Session = Depends(get_db_session)):
    try:
        log = create_log_service(
            level=payload.level,
            source=payload.source,
            message=payload.message,
            email=getattr(payload, "email", None),
            task_key=getattr(payload, "task_key", None),
            rule_type=getattr(payload, "rule_type", None),
            session=session,
        )
        return log
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@router.post("/employees/import")
def import_employees(file_path: str = "resource/projects_employees.xlsx", session: Session = Depends(get_db_session)):
    try:
        result = import_employees_from_excel(file_path=file_path, session=session)
        return {"status": "ok", **result}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@router.post("/employees/import-file")
async def import_employees_file(file: UploadFile = File(...), session: Session = Depends(get_db_session)):
    try:
        data = await file.read()
        result = import_employees_from_excel_stream(file_bytes=data, session=session)
        return {"status": "ok", **result}
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@router.post("/employees", response_model=EmployeeOut)
def create_employee(payload: EmployeeIn, session: Session = Depends(get_db_session)):
    try:
        logger.debug(
            "create_employee payload: username={}, display_name={}, email={}, group_id={}, created_by={}",
            payload.username,
            payload.display_name,
            payload.email,
            str(payload.group_id) if getattr(payload, "group_id", None) is not None else None,
            payload.created_by,
        )
        repo = EmployeeRepository(session)
        emp = repo.upsert_by_username(
            username=payload.username,
            display_name=payload.display_name,
            email=payload.email,
            group_id=str(payload.group_id) if payload.group_id is not None else None,
            created_by=payload.created_by,
        )
        logger.debug(
            "create_employee result: id={} (type={}), username={}, created_at={}",
            getattr(emp, "id", None),
            type(getattr(emp, "id", None)).__name__ if getattr(emp, "id", None) is not None else None,
            getattr(emp, "username", None),
            getattr(emp, "created_at", None),
        )
        return emp
    except Exception as ex:
        logger.exception("create_employee failed: {}", ex)
        raise HTTPException(status_code=500, detail=str(ex))


@router.get("/employees", response_model=list[EmployeeOut])
def list_employees(
    limit: int = 100,
    offset: int = 0,
    group_id: str | None = None,
    username: str | None = None,
    session: Session = Depends(get_db_session),
):
    try:
        repo = EmployeeRepository(session)
        if username:
            emp = repo.find_by_username(username)
            return [emp] if emp else []
        return repo.list(limit=limit, offset=offset, group_id=group_id)
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


@router.get("/employees/{employee_id}", response_model=EmployeeOut)
def get_employee(employee_id: int, session: Session = Depends(get_db_session)):
    try:
        repo = EmployeeRepository(session)
        emp = repo.get_by_id(employee_id)
        if not emp:
            raise HTTPException(status_code=404, detail="Employee not found")
        return emp
    except HTTPException:
        raise
    except Exception as ex:
        raise HTTPException(status_code=500, detail=str(ex))


