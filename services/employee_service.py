from __future__ import annotations
from typing import Optional
from io import BytesIO

import pandas as pd
from sqlalchemy.orm import Session

from db.session import get_session
from db.repositories.employee_repo import EmployeeRepository


def _resolve_column(row: pd.Series, candidates: list[str]) -> str:
    for key in candidates:
        if key in row and pd.notna(row[key]):
            value = str(row[key]).strip()
            if value:
                return value
    return ""


def import_employees_from_excel(
    file_path: str = "resource/projects_employees.xlsx",
    session: Optional[Session] = None,
) -> dict[str, int]:
    """Import employees from an Excel file into the `employee` table.

    The function attempts to map common column names to `username`, `display_name`, and `email`.
    Any `impact` column present in the file is ignored; if needed, it should be
    derived from existing database data elsewhere in the application.
    """

    # Read Excel and normalize columns
    df = pd.read_excel(file_path, engine="openpyxl")
    df.columns = [str(c).strip().lower() for c in df.columns]

    username_cols = ["username", "account_id", "accountid", "user", "account"]
    display_name_cols = ["display_name", "name", "fullname", "full_name"]
    email_cols = ["email", "mail", "email_address", "emailaddress"]

    processed = 0
    created_session = False

    if session is None:
        created_session = True

    def _run_import(sess: Session) -> None:
        nonlocal processed
        repo = EmployeeRepository(sess)
        for _, row in df.iterrows():
            username = _resolve_column(row, username_cols)
            display_name = _resolve_column(row, display_name_cols)
            email = _resolve_column(row, email_cols)

            if not username:
                continue
            # Email/display_name may be empty in source; default to empty strings
            repo.upsert_by_username(username=username, display_name=display_name, email=email)
            processed += 1

    if created_session:
        with get_session() as s:
            _run_import(s)
    else:
        _run_import(session)  # type: ignore[arg-type]

    return {"processed": processed}


def import_employees_from_excel_stream(
    file_bytes: bytes,
    session: Optional[Session] = None,
) -> dict[str, int]:
    """Import employees from an uploaded Excel file (bytes)."""
    df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl")
    df.columns = [str(c).strip().lower() for c in df.columns]

    username_cols = ["username", "account_id", "accountid", "user", "account"]
    display_name_cols = ["display_name", "name", "fullname", "full_name"]
    email_cols = ["email", "mail", "email_address", "emailaddress"]
    group_id_cols = ["group_id", "group", "groupid"]
    created_by_cols = ["created_by", "createby", "createdby", "author"]

    processed = 0
    created_session = False

    if session is None:
        created_session = True

    def _run_import(sess: Session) -> None:
        nonlocal processed
        repo = EmployeeRepository(sess)
        for _, row in df.iterrows():
            username = _resolve_column(row, username_cols)
            if not username:
                continue
            display_name = _resolve_column(row, display_name_cols)
            email = _resolve_column(row, email_cols)
            group_id = _resolve_column(row, group_id_cols) or None
            created_by = _resolve_column(row, created_by_cols) or None

            repo.upsert_by_username(
                username=username,
                display_name=display_name,
                email=email,
                group_id=group_id,
                created_by=created_by,
            )
            processed += 1

    if created_session:
        with get_session() as s:
            _run_import(s)
    else:
        _run_import(session)  # type: ignore[arg-type]

    return {"processed": processed}


