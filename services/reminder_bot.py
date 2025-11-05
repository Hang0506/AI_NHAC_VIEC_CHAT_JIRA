import os
import sys
import json
import argparse
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, List
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv

from logger import get_logger
from services.log_service import create_log as create_log_service, create_logs_batch
from services.jira_utils import JiraClient
from services.rules import (
     evaluate_missing_logtime,
     evaluate_missing_description,
     evaluate_pre_version_reminder,
     evaluate_post_version_alert,
     evaluate_assignee_changed,
     evaluate_due_date_overdue,
     evaluate_created_recently,
     is_test_email,
     TEST_EMAILS,
     MISSING_LOGTIME,
     MISSING_DESCRIPTION,
     PRE_VERSION_REMINDER,
     POST_VERSION_ALERT,
     ASSIGNEE_CHANGED,
     DUE_DATE_OVERDUE,
     RECENTLY_CREATED,
)
from services.chat_api import send_message_fpt
from db.session import get_session
from db.repositories.log_repo import LogRepository
from db.repositories.employee_repo import EmployeeRepository
from core.config import settings
from sqlalchemy.inspection import inspect as sa_inspect

logger = get_logger()

# Timezone Việt Nam
VN_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")

def get_vn_now() -> datetime:
    """Lấy datetime hiện tại theo giờ Việt Nam."""
    return datetime.now(VN_TIMEZONE)

def _load_json(path: str) -> dict:
     with open(path, "r", encoding="utf-8") as f:
         return json.load(f)

def _ensure_dirs(path: str):
     os.makedirs(os.path.dirname(path), exist_ok=True)

def _safe_get_log_attr(log_obj, attr_name: str):
     """Safely get attribute from SQLAlchemy ORM object or Row-like object.
     Tries direct attribute, Row._mapping, SQLAlchemy inspect, then __dict__.
     """
     # 1) Direct attribute
     try:
         value = getattr(log_obj, attr_name, None)
         if value is not None:
             return value
     except Exception:
         pass
     # 2) Row mapping (for select() Row objects)
     try:
         mapping = getattr(log_obj, "_mapping", None)
         if mapping is not None and attr_name in mapping:
             return mapping[attr_name]
     except Exception:
         pass
     # 3) SQLAlchemy inspect for ORM attributes
     try:
         ins = sa_inspect(log_obj)
         attrs = getattr(ins, "attrs", None)
         if attrs is not None and hasattr(attrs, attr_name):
             return getattr(attrs, attr_name).value
     except Exception:
         pass
     # 4) __dict__ fallback
     try:
         d = getattr(log_obj, "__dict__", None)
         if isinstance(d, dict) and attr_name in d:
             return d[attr_name]
     except Exception:
         pass
     return None

def _read_employees(path: str) -> pd.DataFrame:
     """Load employees mapping from DB (email -> chat_id via group_id).
     The CSV file is no longer used.
     """
     logger.info("Loading employees mapping from DB (ignoring CSV)")
     try:
         with get_session() as session:
             repo = EmployeeRepository(session)
             # Fetch a large batch of employees; adjust limit if needed
             employees = repo.list(limit=10000, offset=0, group_id=None)
             records = []
             for emp in employees:
                 email = (getattr(emp, "email", "") or "").strip()
                 chat_id = (getattr(emp, "group_id", "") or "").strip()
                 if email:
                     records.append({"email": email, "chat_id": chat_id})
             df = pd.DataFrame.from_records(records, columns=["email", "chat_id"]) if records else pd.DataFrame(columns=["email", "chat_id"])
             logger.info(f"Loaded employees from DB: {len(df)} rows")
             return df
     except Exception as ex:
         logger.exception(f"Failed to load employees from DB: {ex}")
         return pd.DataFrame(columns=["email", "chat_id"])

def _lookup_chat_id(df: pd.DataFrame, email: str) -> str:
     if not email:
         return ""
     row = df.loc[df["email"].str.lower() == str(email).lower()]
     if row.empty:
         logger.debug(f"No chat_id mapping for email: {email}")
         return ""
     chat_id = row.iloc[0]["chat_id"]
     logger.debug(f"Mapped email {email} -> chat_id '{chat_id}'")
     return chat_id if isinstance(chat_id, str) else ""

def _load_history(path: str, email: str | None = None, task_key: str | None = None):
    """Load reminder send history from DB logs (source='reminder_bot').
    Can filter by email and/or task_key.
    The CSV file is ignored to avoid parsing issues.
    """
    try:
        logger.info(f"Loading history from DB: email={email}, task_key={task_key}")
        rows: list[dict] = []
        with get_session() as session:
            if email or task_key:
                logs = LogRepository(session).list_by_email_and_task(
                    "reminder_bot", email=email, task_key=task_key, limit=5000
                )
                logger.info(f"Query by email/task_key: found {len(logs)} logs from DB")
            else:
                logs = LogRepository(session).list_by_source("reminder_bot", limit=5000)
                logger.info(f"Query all: found {len(logs)} logs from DB")

            for log in logs:
                try:
                    # Use email and task_key from DB columns if available, otherwise parse from message
                    rec = {}
                    log_email = getattr(log, "email", None)
                    log_task_key = getattr(log, "task_key", None)
                    log_created_at = getattr(log, "created_at", None)

                    if log_email and log_task_key:
                        # Use DB columns directly
                        rec = {
                            "task_key": log_task_key,
                            "to": log_email,
                            "sent_at": log_created_at.isoformat() if hasattr(log_created_at, "isoformat") else str(log_created_at),
                        }
                        # Include rule_type from DB column if available
                        try:
                            db_rule_type = getattr(log, "rule_type", None)
                            if db_rule_type:
                                rec["rule_type"] = db_rule_type
                        except Exception:
                            pass
                        logger.debug(
                            f"Using DB columns: task_key={log_task_key}, email={log_email}, created_at={log_created_at}"
                        )
                        # Try to parse message for additional fields
                        try:
                            msg_data = json.loads(getattr(log, "message", "{}"))
                            if isinstance(msg_data, dict):
                                rec.update(msg_data)
                                logger.debug(
                                    f"Parsed message: {json.dumps(msg_data, ensure_ascii=False)}"
                                )
                        except Exception as ex:
                            logger.debug(f"Failed to parse message: {ex}")
                    else:
                        # Fallback: parse from message (backward compatible)
                        rec = json.loads(getattr(log, "message", ""))
                        logger.debug(
                            f"Using message fallback: {json.dumps(rec, ensure_ascii=False)}"
                        )

                    if isinstance(rec, dict) and rec.get("task_key") and rec.get("to"):
                        # Use email and task_key from DB if available
                        if log_email:
                            rec["to"] = log_email
                            logger.debug(f"Updated rec['to'] from DB: {log_email}")
                        if log_task_key:
                            rec["task_key"] = log_task_key
                            logger.debug(f"Updated rec['task_key'] from DB: {log_task_key}")
                        rows.append(rec)
                        logger.debug(
                            f"Added history record: task_key={rec.get('task_key')}, to={rec.get('to')}, rule_type={rec.get('rule_type')}, sent_at={rec.get('sent_at')}"
                        )
                except Exception as ex:
                    logger.warning(f"Failed to parse log record: {ex}")
                    continue

        logger.info(
            f"Loaded reminder history from DB: {len(rows)} records (email={email}, task_key={task_key})"
        )
        if rows:
            logger.info(
                f"Sample history records: {json.dumps(rows[:3], ensure_ascii=False, indent=2)}"
            )
        return rows
    except Exception as ex:
        logger.exception(f"Failed to load history from DB: {ex}")
        return []

def _append_history(path: str, record: dict):
     # Keep CSV write as best-effort (optional), but DB is the source of truth
     try:
         _ensure_dirs(path)
         exists = os.path.exists(path)
         is_empty = False
         if exists:
             try:
                 is_empty = os.path.getsize(path) == 0
             except Exception:
                 is_empty = False
         header_needed = (not exists) or is_empty
         df = pd.DataFrame([record])
         df.to_csv(path, mode='a', header=header_needed, index=False)
     except Exception:
         # Ignore CSV errors silently; DB logging below is primary
         pass
     logger.debug(f"Appended history record for task {record.get('task_key')} rule {record.get('rule_type')} -> {record.get('status')}")


def _append_history_service(record: dict):
    """Persist reminder send history via shared service (no HTTP)."""
    try:
        level = "INFO" if record.get("status") == "sent" else "WARNING"
        message = json.dumps(record, ensure_ascii=False)
        email = record.get("to") or record.get("email")
        task_key = record.get("task_key")
        # Pass rule_type explicitly to DB for indexing/filtering
        create_log_service(level=level, source="reminder_bot", message=message, email=email, task_key=task_key, rule_type=record.get("rule_type"))
    except Exception as ex:
        logger.exception(f"Failed to write history via service: {ex}")

def _check_history_from_db(task_key: str, email: str, rule_code: str, resend_after_hours: int) -> bool:
    """Check history directly from DB by task_key and email.
    Returns True if already sent within resend_after_hours, False otherwise.
    """
    try:
        with get_session() as session:
            # Query with rule_type if the column is available; repository supports it
            logs = LogRepository(session).list_by_email_and_task("reminder_bot", email=email, task_key=task_key, rule_type=rule_code, limit=100)
        
        now = get_vn_now()
        for log in logs:
            try:
                # Prefer DB column if populated; fallback to message JSON for backward compatibility
                log_rule_type = getattr(log, "rule_type", None)
                if not log_rule_type:
                    msg_data = json.loads(getattr(log, "message", "{}"))
                    if isinstance(msg_data, dict):
                        log_rule_type = msg_data.get("rule_type")
                if log_rule_type != rule_code:
                    continue
                # Check sent_at from message or created_at
                sent_at_str = None
                try:
                    msg_data2 = json.loads(getattr(log, "message", "{}"))
                    if isinstance(msg_data2, dict):
                        sent_at_str = msg_data2.get("sent_at")
                except Exception:
                    sent_at_str = None
                if not sent_at_str:
                    created = getattr(log, "created_at", None)
                    sent_at_str = created.isoformat() if hasattr(created, "isoformat") else str(created)
                sent_at = datetime.fromisoformat(str(sent_at_str).replace("Z", "+00:00"))
                time_diff = now - sent_at
                if time_diff < timedelta(hours=resend_after_hours):
                    logger.debug(f"Found recent history: {task_key} {rule_code} to {email} ({time_diff.total_seconds()/3600:.1f}h ago)")
                    return True
            except Exception:
                continue
        return False
    except Exception as ex:
        logger.warning(f"Failed to check history from DB: {ex}")
        return False

def _already_logged_today_by_level(task_key: str, rule_code: str, level: str) -> bool:
    """Check if there is already a log today with same task_key, rule_type and level.
    This is used to avoid sending duplicate notifications within the same day.
    """
    try:
        # Timezone context for local-day comparison (giờ Việt Nam)
        tz = VN_TIMEZONE
        tz_name = "Asia/Ho_Chi_Minh"
        today_local = datetime.now(tz).date()

        print(
            f"[Bot][Debug] same-day check start -> task_key={task_key} rule_type={rule_code} level_check={level} tz={tz_name} today_local={today_local}"
        )

        match_found = False
        with get_session() as session:
            # Filter by source, task, rule; email-independent per requirement
            logs = LogRepository(session).list_by_email_and_task(
                "reminder_bot", email=None, task_key=task_key, rule_type=rule_code, limit=300
            )
            print(
                f"[Bot][Debug] fetched logs: count={len(logs)} type={type(logs).__name__} task_key={task_key} rule_type={rule_code} level_check={level}"
            )
            print("[Bot][Debug] about to enter loop over logs ...")

            # Print a few samples of raw logs to inspect structure
            try:
                for sidx, slog in enumerate(list(logs)[:3]):
                    try:
                        print(
                            f"[Bot][Debug] logs sample[{sidx}] type={type(slog).__name__} repr={repr(slog)[:300]}"
                        )
                        if hasattr(slog, "__dict__"):
                            try:
                                keys = [k for k in slog.__dict__.keys() if not str(k).startswith("_")]
                                print(f"[Bot][Debug] logs sample[{sidx}] __dict__ keys={keys}")
                            except Exception:
                                pass
                        mapping = getattr(slog, "_mapping", None)
                        if mapping is not None:
                            try:
                                mk = list(mapping.keys())
                                print(f"[Bot][Debug] logs sample[{sidx}] _mapping keys={mk[:20]}")
                            except Exception:
                                pass
                    except Exception as e_samp:
                        print(f"[Bot][Debug] logs sample[{sidx}] print error: {e_samp}")
            except Exception as e_samp_outer:
                print(f"[Bot][Debug] logs samples outer error: {e_samp_outer}")

            try:
                for idx, log in enumerate(logs):
                    print(f"[Bot][Debug] loop entered, idx={idx}")
                    print(f"[Bot][Debug] pre-try, idx={idx}, log_type={type(log).__name__}")
                    try:
                        print(f"[Bot][Debug] inside try, idx={idx}")
                        print(
                            f"[Bot][Debug] hasattr -> id={hasattr(log,'id')} level={hasattr(log,'level')} created_at={hasattr(log,'created_at')}"
                        )
                        try:
                            print(f"[Bot][Debug] repr(log)[:200] = {repr(log)[:200]}")
                        except Exception:
                            pass
                        try:
                            mapping_keys = list(getattr(log, "_mapping", {}).keys()) if getattr(log, "_mapping", None) is not None else []
                            print(f"[Bot][Debug] mapping keys = {mapping_keys[:20]}")
                        except Exception:
                            pass
                        # Safely read attributes via helper (handles ORM/Row)
                        log_id = _safe_get_log_attr(log, "id")
                        created_at = _safe_get_log_attr(log, "created_at")
                        log_level = _safe_get_log_attr(log, "level")
                        print(
                            f"[Bot][Debug] eval log id={log_id} level={log_level} created_at_raw={created_at} type={type(log).__name__}"
                        )
                        if not created_at or not log_level:
                            print(
                                f"[Bot][Debug] -> skip (missing created_at or level) id={log_id}"
                            )
                            continue
                        # Convert created_at to local tz date for same-day comparison
                        ca_local = created_at if isinstance(created_at, datetime) and created_at.tzinfo else (
                            created_at.replace(tzinfo=VN_TIMEZONE) if isinstance(created_at, datetime) else None
                        )
                        if isinstance(ca_local, datetime):
                            ca_local = ca_local.astimezone(tz)
                            log_date_local = ca_local.date()
                        else:
                            print(f"[Bot][Debug] created_at is not datetime -> {created_at} (id={log_id})")
                            continue
                        print(
                            f"[Bot][Debug] compare -> local_date={log_date_local} vs today={today_local}, level={str(log_level).upper()} vs {str(level).upper()} (id={log_id})"
                        )
                        if log_date_local == today_local and str(log_level).upper() == str(level).upper():
                            logger.info(
                                f"Found same-day duplicate: task={task_key} rule={rule_code} level={level})"
                            )
                            print(
                                f"[Bot][Debug] MATCH -> return True (id={log_id})"
                            )
                            match_found = True
                            break
                        else:
                            print(
                                f"[Bot][Debug] not match -> continue (id={log_id})"
                            )
                    except Exception as e:
                        try:
                            print(f"[Bot][Debug] exception while processing log id={getattr(log, 'id', None)}: {e}")
                        except Exception:
                            pass
                        continue
            except Exception as outer_e:
                print(f"[Bot][Debug] loop construction/iteration exception: {outer_e}")

        print(f"[Bot][Debug] FINAL same-day duplicate={match_found}")
        return match_found
    except Exception as ex:
        logger.warning(f"Failed to check same-day duplicate logs: {ex}")
        return False

def _already_sent(history_rows: list, task_key: str, rule_code: str, to_value: str, resend_after_hours: int) -> bool:
     """Kiểm tra xem đã gửi nhắc trong vòng X giờ chưa.
     Returns True nếu đã gửi trong vòng X giờ (để skip, không gửi lại).
     Returns False nếu chưa gửi hoặc đã gửi quá X giờ (cho phép gửi lại).
     """
     logger.info(f"Checking history: task_key={task_key}, rule_code={rule_code}, to={to_value}, resend_after_hours={resend_after_hours}")
     logger.info(f"Total history rows to check: {len(history_rows)}")
     now = get_vn_now()
     matched_count = 0
     for r in history_rows:
         rec_task_key = r.get("task_key")
         rec_rule_type = r.get("rule_type")
         rec_to = r.get("to")
         rec_sent_at = r.get("sent_at")
         
         logger.debug(f"Checking record: task_key={rec_task_key}, rule_type={rec_rule_type}, to={rec_to}, sent_at={rec_sent_at}")
         
         if rec_task_key == task_key and rec_rule_type == rule_code and rec_to == to_value:
             matched_count += 1
             logger.info(f"Found matching record #{matched_count}: task_key={rec_task_key}, rule_type={rec_rule_type}, to={rec_to}, sent_at={rec_sent_at}")
             try:
                 # Parse sent_at, handle both ISO format and datetime string
                 sent_at_str = rec_sent_at
                 if isinstance(sent_at_str, str):
                     sent_at_str = sent_at_str.replace("Z", "+00:00")
                 sent_at = datetime.fromisoformat(sent_at_str) if isinstance(sent_at_str, str) else sent_at_str
                 if sent_at.tzinfo is None:
                     sent_at = sent_at.replace(tzinfo=VN_TIMEZONE)
                 
                 time_diff = now - sent_at
                 hours_diff = time_diff.total_seconds() / 3600.0
                 logger.info(f"Time difference: {hours_diff:.2f} hours (threshold: {resend_after_hours} hours)")
                 
                 # Nếu đã gửi trong vòng X giờ, return True để skip (không gửi lại)
                 if time_diff < timedelta(hours=resend_after_hours):
                     logger.info(f"SKIP SEND: Already sent {hours_diff:.2f}h ago (within {resend_after_hours}h threshold) for {task_key} {rule_code} to {to_value}")
                     return True
                 else:
                     logger.info(f"ALLOW SEND: Last sent {hours_diff:.2f}h ago (over {resend_after_hours}h threshold) for {task_key} {rule_code} to {to_value}")
             except Exception as ex:
                 # Nếu không parse được, log warning và coi như chưa gửi (cho phép gửi lại)
                 logger.warning(f"Failed to parse sent_at '{rec_sent_at}' for record: {ex}, allowing send")
                 continue
     
     if matched_count == 0:
         logger.info(f"NO MATCH: No history found for task_key={task_key}, rule_code={rule_code}, to={to_value}, allowing send")
     else:
         logger.info(f"Checked {matched_count} matching records, all were over threshold, allowing send")
     
     return False

def build_message(task: dict, code: str, data: Optional[dict] = None) -> str:
    """Tạo nội dung tin nhắn thân thiện cho từng rule."""
    url = task.get("task_url")
    key = task.get("key")
    summary = task.get("summary", "")
    
    if code == MISSING_LOGTIME:
        return (
            f"⏰ Anh/chị ơi, mình quên logtime cho task [{key}] - {summary} rồi nè. "
            f"Nhớ cập nhật sớm nha 👉 {url}"
        )
    
    if code == MISSING_DESCRIPTION:
        reporter = task.get("reporter_email") or ""
        return (
            f"📝 Task [{key}] - {summary} chưa có mô tả chi tiết đó ạ. "
            f"Anh/chị {reporter} bổ sung giúp em để dev đỡ phải đoán nha 😅 👉 {url}"
        )
    
    if code == PRE_VERSION_REMINDER and data:
        return (
            f"📦 Task [{key}] - {summary} thuộc version **{data['fv_name']}** "
            f"sắp release trong {data['days']} ngày nữa đó ạ. "
            f"Nếu chưa lên UAT thì mình check gấp giúp em nha 🙏 👉 {url}"
        )
    
    if code == POST_VERSION_ALERT and data:
        return (
            f"🚨 Task [{key}] - {summary} thuộc version **{data['fv_name']}** "
            f"đã quá hạn release ({data['release_date']}) mà chưa chuyển trạng thái Complete. "
            f"Mình kiểm tra giúp em với nha 🕐 👉 {url}"
        )
    
    if code == ASSIGNEE_CHANGED and data:
        old = data.get("old_assignee") or ""
        new = task.get("assignee_email") or ""
        return (
            f"👋 Task [{key}] - {summary} vừa được giao cho mình bắt đầu làm thôi nè) "
            f" 💪 👉 {url}"
        )
    
    if code == DUE_DATE_OVERDUE and data:
        return (
            f"⏳ Task [{key}] - {summary} đã quá hạn due date ({data.get('due_date')}) "
            f"{data.get('days_overdue')} ngày rồi ạ. Mình xử lý giúp em nhé 👉 {url}"
        )

    if code == RECENTLY_CREATED and data:
        return (
            f"🆕 Task [{key}] - {summary} vừa được tạo gần đây (created: {data.get('created_at')}). "
            f"Mời anh/chị vào xem và bắt đầu xử lý nha 👉 {url}"
        )
    
    return f"ℹ️ Task [{key}] - {summary}: {url}"

def build_combined_message(task: dict, findings: List[Tuple[str, Optional[dict], str]]) -> str:
    """Tạo tin nhắn tổng hợp (giọng dễ thương, thân thiện) cho nhiều rule cùng một task."""
    url = task.get("task_url")
    task_key = task.get("key", "")
    task_summary = task.get("summary", "")

    messages = []

    for code, data, _ in findings:
        if code == MISSING_LOGTIME:
            messages.append("⏰ Anh/chị ơi, mình ở CI Testing hơi lâu mà chưa logtime đó nha 😅.")
        elif code == MISSING_DESCRIPTION:
            reporter = task.get("reporter_email") or "Reporter"
            messages.append(f"📝 Task chưa có description. Anh/chị {reporter} bổ sung giúp, để chúng ta làm việc nhanh hơn nha 🙏.")
        elif code == PRE_VERSION_REMINDER and data:
            messages.append(
                f"📦 Task thuộc version **{data['fv_name']}** sắp release trong {data['days']} ngày nữa. "
                f".Chưa được chuyển trạng thái qua UAT or Ready UAT 🕵️‍♂️."
            )
        elif code == POST_VERSION_ALERT and data:
            messages.append(
                f"🚨 Task thuộc version **{data['fv_name']}** đã quá hạn release ({data['release_date']}) "
                f"mà vẫn chưa chuyển trạng thái Complete."
            )
        elif code == ASSIGNEE_CHANGED and data:
            messages.append("👋 Bé bot báo nè! Task này vừa được gán cho anh/chị đó 💪.")
        elif code == DUE_DATE_OVERDUE and data:
            messages.append(
                f"⏳ Task đã quá hạn due date ({data.get('due_date')}) {data.get('days_overdue')} ngày."
            )
        elif code == RECENTLY_CREATED and data:
            messages.append(
                f"🆕 Task vừa được tạo (created: {data.get('created_at')}). Mời vào xem cho nóng nha!"
            )

    if messages:
        combined = (
            f"🎯 Task [{task_key}] - {task_summary}:\n"
            + "\n".join(f"• {msg}" for msg in messages)
            + f"\n\n🔗 Link kiểm tra nhanh: {url}\n"
            + ". Mình xử lý giúp em với nha 🏃‍♀️.— Bé bot nhà FRT thân ái nhắc nhẹ ❤️"
        )
        return combined

    return f"ℹ️ Task [{task_key}] - {task_summary}: {url}"

def run_once():
    load_dotenv()
    jira_url = os.getenv("JIRA_URL")
    jira_user = os.getenv("JIRA_USERNAME")
    jira_token = os.getenv("JIRA_TOKEN")
    jira_auth_type = os.getenv("JIRA_AUTH_TYPE", "basic").lower()
    # FPT Chat API
    chat_base_url = os.getenv("FPT_CHAT_BASE_URL", "https://api-chat.fpt.com/bot-external-api/ext-bot")
    chat_bot_id = os.getenv("FPT_CHAT_BOT_ID", "")
    schedule_minutes = int(os.getenv("SCHEDULE_INTERVAL_MINUTES", "15"))
    employees_file = os.getenv("EMPLOYEES_FILE", "employees.csv")
    history_path = os.getenv("HISTORY_FILE", "history.csv")
    #projects = [p.strip() for p in (os.getenv("JIRA_PROJECTS", "FC,FSS,PPFP,FADS").split(",")) if p.strip()]
    projects = [p.strip() for p in (os.getenv("JIRA_PROJECTS", "TADS,IPTPE,PPFP").split(",")) if p.strip()]

    config_path = os.path.join(os.getcwd(), "rules_config.json")
    config = _load_json(config_path) if os.path.exists(config_path) else {}
    ci_wait = int(config.get("ci_testing_wait_minutes", 5))
    pre_days = int(config.get("pre_version_days", 2))
    # Dedup window per requirement: 3 hours by default
    resend_after_hours = int(config.get("resend_after_hours", 3))
    # Rule 1 window: consider tasks assigned within last 60 minutes by default
    assignee_change_wait = int(config.get("assignee_change_wait_minutes", 60))
    # Recently created window: default to same as assignee_change_wait
    created_wait = int(config.get("created_wait_minutes", assignee_change_wait))
    domains_allowed = config.get("domains_allowed", ["FRT"])
    # CR scan days: quét CR tasks trong vòng X ngày
    cr_scan_days = int(config.get("cr_scan_days", 1))
    # Excluded statuses for search (configurable)
    excluded_statuses = config.get("excluded_statuses") or [
        "5. Golive",
        "Closed",
        "Cancel",
        "Rejected",
        "9. CLOSED",
        "10. Cancelled",
        "COMPLETED",
    ]
    # Parallel search controls
    jira_parallel = bool(config.get("jira_parallel", True))
    jira_parallel_workers = int(config.get("jira_parallel_workers", 8))
    jira_page_size = int(config.get("jira_page_size", 400))

    logger.info("Starting reminder run")
    logger.debug(f"Jira URL: {jira_url}, user: {jira_user}, auth: {jira_auth_type}")
    logger.debug(f"Projects: {projects}, schedule_minutes: {schedule_minutes}")
    logger.debug(f"Config -> ci_wait: {ci_wait} min, pre_days: {pre_days}, resend_after_hours: {resend_after_hours}, assignee_change_wait: {assignee_change_wait} min, cr_scan_days: {cr_scan_days} days")

    # Load employees mapping from DB
    logger.info("Loading employees mapping from DB")
    employees_df = _read_employees(employees_file)
    logger.info(f"Loaded {len(employees_df)} employees from DB")

    # Load history from DB
    logger.info("Loading reminder history from DB")
    history_rows = _load_history(history_path)
    logger.info(f"Loaded {len(history_rows)} history records from DB")

    # Prepare services
    jira = JiraClient(jira_url, jira_user, jira_token, projects, auth_type=jira_auth_type)
    # Ping để xác nhận kết nối Jira
    print(f"[Bot] Jira ping...")
    jira.ping()
    # Fetch tasks updated recently (bao gồm CR tasks trong vòng cr_scan_days)
    logger.info(f"Fetching tasks CR tasks in last {cr_scan_days} days")
    print(f"[Bot] Fetching tasks: last {schedule_minutes} minutes, CR tasks in last {cr_scan_days} days, projects={projects}")
    tasks = jira.search_recent_tasks(
        schedule_minutes,
        cr_scan_days,
        excluded_statuses=excluded_statuses,
        test_emails=TEST_EMAILS if TEST_EMAILS else None,
        parallel=jira_parallel,
        parallel_workers=jira_parallel_workers,
        page_size=jira_page_size,
    )
    logger.info(f"Fetched {len(tasks)} tasks")
    print(f"[Bot] Tasks fetched: {len(tasks)}")

    count_attempt = 0
    count_sent = 0
    
    # Parallel processing configuration
    task_parallel = bool(config.get("task_parallel", True))
    task_parallel_workers = int(config.get("task_parallel_workers", 4))
    
    # Thread-safe list to collect records
    all_records = []
    records_lock = threading.Lock()
    print(f"[Bot] Initialized all_records list (empty)")
    
    def process_task(task: dict) -> List[dict]:
        """Xử lý một task và trả về list records để lưu log."""
        task_records = []
        task_key = task.get('key', 'unknown')  # Define task_key ở đầu function để tránh scope issue
        try:
            logger.info(f"task {task.get('key')} - {task.get('summary')} - {task.get('assignee_email')} - {task.get('reporter_email')} - {task.get('status')}")
            print(f"[Bot] Evaluate task {task.get('key')} status={task.get('status')} assignee={task.get('assignee_email')} reporter={task.get('reporter_email')}")
            # Evaluate rules
            findings = []  # (code, data, recipient_email)

            r1 = evaluate_missing_logtime(task, ci_wait)
            if r1:
                logger.debug(f"Rule hit: MISSING_LOGTIME for {task.get('key')}")
                findings.append((MISSING_LOGTIME, None, task.get("assignee_email")))

            r2 = evaluate_missing_description(task)
            if r2:
                logger.debug(f"Rule hit: MISSING_DESCRIPTION for {task.get('key')}")
                findings.append((MISSING_DESCRIPTION, None, task.get("reporter_email")))

            r3 = evaluate_pre_version_reminder(task, pre_days)
            if isinstance(r3, dict):
                logger.debug(f"Rule hit: PRE_VERSION_REMINDER for {task.get('key')} -> {r3}")
                findings.append((PRE_VERSION_REMINDER, r3, task.get("assignee_email")))

            r4 = evaluate_post_version_alert(task)
            if isinstance(r4, dict):
                logger.debug(f"Rule hit: POST_VERSION_ALERT for {task.get('key')} -> {r4}")
                findings.append((POST_VERSION_ALERT, r4, task.get("assignee_email")))

            # Due date overdue
            r6 = evaluate_due_date_overdue(task)
            if isinstance(r6, dict):
                logger.debug(f"Rule hit: DUE_DATE_OVERDUE for {task.get('key')} -> {r6}")
                findings.append((DUE_DATE_OVERDUE, r6, task.get("assignee_email")))

            # Check assignee changed - need to get changelog info first
            if task.get("assignee_email"):
                if task.get("last_assignee_changed_at") is None:
                    try:
                        changed_at = jira.get_last_assignee_change(task.get("key"))
                        task["last_assignee_changed_at"] = changed_at
                    except Exception as ex:
                        logger.warning(f"Failed to get last assignee change for {task.get('key')}: {ex}")
                        task["last_assignee_changed_at"] = None
                
                r5 = evaluate_assignee_changed(task, assignee_change_wait)
                if isinstance(r5, dict):
                    logger.debug(f"Rule hit: ASSIGNEE_CHANGED for {task.get('key')} -> {r5}")
                    findings.append((ASSIGNEE_CHANGED, r5, task.get("assignee_email")))

            # Recently created tasks
            r7 = evaluate_created_recently(task, created_wait)
            if isinstance(r7, dict):
                logger.debug(f"Rule hit: RECENTLY_CREATED for {task.get('key')} -> {r7}")
                findings.append((RECENTLY_CREATED, r7, task.get("assignee_email") or task.get("reporter_email")))

            print(f"[Bot] Findings for {task_key}: {len(findings)}")
            logger.info(f"Task {task_key}: found {len(findings)} findings: {[code for code, _, _ in findings]}")

            # Normalize recipients and group findings by recipient
            recipient_findings = {}
            recipients_after_normalize = 0
            skipped_no_recipient = 0
            for code, data, recipient_email in findings:
                if not recipient_email:
                    recipient_email = task.get("reporter_email")
                if not recipient_email:
                    skipped_no_recipient += 1
                    logger.debug(f"Skip send: no recipient for task {task_key} rule {code}")
                    print(f"[Bot] Skip send {task_key} {code}: no recipient")
                    continue
                
                if recipient_email not in recipient_findings:
                    recipient_findings[recipient_email] = []
                    recipients_after_normalize += 1
                recipient_findings[recipient_email].append((code, data, recipient_email))
            
            print(f"[Bot] Task {task_key}: recipients_after_normalize={recipients_after_normalize}, skipped_no_recipient={skipped_no_recipient}")
            logger.info(f"Task {task_key}: recipients_after_normalize={recipients_after_normalize}, skipped_no_recipient={skipped_no_recipient}")

            # Send one combined message per recipient
            skipped_test_email = 0
            skipped_already_sent = 0
            skipped_same_day = 0
            messages_sent = 0
            
            for recipient_email, recipient_finding_list in recipient_findings.items():
                if not is_test_email(recipient_email):
                    skipped_test_email += 1
                    logger.debug(f"Skip send: {recipient_email} not in test email list")
                    print(f"[Bot] Skip send {task_key}: {recipient_email} not in test email list")
                    continue
                
                # Check if any rule was already sent or same-day duplicate exists
                should_skip = False
                skipped_rule_code = None
                skip_reason = ""
                for code, data, _ in recipient_finding_list:
                    if _already_sent(history_rows, task["key"], code, recipient_email, resend_after_hours):
                        logger.info(f"Skip send: rule {code} for task {task_key} already sent within last {resend_after_hours}h to {recipient_email}")
                        skipped_already_sent += 1
                        should_skip = True
                        skipped_rule_code = code
                        skip_reason = f"already sent within last {resend_after_hours}h"
                        break

                    if _already_logged_today_by_level(task["key"], code, "INFO"):
                        logger.info(f"SKIP SEND (same-day duplicate): task {task_key} rule {code} level INFO")
                        print(f"[Bot] Skip send {task_key}: trùng rule {code} và level INFO trong ngày — chờ qua ngày báo lại sau")
                        skipped_same_day += 1
                        should_skip = True
                        skipped_rule_code = code
                        skip_reason = "same-day duplicate (rule & level)"
                        break

                if should_skip:
                    print(f"[Bot] Skip send {task_key}: {skip_reason} for rule {skipped_rule_code} -> {recipient_email}")
                    logger.info(f"SKIPPED: task {task_key} rule {skipped_rule_code} to {recipient_email} ({skip_reason})")
                    continue

                # Build combined message
                if len(recipient_finding_list) == 1:
                    code, data, _ = recipient_finding_list[0]
                    text = build_message(task, code, data)
                    rule_codes = [code]
                else:
                    text = build_combined_message(task, recipient_finding_list)
                    rule_codes = [code for code, _, _ in recipient_finding_list]

                # mapping chat id
                chat_id = _lookup_chat_id(employees_df, recipient_email) or recipient_email
                print(f"[Bot] Send -> task={task.get('key')} rules={rule_codes} to={recipient_email} group={chat_id if chat_id and chat_id != recipient_email else None}")

                # Attempt send
                logger.info(f"Sending combined message for {task.get('key')} rules {rule_codes} to {recipient_email} (group_id: {chat_id if chat_id and chat_id != recipient_email else None})")
                ok, resp = send_message_fpt(
                    chat_base_url,
                    chat_bot_id,
                    text,
                    user_emails=[recipient_email] if recipient_email else None,
                    group_id=chat_id if chat_id and chat_id != recipient_email else None,
                )
                if ok:
                    messages_sent += 1
                    logger.info(f"Sent OK for {task_key} rules {rule_codes} to {recipient_email}")
                else:
                    logger.warning(f"Send FAILED for {task_key} rules {rule_codes} to {recipient_email}")
                logger.debug(f"Send response: {resp}")

                # Collect records for later batch save
                for code, data, _ in recipient_finding_list:
                    record = {
                        "task_key": task["key"],
                        "rule_type": code,
                        "to": recipient_email,
                        "sent_at": get_vn_now().isoformat(),
                        "status": "sent" if ok else "failed",
                        "response": json.dumps(resp) if isinstance(resp, dict) else str(resp),
                    }
                    task_records.append(record)
            
            # Log summary for this task
            print(f"[Bot] Task {task_key} summary: findings={len(findings)}, recipients_after_normalize={recipients_after_normalize}, skipped_test_email={skipped_test_email}, skipped_already_sent={skipped_already_sent}, skipped_same_day={skipped_same_day}, messages_sent={messages_sent}, records_created={len(task_records)}")
            logger.info(f"Task {task_key} summary: findings={len(findings)}, recipients_after_normalize={recipients_after_normalize}, skipped_test_email={skipped_test_email}, skipped_already_sent={skipped_already_sent}, skipped_same_day={skipped_same_day}, messages_sent={messages_sent}, records_created={len(task_records)}")
        except Exception as ex:
            logger.exception(f"Error processing task {task_key}: {ex}")
            print(f"[Bot] ERROR processing task {task_key}: {ex}")
        
        print(f"[Bot] process_task({task_key}) returning {len(task_records)} records")
        logger.debug(f"process_task({task_key}) returning {len(task_records)} records: {[r.get('rule_type') for r in task_records]}")
        return task_records

    # Process tasks in parallel or sequentially
    if task_parallel and len(tasks) > 1:
        print(f"[Bot] Processing {len(tasks)} tasks in parallel with {task_parallel_workers} workers")
        logger.info(f"Processing {len(tasks)} tasks in parallel with {task_parallel_workers} workers")
        
        with ThreadPoolExecutor(max_workers=task_parallel_workers) as executor:
            futures = {executor.submit(process_task, task): task for task in tasks}
            completed = 0
            for future in as_completed(futures):
                task = futures[future]
                try:
                    records = future.result()
                    print(f"[Bot] future.result() for task {task.get('key')}: type={type(records)}, value={records}")
                    logger.debug(f"future.result() for task {task.get('key')}: type={type(records)}, len={len(records) if isinstance(records, list) else 'N/A'}, records={records}")
                    if records is None:
                        records = []
                        print(f"[Bot] WARNING: task {task.get('key')} returned None, converted to empty list")
                    if not isinstance(records, list):
                        print(f"[Bot] WARNING: task {task.get('key')} returned non-list type {type(records)}, converting to list")
                        records = list(records) if records else []
                    with records_lock:
                        before_count = len(all_records)
                        all_records.extend(records)
                        after_count = len(all_records)
                    completed += 1
                    count_attempt += len([r for r in records if r.get("status") == "sent" or r.get("status") == "failed"])
                    count_sent += len([r for r in records if r.get("status") == "sent"])
                except Exception as ex:
                    logger.exception(f"Error in parallel task processing for {task.get('key', 'unknown')}: {ex}")
                    print(f"[Bot] ERROR in parallel task processing for {task.get('key', 'unknown')}: {ex}")
    else:
        print(f"[Bot] Processing {len(tasks)} tasks sequentially")
        logger.info(f"Processing {len(tasks)} tasks sequentially")
        for task in tasks:
            records = process_task(task)
            all_records.extend(records)
            count_attempt += len([r for r in records if r.get("status") == "sent" or r.get("status") == "failed"])
            count_sent += len([r for r in records if r.get("status") == "sent"])

    # Batch save all records to DB after processing all tasks
    print(f"[Bot] Total tasks processed: {len(tasks)}, Total records collected: {len(all_records)}")
    logger.info(f"Total tasks processed: {len(tasks)}, Total records collected: {len(all_records)}")
    print(f"[Bot] Saving {len(all_records)} records to DB...")
    logger.info(f"Batch saving {len(all_records)} records to DB")
    
    # Save to CSV (legacy support)
    for record in all_records:
        _append_history(history_path, record)
    
    # Batch insert to DB
    if all_records:
        try:
            # Convert records to format for batch insert
            batch_records = []
            for record in all_records:
                level = "INFO" if record.get("status") == "sent" else "WARNING"
                message = json.dumps(record, ensure_ascii=False)
                email = record.get("to") or record.get("email")
                task_key = record.get("task_key")
                rule_type = record.get("rule_type")
                
                batch_records.append({
                    "level": level,
                    "source": "reminder_bot",
                    "message": message,
                    "email": email,
                    "task_key": task_key,
                    "rule_type": rule_type,
                })
            
            # Batch insert
            create_logs_batch(batch_records)
            print(f"[Bot] Batch inserted {len(batch_records)} records to DB")
            logger.info(f"Batch inserted {len(batch_records)} records to DB")
        except Exception as ex:
            logger.exception(f"Failed to batch insert records: {ex}")
            print(f"[Bot] ERROR: Failed to batch insert records: {ex}")
            # Fallback: insert one by one
            logger.info("Falling back to individual inserts")
            for record in all_records:
                try:
                    _append_history_service(record)
                except Exception as ex2:
                    logger.warning(f"Failed to insert individual record: {ex2}")
    
    print(f"[Bot] Saved {len(all_records)} records to DB")

    logger.info(f"Attempts: {count_attempt}, Sent: {count_sent}")

def parse_and_run():
     parser = argparse.ArgumentParser()
     parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
     args = parser.parse_args()
     if args.once:
         run_once()
     else:
         # default single run for now
         run_once()

if __name__ == "__main__":
    parse_and_run()
