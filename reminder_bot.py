import os
import sys
import json
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, List
import pandas as pd
 
from dotenv import load_dotenv
 
from logger import get_logger
from jira_utils import JiraClient
from rules import (
     evaluate_missing_logtime,
     evaluate_missing_description,
     evaluate_pre_version_reminder,
     evaluate_post_version_alert,
     evaluate_assignee_changed,
     MISSING_LOGTIME,
     MISSING_DESCRIPTION,
     PRE_VERSION_REMINDER,
     POST_VERSION_ALERT,
     ASSIGNEE_CHANGED,
)
from chat_api import send_message_fpt
 
logger = get_logger()
 
def _load_json(path: str) -> dict:
     with open(path, "r", encoding="utf-8") as f:
         return json.load(f)
 
def _ensure_dirs(path: str):
     os.makedirs(os.path.dirname(path), exist_ok=True)
 
def _read_employees(path: str) -> pd.DataFrame:
     logger.info(f"Loading employees mapping from: {path}")
     if not os.path.exists(path):
         logger.warning(f"Employees file not found: {path}")
         return pd.DataFrame(columns=["email", "chat_id"]) 
     _, ext = os.path.splitext(path.lower())
     if ext in (".xlsx", ".xls"):
         df = pd.read_excel(path)
     else:
         # Try normal read first (expects header row). If no email-like column is found, fallback to no-header.
         df = pd.read_csv(path, keep_default_na=False)
         lower_cols = [c.lower() for c in df.columns]
         if not any(c in ("email", "e-mail", "chat_id") for c in lower_cols):
             logger.info("employees.csv seems to have no header; re-reading with header=None")
             df = pd.read_csv(path, header=None, names=["email", "chat_id"], keep_default_na=False)
     # Normalize columns
     cols = {c.lower(): c for c in df.columns}
     email_col = cols.get("email") or cols.get("e-mail") or list(df.columns)[0]
     chat_col = cols.get("chat_id") if "chat_id" in cols else None
     if chat_col is None:
         df = df.rename(columns={email_col: "email"})
         df["chat_id"] = ""
     else:
         df = df.rename(columns={email_col: "email", chat_col: "chat_id"})
     df["email"] = df["email"].fillna("").astype(str).str.strip()
     df["chat_id"] = df["chat_id"].fillna("").astype(str).str.strip()
     # Drop empty or invalid email rows
     df = df[(df["email"] != "") & (df["email"].str.lower() != "nan")]
     logger.info(f"Loaded employees: {len(df)} rows")
     return df[["email", "chat_id"]]
 
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
 
def _load_history(path: str):
     if not os.path.exists(path):
         return []
     try:
         # Tránh lỗi khi file rỗng
         if os.path.getsize(path) == 0:
             logger.info(f"History file exists but empty: {path}")
             return []
         rows = pd.read_csv(path).to_dict(orient="records")
         logger.info(f"Loaded reminder history: {len(rows)} records from {path}")
         return rows
     except Exception as ex:
         logger.exception(f"Failed to load history from {path}: {ex}")
         return []
 
def _append_history(path: str, record: dict):
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
     logger.debug(f"Appended history record for task {record.get('task_key')} rule {record.get('rule_type')} -> {record.get('status')}")
 
def _already_sent(history_rows: list, task_key: str, rule_code: str, to_value: str, resend_after_hours: int) -> bool:
     now = datetime.now(timezone.utc)
     for r in history_rows:
         if r.get("task_key") == task_key and r.get("rule_type") == rule_code and r.get("to") == to_value:
             try:
                 sent_at = datetime.fromisoformat(r.get("sent_at"))
             except Exception:
                 return True
             if now - sent_at < timedelta(hours=resend_after_hours):
                 logger.debug(f"Skip send: recently sent for {task_key} {rule_code} to {to_value}")
                 return True
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
            f"đã quá hạn release ({data['release_date']}) mà chưa thấy lên Production. "
            f"Mình kiểm tra giúp em với nha 🕐 👉 {url}"
        )
    
    if code == ASSIGNEE_CHANGED and data:
        old = data.get("old_assignee") or ""
        new = task.get("assignee_email") or ""
        return (
            f"👋 Task [{key}] - {summary} vừa được giao cho mình bắt đầu làm thôi nè) "
            f" 💪 👉 {url}"
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
            messages.append(f"📝 Task chưa có description. Anh/chị {reporter} bổ sung giúp em với, để dev đỡ đoán nha 🙏.")
        elif code == PRE_VERSION_REMINDER and data:
            messages.append(
                f"📦 Task thuộc version **{data['fv_name']}** sắp release trong {data['days']} ngày nữa. "
                f"Nếu chưa lên UAT thì mình check giúp em nha 🕵️‍♂️."
            )
        elif code == POST_VERSION_ALERT and data:
            messages.append(
                f"🚨 Task thuộc version **{data['fv_name']}** đã quá hạn release ({data['release_date']}) "
                f"mà vẫn chưa thấy lên Production. Mình xử lý gấp giúp em với nha 🏃‍♀️."
            )
        elif code == ASSIGNEE_CHANGED and data:
            messages.append("👋 Bé bot báo nè! Task này vừa được gán cho anh/chị đó, cùng chiến thôi 💪.")

    if messages:
        combined = (
            f"🎯 Task [{task_key}] - {task_summary}:\n"
            + "\n".join(f"• {msg}" for msg in messages)
            + f"\n\n🔗 Link kiểm tra nhanh: {url}\n"
            + "— Bé bot nhà FRT thân ái nhắc nhẹ ❤️"
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
    #projects = [p.strip() for p in (os.getenv("JIRA_PROJECTS", "FC,FSS,PPFP,FADS").split(",")) if p.strip()]
    projects = [p.strip() for p in (os.getenv("JIRA_PROJECTS", "PPFP,FADS").split(",")) if p.strip()]
    history_path = os.getenv("REMINDER_HISTORY_FILE", "data/reminder_logs.csv")

    config_path = os.path.join(os.getcwd(), "rules_config.json")
    config = _load_json(config_path) if os.path.exists(config_path) else {}
    ci_wait = int(config.get("ci_testing_wait_minutes", 5))
    pre_days = int(config.get("pre_version_days", 2))
    resend_after_hours = int(config.get("resend_after_hours", 8))
    assignee_change_wait = int(config.get("assignee_change_wait_minutes", 5))
    domains_allowed = config.get("domains_allowed", ["FRT"])

    logger.info("Starting reminder run")
    logger.debug(f"Jira URL: {jira_url}, user: {jira_user}, auth: {jira_auth_type}")
    logger.debug(f"Projects: {projects}, schedule_minutes: {schedule_minutes}")
    logger.debug(f"Config -> ci_wait: {ci_wait} min, pre_days: {pre_days}, resend_after_hours: {resend_after_hours}, assignee_change_wait: {assignee_change_wait} min")
    logger.debug(f"Employees file: {employees_file}, history path: {history_path}")

    # Prepare services
    jira = JiraClient(jira_url, jira_user, jira_token, projects, auth_type=jira_auth_type)
    # Ping để xác nhận kết nối Jira
    print(f"[Bot] Jira ping...")
    jira.ping()
    employees_df = _read_employees(employees_file)
    history_rows = _load_history(history_path)

    # Fetch tasks updated recently
    logger.info(f"Fetching tasks updated in last {schedule_minutes} minutes for projects {projects}")
    print(f"[Bot] Fetching tasks: last {schedule_minutes} minutes, projects={projects}")
    tasks = jira.search_recent_tasks(schedule_minutes)
    logger.info(f"Fetched {len(tasks)} tasks")
    print(f"[Bot] Tasks fetched: {len(tasks)}")

    count_attempt = 0
    count_sent = 0

    for task in tasks:
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
            # Send to assignee and leader if available; here we only handle assignee + optional reporter as leader fallback
            logger.debug(f"Rule hit: POST_VERSION_ALERT for {task.get('key')} -> {r4}")
            findings.append((POST_VERSION_ALERT, r4, task.get("assignee_email")))

        # Check assignee changed - need to get changelog info first
        # Only check if task was recently updated to avoid unnecessary API calls
        if task.get("assignee_email"):
            # Lazy load last_assignee_changed_at only when needed
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

        print(f"[Bot] Findings for {task.get('key')}: {len(findings)}")

        # Normalize recipients and group findings by recipient
        recipient_findings = {}  # recipient_email -> list of (code, data, recipient_email)
        for code, data, recipient_email in findings:
            if not recipient_email:
                recipient_email = task.get("reporter_email")
            if not recipient_email:
                logger.debug(f"Skip send: no recipient for task {task.get('key')} rule {code}")
                print(f"[Bot] Skip send {task.get('key')} {code}: no recipient")
                continue
            
            if recipient_email not in recipient_findings:
                recipient_findings[recipient_email] = []
            recipient_findings[recipient_email].append((code, data, recipient_email))

        # Send one combined message per recipient
        for recipient_email, recipient_finding_list in recipient_findings.items():
            # Check if any rule was already sent (use first rule for dedup check)
            # Note: We check if ALL rules were sent, if any was sent recently, skip this recipient
            should_skip = False
            for code, data, _ in recipient_finding_list:
                if _already_sent(history_rows, task["key"], code, recipient_email, resend_after_hours):
                    logger.debug(f"Skip send: rule {code} for task {task.get('key')} already sent within last {resend_after_hours}h to {recipient_email}")
                    should_skip = True
                    break
            
            if should_skip:
                print(f"[Bot] Skip send {task.get('key')}: already sent to {recipient_email} within last {resend_after_hours}h")
                continue

            # Build combined message
            if len(recipient_finding_list) == 1:
                # Single rule - use original message format
                code, data, _ = recipient_finding_list[0]
                text = build_message(task, code, data)
                rule_codes = [code]
            else:
                # Multiple rules - use combined format
                text = build_combined_message(task, recipient_finding_list)
                rule_codes = [code for code, _, _ in recipient_finding_list]

            # mapping chat id
            chat_id = _lookup_chat_id(employees_df, recipient_email) or recipient_email
            print(f"[Bot] Send -> task={task.get('key')} rules={rule_codes} to={recipient_email} group={chat_id if chat_id and chat_id != recipient_email else None}")

            # Attempt send: try by email first; if fails, fallback to groupId (from employees.csv chat_id column)
            logger.info(f"Sending combined message for {task.get('key')} rules {rule_codes} to {recipient_email} (group_id: {chat_id if chat_id and chat_id != recipient_email else None})")
            ok, resp = send_message_fpt(
                chat_base_url,
                chat_bot_id,
                text,
                user_emails=[recipient_email] if recipient_email else None,
                group_id=chat_id if chat_id and chat_id != recipient_email else None,
            )
            count_attempt += 1
            if ok:
                count_sent += 1
                logger.info(f"Sent OK for {task.get('key')} rules {rule_codes} to {recipient_email}")
            else:
                logger.warning(f"Send FAILED for {task.get('key')} rules {rule_codes} to {recipient_email}")
            logger.debug(f"Send response: {resp}")

            # Log history for each rule (to track individual rule sends)
            for code, data, _ in recipient_finding_list:
                record = {
                    "task_key": task["key"],
                    "rule_type": code,
                    "to": recipient_email,
                    "sent_at": datetime.now(timezone.utc).isoformat(),
                    "status": "sent" if ok else "failed",
                    "response": json.dumps(resp) if isinstance(resp, dict) else str(resp),
                }
                _append_history(history_path, record)

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

