from __future__ import annotations
from datetime import datetime
from zoneinfo import ZoneInfo

from db.session import get_session
from db.repositories.employee_repo import EmployeeRepository
from db.repositories.reminder_repo import ReminderRepository
from services.jira_service import JiraService
from services.chat_service import ChatService

# Timezone Việt Nam
VN_TIMEZONE = ZoneInfo("Asia/Ho_Chi_Minh")

def get_vn_now() -> datetime:
    """Lấy datetime hiện tại theo giờ Việt Nam."""
    return datetime.now(VN_TIMEZONE)


class ReminderService:
    def __init__(self, jira_service: JiraService, chat_service: ChatService) -> None:
        self.jira = jira_service
        self.chat = chat_service

    async def sync_from_jira(self) -> int:
        employees = self.jira.fetch_employees()
        tasks = self.jira.fetch_tasks()

        synced = 0
        with get_session() as session:
            emp_repo = EmployeeRepository(session)
            rem_repo = ReminderRepository(session)

            # Upsert employees
            username_to_id: dict[str, int] = {}
            for emp in employees:
                e = emp_repo.upsert_by_username(
                    username=emp.get("username", ""),
                    display_name=emp.get("display_name", ""),
                    email=emp.get("email", ""),
                )
                username_to_id[e.username] = e.id or 0  # type: ignore[attr-defined]

            # Create reminders from tasks
            for t in tasks:
                username = t.get("assignee")
                if not username or username not in username_to_id:
                    continue
                employee_id = username_to_id[username]
                issue_key = t.get("issue_key", "")
                message = t.get("message", f"Reminder for {issue_key}")
                due_at = t.get("due_at") or get_vn_now()
                rem_repo.create(employee_id, issue_key, message, due_at)
                synced += 1

        return synced

    async def send_due_reminders(self) -> int:
        now = get_vn_now()
        sent = 0
        with get_session() as session:
            rem_repo = ReminderRepository(session)
            due = rem_repo.due_unsent(now)
            for reminder in due:
                # For simplicity, we assume username can be resolved via relationship
                username = reminder.employee.username  # type: ignore[attr-defined]
                if self.chat.send_message(username=username, message=reminder.message):
                    rem_repo.mark_sent(reminder)
                    sent += 1
        return sent
