from typing import Generator
from fastapi import Depends

from services.reminder_service import ReminderService
from services.jira_service import JiraService
from services.chat_service import ChatService
from db.session import get_session


def get_jira_service() -> JiraService:
    return JiraService()


def get_chat_service() -> ChatService:
    return ChatService()


def get_reminder_service(
    jira: JiraService = Depends(get_jira_service),
    chat: ChatService = Depends(get_chat_service),
) -> ReminderService:
    return ReminderService(jira_service=jira, chat_service=chat)


def get_db_session() -> Generator:
    with get_session() as session:
        yield session
