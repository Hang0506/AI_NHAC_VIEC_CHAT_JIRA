from typing import Any

from core.config import settings

try:
    from services import jira_utils  # type: ignore
except Exception:  # pragma: no cover
    try:
        import jira_utils  # type: ignore
    except Exception:
        jira_utils = None  # type: ignore


class JiraService:
    def __init__(self) -> None:
        self.base_url = settings.jira_base_url

    def fetch_tasks(self) -> list[dict[str, Any]]:
        if jira_utils and hasattr(jira_utils, "fetch_tasks"):
            return jira_utils.fetch_tasks(self.base_url, settings.jira_username, settings.jira_api_token)  # type: ignore
        # Fallback stub
        return []

    def fetch_employees(self) -> list[dict[str, Any]]:
        if jira_utils and hasattr(jira_utils, "fetch_employees"):
            return jira_utils.fetch_employees(self.base_url, settings.jira_username, settings.jira_api_token)  # type: ignore
        return []
