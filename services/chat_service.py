from core.config import settings

try:
    import chat_api  # type: ignore
except Exception:  # pragma: no cover
    chat_api = None  # type: ignore


class ChatService:
    def __init__(self) -> None:
        self.base_url = settings.chat_base_url
        self.api_key = settings.chat_api_key

    def send_message(self, username: str, message: str) -> bool:
        if chat_api and hasattr(chat_api, "send_message"):
            return bool(chat_api.send_message(self.base_url, self.api_key, username, message))  # type: ignore
        # Fallback stub
        return False
