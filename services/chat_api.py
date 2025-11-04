import time
import requests
from typing import Optional, Tuple, List
from logger import get_logger

logger = get_logger()


def _is_allowed_domain(email: str, allowed_domains: list) -> bool:
    if not email or "@" not in email:
        return False
    domain = email.split("@")[-1]
    return any(domain.upper().startswith(d.upper()) for d in allowed_domains)


def send_message_fpt(
    base_url: str,
    bot_id: str,
    text: str,
    user_emails: Optional[List[str]] = None,
    group_id: Optional[str] = None,
    max_retries: int = 3,
    timeout: int = 15,
) -> Tuple[bool, dict]:
    """Send message via FPT Chat external bot API.

    Tries userEmails first; if not successful and group_id is provided, retries with groupId.
    """
    # Print inputs (mask text to preview only)
    text_preview = (text[:60] + "...") if isinstance(text, str) and len(text) > 60 else text
    print(f"[Chat] INPUT base_url={base_url} bot_id={bot_id} text='{text_preview}' emails={user_emails} group_id={group_id} retries={max_retries} timeout={timeout}")
    bot_id_sanitized = (str(bot_id) if bot_id is not None else "").strip().strip("/")
    if not bot_id_sanitized:
        err = {"error": "Missing FPT_CHAT_BOT_ID"}
        logger.error("Chat API missing bot_id (FPT_CHAT_BOT_ID)")
        print("[Chat] ERROR: missing bot_id (FPT_CHAT_BOT_ID)")
        return False, err
    url = f"{base_url.rstrip('/')}/bot/{bot_id_sanitized}/send-message"
    headers = {"Content-Type": "application/json", "accept": "*/*"}
    # Optional bearer token support via env FPT_CHAT_TOKEN
    chat_token = os.getenv("FPT_CHAT_TOKEN") if 'os' in globals() else None
    try:
        import os as _os
        chat_token = chat_token or _os.getenv("FPT_CHAT_TOKEN")
    except Exception:
        pass
    if chat_token:
        headers["Authorization"] = f"Bearer {chat_token}"
    logger.info(f"Chat API URL: {url}")
    print(f"[Chat] POST {url}")

    last_error: Optional[dict] = None

    # Case A: both email and group provided -> ưu tiên email 1 lần, lỗi thì chuyển qua group (retries)
    if user_emails and group_id:
        # one-shot email
        payload_email = {"userEmails": user_emails, "text": text}
        try:
            logger.debug(f"Attempt 1 (emails, prefer-first): to={user_emails}")
            print(f"[Chat] attempt=1 via emails (prefer-first) -> {len(user_emails)} recipients")
            print(f"[Chat] payload(emails)={payload_email}")
            resp = requests.post(url, json=payload_email, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                logger.info(f"Sent message to emails {user_emails}")
                print(f"[Chat] -> {resp.status_code}")
                try:
                    return True, resp.json() if resp.content else {"status": resp.status_code}
                except Exception:
                    return True, {"status": resp.status_code, "text": resp.text}
            last_error = {"status": resp.status_code, "detail": resp.text}
            logger.warning(f"Chat API non-200 (emails prefer-first): {last_error}")
            print(f"[Chat] -> {resp.status_code}: {resp.text[:120]}")
        except requests.RequestException as e:
            last_error = {"error": str(e)}
            logger.warning(f"Network issue (emails prefer-first): {e}")
            print(f"[Chat] network error (emails prefer-first): {e}")

        # fallback group with retries
        payload_group = {"groupId": group_id, "text": text}
        for attempt in range(1, max_retries + 1):
            try:
                logger.debug(f"Attempt {attempt} (group fallback): groupId={group_id}")
                print(f"[Chat] attempt={attempt} via group (fallback) -> {group_id}")
                print(f"[Chat] payload(group)={payload_group}")
                resp = requests.post(url, json=payload_group, headers=headers, timeout=timeout)
                if resp.status_code == 200:
                    logger.info(f"Sent message to group {group_id}")
                    print(f"[Chat] -> {resp.status_code}")
                    try:
                        return True, resp.json() if resp.content else {"status": resp.status_code}
                    except Exception:
                        return True, {"status": resp.status_code, "text": resp.text}
                last_error = {"status": resp.status_code, "detail": resp.text}
                logger.warning(f"Chat API non-200 (group fallback): {last_error}")
                print(f"[Chat] -> {resp.status_code}: {resp.text[:120]}")
            except requests.RequestException as e:
                last_error = {"error": str(e)}
                logger.warning(f"Network issue (group fallback) attempt={attempt}: {e}")
                print(f"[Chat] network error (group fallback) attempt={attempt}: {e}")
            time.sleep(min(2 ** attempt, 8))
        return False, last_error or {}

    # Case B: only emails -> retries on emails
    if user_emails:
        payload = {"userEmails": user_emails, "text": text}
        for attempt in range(1, max_retries + 1):
            try:
                logger.debug(f"Attempt {attempt} (emails): to={user_emails}")
                print(f"[Chat] attempt={attempt} via emails -> {len(user_emails)} recipients")
                print(f"[Chat] payload(emails)={payload}")
                resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
                if resp.status_code == 200:
                    logger.info(f"Sent message to emails {user_emails}")
                    print(f"[Chat] -> {resp.status_code}")
                    try:
                        return True, resp.json() if resp.content else {"status": resp.status_code}
                    except Exception:
                        return True, {"status": resp.status_code, "text": resp.text}
                last_error = {"status": resp.status_code, "detail": resp.text}
                logger.warning(f"Chat API non-200 (emails): {last_error}")
                print(f"[Chat] -> {resp.status_code}: {resp.text[:120]}")
            except requests.RequestException as e:
                last_error = {"error": str(e)}
                logger.warning(f"Network issue on attempt {attempt} (emails): {e}")
                print(f"[Chat] network error (emails) attempt={attempt}: {e}")
            time.sleep(min(2 ** attempt, 8))
        return False, last_error or {}

    # Case C: only group -> retries on group
    if group_id:
        payload = {"groupId": group_id, "text": text}
        for attempt in range(1, max_retries + 1):
            try:
                logger.debug(f"Attempt {attempt} (group): groupId={group_id}")
                print(f"[Chat] attempt={attempt} via group -> {group_id}")
                print(f"[Chat] payload(group)={payload}")
                resp = requests.post(url, json=payload, headers=headers, timeout=timeout)
                if resp.status_code == 200:
                    logger.info(f"Sent message to group {group_id}")
                    print(f"[Chat] -> {resp.status_code}")
                    try:
                        return True, resp.json() if resp.content else {"status": resp.status_code}
                    except Exception:
                        return True, {"status": resp.status_code, "text": resp.text}
                last_error = {"status": resp.status_code, "detail": resp.text}
                logger.warning(f"Chat API non-200 (group): {last_error}")
                print(f"[Chat] -> {resp.status_code}: {resp.text[:120]}")
            except requests.RequestException as e:
                last_error = {"error": str(e)}
                logger.warning(f"Network issue on attempt {attempt} (group): {e}")
                print(f"[Chat] network error (group) attempt={attempt}: {e}")
            time.sleep(min(2 ** attempt, 8))
        return False, last_error or {}

    # Case D: neither provided
    return False, {"error": "No recipients: both user_emails and group_id are empty"}
