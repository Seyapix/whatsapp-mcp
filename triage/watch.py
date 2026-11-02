#!/usr/bin/env python3
"""Poll WhatsApp group messages, classify them with Poe, and create ClickUp tasks."""

from __future__ import annotations

import json
import sqlite3
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
TRIAGE_DIR = Path(__file__).resolve().parent
MESSAGES_DB = REPO_ROOT / "whatsapp-bridge" / "store" / "messages.db"
WHATSAPP_STORE_DB = REPO_ROOT / "whatsapp-bridge" / "store" / "whatsapp.db"
ENV_PATH = TRIAGE_DIR / ".env"
STATE_PATH = TRIAGE_DIR / "state.json"

GROUP_NAME = "Test MCP wts"
EXCLUDED_SENDERS = {"85292472425", "249040038031362"}
POLL_SECONDS = 5

POE_URL = "https://api.poe.com/v1/chat/completions"
CLICKUP_TASK_URL = "https://api.clickup.com/api/v2/list/{list_id}/task"
WHATSAPP_SEND_URL = "http://localhost:8080/api/send"
NOTIFY_NUMBER = "85267037123"

SYSTEM_PROMPT = (
    'You are a triage classification agent responsible for analyzing incoming user '
    "messages to categorize them into `bug`, `support`, or `other`. Categorize a "
    "message as `bug` if it reports system errors, broken features, unexpected "
    "behavior, or failed operations (such as disappearing records, null values, or "
    "failed exports), as `support` if the user is requesting technical assistance, "
    "troubleshooting, or operational guidance for existing system features, and as "
    "`other` for general business inquiries, sales/pricing questions, non-technical "
    "feedback, feature requests, marketing chatter, greetings, or off-topic spam. "
    "Evaluate the incoming message strictly against these definitions and return "
    'ONLY a JSON object with five fields: `category` (string: "bug", "support", or '
    '"other"), `title` (a concise summary string using the format `[Bug] <short '
    "summary>` or `[Support] <short summary>` if category is `bug` or `support`, "
    "otherwise `null`), `problem` (a brief description of the reported core issue "
    "if category is `bug` or `support`, otherwise `null`), `source` (sender and "
    "timestamp information if available in context, otherwise `null`), and "
    "`original_text` (the full raw text of the incoming message if category is "
    "`bug` or `support`, otherwise `null`), with no additional surrounding text or "
    "markdown formatting outside the JSON object."
)


def log(message: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {message}", flush=True)


def load_env(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if path.exists():
        for raw_line in path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def require_config() -> dict[str, str]:
    file_env = load_env(ENV_PATH)
    config = {
        "POE_API_KEY": file_env.get("POE_API_KEY") or "",
        "CLICKUP_API_TOKEN": file_env.get("CLICKUP_API_TOKEN") or "",
        "CLICKUP_LIST_ID": file_env.get("CLICKUP_LIST_ID") or "",
        "CLICKUP_PARENT_TASK_ID": file_env.get("CLICKUP_PARENT_TASK_ID") or "",
    }
    missing = [key for key, value in config.items() if not value.strip()]
    if missing:
        log(
            "Missing values in triage/.env: "
            + ", ".join(missing)
            + ". Copy triage/.env.example to triage/.env and fill them in."
        )
        sys.exit(1)
    return config


def load_state() -> dict[str, Any]:
    if STATE_PATH.exists():
        try:
            data = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict) and data.get("started_at") and isinstance(data.get("seen_ids"), list):
                return {
                    "started_at": data["started_at"],
                    "seen_ids": list(data["seen_ids"]),
                }
        except (OSError, json.JSONDecodeError) as exc:
            log(f"Could not read state file, starting fresh: {exc}")
    started_at = datetime.now(timezone.utc).isoformat()
    state = {"started_at": started_at, "seen_ids": []}
    save_state(state)
    log(f"First start. Classifying messages after {started_at}")
    return state


def save_state(state: dict[str, Any]) -> None:
    STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")


def sender_user_id(sender: str) -> str:
    user = sender.split("@", 1)[0]
    return user.split(":", 1)[0]


def is_excluded_sender(sender: str) -> bool:
    return sender_user_id(sender) in EXCLUDED_SENDERS


def format_phone_number(number: str) -> str:
    digits = "".join(ch for ch in number if ch.isdigit())
    return f"+{digits}" if digits else number


def resolve_sender_phone(sender: str) -> str:
    user_id = sender_user_id(sender)
    if not WHATSAPP_STORE_DB.exists():
        return sender
    try:
        conn = sqlite3.connect(f"file:{WHATSAPP_STORE_DB.as_posix()}?mode=ro", uri=True, timeout=5)
        try:
            row = conn.execute(
                "SELECT pn FROM whatsmeow_lid_map WHERE lid = ? OR pn = ?",
                (user_id, user_id),
            ).fetchone()
        finally:
            conn.close()
    except sqlite3.Error:
        row = None
    if row and row[0]:
        return format_phone_number(row[0])
    return sender


def message_key(message_id: str, chat_jid: str) -> str:
    return f"{message_id}:{chat_jid}"


def parse_timestamp(value: str) -> datetime:
    text = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = datetime.strptime(value[:19], "%Y-%m-%d %H:%M:%S")
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def fetch_new_messages(started_at: str) -> list[dict[str, Any]]:
    if not MESSAGES_DB.exists():
        log(f"Waiting for database at {MESSAGES_DB}")
        return []

    started = parse_timestamp(started_at)
    conn = sqlite3.connect(str(MESSAGES_DB))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT messages.id, messages.chat_jid, messages.sender, messages.content,
                   messages.timestamp, chats.name AS chat_name
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE chats.name = ?
              AND TRIM(COALESCE(messages.content, '')) != ''
            ORDER BY messages.timestamp ASC
            """,
            (GROUP_NAME,),
        ).fetchall()
    finally:
        conn.close()

    messages = []
    for row in rows:
        if is_excluded_sender(row["sender"]):
            continue
        try:
            if parse_timestamp(str(row["timestamp"])) <= started:
                continue
        except ValueError:
            continue
        messages.append(
            {
                "id": row["id"],
                "chat_jid": row["chat_jid"],
                "sender": row["sender"],
                "sender_phone": resolve_sender_phone(row["sender"]),
                "content": row["content"],
                "timestamp": row["timestamp"],
                "chat_name": row["chat_name"],
            }
        )
    return messages


def build_user_content(message: dict[str, Any]) -> str:
    return (
        f"Subject: {message['chat_name']}\n\n"
        f"From: {message.get('sender_phone') or message['sender']}\n"
        f"Timestamp: {message['timestamp']}\n\n"
        f"Body: {message['content']}"
    )


def http_json(
    url: str,
    payload: dict[str, Any],
    headers: dict[str, str],
) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Request to {url} failed: {exc.reason}") from exc
    if not raw:
        return {}
    return json.loads(raw)


def strip_json_fence(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("```"):
        lines = stripped.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        stripped = "\n".join(lines).strip()
    return stripped


def classify_message(api_key: str, message: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "model": "GPT-4o-mini",
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": build_user_content(message)},
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    response = http_json(POE_URL, payload, headers)
    try:
        content = response["choices"][0]["message"]["content"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(f"Unexpected Poe response: {response}") from exc
    return json.loads(strip_json_fence(content))


def fallback_title(message_text: str) -> str:
    first_line = message_text.strip().splitlines()[0].strip()
    return first_line[:200] if first_line else "WhatsApp triage"


def create_clickup_task(
    token: str,
    list_id: str,
    parent_task_id: str,
    classification: dict[str, Any],
    message: dict[str, Any],
) -> tuple[str, str]:
    title = classification.get("title")
    if not title or not str(title).strip() or str(title).strip().lower() == "null":
        title = fallback_title(message["content"])
    title = str(title).strip()

    description_parts = [
        f"**Problem:** {classification.get('problem') or ''}",
        f"**Source:** {classification.get('source') or ''}",
        "**Original message:**",
        classification.get("original_text") or message["content"],
    ]
    payload = {
        "name": title,
        "markdown_description": "\n\n".join(description_parts),
        "parent": parent_task_id,
    }
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
    }
    url = CLICKUP_TASK_URL.format(list_id=list_id)
    response = http_json(url, payload, headers)
    task_url = response.get("url") or response.get("id")
    if not task_url:
        raise RuntimeError(f"Unexpected ClickUp response: {response}")
    return title, str(task_url)


def send_subtask_whatsapp(title: str, task_url: str) -> None:
    payload = {
        "recipient": NOTIFY_NUMBER,
        "message": f"{title}\n{task_url}",
    }
    headers = {"Content-Type": "application/json"}
    response = http_json(WHATSAPP_SEND_URL, payload, headers)
    if not response.get("success", False):
        raise RuntimeError(response.get("message") or response)


def process_message(config: dict[str, str], message: dict[str, Any]) -> None:
    preview = message["content"].replace("\n", " ")[:80]
    log(f"Classifying {message['id']} from {message['sender']}: {preview}")
    classification = classify_message(config["POE_API_KEY"], message)
    category = str(classification.get("category") or "").strip().lower()
    log(f"Poe category for {message['id']}: {category or 'unknown'}")
    if category in {"bug", "support"}:
        title, task_url = create_clickup_task(
            config["CLICKUP_API_TOKEN"],
            config["CLICKUP_LIST_ID"],
            config["CLICKUP_PARENT_TASK_ID"],
            classification,
            message,
        )
        log(f"Created ClickUp subtask: {task_url}")
        try:
            send_subtask_whatsapp(title, task_url)
            log(f"Sent subtask to {NOTIFY_NUMBER}")
        except Exception as exc:
            log(f"Failed to send subtask to {NOTIFY_NUMBER}: {exc}")
    elif category == "other":
        log(f"Skipping ClickUp task for {message['id']}")
    else:
        log(f"Unknown category {category!r} for {message['id']}; skipping task")


def mark_seen(state: dict[str, Any], key: str) -> None:
    if key not in state["seen_ids"]:
        state["seen_ids"].append(key)
        save_state(state)


def run() -> None:
    config = require_config()
    state = load_state()
    seen = set(state["seen_ids"])
    log(f"Watching group {GROUP_NAME!r} every {POLL_SECONDS}s")

    while True:
        try:
            messages = fetch_new_messages(state["started_at"])
        except sqlite3.Error as exc:
            log(f"Database error: {exc}")
            time.sleep(POLL_SECONDS)
            continue

        for message in messages:
            key = message_key(message["id"], message["chat_jid"])
            if key in seen:
                continue
            try:
                process_message(config, message)
            except json.JSONDecodeError as exc:
                log(f"Could not parse Poe JSON for {message['id']}: {exc}")
            except Exception as exc:
                log(f"Failed to process {message['id']}: {exc}")
                continue
            seen.add(key)
            mark_seen(state, key)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log("Stopped")
