#!/usr/bin/env python3
"""Poll WhatsApp group messages, classify them with Poe, and create ClickUp tasks."""

from __future__ import annotations

import json
import mimetypes
import sqlite3
import sys
import time
import uuid
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
ALBUM_GAP_SECONDS = 5
ALBUM_SETTLE_SECONDS = 8

POE_URL = "https://api.poe.com/v1/chat/completions"
CLICKUP_TASK_URL = "https://api.clickup.com/api/v2/list/{list_id}/task"
CLICKUP_ATTACHMENT_URL = "https://api.clickup.com/api/v2/task/{task_id}/attachment"
WHATSAPP_SEND_URL = "http://localhost:8080/api/send"
WHATSAPP_DOWNLOAD_URL = "http://localhost:8080/api/download"
NOTIFY_NUMBER = "85267037123"
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"}

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


def album_columns_sql(conn: sqlite3.Connection) -> str:
    names = {row[1] for row in conn.execute("PRAGMA table_info(messages)").fetchall()}
    if {"album_id", "album_index", "album_size"} <= names:
        return "messages.album_id, messages.album_index, messages.album_size"
    return "NULL AS album_id, NULL AS album_index, NULL AS album_size"


def message_from_row(row: sqlite3.Row) -> dict[str, Any]:
    keys = set(row.keys())
    album_index = row["album_index"] if "album_index" in keys else None
    album_size = row["album_size"] if "album_size" in keys else None
    return {
        "id": row["id"],
        "chat_jid": row["chat_jid"],
        "sender": row["sender"],
        "sender_phone": resolve_sender_phone(row["sender"]),
        "content": row["content"] or "",
        "timestamp": row["timestamp"],
        "media_type": row["media_type"] or "",
        "filename": row["filename"] or "",
        "chat_name": row["chat_name"],
        "album_id": (row["album_id"] or "") if "album_id" in keys else "",
        "album_index": album_index,
        "album_size": album_size,
    }


def is_candidate_message(row: sqlite3.Row) -> bool:
    media_type = str(row["media_type"] or "").strip().lower()
    if media_type == "album":
        return False
    if str(row["content"] or "").strip():
        return True
    if media_type == "image":
        return True
    if media_type == "document":
        suffix = Path(str(row["filename"] or "")).suffix.lower()
        return suffix in IMAGE_EXTENSIONS
    return False


def fetch_new_messages(started_at: str) -> list[dict[str, Any]]:
    if not MESSAGES_DB.exists():
        log(f"Waiting for database at {MESSAGES_DB}")
        return []

    started = parse_timestamp(started_at)
    conn = sqlite3.connect(str(MESSAGES_DB))
    conn.row_factory = sqlite3.Row
    try:
        album_cols = album_columns_sql(conn)
        rows = conn.execute(
            f"""
            SELECT messages.id, messages.chat_jid, messages.sender, messages.content,
                   messages.timestamp, messages.media_type, messages.filename,
                   chats.name AS chat_name, {album_cols}
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE chats.name = ?
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
        if not is_candidate_message(row):
            continue
        try:
            if parse_timestamp(str(row["timestamp"])) <= started:
                continue
        except ValueError:
            continue
        messages.append(message_from_row(row))
    return messages


def fetch_album_members(chat_jid: str, album_id: str, started_at: str) -> list[dict[str, Any]]:
    started = parse_timestamp(started_at)
    conn = sqlite3.connect(str(MESSAGES_DB))
    conn.row_factory = sqlite3.Row
    try:
        album_cols = album_columns_sql(conn)
        rows = conn.execute(
            f"""
            SELECT messages.id, messages.chat_jid, messages.sender, messages.content,
                   messages.timestamp, messages.media_type, messages.filename,
                   chats.name AS chat_name, {album_cols}
            FROM messages
            JOIN chats ON messages.chat_jid = chats.jid
            WHERE messages.chat_jid = ?
              AND (messages.album_id = ? OR messages.id = ?)
            ORDER BY messages.timestamp ASC, messages.id ASC
            """,
            (chat_jid, album_id, album_id),
        ).fetchall()
    finally:
        conn.close()

    parent_size = None
    members: list[dict[str, Any]] = []
    for row in rows:
        if is_excluded_sender(row["sender"]):
            continue
        try:
            if parse_timestamp(str(row["timestamp"])) <= started:
                continue
        except ValueError:
            continue
        message = message_from_row(row)
        media_type = str(message.get("media_type") or "").strip().lower()
        if media_type == "album" or (
            message["id"] == album_id and not is_image_message(message)
        ):
            if message.get("album_size"):
                parent_size = int(message["album_size"])
            continue
        if not is_candidate_message(row):
            continue
        members.append(message)
    if parent_size:
        for message in members:
            if not message.get("album_size"):
                message["album_size"] = parent_size
    return members


def is_image_message(message: dict[str, Any]) -> bool:
    media_type = str(message.get("media_type") or "").strip().lower()
    if media_type == "image":
        return True
    if media_type != "document":
        return False
    suffix = Path(str(message.get("filename") or "")).suffix.lower()
    return suffix in IMAGE_EXTENSIONS


def message_body(message: dict[str, Any]) -> str:
    return str(message.get("content") or "").strip()


def album_sort_key(message: dict[str, Any]) -> tuple[int, str, str]:
    index = message.get("album_index")
    try:
        order = int(index)
    except (TypeError, ValueError):
        order = 10**9
    return (order, str(message.get("timestamp") or ""), str(message.get("id") or ""))


def cluster_album_size(cluster: list[dict[str, Any]]) -> int:
    sizes: list[int] = []
    for message in cluster:
        size = message.get("album_size")
        try:
            if size:
                sizes.append(int(size))
        except (TypeError, ValueError):
            continue
    return max(sizes) if sizes else 0


def cluster_by_proximity(messages: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    ordered = sorted(
        messages,
        key=lambda item: (
            item["chat_jid"],
            item["sender"],
            str(item["timestamp"]),
            item["id"],
        ),
    )
    clusters: list[list[dict[str, Any]]] = []
    used: set[str] = set()
    for index, message in enumerate(ordered):
        key = message_key(message["id"], message["chat_jid"])
        if key in used:
            continue
        if not is_image_message(message):
            used.add(key)
            clusters.append([message])
            continue
        cluster = [message]
        used.add(key)
        has_caption = bool(message_body(message))
        last_ts = parse_timestamp(str(message["timestamp"]))
        for other in ordered[index + 1 :]:
            other_key = message_key(other["id"], other["chat_jid"])
            if other_key in used:
                continue
            if other["chat_jid"] != message["chat_jid"] or other["sender"] != message["sender"]:
                continue
            if not is_image_message(other):
                continue
            other_ts = parse_timestamp(str(other["timestamp"]))
            if (other_ts - last_ts).total_seconds() > ALBUM_GAP_SECONDS:
                break
            other_caption = bool(message_body(other))
            if other_caption and has_caption:
                continue
            cluster.append(other)
            used.add(other_key)
            last_ts = other_ts
            has_caption = has_caption or other_caption
        clusters.append(cluster)
    return clusters


def cluster_unseen_messages(
    messages: list[dict[str, Any]],
    seen: set[str],
    started_at: str,
) -> list[list[dict[str, Any]]]:
    remaining: list[dict[str, Any]] = []
    album_groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for message in messages:
        key = message_key(message["id"], message["chat_jid"])
        if key in seen:
            continue
        album_id = str(message.get("album_id") or "").strip()
        if album_id:
            album_groups.setdefault((message["chat_jid"], album_id), []).append(message)
        else:
            remaining.append(message)

    clusters: list[list[dict[str, Any]]] = []
    for (chat_jid, album_id), partial in album_groups.items():
        members = fetch_album_members(chat_jid, album_id, started_at)
        if members:
            clusters.append(members)
        else:
            clusters.append(partial)
    clusters.extend(cluster_by_proximity(remaining))
    return clusters


def cluster_is_ready(cluster: list[dict[str, Any]], now: datetime) -> bool:
    images = [message for message in cluster if is_image_message(message)]
    if not images:
        return True
    album_size = cluster_album_size(cluster)
    if album_size and len(images) >= album_size:
        return True
    newest = max(parse_timestamp(str(message["timestamp"])) for message in cluster)
    if newest.tzinfo is None:
        newest = newest.replace(tzinfo=timezone.utc)
    return (now - newest).total_seconds() >= ALBUM_SETTLE_SECONDS


def build_user_content(message: dict[str, Any]) -> str:
    return (
        f"Subject: {message['chat_name']}\n\n"
        f"From: {message.get('sender_phone') or message['sender']}\n"
        f"Timestamp: {message['timestamp']}\n\n"
        f"Body: {message_body(message)}"
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
    lines = (message_text or "").strip().splitlines()
    first_line = lines[0].strip() if lines else ""
    return first_line[:200] if first_line else "WhatsApp triage"


def create_clickup_task(
    token: str,
    list_id: str,
    parent_task_id: str,
    classification: dict[str, Any],
    message: dict[str, Any],
) -> tuple[str, str, str]:
    title = classification.get("title")
    if not title or not str(title).strip() or str(title).strip().lower() == "null":
        title = fallback_title(message_body(message))
    title = str(title).strip()

    description_parts = [
        f"**Problem:** {classification.get('problem') or ''}",
        f"**Source:** {classification.get('source') or ''}",
        "**Original message:**",
        classification.get("original_text") or message_body(message),
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
    task_id = response.get("id")
    task_url = response.get("url") or task_id
    if not task_id or not task_url:
        raise RuntimeError(f"Unexpected ClickUp response: {response}")
    return title, str(task_url), str(task_id)


def download_whatsapp_media(message_id: str, chat_jid: str) -> tuple[str, str]:
    payload = {"message_id": message_id, "chat_jid": chat_jid}
    headers = {"Content-Type": "application/json"}
    response = http_json(WHATSAPP_DOWNLOAD_URL, payload, headers)
    if not response.get("success"):
        raise RuntimeError(response.get("message") or response)
    path = response.get("path")
    if not path:
        raise RuntimeError(f"Download response missing path: {response}")
    filename = response.get("filename") or Path(path).name
    return str(path), str(filename)


def attach_clickup_file(token: str, task_id: str, file_path: str, filename: str) -> None:
    path = Path(file_path)
    if not path.is_file():
        raise RuntimeError(f"Downloaded media file not found: {file_path}")
    name = filename or path.name
    file_bytes = path.read_bytes()
    content_type = mimetypes.guess_type(name)[0] or "application/octet-stream"
    boundary = uuid.uuid4().hex
    header = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="attachment"; filename="{name}"\r\n'
        f"Content-Type: {content_type}\r\n"
        f"\r\n"
    ).encode("utf-8")
    footer = f"\r\n--{boundary}--\r\n".encode("utf-8")
    body = header + file_bytes + footer
    url = CLICKUP_ATTACHMENT_URL.format(task_id=task_id)
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": token,
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            raw = response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} from {url}: {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Request to {url} failed: {exc.reason}") from exc
    if raw:
        json.loads(raw)


def send_subtask_whatsapp(title: str, task_url: str) -> None:
    payload = {
        "recipient": NOTIFY_NUMBER,
        "message": f"{title}\n{task_url}",
    }
    headers = {"Content-Type": "application/json"}
    response = http_json(WHATSAPP_SEND_URL, payload, headers)
    if not response.get("success", False):
        raise RuntimeError(response.get("message") or response)


def attach_images_to_task(
    token: str,
    task_id: str,
    images: list[dict[str, Any]],
) -> None:
    seen_paths: set[str] = set()
    for image in sorted(images, key=album_sort_key):
        try:
            file_path, filename = download_whatsapp_media(
                image["id"],
                image["chat_jid"],
            )
            resolved = str(Path(file_path).resolve())
            if resolved in seen_paths:
                raise RuntimeError(
                    f"Download for {image['id']} reused {resolved}; "
                    "media cache collided with another album image"
                )
            seen_paths.add(resolved)
            suffix = Path(filename or image.get("filename") or file_path).suffix or ".jpg"
            attach_name = f"{image['id']}{suffix.lower()}"
            attach_clickup_file(
                token,
                task_id,
                file_path,
                attach_name,
            )
            log(f"Attached image to ClickUp task {task_id}: {attach_name}")
        except Exception as exc:
            log(f"Failed to attach image for {image['id']}: {exc}")


def process_message(
    config: dict[str, str],
    message: dict[str, Any],
    image_messages: list[dict[str, Any]] | None = None,
) -> None:
    body = message_body(message)
    if not body:
        log(f"Skipping {message['id']}: no caption or text to classify")
        return
    preview = body.replace("\n", " ")[:80]
    log(f"Classifying {message['id']} from {message['sender']}: {preview}")
    classification = classify_message(config["POE_API_KEY"], message)
    category = str(classification.get("category") or "").strip().lower()
    log(f"Poe category for {message['id']}: {category or 'unknown'}")
    if category in {"bug", "support"}:
        title, task_url, task_id = create_clickup_task(
            config["CLICKUP_API_TOKEN"],
            config["CLICKUP_LIST_ID"],
            config["CLICKUP_PARENT_TASK_ID"],
            classification,
            message,
        )
        log(f"Created ClickUp subtask: {task_url}")
        images = image_messages
        if images is None:
            images = [message] if is_image_message(message) else []
        if images:
            attach_images_to_task(config["CLICKUP_API_TOKEN"], task_id, images)
        try:
            send_subtask_whatsapp(title, task_url)
            log(f"Sent subtask to {NOTIFY_NUMBER}")
        except Exception as exc:
            log(f"Failed to send subtask to {NOTIFY_NUMBER}: {exc}")
    elif category == "other":
        log(f"Skipping ClickUp task for {message['id']}")
    else:
        log(f"Unknown category {category!r} for {message['id']}; skipping task")


def process_cluster(config: dict[str, str], cluster: list[dict[str, Any]]) -> None:
    caption_message = next((message for message in cluster if message_body(message)), None)
    if caption_message is None:
        ids = ", ".join(message["id"] for message in cluster)
        log(f"Skipping {ids}: no caption or text to classify")
        return
    images = [message for message in cluster if is_image_message(message)]
    process_message(config, caption_message, image_messages=images)


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

        now = datetime.now(timezone.utc)
        for cluster in cluster_unseen_messages(messages, seen, state["started_at"]):
            keys = [message_key(message["id"], message["chat_jid"]) for message in cluster]
            if all(key in seen for key in keys):
                continue
            if not cluster_is_ready(cluster, now):
                continue
            try:
                process_cluster(config, cluster)
            except json.JSONDecodeError as exc:
                ids = ", ".join(message["id"] for message in cluster)
                log(f"Could not parse Poe JSON for {ids}: {exc}")
                continue
            except Exception as exc:
                ids = ", ".join(message["id"] for message in cluster)
                log(f"Failed to process {ids}: {exc}")
                continue
            for key in keys:
                if key not in seen:
                    seen.add(key)
                    mark_seen(state, key)

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    try:
        run()
    except KeyboardInterrupt:
        log("Stopped")
