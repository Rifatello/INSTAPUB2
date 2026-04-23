from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from typing import Optional


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


def accounts_path() -> Path:
    return DATA_DIR / "accounts.json"


def state_path() -> Path:
    return DATA_DIR / "state.json"


def queue_path() -> Path:
    return DATA_DIR / "queue.json"


def rejected_path() -> Path:
    return DATA_DIR / "rejected.json"


def load_accounts() -> list[dict[str, Any]]:
    data = read_json(accounts_path(), default=[])
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict) and x.get("status") == "active"]


def get_account(account_id: str) -> Optional[dict[str, Any]]:
    for account in load_accounts():
        if str(account.get("account_id", "")).strip() == account_id:
            return account
    return None


def toggle_account_schedule(account_id: str) -> bool:
    path = accounts_path()
    accounts = read_json(path, default=[])
    new_status = False
    for acc in accounts:
        if str(acc.get("account_id", "")).strip() == account_id:
            current = acc.get("schedule_enabled", False)
            acc["schedule_enabled"] = not current
            new_status = acc["schedule_enabled"]
            break
    write_json(path, accounts)
    return new_status


def load_state() -> dict[str, Any]:
    data = read_json(state_path(), default={})
    if not isinstance(data, dict):
        data = {}
    data.setdefault("pending_previews", {})
    return data


def save_state(data: dict[str, Any]) -> None:
    write_json(state_path(), data)


def load_queue() -> list[dict[str, Any]]:
    data = read_json(queue_path(), default=[])
    return data if isinstance(data, list) else []


def save_queue(data: list[dict[str, Any]]) -> None:
    write_json(queue_path(), data)


def load_approved_videos(limit: int = 20, account_id: Optional[str] = None) -> list[dict[str, Any]]:
    queue = load_queue()
    approved_statuses = {"queued", "publish_now", "ready_for_publish", "published", "publish_requested"}
    items = [x for x in queue if isinstance(x, dict) and x.get("status") in approved_statuses]
    if account_id:
        items = [x for x in items if str(x.get("account_id", "")) == account_id]
    return items[-limit:][::-1]


def delete_approved_preview(preview_id: str) -> bool:
    queue = load_queue()
    original_len = len(queue)
    new_queue = [x for x in queue if str(x.get("preview_id", "")) != preview_id]
    if len(new_queue) < original_len:
        save_queue(new_queue)
        return True
    return False


def load_rejected() -> list[dict[str, Any]]:
    data = read_json(rejected_path(), default=[])
    return data if isinstance(data, list) else []


def save_rejected(data: list[dict[str, Any]]) -> None:
    write_json(rejected_path(), data)


def _extract_pending_preview(preview_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    state = load_state()
    pending = state.setdefault("pending_previews", {})
    if not isinstance(pending, dict):
        pending = {}
        state["pending_previews"] = pending

    preview = pending.get(preview_id)
    if not isinstance(preview, dict):
        raise ValueError("Preview не найден или уже обработан.")

    pending.pop(preview_id, None)
    return preview, state


def approve_preview(preview_id: str) -> dict[str, Any]:
    preview, state = _extract_pending_preview(preview_id)
    now_iso = datetime.now(timezone.utc).isoformat()
    preview["status"] = "queued"
    preview["approved_at"] = now_iso

    queue = load_queue()
    queue.append(preview)
    save_queue(queue)
    save_state(state)
    return preview


def reject_preview(preview_id: str) -> dict[str, Any]:
    preview, state = _extract_pending_preview(preview_id)
    now_iso = datetime.now(timezone.utc).isoformat()
    preview["status"] = "rejected"
    preview["rejected_at"] = now_iso

    rejected = load_rejected()
    rejected.append(preview)
    save_rejected(rejected)
    save_state(state)
    return preview


def publish_preview(preview_id: str) -> dict[str, Any]:
    preview, state = _extract_pending_preview(preview_id)
    now_iso = datetime.now(timezone.utc).isoformat()
    preview["status"] = "publish_now"
    preview["publish_requested_at"] = now_iso

    queue = load_queue()
    queue.append(preview)
    save_queue(queue)
    save_state(state)
    return preview


def mark_publish_requested(
    preview_id: str,
    task_id: Optional[str],
    video_url: Optional[str],
    scheduled_at: Optional[int],
) -> dict[str, Any]:
    preview, state = _extract_pending_preview(preview_id)
    now_iso = datetime.now(timezone.utc).isoformat()
    preview["status"] = "publish_requested"
    preview["published_requested_at"] = now_iso
    preview["taskId"] = task_id
    preview["video_url"] = video_url
    preview["scheduled_at"] = scheduled_at

    queue = load_queue()
    queue.append(preview)
    save_queue(queue)
    save_state(state)
    return preview
