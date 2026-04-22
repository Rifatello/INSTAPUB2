from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any
from typing import Union

import requests

from bot.storage_repo import load_accounts, load_queue, save_queue


PUBLISH_SERVICE_URL = os.getenv("PUBLISH_SERVICE_URL", "http://127.0.0.1:8010/publish").strip()


def _mark_ready(item: dict[str, Any]) -> None:
    item["status"] = "ready_for_publish"
    item["processed_at"] = datetime.now(timezone.utc).isoformat()


def _accounts_by_id() -> dict[str, dict[str, Any]]:
    return {str(x.get("account_id")): x for x in load_accounts() if isinstance(x, dict)}


def _publish_via_service(item: dict[str, Any]) -> tuple[bool, Union[dict[str, Any], str]]:
    preview_video = str(item.get("preview_video", "")).strip()
    if not preview_video:
        return False, "preview_video is empty"

    accounts = _accounts_by_id()
    account_id = str(item.get("account_id", "")).strip()
    account = accounts.get(account_id, {})
    cloud_phone_id = str(account.get("cloud_phone_id") or "").strip()
    if not cloud_phone_id:
        return False, f"cloud_phone_id is missing for account {account_id}"
    if not cloud_phone_id.isdigit():
        return False, f"cloud_phone_id must be numeric for account {account_id}"

    caption = str(item.get("caption", "")).strip()
    payload = {"video_path": preview_video, "caption": caption, "cloud_phone_id": cloud_phone_id}

    try:
        response = requests.post(PUBLISH_SERVICE_URL, json=payload, timeout=180)
        if response.status_code // 100 != 2:
            return False, f"HTTP {response.status_code}: {response.text[:400]}"
        data = response.json()
        if not isinstance(data, dict) or not data.get("success"):
            return False, f"Invalid publish response: {data}"
        return True, data
    except Exception as exc:
        return False, str(exc)


def process_queue_once(process_queued_in_test_mode: bool = False) -> dict[str, Any]:
    queue = load_queue()
    publish_now_items = [x for x in queue if isinstance(x, dict) and x.get("status") == "publish_now"]
    queued_items = [x for x in queue if isinstance(x, dict) and x.get("status") == "queued"]
    updated_ids: list[str] = []

    if publish_now_items:
        target = publish_now_items[0]
        _mark_ready(target)
        updated_ids.append(str(target.get("preview_id", "unknown")))
    elif process_queued_in_test_mode and queued_items:
        target = queued_items[0]
        _mark_ready(target)
        updated_ids.append(str(target.get("preview_id", "unknown")))

    save_queue(queue)
    return {
        "total": len(queue),
        "publish_now_found": len(publish_now_items),
        "queued_found": len(queued_items),
        "updated": len(updated_ids),
        "updated_preview_ids": updated_ids,
    }


def publish_queue_once() -> dict[str, Any]:
    queue = load_queue()
    now_iso = datetime.now(timezone.utc).isoformat()

    publish_candidates = [
        x
        for x in queue
        if isinstance(x, dict) and x.get("status") in {"ready_for_publish", "publish_now", "queued"}
    ]
    if not publish_candidates:
        return {"total": len(queue), "candidates": 0, "published": 0, "failed": 0, "message": "nothing to publish"}

    target = publish_candidates[0]
    ok, result = _publish_via_service(target)
    if ok:
        target["status"] = "publish_requested"
        target["published_requested_at"] = now_iso
        target["taskId"] = str(result.get("task_id") or "")
        target["video_url"] = str(result.get("video_url") or "")
        target["scheduled_at"] = result.get("scheduled_at")
        target["publish_result"] = "ok"
        published = 1
        failed = 0
    else:
        target["status"] = "failed"
        target["failed_at"] = now_iso
        target["publish_error"] = str(result)
        published = 0
        failed = 1

    save_queue(queue)
    return {
        "total": len(queue),
        "candidates": len(publish_candidates),
        "published": published,
        "failed": failed,
        "preview_id": str(target.get("preview_id", "unknown")),
        "status": str(target.get("status", "unknown")),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Process local queue.json once")
    parser.add_argument(
        "--process-queued",
        action="store_true",
        help="In test mode, also process one queued item when publish_now is absent.",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Publish one ready item to GeeLark API and update queue status.",
    )
    args = parser.parse_args()

    if args.publish:
        result = publish_queue_once()
        print(f"Queue items: {result['total']}")
        print(f"publish candidates: {result['candidates']}")
        print(f"published: {result['published']}")
        print(f"failed: {result['failed']}")
        print(f"preview_id: {result.get('preview_id', '-')}")
        print(f"status: {result.get('status', '-')}")
        if result.get("message"):
            print(result["message"])
    else:
        result = process_queue_once(process_queued_in_test_mode=args.process_queued)
        print(f"Queue items: {result['total']}")
        print(f"publish_now found: {result['publish_now_found']}")
        print(f"queued found: {result['queued_found']}")
        print(f"updated items: {result['updated']}")
        if result["updated_preview_ids"]:
            print("updated preview_ids:", ", ".join(result["updated_preview_ids"]))
        else:
            print("updated preview_ids: none")


if __name__ == "__main__":
    main()
