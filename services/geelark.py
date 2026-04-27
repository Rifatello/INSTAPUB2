from __future__ import annotations

import hashlib
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from typing import Optional

import requests

from config import settings as _settings  # noqa: F401  # ensure .env is loaded before os.getenv reads
from utils.logger import get_logger
from utils.retry import with_retry


logger = get_logger("geelark")

GEELARK_BASE_URL = os.getenv("GEELARK_BASE_URL", "https://open.geelark.com").rstrip("/")
GEELARK_PUBLISH_PATH = os.getenv("GEELARK_PUBLISH_PATH", "616420673648066993")
GEELARK_WARMUP_PATH = "Прогрев аккаунтов Instagram через ИИ20260423150618"
GEELARK_BEARER_TOKEN = os.getenv("GEELARK_BEARER_TOKEN", "").strip()
GEELARK_APP_ID = os.getenv("GEELARK_APP_ID", "").strip()
GEELARK_API_KEY = os.getenv("GEELARK_API_KEY", "").strip()
GEELARK_CLOUD_PHONE_ID = os.getenv("GEELARK_CLOUD_PHONE_ID", "").strip()
GEELARK_CUSTOM_TASK_CREATE_URL = "https://openapi.geelark.com/open/v1/task/rpa/add"
GEELARK_TASK_FLOW_LIST_URL = "https://openapi.geelark.com/open/v1/task/flow/list"
GEELARK_TASK_FLOW_PAGE_SIZE = int(os.getenv("GEELARK_TASK_FLOW_PAGE_SIZE", "100"))
PUBLISH_RETRIES = int(os.getenv("PUBLISH_RETRIES", "3"))
PUBLISH_RETRY_DELAY_SEC = float(os.getenv("PUBLISH_RETRY_DELAY_SEC", "2"))


def publish_to_geelark(video_url: str, caption: str, cloud_phone_id: Optional[str] = None) -> dict[str, Any]:
    params = {
        "description": caption,
        "video": [video_url],
    }
    return execute_geelark_task(
        task_path=GEELARK_PUBLISH_PATH,
        cloud_phone_id=cloud_phone_id,
        params=params,
        delay_seconds=30,
    )


def warmup_geelark_account(cloud_phone_id: str) -> dict[str, Any]:
    params = {
        "NumberOfVideosViewed": 5,
        "SearchKeyword": "",
    }
    return execute_geelark_task(
        task_path=GEELARK_WARMUP_PATH,
        cloud_phone_id=cloud_phone_id,
        params=params,
        delay_seconds=180,  # 3 minutes
    )


def execute_geelark_task(
    task_path: str,
    params: dict[str, Any],
    cloud_phone_id: Optional[str] = None,
    delay_seconds: int = 30,
) -> dict[str, Any]:
    if not GEELARK_BEARER_TOKEN and not (GEELARK_APP_ID and GEELARK_API_KEY):
        raise RuntimeError("Set GEELARK_BEARER_TOKEN or GEELARK_APP_ID+GEELARK_API_KEY")

    phone_id = (cloud_phone_id or GEELARK_CLOUD_PHONE_ID).strip()
    if not phone_id:
        raise RuntimeError("cloud phone id is empty")

    schedule_at = int((datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)).timestamp())

    def _headers_token(trace_id: str) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "traceId": trace_id,
            "Authorization": f"Bearer {GEELARK_BEARER_TOKEN}",
        }

    def _headers_key(trace_id: str) -> dict[str, str]:
        ts = str(int(time.time() * 1000))
        nonce = trace_id[:6]
        raw = f"{GEELARK_APP_ID}{trace_id}{ts}{nonce}{GEELARK_API_KEY}"
        sign = hashlib.sha256(raw.encode("utf-8")).hexdigest().upper()
        return {
            "Content-Type": "application/json",
            "traceId": trace_id,
            "appId": GEELARK_APP_ID,
            "ts": ts,
            "nonce": nonce,
            "sign": sign,
        }

    def _auth_headers(trace_id: str) -> dict[str, str]:
        if GEELARK_BEARER_TOKEN:
            return _headers_token(trace_id)
        return _headers_key(trace_id)

    def _load_flow_params(flow_id: str) -> list[str]:
        page = 1
        while True:
            trace_id = str(uuid.uuid4())
            headers = _auth_headers(trace_id)
            response = requests.post(
                GEELARK_TASK_FLOW_LIST_URL,
                json={"page": page, "pageSize": GEELARK_TASK_FLOW_PAGE_SIZE},
                headers=headers,
                timeout=60,
            )
            response.raise_for_status()
            data = response.json()
            code = data.get("code")
            if code not in (0, "0", None):
                raise RuntimeError(f"GeeLark flow list error code={code}, msg={data.get('msg')}")

            data_block = data.get("data", {}) if isinstance(data.get("data"), dict) else {}
            items = data_block.get("items", []) if isinstance(data_block.get("items"), list) else []
            for item in items:
                if str(item.get("id", "")).strip() == flow_id:
                    raw_params = item.get("params", [])
                    if not isinstance(raw_params, list):
                        return []
                    return [str(x).strip() for x in raw_params if str(x).strip()]

            total = int(data_block.get("total") or 0)
            if page * GEELARK_TASK_FLOW_PAGE_SIZE >= total or not items:
                break
            page += 1

        raise RuntimeError(f"GeeLark flowId not found: {flow_id}")

    def _build_param_map(flow_params: list[str]) -> dict[str, Any]:
        caption = str(params.get("description", "")).strip()
        raw_video = params.get("video", [])
        video_url = ""
        if isinstance(raw_video, list) and raw_video:
            video_url = str(raw_video[0]).strip()
        elif isinstance(raw_video, str):
            video_url = raw_video.strip()

        param_map: dict[str, Any] = {}
        for key in flow_params:
            low = key.lower()
            if "video" in low:
                param_map[key] = [video_url] if video_url else []
            elif "sameurl" in low:
                param_map[key] = ""
            elif any(token in low for token in ("caption", "desc", "description", "title", "text", "content")):
                param_map[key] = caption
            elif "url" in low:
                param_map[key] = video_url
            else:
                param_map[key] = caption
        return param_map

    # Для числового ID запускаем кастомный flow по официальному endpoint /open/v1/task/rpa/add.
    if task_path.isdigit():
        endpoint = GEELARK_CUSTOM_TASK_CREATE_URL
        flow_params = _load_flow_params(task_path)
        payload = {
            "scheduleAt": schedule_at,
            "id": phone_id,
            "flowId": task_path,
            "paramMap": _build_param_map(flow_params),
        }
    elif task_path == GEELARK_PUBLISH_PATH or task_path == "/open/v1/rpa/task/instagramPubReels":
        endpoint = "https://openapi.geelark.com/open/v1/rpa/task/instagramPubReels"
        payload = {
            "scheduleAt": schedule_at,
            "id": phone_id,
            "description": params.get("description", ""),
            "video": params.get("video", []),
        }
    elif task_path.startswith("/"):
        endpoint = f"{GEELARK_BASE_URL}{task_path}"
        payload = {
            "scheduleAt": schedule_at,
            "id": phone_id,
            "description": params.get("description", ""),
            "video": params.get("video", []),
        }
    else:
        raise RuntimeError(f"Unsupported GeeLark task path: {task_path}")

    def _send_once(headers: dict[str, str]) -> dict[str, Any]:
        try:
            logger.info("Sending request to Geelark: URL=%s, Payload=%s", endpoint, payload)
            response = requests.post(endpoint, json=payload, headers=headers, timeout=60)
            
            # Логируем ответ даже при ошибке
            try:
                data: dict[str, Any] = response.json()
            except Exception:
                data = {"raw_content": response.text}
                
            logger.info("GeeLark response status=%s: %s", response.status_code, data)
            
            if response.status_code != 200:
                logger.error("GeeLark error response: %s", response.text)
                
            response.raise_for_status()
            return data
        except requests.exceptions.HTTPError as e:
            logger.error("HTTP error from Geelark: %s, Response: %s", e, response.text)
            raise RuntimeError(f"Geelark API error: {response.status_code} - {response.text}")
        except Exception as e:
            logger.error("Unexpected error calling Geelark: %s", e)
            raise RuntimeError(f"Failed to call Geelark: {str(e)}")

    def _send() -> dict[str, Any]:
        trace_id = str(uuid.uuid4())
        logger.info("Executing GeeLark task=%s endpoint=%s traceId=%s", task_path, endpoint, trace_id)
        logger.info("Payload: %s", payload)
        data: dict[str, Any]
        data = _send_once(_auth_headers(trace_id))

        code = data.get("code")
        if code in (40003, "40003") and GEELARK_APP_ID and GEELARK_API_KEY:
            logger.info("Token verification failed, retrying with key-sign auth")
            data = _send_once(_headers_key(str(uuid.uuid4())))

        task_id = data.get("data", {}).get("taskId") if isinstance(data.get("data"), dict) else None
        if task_id:
            logger.info("GeeLark taskId: %s", task_id)
        code = data.get("code")
        if code not in (0, "0", None):
            raise RuntimeError(f"GeeLark API error code={code}, msg={data.get('msg')}")
        return {
            "scheduled_at": schedule_at,
            "task_id": task_id,
            "response": data,
        }

    return with_retry(_send, retries=PUBLISH_RETRIES, delay_sec=PUBLISH_RETRY_DELAY_SEC)
