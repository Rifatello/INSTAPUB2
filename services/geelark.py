from __future__ import annotations

import hashlib
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any
from typing import Optional

import requests

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

    # Для стандартных Reels используем проверенный endpoint openapi.
    if task_path == GEELARK_PUBLISH_PATH:
        endpoint = "https://openapi.geelark.com/open/v1/rpa/task/instagramPubReels"
    elif task_path.isdigit():
        # Для кастомных задач по ID используем хост open.geelark.com (где путь /v1/rpa/start существует)
        endpoint = "https://open.geelark.com/v1/rpa/start"
    elif task_path.startswith("/"):
        endpoint = f"{GEELARK_BASE_URL}{task_path}"
    else:
        endpoint = "https://open.geelark.com/v1/rpa/start"

    schedule_at = int((datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)).timestamp())
    
    # Формируем payload
    if "/v1/rpa/start" in endpoint:
        payload = {
            "scheduleAt": schedule_at,
            "id": phone_id,
            "taskId": task_path,
            "params": params
        }
    else:
        # Старый формат для instagramPubReels
        payload = {
            "scheduleAt": schedule_at,
            "id": phone_id,
            "description": params.get("description", ""),
            "video": params.get("video", []),
            "taskPath": task_path if task_path.isdigit() else None
        }

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
        if GEELARK_BEARER_TOKEN:
            data = _send_once(_headers_token(trace_id))
        else:
            data = _send_once(_headers_key(trace_id))

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
