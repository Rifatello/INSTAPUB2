from __future__ import annotations

from typing import Any
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from services.geelark import publish_to_geelark, warmup_geelark_account
from services.uploader import upload_video
from utils.logger import get_logger


logger = get_logger("publish_api")
app = FastAPI(title="Publish API", version="1.0.0")


class WarmupRequest(BaseModel):
    cloud_phone_id: str


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/warmup")
def warmup(payload: WarmupRequest) -> dict[str, Any]:
    try:
        geelark_response = warmup_geelark_account(payload.cloud_phone_id)
        result = {
            "success": True,
            "task_id": geelark_response.get("task_id"),
            "scheduled_at": geelark_response.get("scheduled_at"),
            "geelark_response": geelark_response.get("response"),
        }
        logger.info("Warmup success: %s", result)
        return result
    except Exception as exc:
        logger.exception("Warmup failed")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": str(exc),
            },
        ) from exc


class PublishRequest(BaseModel):
    video_path: str
    caption: str
    cloud_phone_id: Optional[str] = None


@app.post("/publish")
def publish(payload: PublishRequest) -> dict[str, Any]:
    try:
        logger.info("Publish request received: video_path=%s, phone_id=%s", payload.video_path, payload.cloud_phone_id)
        
        # Шаг 1: Загрузка видео
        logger.info("Step 1/2: Uploading video...")
        video_url = upload_video(payload.video_path)
        logger.info("Step 1/2 Success: Video URL is %s", video_url)
        
        # Шаг 2: Отправка в Geelark
        logger.info("Step 2/2: Sending to Geelark...")
        geelark_response = publish_to_geelark(
            video_url=video_url,
            caption=payload.caption,
            cloud_phone_id=payload.cloud_phone_id,
        )
        
        result = {
            "success": True,
            "task_id": geelark_response.get("task_id"),
            "video_url": video_url,
            "scheduled_at": geelark_response.get("scheduled_at"),
            "geelark_response": geelark_response.get("response"),
        }
        logger.info("Step 2/2 Success: Task scheduled. Result: %s", result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.error("Publish failed at some step: %s", str(exc), exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": str(exc),
            },
        ) from exc
