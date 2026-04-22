from __future__ import annotations

from typing import Any
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from services.geelark import publish_to_geelark
from services.uploader import upload_video
from utils.logger import get_logger


logger = get_logger("publish_api")
app = FastAPI(title="Publish API", version="1.0.0")


class PublishRequest(BaseModel):
    video_path: str
    caption: str
    cloud_phone_id: Optional[str] = None


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/publish")
def publish(payload: PublishRequest) -> dict[str, Any]:
    try:
        video_url = upload_video(payload.video_path)
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
        logger.info("Publish success: %s", result)
        return result
    except Exception as exc:  # noqa: BLE001
        logger.exception("Publish failed")
        raise HTTPException(
            status_code=500,
            detail={
                "success": False,
                "error": str(exc),
            },
        ) from exc
