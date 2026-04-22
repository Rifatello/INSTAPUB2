from __future__ import annotations

import os
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = BASE_DIR / ".env"


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ.setdefault(key, value)


_load_dotenv(ENV_PATH)


GEELARK_BASE_URL = os.getenv("GEELARK_BASE_URL", "https://open.geelark.com").rstrip("/")
GEELARK_PUBLISH_PATH = os.getenv("GEELARK_PUBLISH_PATH", "/open/v1/rpa/task/instagramPubReels")
GEELARK_BEARER_TOKEN = os.getenv("GEELARK_BEARER_TOKEN", "").strip()
GEELARK_CLOUD_PHONE_ID = os.getenv("GEELARK_CLOUD_PHONE_ID", "").strip()

GEELARK_UPLOAD_URL = os.getenv("GEELARK_UPLOAD_URL", "").strip()
GEELARK_UPLOAD_FIELD = os.getenv("GEELARK_UPLOAD_FIELD", "file").strip()
GEELARK_UPLOAD_RESULT_URL_FIELD = os.getenv("GEELARK_UPLOAD_RESULT_URL_FIELD", "data.url").strip()

CLOUDINARY_CLOUD_NAME = os.getenv("CLOUDINARY_CLOUD_NAME", "").strip()
CLOUDINARY_UPLOAD_PRESET = os.getenv("CLOUDINARY_UPLOAD_PRESET", "").strip()
CLOUDINARY_API_KEY = os.getenv("CLOUDINARY_API_KEY", "").strip()
CLOUDINARY_API_SECRET = os.getenv("CLOUDINARY_API_SECRET", "").strip()

S3_PRESIGNED_UPLOAD_URL = os.getenv("S3_PRESIGNED_UPLOAD_URL", "").strip()
S3_PUBLIC_URL = os.getenv("S3_PUBLIC_URL", "").strip()

PUBLISH_RETRIES = int(os.getenv("PUBLISH_RETRIES", "3"))
PUBLISH_RETRY_DELAY_SEC = float(os.getenv("PUBLISH_RETRY_DELAY_SEC", "2"))
