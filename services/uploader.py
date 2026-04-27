from __future__ import annotations

import os
import shutil
import subprocess
import uuid
from pathlib import Path

from config import settings as _settings  # noqa: F401  # ensure .env is loaded before os.getenv reads
from utils.logger import get_logger
from utils.retry import with_retry


logger = get_logger("uploader")


SSH_HOST = os.getenv("VIDEO_SSH_HOST", "").strip()
SSH_PORT = int(os.getenv("VIDEO_SSH_PORT", "22"))
SSH_USER = os.getenv("VIDEO_SSH_USER", "").strip()
SSH_KEY_PATH = os.getenv("VIDEO_SSH_KEY_PATH", "").strip()
SSH_PASSWORD = os.getenv("VIDEO_SSH_PASSWORD", "").strip()
REMOTE_VIDEO_DIR = os.getenv("VIDEO_REMOTE_DIR", "~/videos").strip()
PUBLIC_VIDEO_BASE_URL = os.getenv("VIDEO_PUBLIC_BASE_URL", "").strip().rstrip("/")
PUBLIC_VIDEO_BASE_URL_FILE = os.getenv("VIDEO_PUBLIC_BASE_URL_FILE", "").strip()
UPLOAD_RETRIES = int(os.getenv("UPLOAD_RETRIES", "3"))
UPLOAD_RETRY_DELAY_SEC = float(os.getenv("UPLOAD_RETRY_DELAY_SEC", "2"))


def _run_command(command: list[str]) -> None:
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        stdout = (result.stdout or "").strip()
        raise RuntimeError(f"Command failed: {' '.join(command)} | stderr={stderr} | stdout={stdout}")


def _build_ssh_base() -> list[str]:
    if not SSH_HOST or not SSH_USER:
        raise RuntimeError("VIDEO_SSH_HOST and VIDEO_SSH_USER must be set")
    base = ["ssh", "-p", str(SSH_PORT)]
    if SSH_KEY_PATH:
        base.extend(["-i", SSH_KEY_PATH])
    base.extend(
        [
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
        ]
    )
    return base


def _upload_via_ssh(video_path: Path, remote_filename: str) -> None:
    remote_target = f"{SSH_USER}@{SSH_HOST}:{REMOTE_VIDEO_DIR}/{remote_filename}"
    if SSH_PASSWORD:
        raise RuntimeError("VIDEO_SSH_PASSWORD flow is not implemented. Use SSH key-based auth.")

    ssh_base = _build_ssh_base()
    mkdir_command = ssh_base + [f"{SSH_USER}@{SSH_HOST}", f"mkdir -p {REMOTE_VIDEO_DIR}"]
    scp_command = [
        "scp",
        "-P",
        str(SSH_PORT),
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
    ]
    if SSH_KEY_PATH:
        scp_command.extend(["-i", SSH_KEY_PATH])
    scp_command.extend([str(video_path), remote_target])

    _run_command(mkdir_command)
    _run_command(scp_command)


def _upload_locally(video_path: Path, remote_filename: str) -> None:
    target_dir = Path(REMOTE_VIDEO_DIR).expanduser().resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / remote_filename
    logger.info("Copying file locally to: %s", target_path)
    shutil.copy2(video_path, target_path)


def _resolve_public_video_base_url() -> str:
    if PUBLIC_VIDEO_BASE_URL_FILE:
        file_path = Path(PUBLIC_VIDEO_BASE_URL_FILE).expanduser()
        if file_path.exists():
            dynamic_url = file_path.read_text(encoding="utf-8").strip().rstrip("/")
            if dynamic_url:
                return dynamic_url
    return PUBLIC_VIDEO_BASE_URL


def upload_video(video_path: str) -> str:
    path = Path(video_path).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Video file not found: {path}")

    public_video_base_url = _resolve_public_video_base_url()
    if not public_video_base_url:
        raise RuntimeError("VIDEO_PUBLIC_BASE_URL must be set (or provide VIDEO_PUBLIC_BASE_URL_FILE)")

    remote_filename = f"{uuid.uuid4().hex[:8]}{path.suffix}"
    
    is_local = SSH_HOST.lower() in ("local", "localhost", "127.0.0.1")

    if is_local:
        logger.info("Uploading local video (LOCAL MODE): %s -> %s", path, remote_filename)
    else:
        logger.info("Uploading local video to SSH server: %s -> %s", path, remote_filename)

    def _do_upload() -> str:
        if is_local:
            _upload_locally(path, remote_filename)
        else:
            _upload_via_ssh(path, remote_filename)
        return f"{public_video_base_url}/{remote_filename}"

    public_url = with_retry(_do_upload, retries=UPLOAD_RETRIES, delay_sec=UPLOAD_RETRY_DELAY_SEC)
    logger.info("Upload success. Public URL: %s", public_url)
    return public_url
