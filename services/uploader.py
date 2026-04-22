from __future__ import annotations

import os
import subprocess
import uuid
from pathlib import Path

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


def upload_video(video_path: str) -> str:
    path = Path(video_path).expanduser().resolve()
    if not path.exists() or not path.is_file():
        raise FileNotFoundError(f"Video file not found: {path}")

    if not PUBLIC_VIDEO_BASE_URL:
        raise RuntimeError("VIDEO_PUBLIC_BASE_URL must be set")

    remote_filename = f"{uuid.uuid4().hex[:8]}{path.suffix}"
    logger.info("Uploading local video to SSH server: %s -> %s", path, remote_filename)

    def _do_upload() -> str:
        _upload_via_ssh(path, remote_filename)
        return f"{PUBLIC_VIDEO_BASE_URL}/{remote_filename}"

    public_url = with_retry(_do_upload, retries=UPLOAD_RETRIES, delay_sec=UPLOAD_RETRY_DELAY_SEC)
    logger.info("Upload success. Public URL: %s", public_url)
    return public_url
