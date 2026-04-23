from __future__ import annotations

import json
import os
import random
import re
import subprocess
import textwrap
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from openai import OpenAI
from pydantic import BaseModel


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
STORAGE_DIR = BASE_DIR / "storage"
SUPPORTED_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm"}
SUPPORTED_MUSIC_EXTENSIONS = {".mp3", ".wav", ".m4a", ".aac", ".mp4"}
DEFAULT_CTA = "👉 Ссылка в био"
FFMPEG_BIN = os.getenv("FFMPEG_BIN", "/opt/homebrew/opt/ffmpeg-full/bin/ffmpeg")
FFPROBE_BIN = os.getenv(
    "FFPROBE_BIN",
    str((Path(FFMPEG_BIN).parent / "ffprobe")) if Path(FFMPEG_BIN).parent else "ffprobe",
)


class GeneratePreviewRequest(BaseModel):
    account_id: str


class GeneratePreviewResponse(BaseModel):
    preview_id: str
    account_id: str
    source_video: str
    preview_video: str
    hook: str
    description: str
    caption: str
    status: str


class RegeneratePreviewRequest(BaseModel):
    preview_id: str
    refresh: str  # hook | description | music


app = FastAPI(title="Content API", version="0.1.0")


def _read_json(path: Path) -> Any:
    if not path.exists():
        raise FileNotFoundError(f"Missing required file: {path}")
    with path.open("r", encoding="utf-8") as fp:
        return json.load(fp)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


def _load_accounts() -> list[dict[str, Any]]:
    accounts = _read_json(DATA_DIR / "accounts.json")
    if not isinstance(accounts, list):
        raise ValueError("accounts.json must contain a list")
    return accounts


def _get_account(account_id: str) -> dict[str, Any]:
    for account in _load_accounts():
        if account.get("account_id") == account_id:
            if account.get("status") != "active":
                raise HTTPException(status_code=400, detail="Account is not active")
            return account
    raise HTTPException(status_code=404, detail=f"Account not found: {account_id}")


def _choose_source_video(account: dict[str, Any]) -> Path:
    folder = Path(str(account["video_folder"]))
    if not folder.is_absolute():
        folder = BASE_DIR / folder
    if not folder.exists():
        raise HTTPException(status_code=400, detail=f"Video folder not found: {folder}")

    candidates = [
        p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_VIDEO_EXTENSIONS
    ]
    if not candidates:
        raise HTTPException(status_code=400, detail=f"No source videos in {folder}")
    return random.choice(candidates)


def choose_music_track(account: dict[str, Any], exclude_track: Path | None = None) -> Path | None:
    music_folder_value = str(account.get("music_folder", "")).strip()
    if not music_folder_value:
        return None
    folder = Path(music_folder_value)
    if not folder.is_absolute():
        folder = BASE_DIR / folder
    if not folder.exists():
        return None

    candidates = [
        p for p in folder.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_MUSIC_EXTENSIONS
    ]
    if exclude_track is not None:
        exclude_resolved = str(exclude_track.resolve()) if exclude_track.exists() else str(exclude_track)
        filtered = []
        for track in candidates:
            track_resolved = str(track.resolve()) if track.exists() else str(track)
            if track_resolved != exclude_resolved:
                filtered.append(track)
        candidates = filtered or candidates

    if not candidates:
        return None
    return random.choice(candidates)


def _load_text_library(filename: str) -> list[str]:
    data = _read_json(DATA_DIR / filename)
    if not isinstance(data, list) or not data:
        raise HTTPException(status_code=500, detail=f"{filename} must be a non-empty list")
    return [str(x).strip() for x in data if str(x).strip()]


def _build_caption(hook: str, description: str) -> str:
    return f"{hook}\n\n{description}\n\n{DEFAULT_CTA}"


def _pick_examples(items: list[str], k: int = 3) -> list[str]:
    if not items:
        return []
    return random.sample(items, k=min(k, len(items)))


def _fallback_content(hooks_examples: list[str], descriptions_examples: list[str]) -> dict[str, str]:
    hook = random.choice(hooks_examples).strip()
    description = random.choice(descriptions_examples).strip()
    caption = _build_caption(hook, description)
    return {"hook": hook, "description": description, "caption": caption}


def _get_llm_config() -> tuple[str, str, str]:
    provider = os.getenv("LLM_PROVIDER", "openai").strip().lower()
    if provider == "deepseek":
        api_key = os.getenv("DEEPSEEK_API_KEY", "").strip()
        base_url = os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com/v1").strip()
        model = os.getenv("DEEPSEEK_MODEL", "deepseek-chat").strip()
        return api_key, base_url, model

    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    base_url = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
    return api_key, base_url, model


def _extract_json_content(raw_text: str) -> dict[str, Any]:
    raw_text = raw_text.strip()
    if not raw_text:
        raise ValueError("Empty LLM response")

    try:
        return json.loads(raw_text)
    except json.JSONDecodeError:
        start = raw_text.find("{")
        end = raw_text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("LLM response is not valid JSON")
        return json.loads(raw_text[start : end + 1])


def _is_valid_generated_content(content: dict[str, Any]) -> bool:
    hook = str(content.get("hook", "")).strip()
    description = str(content.get("description", "")).strip()
    caption = str(content.get("caption", "")).strip()
    return bool(
        hook
        and description
        and caption
        and len(hook) >= 15
        and len(description) >= 40
        and DEFAULT_CTA in caption
    )


def generate_content(
    account: dict[str, Any], hooks_examples: list[str], descriptions_examples: list[str]
) -> dict[str, str]:
    fallback = _fallback_content(hooks_examples, descriptions_examples)
    api_key, base_url, model = _get_llm_config()
    if not api_key:
        return fallback

    theme = str(account.get("theme", "")).strip()
    style = str(account.get("style", "")).strip()

    system_prompt = (
        "Ты генерируешь Instagram-контент на русском языке. "
        "Стиль: короткие строки, плотный, жесткий, маркетинговый, без воды. "
        "Текст должен быть пригоден для публикации в Instagram. "
        "Без markdown, без пояснений, без служебного текста. "
        "Используй примеры только как ориентир по стилю. "
        "Верни строго JSON с ключами: hook, description, caption."
    )
    user_prompt = (
        f"Account theme: {theme}\n"
        f"Account style: {style}\n\n"
        "Hook examples:\n"
        + "\n".join(f"- {x}" for x in hooks_examples)
        + "\n\nDescription examples:\n"
        + "\n".join(f"- {x}" for x in descriptions_examples)
        + "\n\nTask:\n"
        "1) Write a new hook in similar style.\n"
        "2) Write a new description in similar style.\n"
        f"3) Build caption exactly as: hook + blank line + description + blank line + {DEFAULT_CTA}\n"
        "4) Return only JSON, no markdown."
    )

    try:
        client = OpenAI(api_key=api_key, base_url=base_url)
        response = client.chat.completions.create(
            model=model,
            temperature=0.8,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        )
        raw_text = (response.choices[0].message.content or "").strip()
        parsed = _extract_json_content(raw_text)

        hook = str(parsed.get("hook", "")).strip()
        description = str(parsed.get("description", "")).strip()
        caption = str(parsed.get("caption", "")).strip()
        if not caption and hook and description:
            caption = _build_caption(hook, description)

        candidate = {"hook": hook, "description": description, "caption": caption}
        if not _is_valid_generated_content(candidate):
            return fallback
        return candidate
    except Exception:
        return fallback


def _preview_output_path(preview_id: str) -> Path:
    return STORAGE_DIR / "processing" / f"{preview_id}.mp4"


def _escape_drawtext_text(text: str) -> str:
    escaped = text.replace("\\", r"\\")
    escaped = escaped.replace(":", r"\:")
    escaped = escaped.replace("'", r"\'")
    escaped = escaped.replace("%", r"\%")
    return escaped


def _build_drawtext_filter(lines: list[str]) -> str:
    filters = []
    font_path = "/System/Library/Fonts/Supplemental/Arial Bold.ttf"
    base_y_expr = "h*0.08"
    font_size = 64
    line_spacing = 16

    for i, line in enumerate(lines):
        line_escaped = _escape_drawtext_text(line)
        # Вычисляем Y для каждой строки отдельно
        y_expr = f"{base_y_expr}+{i}*({font_size}+{line_spacing})"
        f = (
            "drawtext="
            f"fontfile='{font_path}':"
            f"text='{line_escaped}':"
            "fontcolor=white:"
            f"fontsize={font_size}:"
            "bordercolor=black:"
            "borderw=3:"
            "x=(w-text_w)/2:"
            f"y={y_expr}:"
            "fix_bounds=true"
        )
        filters.append(f)

    return ",".join(filters)


def _has_audio_stream(video_path: Path) -> bool:
    command = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-select_streams",
        "a:0",
        "-show_entries",
        "stream=codec_type",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    try:
        result = subprocess.run(command, check=False, capture_output=True, text=True)
        return result.returncode == 0 and bool((result.stdout or "").strip())
    except Exception:
        return False


def _probe_duration_seconds(video_path: Path) -> float:
    command = [
        FFPROBE_BIN,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "default=noprint_wrappers=1:nokey=1",
        str(video_path),
    ]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        return float((result.stdout or "0").strip() or 0)
    except Exception:
        return 0.0


def _rand_signed_abs(min_abs: float, max_abs: float) -> float:
    value = random.uniform(min_abs, max_abs)
    return value if random.choice([True, False]) else -value


def _build_unique_video_filter(duration: float, speed: float) -> tuple[str, float, float]:
    # Целевая длительность на выходе: от 5.0 до 8.0 секунд
    # final_duration = segment_length / speed
    # Отсюда segment_length = final_duration * speed
    target_final = random.uniform(5.5, 7.5)
    needed_segment = target_final * speed

    # Если исходник короче, чем нужно даже с учетом скорости, берем весь исходник
    if needed_segment > duration:
        start_val = 0.0
        end_val = duration
    else:
        # Иначе выбираем случайный кусок нужной длины
        max_start = duration - needed_segment
        start_val = random.uniform(0, max_start)
        end_val = start_val + needed_segment

    crop_ratio = random.uniform(0.95, 0.98)
    brightness = _rand_signed_abs(0.02, 0.05)
    contrast = 1.0 + _rand_signed_abs(0.02, 0.05)
    saturation = 1.0 + _rand_signed_abs(0.05, 0.1)
    hue_shift = _rand_signed_abs(1.0, 5.0)
    rotation = _rand_signed_abs(0.002, 0.006)
    fps = random.choice([29.97, 30.05, 30.15])

    vf = (
        f"trim=start={start_val:.3f}:end={end_val:.3f},"
        "setpts=PTS-STARTPTS,"
        f"crop=iw*{crop_ratio:.4f}:ih*{crop_ratio:.4f},"
        f"rotate=a={rotation:.4f}:ow=iw:oh=ih:c=black,"
        f"eq=brightness={brightness:.4f}:contrast={contrast:.4f}:saturation={saturation:.4f},"
        f"hue=h={hue_shift:.3f},"
        "noise=alls=1:allf=t+u,"
        "vignette=PI/100,"
        f"setpts=PTS/{speed:.4f},"
        f"fps={fps},"
        "scale=trunc(iw/2)*2:trunc(ih/2)*2"
    )
    return vf, start_val, end_val


def render_unique_preview(preview_video: Path) -> None:
    duration = _probe_duration_seconds(preview_video)
    if duration <= 0.5:
        raise HTTPException(status_code=500, detail="Cannot uniqueize: invalid video duration")

    has_audio = _has_audio_stream(preview_video)
    
    # Если видео слишком короткое, замедляем его сильнее, чтобы попасть в 5с
    if duration < 5.0:
        speed = random.uniform(0.6, 0.8)
    else:
        speed = random.uniform(0.9, 1.1)

    vf, start_val, end_val = _build_unique_video_filter(duration, speed)
    volume = random.uniform(0.7, 0.9)

    tmp_output = preview_video.with_name(f"{preview_video.stem}.unique.mp4")
    noise_input = "anoisesrc=color=white:amplitude=0.008:sample_rate=44100"

    if has_audio:
        highpass_f = random.randint(100, 250)
        lowpass_f = random.randint(10000, 15000)
        af = (
            f"atrim=start={start_val:.3f}:end={end_val:.3f},"
            "asetpts=PTS-STARTPTS,"
            f"volume={volume:.4f},"
            f"highpass=f={highpass_f},"
            f"lowpass=f={lowpass_f},"
            f"atempo={speed:.4f}"
        )
        filter_complex = (
            f"[0:v]{vf}[vout];"
            f"[0:a]{af}[abase];"
            "[1:a]volume=0.10[anoise];"
            "[abase][anoise]amix=inputs=2:weights='1 0.25':duration=first[aout]"
        )
    else:
        filter_complex = (
            f"[0:v]{vf}[vout];"
            f"[1:a]volume=0.20,atempo={speed:.4f}[aout]"
        )

    command = [
        FFMPEG_BIN,
        "-y",
        "-i",
        str(preview_video),
        "-f",
        "lavfi",
        "-i",
        noise_input,
        "-filter_complex",
        filter_complex,
        "-map",
        "[vout]",
        "-map",
        "[aout]",
        "-shortest",
        "-c:v",
        "libx264",
        "-c:a",
        "aac",
        str(tmp_output),
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
        tmp_output.replace(preview_video)
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise HTTPException(status_code=500, detail=f"FFmpeg uniqueize failed: {stderr[-400:]}") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Uniqueize failed: {exc}") from exc
    finally:
        if tmp_output.exists():
            try:
                tmp_output.unlink()
            except Exception:
                pass


def render_preview_with_hook(
    source_video: Path,
    preview_video: Path,
    hook: str,
    music_track: Path | None = None,
    force_9_16: bool = False,
) -> None:
    # Агрессивная очистка от спецсимволов и эмодзи
    clean_hook = re.sub(r'[^a-zA-Zа-яА-ЯёЁ0-9\s.,!?;:()"\-]', '', hook)
    clean_hook = clean_hook.strip().replace("\r", "")
    
    # Разбиваем на строки в Python
    lines = [line.strip() for line in textwrap.wrap(clean_hook, width=22)]
    if not lines:
        lines = [""]

    drawtext_filter = _build_drawtext_filter(lines)
    
    # Фильтр для кропа в 9:16 (центровка и приведение к 1080x1920)
    if force_9_16:
        v_filters = f"crop='min(iw,ih*9/16):min(ih,iw*16/9)',scale=1080:1920,{drawtext_filter}"
    else:
        v_filters = drawtext_filter

    has_source_audio = _has_audio_stream(source_video)
    if music_track is not None:
        filter_complex = f"[0:v]{v_filters}[vout];[1:a]volume=0.5[aout]"
        command = [
            FFMPEG_BIN,
            "-y",
            "-i",
            str(source_video),
            "-stream_loop",
            "-1",
            "-i",
            str(music_track),
            "-filter_complex",
            filter_complex,
            "-map",
            "[vout]",
            "-map",
            "[aout]",
            "-shortest",
            "-c:v",
            "libx264",
            "-c:a",
            "aac",
            str(preview_video),
        ]
    else:
        command = [FFMPEG_BIN, "-y", "-i", str(source_video), "-vf", v_filters]
        if has_source_audio:
            command.extend(["-c:a", "copy"])
        else:
            # У источника нет аудио-дорожки: явно отключаем аудио, чтобы ffmpeg не падал.
            command.append("-an")
        command.append(str(preview_video))

    try:
        print("Using ffmpeg binary:", FFMPEG_BIN)
        print("Source has audio:", has_source_audio)
        if music_track is not None:
            print("Using music track:", str(music_track))
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        if result.returncode != 0:
            raise HTTPException(status_code=500, detail="FFmpeg render failed.")
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail="FFmpeg is not installed or not in PATH.") from exc
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        error_tail = stderr[-400:] if stderr else "unknown error"
        raise HTTPException(status_code=500, detail=f"FFmpeg render failed: {error_tail}") from exc


def _save_preview_state(
    preview_id: str,
    account_id: str,
    source_video: Path,
    preview_video: Path,
    hook: str,
    description: str,
    caption: str,
    music_track: Path | None = None,
) -> None:
    state_path = DATA_DIR / "state.json"
    state = _read_json(state_path)

    pending = state.setdefault("pending_previews", {})
    history = state.setdefault("history", [])
    service = state.setdefault("service", {})
    recent_hooks = state.setdefault("recent_hooks", [])
    recent_descriptions = state.setdefault("recent_descriptions", [])

    now_iso = datetime.now(timezone.utc).isoformat()
    preview_record = {
        "preview_id": preview_id,
        "account_id": account_id,
        "source_video": str(source_video),
        "preview_video": str(preview_video),
        "hook": hook,
        "description": description,
        "caption": caption,
        "music_track": str(music_track) if music_track else None,
        "status": "preview_ready",
        "created_at": now_iso,
    }

    pending[preview_id] = preview_record
    history.append(preview_record)
    recent_hooks.append(hook)
    recent_descriptions.append(description)

    state["recent_hooks"] = recent_hooks[-20:]
    state["recent_descriptions"] = recent_descriptions[-20:]
    state["history"] = history[-200:]
    service["last_preview_id"] = preview_id
    service["updated_at"] = now_iso
    state["service"] = service

    _write_json(state_path, state)


def _build_preview_response(record: dict[str, Any]) -> GeneratePreviewResponse:
    return GeneratePreviewResponse(
        preview_id=str(record["preview_id"]),
        account_id=str(record["account_id"]),
        source_video=str(record["source_video"]),
        preview_video=str(record["preview_video"]),
        hook=str(record["hook"]),
        description=str(record["description"]),
        caption=str(record["caption"]),
        status=str(record.get("status", "preview_ready")),
    )


def _load_pending_preview(preview_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    state_path = DATA_DIR / "state.json"
    state = _read_json(state_path)
    pending = state.setdefault("pending_previews", {})
    if not isinstance(pending, dict):
        raise HTTPException(status_code=500, detail="state.json pending_previews is invalid")

    record = pending.get(preview_id)
    if not isinstance(record, dict):
        raise HTTPException(status_code=404, detail=f"Preview not found: {preview_id}")
    return state, record


def _persist_pending_preview(state: dict[str, Any], record: dict[str, Any]) -> None:
    pending = state.setdefault("pending_previews", {})
    history = state.setdefault("history", [])
    service = state.setdefault("service", {})
    recent_hooks = state.setdefault("recent_hooks", [])
    recent_descriptions = state.setdefault("recent_descriptions", [])

    preview_id = str(record["preview_id"])
    now_iso = datetime.now(timezone.utc).isoformat()
    record["updated_at"] = now_iso
    pending[preview_id] = record
    history.append(record.copy())
    recent_hooks.append(str(record.get("hook", "")))
    recent_descriptions.append(str(record.get("description", "")))
    state["recent_hooks"] = recent_hooks[-20:]
    state["recent_descriptions"] = recent_descriptions[-20:]
    state["history"] = history[-200:]
    service["last_preview_id"] = preview_id
    service["updated_at"] = now_iso
    state["service"] = service
    _write_json(DATA_DIR / "state.json", state)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/generate-preview", response_model=GeneratePreviewResponse)
def generate_preview(payload: GeneratePreviewRequest) -> GeneratePreviewResponse:
    account = _get_account(payload.account_id)
    source_video = _choose_source_video(account)
    music_track = choose_music_track(account)
    hooks_library = _load_text_library("hooks.json")
    descriptions_library = _load_text_library("descriptions.json")
    hook_examples = _pick_examples(hooks_library, k=3)
    description_examples = _pick_examples(descriptions_library, k=3)
    generated = generate_content(account, hook_examples, description_examples)

    hook = generated["hook"].strip()
    description = generated["description"].strip()
    caption = generated["caption"].strip()
    if not hook or not description or not caption:
        fallback = _fallback_content(hooks_library, descriptions_library)
        hook = fallback["hook"]
        description = fallback["description"]
        caption = fallback["caption"]

    preview_id = str(uuid.uuid4())
    preview_video = _preview_output_path(preview_id)
    preview_video.parent.mkdir(parents=True, exist_ok=True)

    render_preview_with_hook(
        source_video=source_video,
        preview_video=preview_video,
        hook=hook,
        music_track=music_track,
    )

    # Автоматическая уникализация после рендеринга
    render_unique_preview(preview_video)

    _save_preview_state(
        preview_id=preview_id,
        account_id=str(account["account_id"]),
        source_video=source_video,
        preview_video=preview_video,
        hook=hook,
        description=description,
        caption=caption,
        music_track=music_track,
    )

    return GeneratePreviewResponse(
        preview_id=preview_id,
        account_id=str(account["account_id"]),
        source_video=str(source_video),
        preview_video=str(preview_video),
        hook=hook,
        description=description,
        caption=caption,
        status="preview_ready",
    )


@app.post("/regenerate-preview", response_model=GeneratePreviewResponse)
def regenerate_preview(payload: RegeneratePreviewRequest) -> GeneratePreviewResponse:
    refresh_type = payload.refresh.strip().lower()
    if refresh_type not in {"hook", "description", "music", "unique", "format_9_16"}:
        raise HTTPException(
            status_code=400,
            detail="refresh must be one of: hook, description, music, unique, format_9_16",
        )

    state, record = _load_pending_preview(payload.preview_id)
    account = _get_account(str(record.get("account_id", "")))

    source_video = Path(str(record.get("source_video", "")))
    if not source_video.exists():
        raise HTTPException(status_code=400, detail=f"Source video not found: {source_video}")

    preview_video = Path(str(record.get("preview_video", "")))
    preview_video.parent.mkdir(parents=True, exist_ok=True)

    hook = str(record.get("hook", "")).strip()
    description = str(record.get("description", "")).strip()
    music_track = Path(record["music_track"]) if record.get("music_track") else None

    hooks_library = _load_text_library("hooks.json")
    descriptions_library = _load_text_library("descriptions.json")

    if refresh_type == "hook":
        generated = generate_content(account, _pick_examples(hooks_library, 3), _pick_examples(descriptions_library, 3))
        hook = str(generated.get("hook", "")).strip() or hook
    elif refresh_type == "description":
        generated = generate_content(account, _pick_examples(hooks_library, 3), _pick_examples(descriptions_library, 3))
        description = str(generated.get("description", "")).strip() or description
    elif refresh_type == "music":
        music_track = choose_music_track(account, exclude_track=music_track)
    elif refresh_type == "unique":
        # keep current text/music, only transform media with randomized params
        render_unique_preview(preview_video)
    elif refresh_type == "format_9_16":
        record["is_9_16"] = True

    caption = _build_caption(hook, description)
    if refresh_type not in {"unique"}:
        is_9_16 = bool(record.get("is_9_16", False))
        render_preview_with_hook(
            source_video=source_video,
            preview_video=preview_video,
            hook=hook,
            music_track=music_track,
            force_9_16=is_9_16,
        )
        # Re-apply uniqueization after re-rendering
        render_unique_preview(preview_video)

    record["hook"] = hook
    record["description"] = description
    record["caption"] = caption
    record["music_track"] = str(music_track) if music_track else None
    record["status"] = "preview_ready"

    _persist_pending_preview(state, record)
    return _build_preview_response(record)
