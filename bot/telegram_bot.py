from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import uuid
import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes
from requests import RequestException

try:
    from .storage_repo import (
        approve_preview,
        delete_approved_preview,
        get_account,
        load_accounts,
        load_approved_videos,
        load_state,
        mark_publish_requested,
        reject_preview,
        save_state,
        toggle_account_schedule,
    )
except ImportError:
    from storage_repo import (
        approve_preview,
        delete_approved_preview,
        get_account,
        load_accounts,
        load_approved_videos,
        load_state,
        mark_publish_requested,
        reject_preview,
        save_state,
        toggle_account_schedule,
    )
try:
    from services.geelark import publish_to_geelark
    from services.uploader import upload_video
except ImportError:
    publish_to_geelark = None
    upload_video = None


BASE_DIR = Path(__file__).resolve().parents[1]


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


_load_dotenv(BASE_DIR / ".env")

CONTENT_API_BASE_URL = os.getenv("CONTENT_API_BASE_URL", "http://127.0.0.1:8000")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
PUBLISH_API_BASE_URL = os.getenv("PUBLISH_API_BASE_URL", "http://127.0.0.1:8010")
SUPPORTED_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm"}
HOOK_PROMPTS_DIR = BASE_DIR / "data" / "hook_prompts"
DESCRIPTION_PROMPTS_DIR = BASE_DIR / "data" / "description_prompts"


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🤖 Сгенерировать с помощью LLM", callback_data="menu:gen_ai"),
            ],
            [
                InlineKeyboardButton("⚙️ Сгенерировать с помощью Генератора", callback_data="menu:gen_generator"),
            ],
            [InlineKeyboardButton("Одобренные видео", callback_data="menu:approved")],
            [InlineKeyboardButton("🔑 Вставить API ключ", callback_data="menu:api_key")],
            [InlineKeyboardButton("🧠 Обновить промпт ХУК", callback_data="menu:hook_prompt")],
            [InlineKeyboardButton("📝 Обновить промпт ОПИСАНИЯ", callback_data="menu:desc_prompt")],
            [InlineKeyboardButton("🎬 Исходные видео", callback_data="menu:v_lib")],
            [InlineKeyboardButton("🎵 Библиотека музыки", callback_data="menu:m_lib")],
        ]
    )


def account_keyboard(prefix: str = "account") -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for account in load_accounts():
        title = str(account.get("name", account.get("account_id", "Account")))
        account_id = str(account.get("account_id", "")).strip()
        if account_id:
            rows.append([InlineKeyboardButton(title, callback_data=f"{prefix}:{account_id}")])
    rows.append([InlineKeyboardButton("Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def account_keyboard_mode(mode: str) -> InlineKeyboardMarkup:
    """Account selection keyboard that jumps directly to generation with a fixed mode."""
    label = "⚙️ Генератор" if mode == "generator" else "🤖 LLM / ИИ"
    rows: list[list[InlineKeyboardButton]] = []
    for account in load_accounts():
        title = str(account.get("name", account.get("account_id", "Account")))
        account_id = str(account.get("account_id", "")).strip()
        if account_id:
            rows.append([InlineKeyboardButton(title, callback_data=f"gen_prev:{account_id}:{mode}")])
    rows.append([InlineKeyboardButton("Назад", callback_data="menu:main")])
    return InlineKeyboardMarkup(rows)


def hook_prompt_keyboard(account_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Примеры ХУКОВ", callback_data=f"hp:examples:{account_id}")],
            [InlineKeyboardButton("Редактировать", callback_data=f"hp:edit:{account_id}")],
            [InlineKeyboardButton("Назад", callback_data="menu:hook_prompt")],
        ]
    )


def hook_prompt_cancel_keyboard(account_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Отмена", callback_data=f"hp:cancel:{account_id}")],
        ]
    )


def description_prompt_keyboard(account_id: str, current_slot: int = 1) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(f"{'✅ ' if current_slot==1 else ''}Промпт 1", callback_data=f"dp:slot:{account_id}:1"),
            InlineKeyboardButton(f"{'✅ ' if current_slot==2 else ''}Промпт 2", callback_data=f"dp:slot:{account_id}:2"),
        ],
        [
            InlineKeyboardButton(f"{'✅ ' if current_slot==3 else ''}Промпт 3", callback_data=f"dp:slot:{account_id}:3"),
            InlineKeyboardButton(f"{'✅ ' if current_slot==4 else ''}Промпт 4", callback_data=f"dp:slot:{account_id}:4"),
        ],
        [
            InlineKeyboardButton(f"{'✅ ' if current_slot==5 else ''}Промпт 5", callback_data=f"dp:slot:{account_id}:5"),
            InlineKeyboardButton(f"{'✅ ' if current_slot==6 else ''}Промпт 6", callback_data=f"dp:slot:{account_id}:6"),
        ],
        [
            InlineKeyboardButton(f"{'✅ ' if current_slot==7 else ''}Промпт 7", callback_data=f"dp:slot:{account_id}:7"),
        ],
        [
            InlineKeyboardButton("Примеры", callback_data=f"dp:examples:{account_id}"),
            InlineKeyboardButton("Редактировать", callback_data=f"dp:edit:{account_id}:{current_slot}"),
        ],
        [InlineKeyboardButton("Назад", callback_data="menu:desc_prompt")],
    ]
    return InlineKeyboardMarkup(rows)


def description_prompt_cancel_keyboard(account_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Отмена", callback_data=f"dp:view:{account_id}")],
        ]
    )


def action_keyboard(preview_id: str, account_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Одобрить", callback_data=f"approve:{preview_id}:{account_id}"),
                InlineKeyboardButton("Опубликовать", callback_data=f"publish:{preview_id}:{account_id}"),
                InlineKeyboardButton("Отклонить", callback_data=f"reject:{preview_id}:{account_id}"),
            ],
            [
                InlineKeyboardButton("Опубликовать в другой аккаунт", callback_data=f"p_o:{preview_id}:{account_id}"),
            ],
            [
                InlineKeyboardButton("🤖 Обновить (ИИ)", callback_data=f"r:d:{preview_id}:ai"),
                InlineKeyboardButton("⚙️ Обновить (Ген)", callback_data=f"r:d:{preview_id}:generator"),
            ],
            [
                InlineKeyboardButton("Обновить хук (ИИ)", callback_data=f"r:h:{preview_id}:ai"),
                InlineKeyboardButton("Обновить хук (Ген)", callback_data=f"r:h:{preview_id}:generator"),
            ],
            [
                InlineKeyboardButton("Обновить музыку", callback_data=f"r:m:{preview_id}"),
                InlineKeyboardButton("Уникализировать", callback_data=f"r:u:{preview_id}"),
            ],
            [
                InlineKeyboardButton("Формат 9:16", callback_data=f"r:f:{preview_id}"),
                InlineKeyboardButton("Генерировать заново", callback_data=f"account:{account_id}"),
            ],
            [InlineKeyboardButton("Назад", callback_data="menu:accounts")],
        ]
    )


def generate_preview(account_id: str, mode: str = "ai") -> dict[str, Any]:
    response = requests.post(
        f"{CONTENT_API_BASE_URL}/generate-preview",
        json={"account_id": account_id, "mode": mode},
        timeout=180,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Invalid response from content API")
    return payload


def _hook_prompt_path(account_id: str) -> Path:
    return HOOK_PROMPTS_DIR / account_id / "prompt.txt"


def _hook_examples_path(account_id: str) -> Path:
    return HOOK_PROMPTS_DIR / account_id / "examples.txt"


def _description_prompt_path(account_id: str, slot: int = 1) -> Path:
    if slot == 1:
        # Check if prompt.txt exists for backward compatibility, otherwise use slot1.txt
        legacy = DESCRIPTION_PROMPTS_DIR / account_id / "prompt.txt"
        if legacy.exists():
            return legacy
    return DESCRIPTION_PROMPTS_DIR / account_id / f"prompt_slot{slot}.txt"


def _description_examples_path(account_id: str) -> Path:
    return DESCRIPTION_PROMPTS_DIR / account_id / "examples.txt"


def _read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except Exception:
        return ""


def _upsert_env_var(path: Path, key: str, value: str) -> None:
    lines: list[str] = []
    if path.exists():
        lines = path.read_text(encoding="utf-8").splitlines()

    key_prefix = f"{key}="
    updated = False
    for idx, line in enumerate(lines):
        if line.strip().startswith(key_prefix):
            lines[idx] = f"{key}={value}"
            updated = True
            break

    if not updated:
        lines.append(f"{key}={value}")

    path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")


def _mask_secret(secret: str) -> str:
    clean = secret.strip()
    if len(clean) <= 6:
        return "*" * len(clean)
    return f"{clean[:3]}{'*' * (len(clean) - 6)}{clean[-3:]}"


def _format_markdown_code_block(text: str, max_len: int = 3200) -> str:
    cleaned = text.replace("```", "'''").strip()
    if len(cleaned) > max_len:
        cleaned = cleaned[:max_len].rstrip() + "\n... [TRUNCATED]"
    return f"```\n{cleaned}\n```"


def _build_hook_prompt_view(account_id: str) -> str:
    prompt_text = _read_text_file(_hook_prompt_path(account_id))
    if not prompt_text:
        prompt_text = "Промпт пока не задан."
    return (
        f"Промпт ХУК для `{account_id}`:\n\n"
        f"{_format_markdown_code_block(prompt_text)}"
    )


def _build_hook_examples_view(account_id: str) -> str:
    examples_text = _read_text_file(_hook_examples_path(account_id))
    if not examples_text:
        examples_text = "Примеры пока не заданы."
    return (
        f"Примеры ХУКОВ для `{account_id}`:\n\n"
        f"{_format_markdown_code_block(examples_text)}"
    )


def _build_description_prompt_view(account_id: str, slot: int = 1) -> str:
    prompt_text = _read_text_file(_description_prompt_path(account_id, slot))
    if not prompt_text:
        prompt_text = "нет промпта"
    return (
        f"Промпт ОПИСАНИЯ для `{account_id}` (Слот #{slot}):\n\n"
        f"{_format_markdown_code_block(prompt_text)}"
    )


def _build_description_examples_view(account_id: str) -> str:
    examples_text = _read_text_file(_description_examples_path(account_id))
    if not examples_text:
        examples_text = "Примеры пока не заданы."
    return (
        f"Примеры ОПИСАНИЙ для `{account_id}`:\n\n"
        f"{_format_markdown_code_block(examples_text)}"
    )


def account_has_source_videos(account_id: str) -> bool:
    account = get_account(account_id)
    if not account:
        return False
    folder = Path(str(account.get("video_folder", "")))
    if not folder.is_absolute():
        folder = BASE_DIR / folder
    if not folder.exists():
        return False
    for p in folder.iterdir():
        if p.is_file() and p.suffix.lower() in SUPPORTED_VIDEO_EXTENSIONS:
            return True
    return False


def regenerate_preview(preview_id: str, refresh: str) -> dict[str, Any]:
    response = requests.post(
        f"{CONTENT_API_BASE_URL}/regenerate-preview",
        json={"preview_id": preview_id, "refresh": refresh},
        timeout=180,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Invalid response from content API")
    return payload


def publish_now(preview_id: str, account_id: str) -> dict[str, Any]:
    state = load_state()
    pending = state.get("pending_previews", {})
    preview = pending.get(preview_id) if isinstance(pending, dict) else None
    if not isinstance(preview, dict):
        raise ValueError("Preview не найден или уже обработан.")

    account = get_account(account_id) or {}
    cloud_phone_id = str(account.get("cloud_phone_id") or "").strip()
    if not cloud_phone_id:
        raise ValueError(f"Для аккаунта {account_id} не настроен cloud_phone_id")
    if not cloud_phone_id.isdigit():
        raise ValueError(f"cloud_phone_id для аккаунта {account_id} должен быть числом")

    payload = {
        "video_path": str(preview.get("preview_video", "")),
        "caption": str(preview.get("caption", "")),
        "cloud_phone_id": cloud_phone_id,
    }
    try:
        response = requests.post(f"{PUBLISH_API_BASE_URL}/publish", json=payload, timeout=180)
        response.raise_for_status()
        data = response.json()
    except RequestException:
        # Fallback: publish directly from bot process if publish API is temporarily unavailable.
        if upload_video is None or publish_to_geelark is None:
            raise
        video_url = upload_video(payload["video_path"])
        geelark_response = publish_to_geelark(
            video_url=video_url,
            caption=payload["caption"],
            cloud_phone_id=cloud_phone_id,
        )
        data = {
            "success": True,
            "task_id": geelark_response.get("task_id"),
            "video_url": video_url,
            "scheduled_at": geelark_response.get("scheduled_at"),
            "geelark_response": geelark_response.get("response"),
        }

    if not isinstance(data, dict) or not data.get("success"):
        raise ValueError(f"Ошибка publish service: {data}")

    mark_publish_requested(
        preview_id=preview_id,
        task_id=str(data.get("task_id") or ""),
        video_url=str(data.get("video_url") or ""),
        scheduled_at=int(data.get("scheduled_at") or 0) if data.get("scheduled_at") is not None else None,
    )
    return data


def warmup_account_now(account_id: str) -> dict[str, Any]:
    account = get_account(account_id) or {}
    cloud_phone_id = str(account.get("cloud_phone_id") or "").strip()
    if not cloud_phone_id:
        raise ValueError(f"Для аккаунта {account_id} не настроен cloud_phone_id")
    
    response = requests.post(
        f"{PUBLISH_API_BASE_URL}/warmup",
        json={"cloud_phone_id": cloud_phone_id},
        timeout=180
    )
    response.raise_for_status()
    return response.json()


async def send_preview_message(message, payload: dict[str, Any], account_id: str) -> None:
    preview_video = Path(str(payload["preview_video"]))
    full_caption = str(payload.get("caption", "")).strip()
    description = str(payload.get("description", "")).strip()
    hook = str(payload.get("hook", "")).strip()

    # If the full caption (including description and CTA) fits, use it.
    # Otherwise, use only the hook for the video and send the description separately.
    if full_caption and len(full_caption) <= 1000:
        caption = full_caption
        # We've included the description in the video caption, so don't send it again.
        description_to_send_separately = ""
    else:
        caption = hook or "New Video"
        description_to_send_separately = description
    preview_id = str(payload["preview_id"])

    if not preview_video.exists():
        await message.reply_text("Preview создан, но видеофайл не найден.", reply_markup=account_keyboard())
        return

    with preview_video.open("rb") as video_fp:
        await message.reply_video(
            video=video_fp,
            caption=caption,
            reply_markup=action_keyboard(preview_id=preview_id, account_id=account_id),
        )

    # Send the technical info about the prompt source
    prompt_source = str(payload.get("prompt_source") or "Unknown")
    is_generator = "генератор" in prompt_source.lower()
    if is_generator:
        tech_note = "⚙️ ТЕХНИЧЕСКАЯ ЗАМЕТКА: сделано с помощью генератора"
    else:
        tech_note = f"⚙️ ТЕХНИЧЕСКАЯ ЗАМЕТКА: Описание сгенерировано с помощью: `{prompt_source}`"
    await message.reply_text(tech_note, parse_mode="Markdown")

    # Send the full generated description as separate Telegram messages
    if description_to_send_separately:
        midpoint = max(1, len(description_to_send_separately) // 2)
        split_at = description_to_send_separately.rfind("\n\n", 0, midpoint)
        if split_at == -1:
            split_at = description_to_send_separately.rfind("\n", 0, midpoint)
        if split_at == -1:
            split_at = midpoint

        first_part = description_to_send_separately[:split_at].strip()
        second_part = description_to_send_separately[split_at:].strip()

        if first_part and second_part:
            await message.reply_text(first_part)
            await message.reply_text(second_part)
        else:
            await message.reply_text(description_to_send_separately)


async def show_main_menu(query) -> None:
    await safe_edit_message_text(query, "Главное меню:", reply_markup=main_menu_keyboard())


async def show_accounts_menu(query) -> None:
    await safe_edit_message_text(query, "Выбери аккаунт для генерации preview:", reply_markup=account_keyboard())


async def show_approved_accounts_menu(query) -> None:
    await safe_edit_message_text(query, "Выбери аккаунт для просмотра одобренных видео:", reply_markup=account_keyboard(prefix="appr_acc"))


async def show_hook_prompt_accounts_menu(query) -> None:
    await safe_edit_message_text(query, 
        "Выбери аккаунт для настройки промпта ХУК:",
        reply_markup=account_keyboard(prefix="hp_acc"),
    )


async def show_description_prompt_accounts_menu(query) -> None:
    await safe_edit_message_text(
        query,
        "Выбери аккаунт для настройки промпта ОПИСАНИЯ:",
        reply_markup=account_keyboard(prefix="dp_acc"),
    )


async def show_approved_menu(query, account_id: str) -> None:
    items = load_approved_videos(limit=20, account_id=account_id)
    if not items:
        text = f"Одобренные видео для `{account_id}` отсутствуют."
        reply_markup = InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="menu:approved")]])
    else:
        text = f"Одобренные видео для `{account_id}` (последние):"
        buttons = []
        for item in items:
            preview_id = str(item.get("preview_id", "unknown"))
            status = str(item.get("status", "unknown"))
            # Кнопка для удаления конкретного видео
            buttons.append([
                InlineKeyboardButton(f"❌ Удалить {preview_id[:8]} ({status})", callback_data=f"del_appr:{preview_id}:{account_id}")
            ])
        
        account = get_account(account_id)
        sched_enabled = account.get("schedule_enabled", False)
        sched_label = "🟢 Расписание: ВКЛ" if sched_enabled else "🔴 Расписание: ВЫКЛ"
        buttons.append([InlineKeyboardButton(sched_label, callback_data=f"t_sched:{account_id}")])
        buttons.append([InlineKeyboardButton("🔥 Прогрев аккаунта", callback_data=f"warmup:{account_id}")])
        
        buttons.append([InlineKeyboardButton("Назад", callback_data="menu:approved")])
        reply_markup = InlineKeyboardMarkup(buttons)

    await safe_edit_message_text(query, text, reply_markup=reply_markup, parse_mode="Markdown")


async def show_library_menu(query, mode: str, account_id: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    account = get_account(account_id)
    if not account:
        await safe_edit_message_text(query, "Аккаунт не найден.")
        return

    # Определяем папку в зависимости от режима (v_lib или m_lib)
    folder_key = "video_folder" if mode == "v_lib" else "music_folder"
    folder_path = Path(str(account.get(folder_key, "")))
    if not folder_path.is_absolute():
        folder_path = BASE_DIR / folder_path

    folder_path.mkdir(parents=True, exist_ok=True)
    files = sorted([f for f in folder_path.iterdir() if f.is_file() and not f.name.startswith(".")], key=lambda x: x.stat().st_mtime, reverse=True)[:10]
    
    # Сохраняем список имен файлов в user_data, чтобы обращаться по индексу
    context.user_data[f"lib_files_{mode}_{account_id}"] = [f.name for f in files]
    
    title = "Видео" if mode == "v_lib" else "Музыка"
    text = f"Библиотека *{title}* для `{account_id}`:\n"
    if not files:
        text += "_Папка пуста_"
    
    buttons = []
    for i, f in enumerate(files):
        filename = f.name
        display_name = (filename[:25] + "..") if len(filename) > 27 else filename
        buttons.append([
            InlineKeyboardButton(f"🗑 {display_name}", callback_data=f"df:{mode}:{account_id}:{i}")
        ])
    
    buttons.append([InlineKeyboardButton("➕ Загрузить ещё", callback_data=f"wait_upload:{mode}:{account_id}")])
    buttons.append([InlineKeyboardButton("Назад", callback_data=f"menu:{mode}")])
    
    await safe_edit_message_text(query, text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode="Markdown")


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    
    # Сохраняем chat_id для рассылки уведомлений
    chat_id = update.message.chat_id
    state = load_state()
    state["admin_chat_id"] = chat_id
    save_state(state)
    
    await update.message.reply_text("Главное меню:", reply_markup=main_menu_keyboard())


async def text_fallback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text("Главное меню:", reply_markup=main_menu_keyboard())


async def safe_answer_query(query) -> None:
    try:
        await query.answer()
    except BadRequest:
        # Callback может устареть, если пользователь нажал на старую кнопку.
        pass


async def safe_clear_reply_markup(query) -> None:
    try:
        await query.edit_message_reply_markup(reply_markup=None)
    except BadRequest:
        # Сообщение могло быть старым/изменённым, игнорируем и продолжаем диалог.
        pass


async def safe_edit_message_text(query, text: str, reply_markup=None, parse_mode: str | None = None) -> None:
    try:
        await query.edit_message_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except BadRequest as exc:
        message = str(exc)
        if "There is no text in the message to edit" in message or "Message is not modified" in message:
            if query.message:
                await query.message.reply_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
            return
        raise


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)

    _, target = query.data.split(":", 1)
    if target != "api_key":
        context.user_data.pop("awaiting_api_key_insert", None)
    if target == "main":
        await show_main_menu(query)
    elif target == "accounts":
        await show_accounts_menu(query)
    elif target == "gen_generator":
        await safe_edit_message_text(
            query,
            "⚙️ *Генератор описаний* — выбери аккаунт:",
            reply_markup=account_keyboard_mode("generator"),
            parse_mode="Markdown",
        )
    elif target == "gen_ai":
        await safe_edit_message_text(
            query,
            "🤖 *LLM / Нейросеть* — выбери аккаунт:",
            reply_markup=account_keyboard_mode("ai"),
            parse_mode="Markdown",
        )
    elif target == "approved":
        await show_approved_accounts_menu(query)
    elif target == "hook_prompt":
        await show_hook_prompt_accounts_menu(query)
    elif target == "desc_prompt":
        await show_description_prompt_accounts_menu(query)
    elif target == "v_lib":
        await safe_edit_message_text(query, "Выбери аккаунт для управления видео:", reply_markup=account_keyboard(prefix="v_acc"))
    elif target == "m_lib":
        await safe_edit_message_text(query, "Выбери аккаунт для управления музыкой:", reply_markup=account_keyboard(prefix="m_acc"))
    elif target == "api_key":
        context.user_data["awaiting_api_key_insert"] = True
        await safe_edit_message_text(
            query,
            (
                "Отправь API ключ одним сообщением.\n\n"
                "Я сохраню его в `.env` как `OPENAI_API_KEY` и `DEEPSEEK_API_KEY`."
            ),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="menu:main")]]),
            parse_mode="Markdown",
        )


async def account_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)

    _, account_id = query.data.split(":", 1)
    if not account_has_source_videos(account_id):
        await query.message.reply_text(
            f"Для `{account_id}` нет исходных видео.\n"
            "Зайди в «Исходные видео» и загрузи хотя бы один файл.",
            reply_markup=account_keyboard(),
            parse_mode="Markdown",
        )
        return

    await safe_edit_message_text(
        query,
        f"Аккаунт: `{account_id}`\n\nВыбери режим генерации ПРЕВЬЮ:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🤖 С помощью НЕЙРОСЕТИ", callback_data=f"gen_prev:{account_id}:ai")],
            [InlineKeyboardButton("⚙️ С помощью ГЕНЕРАТОРА", callback_data=f"gen_prev:{account_id}:generator")],
            [InlineKeyboardButton("Назад", callback_data="menu:accounts")]
        ]),
        parse_mode="Markdown"
    )


async def generate_preview_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)
    
    parts = query.data.split(":", 2)
    account_id = parts[1]
    mode = parts[2] if len(parts) > 2 else "ai"
    
    await safe_edit_message_text(query, f"Генерирую preview (`{mode}`) для `{account_id}`...", parse_mode="Markdown")

    try:
        payload = generate_preview(account_id, mode=mode)
        await send_preview_message(query.message, payload, account_id)
    except Exception as exc:
        await query.message.reply_text(f"Ошибка генерации preview: {exc}", reply_markup=account_keyboard())


async def refresh_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)

    _, refresh_code, preview_id = query.data.split(":", 2)
    refresh_map = {"h": "hook", "d": "description", "m": "music", "u": "unique", "f": "format_9_16"}
    refresh_type = refresh_map.get(refresh_code)
    if not refresh_type:
        await query.message.reply_text("Неизвестный тип обновления.", reply_markup=account_keyboard())
        return
    await safe_clear_reply_markup(query)
    await query.message.reply_text("Обновляю preview...")

    try:
        payload = regenerate_preview(preview_id=preview_id, refresh=refresh_type)
        account_id = str(payload.get("account_id", ""))
        await send_preview_message(query.message, payload, account_id)
    except Exception as exc:
        await query.message.reply_text(f"Ошибка обновления preview: {exc}", reply_markup=account_keyboard())


async def action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)

    action, preview_id, account_id = query.data.split(":", 2)

    try:
        if action == "approve":
            approve_preview(preview_id)
            text = "Добавлено в очередь"
        elif action == "publish":
            publish_result = publish_now(preview_id=preview_id, account_id=account_id)
            text = (
                "Отправлено в публикацию\n"
                f"taskId: {publish_result.get('task_id')}\n"
                f"video_url: {publish_result.get('video_url')}"
            )
        elif action == "reject":
            reject_preview(preview_id)
            text = "Отклонено"
        elif action == "p_o":
            rows = []
            for acc in load_accounts():
                title = str(acc.get("name", acc.get("account_id", "Account")))
                acc_id = str(acc.get("account_id", "")).strip()
                if acc_id:
                    rows.append([InlineKeyboardButton(title, callback_data=f"publish:{preview_id}:{acc_id}")])
            rows.append([InlineKeyboardButton("Отмена", callback_data=f"c_p_o:{preview_id}:{account_id}")])
            try:
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(rows))
            except Exception as e:
                print(f"Error editing markup: {e}")
            return
        elif action == "c_p_o":
            try:
                await query.edit_message_reply_markup(reply_markup=action_keyboard(preview_id, account_id))
            except Exception:
                pass
            return
        else:
            text = "Неизвестное действие."
    except ValueError as exc:
        text = str(exc)
    except Exception as exc:
        text = f"Ошибка обработки preview: {exc}"

    if action in ("approve", "reject"):
        await safe_clear_reply_markup(query)
    elif action == "publish":
        # Restore the original action keyboard so they can publish again to other accounts
        try:
            await query.edit_message_reply_markup(reply_markup=action_keyboard(preview_id, account_id))
        except Exception:
            pass
            
    if text:
        await query.message.reply_text(text, reply_markup=main_menu_keyboard())


async def approved_account_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)
    _, account_id = query.data.split(":", 1)
    await show_approved_menu(query, account_id)


async def delete_approved_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)
    _, preview_id, account_id = query.data.split(":", 2)
    delete_approved_preview(preview_id)
    await show_approved_menu(query, account_id)


async def toggle_schedule_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)
    _, account_id = query.data.split(":", 1)
    toggle_account_schedule(account_id)
    await show_approved_menu(query, account_id)


async def warmup_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)
    _, account_id = query.data.split(":", 1)
    
    try:
        result = warmup_account_now(account_id)
        text = (
            f"✅ Задача прогрева создана!\n"
            f"TaskId: `{result.get('task_id')}`\n"
            f"Запуск через 3 минуты."
        )
    except Exception as exc:
        text = f"❌ Ошибка прогрева: {exc}"
    
    await query.message.reply_text(text, parse_mode="Markdown")


async def hook_prompt_account_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)
    _, account_id = query.data.split(":", 1)
    await safe_edit_message_text(query, 
        _build_hook_prompt_view(account_id),
        reply_markup=hook_prompt_keyboard(account_id),
        parse_mode="Markdown",
    )


async def hook_prompt_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)

    _, action, account_id = query.data.split(":", 2)
    if action == "examples":
        await safe_edit_message_text(query, 
            _build_hook_examples_view(account_id),
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Назад к промпту", callback_data=f"hp:view:{account_id}")],
                    [InlineKeyboardButton("Назад", callback_data="menu:hook_prompt")],
                ]
            ),
            parse_mode="Markdown",
        )
        return

    if action == "edit":
        context.user_data["hook_prompt_edit_account"] = account_id
        await safe_edit_message_text(query, 
            (
                f"Пришли новый текст промпта для `{account_id}` одним сообщением.\n\n"
                "После отправки я сразу сохраню его в файл."
            ),
            reply_markup=hook_prompt_cancel_keyboard(account_id),
            parse_mode="Markdown",
        )
        return

    if action == "cancel":
        context.user_data.pop("hook_prompt_edit_account", None)
        await safe_edit_message_text(query, 
            _build_hook_prompt_view(account_id),
            reply_markup=hook_prompt_keyboard(account_id),
            parse_mode="Markdown",
        )
        return

    if action == "view":
        await safe_edit_message_text(query, 
            _build_hook_prompt_view(account_id),
            reply_markup=hook_prompt_keyboard(account_id),
            parse_mode="Markdown",
        )


async def description_prompt_account_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)
    _, account_id = query.data.split(":", 1)
    
    # Store current slot in user_data
    context.user_data[f"desc_slot_{account_id}"] = 1
    
    await safe_edit_message_text(
        query,
        _build_description_prompt_view(account_id, slot=1),
        reply_markup=description_prompt_keyboard(account_id, current_slot=1),
        parse_mode="Markdown",
    )


async def description_prompt_action_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)
    
    parts = query.data.split(":", 3)
    action = parts[1]
    account_id = parts[2]
    
    current_slot = context.user_data.get(f"desc_slot_{account_id}", 1)

    if action == "slot":
        new_slot = int(parts[3])
        context.user_data[f"desc_slot_{account_id}"] = new_slot
        await safe_edit_message_text(
            query,
            _build_description_prompt_view(account_id, slot=new_slot),
            reply_markup=description_prompt_keyboard(account_id, current_slot=new_slot),
            parse_mode="Markdown",
        )
        return

    if action == "examples":
        await safe_edit_message_text(
            query,
            _build_description_examples_view(account_id),
            reply_markup=InlineKeyboardMarkup(
                [
                    [InlineKeyboardButton("Назад к промпту", callback_data=f"dp:view:{account_id}")],
                    [InlineKeyboardButton("Назад", callback_data="menu:desc_prompt")],
                ]
            ),
            parse_mode="Markdown",
        )
        return

    if action == "edit":
        context.user_data["description_prompt_edit_account"] = account_id
        await safe_edit_message_text(
            query,
            (
                f"Пришли новый текст промпта ОПИСАНИЯ для `{account_id}` одним сообщением.\n\n"
                "После отправки я сразу сохраню его в файл."
            ),
            reply_markup=description_prompt_cancel_keyboard(account_id),
            parse_mode="Markdown",
        )
        return

    if action == "cancel":
        context.user_data.pop("description_prompt_edit_account", None)
        await safe_edit_message_text(
            query,
            _build_description_prompt_view(account_id, slot=current_slot),
            reply_markup=description_prompt_keyboard(account_id, current_slot=current_slot),
            parse_mode="Markdown",
        )
        return

    if action == "view":
        await safe_edit_message_text(
            query,
            _build_description_prompt_view(account_id, slot=current_slot),
            reply_markup=description_prompt_keyboard(account_id, current_slot=current_slot),
            parse_mode="Markdown",
        )
        return


async def library_account_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)
    
    data_parts = query.data.split(":")
    mode = "v_lib" if data_parts[0] == "v_acc" else "m_lib"
    account_id = data_parts[1]
    await show_library_menu(query, mode, account_id, context)


async def delete_file_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)
    
    _, mode, account_id, file_idx = query.data.split(":", 3)
    file_list = context.user_data.get(f"lib_files_{mode}_{account_id}", [])
    
    try:
        filename = file_list[int(file_idx)]
    except (IndexError, ValueError):
        await query.message.reply_text("Файл не найден в списке (возможно, список обновился).")
        return

    account = get_account(account_id)
    if not account: return
    
    folder_key = "video_folder" if mode == "v_lib" else "music_folder"
    folder_path = Path(str(account.get(folder_key, "")))
    if not folder_path.is_absolute():
        folder_path = BASE_DIR / folder_path
        
    file_path = folder_path / filename
    if file_path.exists():
        file_path.unlink()
    
    await show_library_menu(query, mode, account_id, context)


async def wait_upload_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)
    
    _, mode, account_id = query.data.split(":", 2)
    context.user_data["upload_mode"] = mode
    context.user_data["upload_account"] = account_id
    
    title = "видео" if mode == "v_lib" else "музыку"
    await safe_edit_message_text(query, 
        f"Отправьте {title} (как файл или медиа), и я сохраню его для аккаунта `{account_id}`.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data=f"{mode.replace('_lib', '_acc')}:{account_id}")]])
    )


async def file_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    mode = context.user_data.get("upload_mode")
    account_id = context.user_data.get("upload_account")
    
    if not mode or not account_id:
        return

    account = get_account(account_id)
    if not account: return
    
    folder_key = "video_folder" if mode == "v_lib" else "music_folder"
    folder_path = Path(str(account.get(folder_key, "")))
    if not folder_path.is_absolute():
        folder_path = BASE_DIR / folder_path
    
    folder_path.mkdir(parents=True, exist_ok=True)
    
    # Пытаемся получить файл из разных типов медиа
    file_obj = None
    if update.message.video:
        file_obj = update.message.video
    elif update.message.document:
        file_obj = update.message.document
    elif update.message.audio:
        file_obj = update.message.audio
    elif update.message.voice:
        file_obj = update.message.voice

    if not file_obj:
        await update.message.reply_text("Пожалуйста, отправьте видео или аудио файл.")
        return

    tg_file = await context.bot.get_file(file_obj.file_id)
    ext = Path(tg_file.file_path).suffix or (".mp4" if mode == "v_lib" else ".mp3")
    filename = f"tg_{uuid.uuid4().hex[:8]}{ext}"
    dest_path = folder_path / filename
    
    await tg_file.download_to_drive(custom_path=dest_path)
    
    # Сбрасываем режим ожидания
    context.user_data.pop("upload_mode", None)
    context.user_data.pop("upload_account", None)
    
    await update.message.reply_text(f"✅ Файл сохранен как `{filename}`")
    # Возвращаемся в меню библиотеки
    # Для этого нам нужно "подделать" query или просто отправить новое сообщение
    # Но проще отправить новое меню
    # (Создадим имитацию query для вызова show_library_menu)
    class DummyQuery:
        def __init__(self, message): self.message = message
        async def edit_message_text(self, *args, **kwargs): await self.message.reply_text(*args, **kwargs)
    
    await show_library_menu(DummyQuery(update.message), mode, account_id, context)


async def handle_hook_prompt_edit_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.message:
        return False
    account_id = str(context.user_data.get("hook_prompt_edit_account", "")).strip()
    if not account_id:
        return False

    new_prompt = (update.message.text or "").strip()
    if not new_prompt:
        await update.message.reply_text("Пустой текст. Отправь непустой промпт или нажми «Отмена».")
        return True

    prompt_path = _hook_prompt_path(account_id)
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(new_prompt, encoding="utf-8")
    context.user_data.pop("hook_prompt_edit_account", None)

    await update.message.reply_text(
        f"✅ Промпт для `{account_id}` обновлён.\n\n{_build_hook_prompt_view(account_id)}",
        reply_markup=hook_prompt_keyboard(account_id),
        parse_mode="Markdown",
    )
    return True


async def handle_description_prompt_edit_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.message:
        return False
    account_id = str(context.user_data.get("description_prompt_edit_account", "")).strip()
    if not account_id:
        return False

    new_prompt = (update.message.text or "").strip()
    if not new_prompt:
        await update.message.reply_text("Пустой текст. Отправь непустой промпт или нажми «Отмена».")
        return True

    current_slot = context.user_data.get(f"desc_slot_{account_id}", 1)
    prompt_path = _description_prompt_path(account_id, slot=current_slot)
    prompt_path.parent.mkdir(parents=True, exist_ok=True)
    prompt_path.write_text(new_prompt, encoding="utf-8")
    context.user_data.pop("description_prompt_edit_account", None)

    await update.message.reply_text(
        f"✅ Промпт ОПИСАНИЯ для `{account_id}` (Слот #{current_slot}) обновлён.\n\n{_build_description_prompt_view(account_id, slot=current_slot)}",
        reply_markup=description_prompt_keyboard(account_id, current_slot=current_slot),
        parse_mode="Markdown",
    )
    return True


async def handle_api_key_insert_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not update.message:
        return False
    if not context.user_data.get("awaiting_api_key_insert"):
        return False

    api_key = (update.message.text or "").strip()
    if not api_key:
        await update.message.reply_text("Пустой ключ. Отправь непустое значение или нажми «Отмена».")
        return True

    env_path = BASE_DIR / ".env"
    _upsert_env_var(env_path, "OPENAI_API_KEY", api_key)
    _upsert_env_var(env_path, "DEEPSEEK_API_KEY", api_key)
    os.environ["OPENAI_API_KEY"] = api_key
    os.environ["DEEPSEEK_API_KEY"] = api_key
    context.user_data.pop("awaiting_api_key_insert", None)

    await update.message.reply_text(
        (
            "✅ API ключ сохранён.\n"
            f"Маска ключа: `{_mask_secret(api_key)}`\n\n"
            "Если Content API уже запущен, перезапусти его, чтобы он подхватил новый ключ."
        ),
        reply_markup=main_menu_keyboard(),
        parse_mode="Markdown",
    )
    return True


def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

    async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
        print(f"Telegram handler error: {context.error}")
    
    from telegram.ext import MessageHandler, filters
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(account_callback, pattern=r"^account:"))
    app.add_handler(CallbackQueryHandler(approved_account_callback, pattern=r"^appr_acc:"))
    app.add_handler(CallbackQueryHandler(hook_prompt_account_callback, pattern=r"^hp_acc:"))
    app.add_handler(CallbackQueryHandler(hook_prompt_action_callback, pattern=r"^hp:(examples|edit|view|cancel):"))
    app.add_handler(CallbackQueryHandler(description_prompt_account_callback, pattern=r"^dp_acc:"))
    app.add_handler(CallbackQueryHandler(description_prompt_action_callback, pattern=r"^dp:(examples|edit|view|cancel|slot):"))
    app.add_handler(CallbackQueryHandler(library_account_callback, pattern=r"^(v_acc|m_acc):"))
    app.add_handler(CallbackQueryHandler(delete_file_callback, pattern=r"^df:"))
    app.add_handler(CallbackQueryHandler(wait_upload_callback, pattern=r"^wait_upload:"))
    app.add_handler(CallbackQueryHandler(delete_approved_callback, pattern=r"^del_appr:"))
    app.add_handler(CallbackQueryHandler(toggle_schedule_callback, pattern=r"^t_sched:"))
    app.add_handler(CallbackQueryHandler(warmup_callback, pattern=r"^warmup:"))
    app.add_handler(CallbackQueryHandler(refresh_callback, pattern=r"^r:"))
    app.add_handler(CallbackQueryHandler(generate_preview_callback, pattern=r"^gen_prev:"))
    app.add_handler(CallbackQueryHandler(action_callback, pattern=r"^(approve|publish|reject|p_o|c_p_o):"))
    
    app.add_handler(MessageHandler(filters.VIDEO | filters.AUDIO | filters.Document.ALL | filters.VOICE, file_handler))
    async def text_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        if await handle_api_key_insert_text(update, context):
            return
        if await handle_hook_prompt_edit_text(update, context):
            return
        if await handle_description_prompt_edit_text(update, context):
            return
        await text_fallback_handler(update, context)

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_router))
    app.add_error_handler(error_handler)
    
    app.run_polling()


if __name__ == "__main__":
    main()
