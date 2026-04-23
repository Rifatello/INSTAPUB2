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
CONTENT_API_BASE_URL = os.getenv("CONTENT_API_BASE_URL", "http://127.0.0.1:8000")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
PUBLISH_API_BASE_URL = os.getenv("PUBLISH_API_BASE_URL", "http://127.0.0.1:8010")
SUPPORTED_VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm"}


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Генерация видео", callback_data="menu:accounts")],
            [InlineKeyboardButton("Одобренные видео", callback_data="menu:approved")],
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


def action_keyboard(preview_id: str, account_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("Одобрить", callback_data=f"approve:{preview_id}:{account_id}"),
                InlineKeyboardButton("Опубликовать", callback_data=f"publish:{preview_id}:{account_id}"),
                InlineKeyboardButton("Отклонить", callback_data=f"reject:{preview_id}:{account_id}"),
            ],
            [
                InlineKeyboardButton("Обновить описание", callback_data=f"r:d:{preview_id}"),
                InlineKeyboardButton("Обновить хук", callback_data=f"r:h:{preview_id}"),
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


def generate_preview(account_id: str) -> dict[str, Any]:
    response = requests.post(
        f"{CONTENT_API_BASE_URL}/generate-preview",
        json={"account_id": account_id},
        timeout=180,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Invalid response from content API")
    return payload


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
    caption = str(payload["caption"])
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


async def show_main_menu(query) -> None:
    await query.edit_message_text("Главное меню:", reply_markup=main_menu_keyboard())


async def show_accounts_menu(query) -> None:
    await query.edit_message_text("Выбери аккаунт для генерации preview:", reply_markup=account_keyboard())


async def show_approved_accounts_menu(query) -> None:
    await query.edit_message_text("Выбери аккаунт для просмотра одобренных видео:", reply_markup=account_keyboard(prefix="appr_acc"))


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

    await query.edit_message_text(text, reply_markup=reply_markup, parse_mode="Markdown")


async def show_library_menu(query, mode: str, account_id: str, context: ContextTypes.DEFAULT_TYPE) -> None:
    account = get_account(account_id)
    if not account:
        await query.edit_message_text("Аккаунт не найден.")
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
    
    await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons), parse_mode="Markdown")


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


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await safe_answer_query(query)

    _, target = query.data.split(":", 1)
    if target == "main":
        await show_main_menu(query)
    elif target == "accounts":
        await show_accounts_menu(query)
    elif target == "approved":
        await show_approved_accounts_menu(query)
    elif target == "v_lib":
        await query.edit_message_text("Выбери аккаунт для управления видео:", reply_markup=account_keyboard(prefix="v_acc"))
    elif target == "m_lib":
        await query.edit_message_text("Выбери аккаунт для управления музыкой:", reply_markup=account_keyboard(prefix="m_acc"))


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

    await query.edit_message_text(f"Генерирую preview для `{account_id}`...", parse_mode="Markdown")

    try:
        payload = generate_preview(account_id)
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
        else:
            text = "Неизвестное действие."
    except ValueError as exc:
        text = str(exc)
    except Exception as exc:
        text = f"Ошибка обработки preview: {exc}"

    await safe_clear_reply_markup(query)
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
    await query.edit_message_text(
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
    app.add_handler(CallbackQueryHandler(library_account_callback, pattern=r"^(v_acc|m_acc):"))
    app.add_handler(CallbackQueryHandler(delete_file_callback, pattern=r"^df:"))
    app.add_handler(CallbackQueryHandler(wait_upload_callback, pattern=r"^wait_upload:"))
    app.add_handler(CallbackQueryHandler(delete_approved_callback, pattern=r"^del_appr:"))
    app.add_handler(CallbackQueryHandler(toggle_schedule_callback, pattern=r"^t_sched:"))
    app.add_handler(CallbackQueryHandler(warmup_callback, pattern=r"^warmup:"))
    app.add_handler(CallbackQueryHandler(refresh_callback, pattern=r"^r:(h|d|m|u|f):"))
    app.add_handler(CallbackQueryHandler(action_callback, pattern=r"^(approve|publish|reject):"))
    
    app.add_handler(MessageHandler(filters.VIDEO | filters.AUDIO | filters.Document.ALL | filters.VOICE, file_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_fallback_handler))
    app.add_error_handler(error_handler)
    
    app.run_polling()


if __name__ == "__main__":
    main()
