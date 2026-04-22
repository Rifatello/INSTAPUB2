from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import requests
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

try:
    from .storage_repo import (
        approve_preview,
        get_account,
        load_accounts,
        load_approved_videos,
        load_state,
        mark_publish_requested,
        reject_preview,
    )
except ImportError:
    from storage_repo import (
        approve_preview,
        get_account,
        load_accounts,
        load_approved_videos,
        load_state,
        mark_publish_requested,
        reject_preview,
    )


CONTENT_API_BASE_URL = os.getenv("CONTENT_API_BASE_URL", "http://127.0.0.1:8000")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
PUBLISH_API_BASE_URL = os.getenv("PUBLISH_API_BASE_URL", "http://127.0.0.1:8010")


def main_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Генерация видео", callback_data="menu:accounts")],
            [InlineKeyboardButton("Одобренные видео", callback_data="menu:approved")],
        ]
    )


def account_keyboard() -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for account in load_accounts():
        title = str(account.get("name", account.get("account_id", "Account")))
        account_id = str(account.get("account_id", "")).strip()
        if account_id:
            rows.append([InlineKeyboardButton(title, callback_data=f"account:{account_id}")])
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
    response = requests.post(f"{PUBLISH_API_BASE_URL}/publish", json=payload, timeout=180)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict) or not data.get("success"):
        raise ValueError(f"Ошибка publish service: {data}")

    mark_publish_requested(
        preview_id=preview_id,
        task_id=str(data.get("task_id") or ""),
        video_url=str(data.get("video_url") or ""),
        scheduled_at=int(data.get("scheduled_at") or 0) if data.get("scheduled_at") is not None else None,
    )
    return data


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


async def show_approved_menu(query) -> None:
    items = load_approved_videos(limit=20)
    if not items:
        text = "Одобренные видео пока отсутствуют."
    else:
        lines = ["Одобренные видео (последние):"]
        for item in items:
            preview_id = str(item.get("preview_id", "unknown"))
            status = str(item.get("status", "unknown"))
            account_id = str(item.get("account_id", "unknown"))
            lines.append(f"- {preview_id} | {account_id} | {status}")
        text = "\n".join(lines)

    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="menu:main")]]),
    )


async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text("Главное меню:", reply_markup=main_menu_keyboard())


async def menu_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()

    _, target = query.data.split(":", 1)
    if target == "main":
        await show_main_menu(query)
    elif target == "accounts":
        await show_accounts_menu(query)
    elif target == "approved":
        await show_approved_menu(query)


async def account_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.data:
        return
    await query.answer()

    _, account_id = query.data.split(":", 1)
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
    await query.answer()

    _, refresh_code, preview_id = query.data.split(":", 2)
    refresh_map = {"h": "hook", "d": "description", "m": "music", "u": "unique"}
    refresh_type = refresh_map.get(refresh_code)
    if not refresh_type:
        await query.message.reply_text("Неизвестный тип обновления.", reply_markup=account_keyboard())
        return
    await query.edit_message_reply_markup(reply_markup=None)
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
    await query.answer()

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

    await query.edit_message_reply_markup(reply_markup=None)
    await query.message.reply_text(text, reply_markup=main_menu_keyboard())


def main() -> None:
    if not TELEGRAM_BOT_TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    app.add_handler(CommandHandler("start", start_handler))
    app.add_handler(CallbackQueryHandler(menu_callback, pattern=r"^menu:"))
    app.add_handler(CallbackQueryHandler(account_callback, pattern=r"^account:"))
    app.add_handler(CallbackQueryHandler(refresh_callback, pattern=r"^r:(h|d|m|u):"))
    app.add_handler(CallbackQueryHandler(action_callback, pattern=r"^(approve|publish|reject):"))
    app.run_polling()


if __name__ == "__main__":
    main()
