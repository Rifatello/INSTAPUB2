from __future__ import annotations

import json
import os
import random
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"

# Настройки из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CONTENT_API_BASE_URL = os.getenv("CONTENT_API_BASE_URL", "http://127.0.0.1:8002")
PUBLISH_API_BASE_URL = os.getenv("PUBLISH_API_BASE_URL", "http://127.0.0.1:8010")

# Окна публикации (часы)
SCHEDULE_SLOTS = [0, 8, 16]


def read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as fp:
        try:
            return json.load(fp)
        except:
            return default


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)


def send_telegram_notification(chat_id: int, text: str):
    if not TELEGRAM_BOT_TOKEN:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        print(f"Error sending TG notification: {e}")


def run_scheduler():
    print("Scheduler service started...")
    
    while True:
        try:
            now = datetime.now()
            current_hour = now.hour
            
            # Проверяем, находимся ли мы в одном из целевых часов
            if current_hour in SCHEDULE_SLOTS:
                state = read_json(DATA_DIR / "state.json", {})
                admin_chat_id = state.get("admin_chat_id")
                
                if not admin_chat_id:
                    print("No admin_chat_id found in state.json. Waiting...")
                    time.sleep(60)
                    continue

                accounts = read_json(DATA_DIR / "accounts.json", [])
                scheduled_history = state.setdefault("scheduled_history", {}) # account_id -> last_slot_date

                for acc in accounts:
                    acc_id = str(acc.get("account_id", ""))
                    if not acc.get("schedule_enabled"):
                        continue
                    
                    # Уникальный ключ для этого слота: YYYY-MM-DD-HH
                    slot_key = f"{now.strftime('%Y-%m-%d')}-{current_hour}"
                    last_processed = scheduled_history.get(acc_id)
                    
                    if last_processed == slot_key:
                        # Уже опубликовали в этом слоте
                        continue
                    
                    # Если еще не опубликовали, выбираем рандомную минуту в начале часа (0-45)
                    # Чтобы не публиковать ровно в 00:00
                    target_minute = state.setdefault("target_minutes", {}).get(f"{acc_id}-{slot_key}")
                    if target_minute is None:
                        target_minute = random.randint(1, 45)
                        state.setdefault("target_minutes", {})[f"{acc_id}-{slot_key}"] = target_minute
                        write_json(DATA_DIR / "state.json", state)
                    
                    if now.minute >= target_minute:
                        print(f"Time to publish for account {acc_id} (target min: {target_minute})")
                        
                        try:
                            # 1. Генерация видео
                            resp_gen = requests.post(
                                f"{CONTENT_API_BASE_URL}/generate-preview",
                                json={"account_id": acc_id},
                                timeout=180
                            )
                            resp_gen.raise_for_status()
                            gen_data = resp_gen.json()
                            
                            preview_id = gen_data["preview_id"]
                            video_path = gen_data["preview_video"]
                            caption = gen_data["caption"]
                            
                            # 2. Публикация
                            cloud_phone_id = str(acc.get("cloud_phone_id", "")).strip()
                            resp_pub = requests.post(
                                f"{PUBLISH_API_BASE_URL}/publish",
                                json={
                                    "video_path": video_path,
                                    "caption": caption,
                                    "cloud_phone_id": cloud_phone_id
                                },
                                timeout=180
                            )
                            resp_pub.raise_for_status()
                            pub_data = resp_pub.json()
                            
                            # 3. Уведомление
                            pub_time = datetime.now().strftime("%H:%M %d.%m.%Y")
                            notify_text = (
                                f"✅ *ОПУБЛИКОВАНО (АВТО)*\n\n"
                                f"Аккаунт: `{acc_id}`\n"
                                f"Время: {pub_time}\n"
                                f"Статус: Успешно отправлено в Geelark\n"
                                f"TaskID: `{pub_data.get('task_id')}`\n\n"
                                f"*Текст поста:*\n{caption}"
                            )
                            send_telegram_notification(admin_chat_id, notify_text)
                            
                            # Помечаем как выполненное
                            scheduled_history[acc_id] = slot_key
                            write_json(DATA_DIR / "state.json", state)
                            
                        except Exception as e:
                            print(f"Error in auto-publish for {acc_id}: {e}")
                            send_telegram_notification(admin_chat_id, f"❌ *ОШИБКА АВТО-ПУБЛИКАЦИИ*\nАккаунт: `{acc_id}`\nОшибка: {e}")
                            # Не помечаем как выполненное, чтобы попробовать еще раз в этом же слоте
            
            # Спим 1 минуту перед следующей проверкой
            time.sleep(60)
            
        except Exception as e:
            print(f"Global scheduler error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    run_scheduler()
