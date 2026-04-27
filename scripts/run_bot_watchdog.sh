#!/usr/bin/env bash
set -u
cd "$(dirname "$0")/.."
export PATH="/Users/rifat_server/Library/Python/3.9/bin:${PATH:-}"
export PYTHONPATH="/Users/rifat_server/Library/Python/3.9/lib/python/site-packages:${PYTHONPATH:-}"
mkdir -p logs
while true; do
  python3 -u bot/telegram_bot.py >> logs/telegram_bot.log 2>&1
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] bot exited, restarting in 2s" >> logs/telegram_bot.log
  sleep 2
done
