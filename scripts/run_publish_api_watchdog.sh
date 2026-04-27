#!/usr/bin/env bash
set -u
cd "$(dirname "$0")/.."
export PATH="/Users/rifat_server/Library/Python/3.9/bin:${PATH:-}"
export PYTHONPATH="/Users/rifat_server/Library/Python/3.9/lib/python/site-packages:${PYTHONPATH:-}"
mkdir -p logs
while true; do
  python3 -m uvicorn app.main:app --host 127.0.0.1 --port 8010 >> logs/publish_api.log 2>&1
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] publish_api exited, restarting in 2s" >> logs/publish_api.log
  sleep 2
done
