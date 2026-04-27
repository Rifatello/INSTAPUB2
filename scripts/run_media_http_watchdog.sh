#!/usr/bin/env bash
set -u
mkdir -p "$HOME/my_videos"
cd "$HOME/my_videos"
while true; do
  python3 -m http.server 8080 --bind 127.0.0.1 >> "$HOME/my_videos/media_http.log" 2>&1
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] media_http exited, restarting in 2s" >> "$HOME/my_videos/media_http.log"
  sleep 2
done
