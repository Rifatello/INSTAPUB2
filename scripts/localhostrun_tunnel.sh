#!/bin/bash
set -euo pipefail

URL_FILE="${VIDEO_PUBLIC_BASE_URL_FILE:-$HOME/my_videos/public_base_url.txt}"
mkdir -p "$(dirname "$URL_FILE")"

SSH_CMD=(
  /usr/bin/ssh
  -o StrictHostKeyChecking=no
  -o ServerAliveInterval=30
  -R 80:localhost:8080
  nokey@localhost.run
)

while true; do
  "${SSH_CMD[@]}" 2>&1 | while IFS= read -r line; do
    echo "$line"

    if [[ "$line" == *"tunneled with tls termination"* ]] && [[ "$line" =~ (https://[A-Za-z0-9.-]+) ]]; then
      tunnel_url="${BASH_REMATCH[1]}"
      printf '%s\n' "$tunnel_url" > "${URL_FILE}.tmp"
      mv "${URL_FILE}.tmp" "$URL_FILE"
      echo "[tunnel] public url updated: $tunnel_url"
    fi
  done

  echo "[tunnel] ssh session ended, reconnecting in 2s"
  sleep 2
done
