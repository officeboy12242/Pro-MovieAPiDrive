#!/usr/bin/env bash
# mkvbase phone start - run INSIDE proot Ubuntu on Android:
#   bash deploy/phone-start.sh
# Loads ~/mkv.env, takes a wake lock if available, runs pusher + crawler.
# Auto-restarts if it ever crashes; stop with Ctrl+C (wake lock releases on exit).
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -f "$HOME/mkv.env" ]; then
  # shellcheck disable=SC1091
  . "$HOME/mkv.env"
fi

if [ "${MKV_SYNC_KEY:-}" = "PASTE_SYNC_KEY_HERE" ] || [ -z "${MKV_SYNC_KEY:-}" ]; then
  echo "Edit ~/mkv.env first:  nano ~/mkv.env  (paste MKV_SYNC_KEY)"
  exit 1
fi

export MKV_RENDER_URL MKV_SYNC_KEY MKV_MONGODB_URI MKV_DATA_DIR MKV_DISCOVERY
mkdir -p "${MKV_DATA_DIR:-$HOME/mkvdata}"

# wake lock if Termux provides it (inside proot it usually does not; harmless)
command -v termux-wake-lock >/dev/null 2>&1 && termux-wake-lock || true

while true; do
  echo "[phone] pusher starting $(date)"
  .venv/bin/python -m app.pusher --terms "godzilla,interstellar" --discover
  echo "[phone] pusher exited, restarting in 10s"
  sleep 10
done
