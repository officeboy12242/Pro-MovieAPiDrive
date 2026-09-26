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

if [ -z "${MKV_MONGODB_URI:-}" ] || [ "${MKV_MONGODB_URI:-}" = "PASTE_MONGODB_URI_HERE" ]; then
  echo "Edit ~/mkv.env first:  nano ~/mkv.env  (paste MKV_MONGODB_URI - the crawler needs it)"
  exit 1
fi
if [ "${MKV_SYNC_KEY:-}" = "PASTE_SYNC_KEY_HERE" ] || [ -z "${MKV_SYNC_KEY:-}" ]; then
  echo "NOTE: no MKV_SYNC_KEY - crawler still fills the vault via Mongo, but"
  echo "      pushes to Render /sync will fail (401). Paste the key later for"
  echo "      Render-side recent/search serving."
  sleep 3
fi

export MKV_RENDER_URL MKV_SYNC_KEY MKV_MONGODB_URI MKV_DATA_DIR MKV_DISCOVERY
# proot has no display: headless is mandatory on the phone (overridable in ~/mkv.env)
export MKV_HEADLESS="${MKV_HEADLESS:-true}"
mkdir -p "${MKV_DATA_DIR:-$HOME/mkvdata}"

# wake lock if Termux provides it (inside proot it usually does not; harmless)
command -v termux-wake-lock >/dev/null 2>&1 && termux-wake-lock || true

while true; do
  echo "[phone] pusher starting $(date)"
  .venv/bin/python -m app.pusher --terms "godzilla,interstellar" --discover
  echo "[phone] pusher exited, restarting in 10s"
  sleep 10
done
