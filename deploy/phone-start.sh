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
  sleep 2
fi

export MKV_RENDER_URL MKV_SYNC_KEY MKV_MONGODB_URI MKV_DATA_DIR MKV_DISCOVERY

# If someone ran `bash deploy/phone-start.sh` WITHOUT xvfb-run, wrap ourselves.
# Tiny default Xvfb (640x480) breaks Turnstile — force 1280x720.
_XVFB_OPTS="${MKV_XVFB_OPTS:--screen 0 1280x720x24}"
if [ -z "${DISPLAY:-}" ] && command -v xvfb-run >/dev/null 2>&1; then
  echo "[phone] re-exec under xvfb-run ${_XVFB_OPTS}"
  exec xvfb-run -a -s "$_XVFB_OPTS" bash "$0" "$@"
fi

# Headed under Xvfb clears Turnstile; headless almost never does on proot.
if [ -n "${DISPLAY:-}" ] && [ "${MKV_FORCE_HEADLESS:-}" != "1" ]; then
  export MKV_HEADLESS=false
else
  export MKV_HEADLESS="${MKV_HEADLESS:-true}"
fi
export MKV_LEAN_BROWSER=false
# geoip MaxMind lookup often stalls page load on Termux/proot — keep off
export MKV_GEOIP="${MKV_GEOIP:-false}"
export MKV_CLEAR_ATTEMPTS="${MKV_CLEAR_ATTEMPTS:-4}"
export MKV_CLEAR_ATTEMPT_S="${MKV_CLEAR_ATTEMPT_S:-100}"
export MKV_BOOTSTRAP_TIMEOUT="${MKV_BOOTSTRAP_TIMEOUT:-100}"

export MOZ_DISABLE_CONTENT_SANDBOX=1
export MOZ_DISABLE_RDD_SANDBOX=1
export MOZ_DISABLE_SOCKET_PROCESS_SANDBOX=1
export MOZ_DISABLE_GMP_SANDBOX=1
mkdir -p "${MKV_DATA_DIR:-$HOME/mkvdata}"
echo "[phone] MKV_HEADLESS=$MKV_HEADLESS DISPLAY=${DISPLAY:-none} GEOIP=$MKV_GEOIP"

command -v termux-wake-lock >/dev/null 2>&1 && termux-wake-lock || true

while true; do
  echo "[phone] pusher starting $(date)"
  .venv/bin/python -m app.pusher --terms "godzilla,interstellar" --discover
  echo "[phone] pusher exited, restarting in 10s"
  sleep 10
done
