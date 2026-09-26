#!/usr/bin/env bash
# mkvbase phone setup - run ONCE, INSIDE proot Ubuntu on Android:
#   bash deploy/phone-setup.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> system deps (Chromium for CF clear + Camoufox libs + xvfb)"
apt-get update -y
apt-get install -y python3-venv python3-pip git xvfb \
  libgtk-3-0 libdbus-glib-1-2 libxt6 libx11-xcb1 libxcomposite1 \
  libxdamage1 libxrandr2 libasound2 libxkbcommon0 libpango-1.0-0 \
  libcairo2 libgdk-pixbuf-2.0-0 fonts-liberation \
  libgbm1 libxext6 libxfixes3 libxcb-shm0 libxcb1 libxss1 \
  libatk1.0-0 libatk-bridge2.0-0 libcups2 libnss3 libnspr4 \
  libx11-6 libxcb-dri3-0 libdrm2 libxshmfence1 || true
apt-get install -y libatk1.0-0t64 libatk-bridge2.0-0t64 libcups2t64 || true

echo "==> Chromium (needed — Camoufox cannot clear mkvbase CF on Termux)"
apt-get install -y chromium-browser || apt-get install -y chromium || true
if command -v chromium >/dev/null 2>&1; then
  echo "    chromium: $(command -v chromium)"
elif command -v chromium-browser >/dev/null 2>&1; then
  echo "    chromium-browser: $(command -v chromium-browser)"
else
  echo "WARNING: no chromium binary found — CF clear will fail"
fi

echo "==> python venv + deps"
python3 -m venv .venv
.venv/bin/pip install --upgrade pip -q
.venv/bin/pip install -r requirements.txt -q
.venv/bin/pip install -q nodriver

echo "==> fetching Camoufox browser (optional fallback, ~200MB)"
MARKER="$HOME/.mkv_camoufox_ok"
if [ -f "$MARKER" ]; then
  echo "    already fetched (marker found) - skipping"
else
  .venv/bin/python -m camoufox fetch && touch "$MARKER" || true
fi

echo "==> keys file ~/mkv.env"
if [ ! -f "$HOME/mkv.env" ]; then
  cat > "$HOME/mkv.env" <<EOF
MKV_RENDER_URL=https://pro-movieapidrive.onrender.com
MKV_SYNC_KEY=PASTE_SYNC_KEY_HERE
MKV_MONGODB_URI=PASTE_MONGODB_URI_HERE
MKV_DATA_DIR=$HOME/mkvdata
MKV_DISCOVERY=1
MKV_HEADLESS=false
MKV_CLEAR_ENGINE=nodriver
MKV_GEOIP=false
EOF
  echo "created ~/mkv.env - now:  nano ~/mkv.env  (paste Mongo URI + sync key)"
fi

mkdir -p "${MKV_DATA_DIR:-$HOME/mkvdata}"
echo
echo "DONE. Next:"
echo "  1) nano ~/mkv.env"
echo "  2) xvfb-run -a -s \"-screen 0 1280x720x24\" .venv/bin/python -m app.cf_clear_worker --timeout 120"
echo "  3) bash deploy/phone-start.sh"
