#!/usr/bin/env bash
# mkvbase phone setup - run ONCE, INSIDE proot Ubuntu on Android:
#   bash deploy/phone-setup.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> system deps (Xvfb + libs for Chromium)"
apt-get update -y
apt-get install -y python3-venv python3-pip git xvfb \
  libgtk-3-0 libdbus-glib-1-2 libxt6 libx11-xcb1 libxcomposite1 \
  libxdamage1 libxrandr2 libasound2t64 libasound2 libxkbcommon0 libpango-1.0-0 \
  libcairo2 libgdk-pixbuf-2.0-0 fonts-liberation \
  libgbm1 libxext6 libxfixes3 libxcb-shm0 libxcb1 libxss1 \
  libatk1.0-0 libatk-bridge2.0-0 libcups2 libnss3 libnspr4 \
  libx11-6 libxcb-dri3-0 libdrm2 libxshmfence1 libxrandr2 \
  ca-certificates wget || true
apt-get install -y libatk1.0-0t64 libatk-bridge2.0-0t64 libcups2t64 libasound2t64 || true

echo "==> python venv + deps"
python3 -m venv .venv
.venv/bin/pip install --upgrade pip -q
.venv/bin/pip install -r requirements.txt -q
.venv/bin/pip install -q nodriver playwright

echo "==> Playwright Chromium (bundled binary — works in proot, no snap)"
# arm64/amd64 Linux Chromium from Microsoft's CDN
if .venv/bin/python -c "from app.cf_clear_worker import _find_chromium; import sys; sys.exit(0 if _find_chromium() else 1)"; then
  echo "    Chromium already present: $(.venv/bin/python -c 'from app.cf_clear_worker import _find_chromium; print(_find_chromium())')"
else
  .venv/bin/playwright install --with-deps chromium || .venv/bin/playwright install chromium
fi

CHROME="$(.venv/bin/python -c 'from app.cf_clear_worker import _find_chromium; print(_find_chromium() or "")')"
if [ -z "$CHROME" ]; then
  echo "ERROR: Chromium still not found after playwright install"
  echo "  Try: .venv/bin/playwright install chromium"
  exit 1
fi
echo "    OK chrome=$CHROME"

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
MKV_CHROME_PATH=$CHROME
EOF
  echo "created ~/mkv.env — paste Mongo URI (+ sync key)"
else
  # ensure clear-engine + chrome path are set
  grep -q '^MKV_CLEAR_ENGINE=' "$HOME/mkv.env" || echo "MKV_CLEAR_ENGINE=nodriver" >> "$HOME/mkv.env"
  grep -q '^MKV_CHROME_PATH=' "$HOME/mkv.env" || echo "MKV_CHROME_PATH=$CHROME" >> "$HOME/mkv.env"
fi

mkdir -p "${MKV_DATA_DIR:-$HOME/mkvdata}"
echo
echo "DONE. Test CF clear:"
echo "  xvfb-run -a -s \"-screen 0 1280x720x24\" \\"
echo "    env MKV_HEADLESS=false MKV_CLEAR_ENGINE=nodriver MKV_CHROME_PATH=$CHROME \\"
echo "    .venv/bin/python -m app.cf_clear_worker --timeout 120"
echo "Then: bash deploy/phone-start.sh"
