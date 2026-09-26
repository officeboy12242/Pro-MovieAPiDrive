#!/usr/bin/env bash
# mkvbase phone setup - run ONCE, INSIDE proot Ubuntu on Android:
#   bash deploy/phone-setup.sh
# Installs browser runtime + python deps + Camoufox, and creates ~/mkv.env
# where you paste your keys. Then: bash deploy/phone-start.sh
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> system deps (Firefox runtime + venv tooling)"
apt-get update -y
apt-get install -y python3-venv python3-pip git \
  libgtk-3-0 libdbus-glib-1-2 libxt6 libx11-xcb1 libxcomposite1 \
  libxdamage1 libxrandr2 libasound2 libxkbcommon0 libpango-1.0-0 \
  libcairo2 libgdk-pixbuf-2.0-0 fonts-liberation \
  libgbm1 libxext6 libxfixes3 libxcb-shm0 libxcb1 libxss1 \
  libatk1.0-0 libatk-bridge2.0-0 libcups2 || true
# noble (24.04) t64 transition names where the plain ones do not exist
apt-get install -y libatk1.0-0t64 libatk-bridge2.0-0t64 libcups2t64 || true

echo "==> python venv + deps"
python3 -m venv .venv
.venv/bin/pip install --upgrade pip -q
.venv/bin/pip install -r requirements.txt -q

echo "==> fetching Camoufox browser (arm64) - one time, ~200MB"
# marker file: re-running setup must never re-download ~200MB on mobile data
MARKER="$HOME/.mkv_camoufox_ok"
if [ -f "$MARKER" ]; then
  echo "    already fetched (marker found) - skipping"
else
  .venv/bin/python -m camoufox fetch && touch "$MARKER"
fi

echo "==> keys file ~/mkv.env"
if [ ! -f "$HOME/mkv.env" ]; then
  cat > "$HOME/mkv.env" <<EOF
MKV_RENDER_URL=https://pro-movieapidrive.onrender.com
MKV_SYNC_KEY=PASTE_SYNC_KEY_HERE
MKV_MONGODB_URI=PASTE_MONGODB_URI_HERE
MKV_DATA_DIR=$HOME/mkvdata
MKV_DISCOVERY=1
MKV_HEADLESS=true
EOF
  echo "created ~/mkv.env - now:  nano ~/mkv.env  (paste both keys)"
fi

mkdir -p "${MKV_DATA_DIR:-$HOME/mkvdata}"
echo
echo "DONE. Next steps:"
echo "  1) nano ~/mkv.env      <- paste MKV_SYNC_KEY and MKV_MONGODB_URI"
echo "  2) bash deploy/phone-start.sh"
