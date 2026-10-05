#!/usr/bin/env bash
# mkvbase CRAWLER for Oracle Always Free VM - 24/7 harvesting, no PC/phone needed.
#
# Run ON the VM (Ubuntu 22.04/24.04, arm64 or x86_64) after this repo is on it:
#   git clone https://github.com/officeboy12242/Pro-MovieAPiDrive.git ~/mkv
#   cd ~/mkv && bash deploy/oracle-crawler.sh
#
# Then paste your keys once:
#   sudo nano /etc/mkvbase.env
# and restart:
#   sudo systemctl restart mkvbase-crawler
# Watch live:
#   journalctl -u mkvbase-crawler -f
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

if [ "$(id -u)" -eq 0 ]; then
  echo "Run as the normal ubuntu user (sudo is used internally where needed)."
  exit 1
fi

echo "==> system deps (Firefox runtime for Camoufox)"
sudo apt-get update -y
sudo apt-get install -y python3-venv python3-pip git \
  libgtk-3-0 libdbus-glib-1-2 libxt6 libx11-xcb1 libxcomposite1 \
  libxdamage1 libxrandr2 libasound2t64 libxkbcommon0 libpango-1.0-0 \
  libcairo2 libgdk-pixbuf-2.0-0 fonts-liberation || \
sudo apt-get install -y libasound2

echo "==> python venv + deps"
python3 -m venv .venv
.venv/bin/pip install --upgrade pip -q
.venv/bin/pip install -r requirements.txt -q

echo "==> Camoufox browser (one time)"
if [ -f "$HOME/.mkv_camoufox_ok" ]; then
  echo "    already fetched - skipping"
else
  .venv/bin/python -m camoufox fetch && touch "$HOME/.mkv_camoufox_ok"
fi

echo "==> keys file /etc/mkvbase.env"
if [ ! -f /etc/mkvbase.env ]; then
  sudo tee /etc/mkvbase.env >/dev/null <<'EOF'
MKV_RENDER_URL=https://pro-movieapidrive.onrender.com
MKV_SYNC_KEY=PASTE_SYNC_KEY_HERE
MKV_MONGODB_URI=PASTE_MONGODB_URI_HERE
MKV_DATA_DIR=/var/lib/mkvbase
MKV_DISCOVERY=1
MKV_HEADLESS=true
EOF
  sudo mkdir -p /var/lib/mkvbase
  sudo chown "$USER" /var/lib/mkvbase
  echo "    created - now:  sudo nano /etc/mkvbase.env"
fi

echo "==> systemd service (auto-start at boot, auto-restart on crash)"
sudo tee /etc/systemd/system/mkvbase-crawler.service >/dev/null <<EOF
[Unit]
Description=mkvbase crawler (pusher + discovery -> Mongo vault)
After=network-online.target
Wants=network-online.target

[Service]
User=$USER
WorkingDirectory=$ROOT
EnvironmentFile=/etc/mkvbase.env
ExecStart=$ROOT/.venv/bin/python -m app.pusher --terms godzilla,interstellar,predestination,oppenheimer --discover
Restart=always
RestartSec=15
MemoryMax=3G

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now mkvbase-crawler

echo
echo "INSTALLED. Next:"
echo "  1) sudo nano /etc/mkvbase.env   <- paste MKV_SYNC_KEY and MKV_MONGODB_URI"
echo "  2) sudo systemctl restart mkvbase-crawler"
echo "  3) journalctl -u mkvbase-crawler -f    (Ctrl+C to stop watching)"
