#!/usr/bin/env bash
# mkvbase CRAWLER (borrower mode) for a 1GB VM — e.g. Oracle E2 Micro free tier.
#
# A 1GB box cannot clear Cloudflare itself (Firefox wants ~1.5-2GB; it wedges
# at new_page and is OOM-killed — measured on 512MB, same fate on 1GB). So this
# host NEVER launches a browser: it borrows the cleared session another host
# (your PC fleet, or a phone) publishes to Mongo, and mines over plain HTTP
# (~1s/search, ~100-200MB RAM). Keep at least one browser-capable publisher
# running until this host proves itself in its logs.
#
# Run ON the VM (Ubuntu 22.04/24.04, x86_64) after this repo is on it:
#   git clone https://github.com/officeboy12242/Pro-MovieAPiDrive.git ~/mkv
#   cd ~/mkv && bash deploy/oracle-micro.sh
#
# Then paste your keys once:
#   sudo nano /etc/mkvbase.env
# and restart:
#   sudo systemctl restart mkvbase-crawler
# Watch live:
#   journalctl -u mkvbase-crawler -f
#
# Healthy signs: "READY" via shared session, searches logging (1-3s), new rows
# landing. Bad signs: repeated NeedsSession/403 storms — cf_clearance is
# IP-bound, so a datacenter IP may not be able to reuse a home-IP clearance.
# Fallbacks in order: (1) phone-Termux publisher (residential IP clears easiest,
#   see README "Android phone as the pusher"), (2) 2GB swap + browser attempt on
#   this box (fragile), (3) Ampere A1 free shape when Oracle has capacity.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"

if [ "$(id -u)" -eq 0 ]; then
  echo "Run as the normal ubuntu user (sudo is used internally where needed)."
  exit 1
fi

echo "==> 2GB swap (1GB RAM needs breathing room for apt/pip spikes)"
if ! swapon --show=NAME --noheadings | grep -q .; then
  sudo fallocate -l 2G /swapfile || sudo dd if=/dev/zero of=/swapfile bs=1M count=2048
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile >/dev/null
  sudo swapon /swapfile
  grep -q "/swapfile" /etc/fstab || echo "/swapfile none swap sw 0 0" | sudo tee -a /etc/fstab >/dev/null
  echo "    2GB swap on"
else
  echo "    swap already present - skipping"
fi

echo "==> system deps (no browser: borrower mode mines over plain HTTP)"
sudo apt-get update -y
sudo apt-get install -y python3-venv python3-pip git

echo "==> python venv + deps"
python3 -m venv .venv
.venv/bin/pip install --upgrade pip -q
.venv/bin/pip install -r requirements.txt -q

echo "==> keys file /etc/mkvbase.env"
if [ ! -f /etc/mkvbase.env ]; then
  sudo tee /etc/mkvbase.env >/dev/null <<'EOF'
MKV_RENDER_URL=https://pro-movieapidrive.onrender.com
MKV_SYNC_KEY=PASTE_SYNC_KEY_HERE
MKV_MONGODB_URI=PASTE_MONGODB_URI_HERE
MKV_DATA_DIR=/var/lib/mkvbase
MKV_DISCOVERY=1
MKV_HEADLESS=true
# Borrower tuning for 1GB: keep the safe fleet shape (concurrency 2, gap 0.4s).
# Do NOT raise MKV_HTTP_CONCURRENCY here: 6-way concurrency was measured live
# to churn the session (p90 1.4s -> 96s). Throughput comes from yield, not slots.
MKV_HTTP_CONCURRENCY=2
MKV_HTTP_GAP_S=0.4
MKV_IDGAP_AGENTS=4
MKV_IDGAP_GAP_S=4
EOF
  sudo mkdir -p /var/lib/mkvbase
  sudo chown "$USER" /var/lib/mkvbase
  echo "    created - now:  sudo nano /etc/mkvbase.env"
fi

echo "==> systemd service (auto-start at boot, auto-restart on crash)"
sudo tee /etc/systemd/system/mkvbase-crawler.service >/dev/null <<EOF
[Unit]
Description=mkvbase crawler, borrower mode (no browser, shared Mongo session)
After=network-online.target
Wants=network-online.target

[Service]
User=$USER
WorkingDirectory=$ROOT
EnvironmentFile=/etc/mkvbase.env
ExecStart=$ROOT/.venv/bin/python -m app.pusher --terms godzilla,interstellar,predestination,oppenheimer --discover
Restart=always
RestartSec=15
MemoryMax=800M

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable --now mkvbase-crawler

echo
echo "INSTALLED (borrower mode). Next:"
echo "  1) sudo nano /etc/mkvbase.env   <- paste MKV_SYNC_KEY and MKV_MONGODB_URI"
echo "  2) sudo systemctl restart mkvbase-crawler"
echo "  3) journalctl -u mkvbase-crawler -f    (Ctrl+C to stop watching)"
echo "  Keep your PC fleet running as the session publisher until this box shows"
echo "  READY + steady searches. If you see NeedsSession/403 storms, the"
echo "  datacenter IP cannot reuse the clearance - use a phone publisher instead."
