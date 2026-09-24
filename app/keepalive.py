"""Keep-alive: pings /health periodically so Render doesn't sleep the service.

Enable with MKV_KEEPALIVE=true (minutes via MKV_KEEPALIVE_MIN, default 10).
On Render free the service sleeps after 15 min without *inbound* traffic. Only
requests through Render's edge count, so this pings the public URL
(RENDER_EXTERNAL_URL, set by Render automatically). Pinging 127.0.0.1 would
never reach the edge and the instance would sleep anyway.
"""
from __future__ import annotations

import os
import threading
import time
import urllib.request


def start_keepalive() -> None:
    if os.getenv("MKV_KEEPALIVE", "false").lower() not in ("1", "true", "yes"):
        return
    base = (os.getenv("MKV_KEEPALIVE_URL") or os.getenv("RENDER_EXTERNAL_URL")
            or f"http://127.0.0.1:{os.getenv('PORT', '8765')}").rstrip("/")
    interval_min = float(os.getenv("MKV_KEEPALIVE_MIN", "10"))

    def _loop():
        url = f"{base}/health"
        while True:
            time.sleep(interval_min * 60)
            try:
                urllib.request.urlopen(url, timeout=15).read()
            except Exception:
                pass  # best-effort; service sleeping is not fatal

    threading.Thread(target=_loop, daemon=True, name="keepalive").start()
