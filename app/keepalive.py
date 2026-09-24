"""Keep-alive: pings /health periodically so Render doesn't sleep the service.

Enable with MKV_KEEPALIVE=true (minutes via MKV_KEEPALIVE_MIN, default 10).
On Render free/Starter the service sleeps after 15 min idle; self-pinging
/health counts as traffic, so the instance (and our warm Camoufox session)
stays alive.
"""
from __future__ import annotations

import os
import threading
import time
import urllib.request


def start_keepalive() -> None:
    if os.getenv("MKV_KEEPALIVE", "false").lower() not in ("1", "true", "yes"):
        return
    port = os.getenv("PORT", "8765")
    interval_min = float(os.getenv("MKV_KEEPALIVE_MIN", "10"))

    def _loop():
        url = f"http://127.0.0.1:{port}/health"
        while True:
            time.sleep(interval_min * 60)
            try:
                urllib.request.urlopen(url, timeout=15).read()
            except Exception:
                pass  # best-effort; service sleeping is not fatal

    threading.Thread(target=_loop, daemon=True, name="keepalive").start()
