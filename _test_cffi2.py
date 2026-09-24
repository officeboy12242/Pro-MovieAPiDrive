"""Test v3: which curl_cffi impersonation passes CF with captured cookies?
Each variant gets ONE request against bare /api/links. First 200 wins.
"""
import sys
import time

sys.path.insert(0, ".")
from app.engines.camoufox_engine import CamoufoxEngine
from curl_cffi import requests as cffi

BASE = "https://mkvbase.site"

eng = CamoufoxEngine(headless=True, humanize=True)
sess = eng.get_session(f"{BASE}/api/links", timeout_s=120)
print("bootstrap:", round(time.time() - 0, 0), "s | cf_clearance:", "cf_clearance" in sess.cookies,
      "| UA:", (sess.user_agent or "")[:60])
eng.close()

VARIANTS = [
    ("firefox133", {"impersonate": "firefox133"}),
    ("firefox", {"impersonate": "firefox"}),
    ("chrome131", {"impersonate": "chrome131"}),
    ("chrome124", {"impersonate": "chrome124"}),
    ("safari17_0", {"impersonate": "safari17_0"}),
    ("edge101", {"impersonate": "edge101"}),
    ("no-impersonation", {}),
]

winner = None
for name, kwargs in VARIANTS:
    try:
        r = cffi.get(f"{BASE}/api/links", headers={
            "User-Agent": sess.user_agent or "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "*/*",
            "Referer": f"{BASE}/",
        }, cookies=sess.cookies, timeout=30, **kwargs)
        body = r.text[:120].replace("\n", " ")
        verdict = "200 OK <<< WINNER" if r.status_code == 200 else f"{r.status_code}"
        print(f"[{name:16s}] {verdict} | {body[:70]!r}")
        if r.status_code == 200:
            winner = name
            break
    except Exception as e:
        print(f"[{name:16s}] EXC {type(e).__name__}: {str(e)[:80]}")
    time.sleep(2)

print("WINNER:", winner)
sys.exit(0 if winner else 1)
