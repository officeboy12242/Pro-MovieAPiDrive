"""Deep pass: context around each /api call in the bundles + probe new endpoints.

GET-only, uses the cleared session.
"""
from __future__ import annotations

import glob
import json
import os
import re

from curl_cffi import requests as cffi

sess = json.load(open("data/session.json"))
S = {"cookies": sess["cookies"],
     "headers": {"User-Agent": sess["user_agent"], "Referer": "https://mkvbase.site/"},
     "timeout": 25, "impersonate": "firefox133"}
BASE = "https://mkvbase.site"
OUT = "data/site_bundles"

# ---------- 1. context around each api reference ----------
print("=" * 30, "API call context", "=" * 30)
for path in sorted(glob.glob(os.path.join(OUT, "*.js"))):
    txt = open(path, encoding="utf-8", errors="ignore").read()
    for m in re.finditer(r"/api/(?:links|trending|broadcasts|banner-ad)", txt):
        a, b = max(0, m.start() - 220), min(len(txt), m.end() + 260)
        ctx = txt[a:b].replace("\n", " ")
        print(f"\n--- {os.path.basename(path)} @ {m.start()} ---")
        print(ctx[:480])

# ---------- 2. other backend hints ----------
print("\n" + "=" * 30, "backend hints", "=" * 30)
hints = {}
for path in sorted(glob.glob(os.path.join(OUT, "*.js"))):
    txt = open(path, encoding="utf-8", errors="ignore").read()
    for pat, tag in ((r"https?://[a-z0-9.-]+\.(?:supabase\.co|firebaseio\.com|amazonaws\.com|r2\.dev|workers\.dev|vercel\.app|pages\.dev)", "cloud"),
                     (r"(?:supabase|firebase|firestore|mongodb|postgres|mysql|prisma|graphql)", "db-word"),
                     (r"fetch\(\s*[\"'`]([^\"'`]+)[\"'`]", "fetch-literal"),
                     (r"\.get\(\s*[\"'`]([^\"'`]+)[\"'`]", "get-literal")):
        for h in re.findall(pat, txt, re.I):
            hints.setdefault(h, set()).add(os.path.basename(path))
for h, f in sorted(hints.items()):
    print(f"  {h[:80]:82s} {sorted(f)[:1]}")

# ---------- 3. probe the new endpoints ----------
print("\n" + "=" * 30, "probes", "=" * 30)


def probe(label: str, url: str) -> None:
    try:
        r = cffi.get(url, **S)
        t = r.text or ""
        body = t[:300].replace("\n", " ")
        kind = "JSON"
        try:
            obj = json.loads(t[t.find("{"):t.rfind("}") + 1])
            keys = list(obj.keys())[:8]
            n = len(obj.get("results") or obj.get("data") or obj.get("broadcasts") or [])
            kind = f"JSON keys={keys} rows~{n}"
            body = json.dumps({k: (v if not isinstance(v, (list, dict)) else f"<{type(v).__name__} len={len(v)}>")
                               for k, v in obj.items()})[:260]
        except Exception:
            pass
        print(f"{label:34s} HTTP {r.status_code}  {kind}\n    {body}")
    except Exception as e:
        print(f"{label:34s} ERR {type(e).__name__}: {e}")


probe("/api/trending", f"{BASE}/api/trending")
probe("/api/broadcasts?page=1", f"{BASE}/api/broadcasts?page=1")
probe("/api/broadcasts?page=2", f"{BASE}/api/broadcasts?page=2")
probe("/api/broadcasts?page=50", f"{BASE}/api/broadcasts?page=50")
probe("/api/banner-ad", f"{BASE}/api/banner-ad")
