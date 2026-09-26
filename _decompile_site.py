"""Fetch mkvbase.site's frontend bundles and map every backend endpoint they use.

Step 1: homepage -> script list + buildId
Step 2: pull each JS bundle, extract /api/* calls, routes, data-shape hints
Step 3: probe interesting endpoints with the cleared session (GET only)
"""
from __future__ import annotations

import json
import os
import re
import sys

from curl_cffi import requests as cffi

sess = json.load(open("data/session.json"))
S = {"cookies": sess["cookies"],
     "headers": {"User-Agent": sess["user_agent"], "Referer": "https://mkvbase.site/"},
     "timeout": 25, "impersonate": "firefox133"}
BASE = "https://mkvbase.site"
OUT = "data/site_bundles"
os.makedirs(OUT, exist_ok=True)

home = cffi.get(BASE + "/", **S).text
print("homepage bytes:", len(home))

# buildId + script URLs
m = re.search(r'"buildId"\s*:\s*"([^"]+)"', home)
print("buildId:", m.group(1) if m else "(not found)")
scripts = sorted(set(re.findall(r'src="(/_next/static/[^"]+\.js)"', home)))
print(f"scripts referenced from homepage: {len(scripts)}")

# also the _buildManifest / _ssgManifest reveal every route in the app
extra = re.findall(r'static/(chunks|css)/[^"\\]+', home)

saved = []
for path in scripts:
    url = BASE + path
    try:
        r = cffi.get(url, **S)
        name = path.rsplit("/", 1)[-1]
        open(os.path.join(OUT, name), "wb").write(r.content)
        saved.append((name, len(r.content)))
    except Exception as e:
        print("ERR", path, e)
print("saved bundles:", len(saved), "total KB:", sum(n for _, n in saved) // 1024)

# extract endpoint hints from every bundle
api_hits: dict[str, set] = {}
for name, _ in saved:
    txt = open(os.path.join(OUT, name), encoding="utf-8", errors="ignore").read()
    for pat in (r'"/api/[^"{\s]+', r"`/api/[^`{]+", r"'\/api\/[^'{]+'"):
        for hit in re.findall(pat, txt):
            api_hits.setdefault(hit.rstrip("/"), set()).add(name)

print("\n== /api/* endpoints referenced in client bundles ==")
for ep, files in sorted(api_hits.items()):
    print(f"  {ep:50s}  in {sorted(files)[:2]}")

# route manifest hints (Next.js page list) from any buildManifest chunk
print("\n== page routes (from buildManifest-ish chunks) ==")
routes = set()
for name, _ in saved:
    txt = open(os.path.join(OUT, name), encoding="utf-8", errors="ignore").read()
    if "_buildManifest" in txt or "buildManifest" in txt:
        routes |= set(re.findall(r'"/((?:[a-z0-9-]+/)*[a-z0-9-\[\]]*)":\s*\[', txt))
print(sorted(r for r in routes if r)[:40] or "(no manifest chunk on homepage — try a page)")
