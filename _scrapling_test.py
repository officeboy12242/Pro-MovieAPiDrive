"""Scrapling side-test vs mkvbase Cloudflare.

Phase 1 (run with .venv-scrap): StealthyFetcher (headless Camoufox-class
browser) opens https://mkvbase.site/ , waits out the Cloudflare challenge,
saves the cleared cookies to data/scrapling/cookies.json and fetches
/api/links to prove JSON access.

Phase 2 (run with the MAIN .venv): load those cookies into the project's
plain-HTTP client and try a signed search; vault whatever comes back.

Usage:
  .venv-scrap/Scripts/python _scrapling_test.py clear
  .venv/Scripts/python     _scrapling_test.py reuse
"""
from __future__ import annotations

import json
import os
import sys
import time

BASE = "https://mkvbase.site"
OUT_DIR = os.path.join("data", "scrapling")


def phase_clear() -> int:
    from scrapling.fetchers import StealthyFetcher
    os.makedirs(OUT_DIR, exist_ok=True)
    t0 = time.time()
    print("[scrapling] opening mkvbase.site (headless, solving CF)…", flush=True)
    page = StealthyFetcher.fetch(
        BASE + "/",
        headless=True,
        solve_cloudflare=True,
        timeout=120_000,
    )
    took = time.time() - t0
    print(f"[scrapling] page loaded in {took:.0f}s | status={page.status}", flush=True)

    # cookies from the cleared session (list of dicts on this version)
    cookies = {}
    raw = None
    for attr in ("cookies", "cookie_jar"):
        raw = getattr(page, attr, None)
        if raw:
            break
    try:
        for c in (raw or []):
            if isinstance(c, dict):
                if c.get("name"):
                    cookies[c["name"]] = c.get("value")
            elif hasattr(c, "name"):
                cookies[c.name] = c.value
    except Exception as e:
        print("cookie extraction failed:", e, flush=True)
    with open(os.path.join(OUT_DIR, "cookies.json"), "w") as f:
        json.dump({"cookies": cookies, "ts": time.time(),
                   "user_agent": getattr(page, "user_agent", None)}, f, indent=1)
    mkv_ok = all(k in cookies for k in ("mkv_client_key", "mkv_challenge", "mkv_seq"))
    print(f"[scrapling] cookies: {len(cookies)} | mkv_* set: {mkv_ok}", flush=True)

    # try the JSON API in the same cleared session
    api = StealthyFetcher.fetch(
        BASE + "/api/links",
        headless=True,
        solve_cloudflare=True,
        timeout=60_000,
    )
    body = api.body if isinstance(api.body, str) else api.body.decode("utf-8", "replace")
    ok = '"results"' in body
    with open(os.path.join(OUT_DIR, "links.json"), "w", encoding="utf-8") as f:
        f.write(body)
    print(f"[scrapling] /api/links status={api.status} JSON results: {ok}", flush=True)
    print(f"[scrapling] VERDICT phase1: {'PASS' if (mkv_ok or ok) else 'FAIL'} "
          f"({took:.0f}s clear)", flush=True)
    return 0 if (mkv_ok or ok) else 1


def phase_reuse() -> int:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from app.client import MkvbaseClient, NeedsSession  # noqa: E402
    from app.store import make_index  # noqa: E402

    with open(os.path.join(OUT_DIR, "cookies.json"), encoding="utf-8") as f:
        d = json.load(f)
    cookies, ua = d.get("cookies") or {}, d.get("user_agent")
    need = ("mkv_client_key", "mkv_challenge", "mkv_seq")
    have = all(k in cookies for k in need)
    print(f"[reuse] cookies loaded: {len(cookies)} | mkv_* complete: {have}", flush=True)
    if not have:
        print("[reuse] missing mkv_* cookies - run phase 1 first", flush=True)
        return 1

    cli = MkvbaseClient(None)  # browser-free client
    # inject the scrapling-cleared cookies as the live session
    from app.client import Session as _S  # type: ignore
    try:
        s = _S(cookies=cookies, user_agent=ua or "Mozilla/5.0")
    except Exception:
        # Session has a different signature; fall back to attribute stuffing
        cli._session = type("S", (), {"cookies": cookies,
                                      "user_agent": ua or "Mozilla/5.0"})()
    else:
        with cli._lock:
            cli._session = s
    term = "godzilla"
    t0 = time.time()
    try:
        obj = cli.search_http(term)
        rows = [r for r in (obj.get("results") or []) if isinstance(r, dict)]
        print(f"[reuse] search_http('{term}') OK: {len(rows)} rows "
              f"in {time.time() - t0:.1f}s", flush=True)
        index = make_index("data")
        new, upd = index.upsert(rows, source="scrapling")
        print(f"[reuse] vaulted: +{new} new, {upd} updated", flush=True)
        for r in rows[:3]:
            print("   -", r.get("id"), (r.get("title") or "")[:70], flush=True)
        print("[reuse] VERDICT phase2: PASS", flush=True)
        return 0
    except NeedsSession as e:
        print(f"[reuse] plain-HTTP rejected the cookies: {str(e)[:120]}", flush=True)
        print("[reuse] VERDICT phase2: FAIL (cf_clearance likely IP/TLS-bound)", flush=True)
        return 1
    except Exception as e:
        print(f"[reuse] {type(e).__name__}: {str(e)[:160]}", flush=True)
        print("[reuse] VERDICT phase2: FAIL", flush=True)
        return 1


if __name__ == "__main__":
    mode = (sys.argv[1] if len(sys.argv) > 1 else "clear").lower()
    sys.exit(phase_clear() if mode == "clear" else phase_reuse())
