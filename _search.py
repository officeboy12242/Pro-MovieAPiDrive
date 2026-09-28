"""One-shot search — crawl any term from mkvbase NOW, vault it, push to Render.

Like start-pusher.bat, but for a single search instead of the forever loop.
Browser-free: borrows the shared Cloudflare session (the pusher maintains it),
so it is safe to run while the pusher is running. ~1-2s per term.

Usage:
  .venv\\Scripts\\python _search.py "tribhuvan mishra ca topper"
  .venv\\Scripts\\python _search.py "panchayat s04 zip" "lanterns s01"

Rows land in the Mongo vault AND the Render /links index immediately, so the
bot's /movie finds them on the next search.
"""
from __future__ import annotations

import os
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.client import MkvbaseClient, NeedsSession  # noqa: E402
from app.store import make_index  # noqa: E402

ROOT = os.path.dirname(os.path.abspath(__file__))
RENDER = os.getenv("MKV_RENDER_URL", "https://pro-movieapidrive.onrender.com")
GAP_S = float(os.getenv("MKV_SEARCH_GAP_S", "2"))


def _sync_key() -> str:
    try:
        return open(os.path.join(ROOT, "data", "sync_key.txt"), encoding="utf-8").read().strip()
    except OSError:
        return ""


def _push(term: str, rows: list[dict]) -> str:
    payload = {"kind": "search", "term": term, "count": len(rows), "results": rows}
    req = urllib.request.Request(
        f"{RENDER}/sync", data=__import__("json").dumps(payload).encode(), method="POST",
        headers={"Content-Type": "application/json",
                 **({"X-Sync-Key": _sync_key()} if _sync_key() else {})})
    with urllib.request.urlopen(req, timeout=60) as r:
        links = (__import__("json").loads(r.read() or b"{}").get("links") or {})
    return f"pushed (Render new {links.get('new', '?')} total {links.get('total', '?')})"


def main(argv: list[str]) -> int:
    terms = [t.strip() for t in argv if t.strip()]
    if not terms:
        print(__doc__)
        return 1
    client = MkvbaseClient(None)  # no browser engine: borrow the shared session only
    index = make_index(os.path.join(ROOT, "data"))
    failed = 0
    for i, term in enumerate(terms):
        if i:
            time.sleep(GAP_S)
        try:
            try:
                obj = client.search_http(term)
            except NeedsSession:
                # pusher not running / session expired: clear Cloudflare ourselves
                print("  no fresh session - clearing Cloudflare once "
                      "(home IP or browser, this can take ~1-2 min)…", flush=True)
                if not client.ensure_session(timeout_s=210):
                    raise NeedsSession("could not clear Cloudflare")
                obj = client.search_http(term)
        except NeedsSession as e:
            print(f"{term!r}: no usable Cloudflare session ({str(e)[:60]}).\n"
                  f"  Start the fleet once (START-auto-everything.cmd) and retry.",
                  flush=True)
            failed += 1
            continue
        except Exception as e:
            print(f"{term!r}: {type(e).__name__}: {str(e)[:120]}", flush=True)
            failed += 1
            continue
        rows = [r for r in (obj.get("results") or []) if isinstance(r, dict)]
        new, upd = index.upsert(rows, source="manual")
        try:
            push_bit = _push(term, rows)
        except Exception as e:
            push_bit = f"Render push failed ({type(e).__name__}: {str(e)[:60]})"
        print(f"{term!r}: {len(rows)} rows | vault +{new} new, {upd} updated | {push_bit}",
              flush=True)
        for r in rows[:5]:
            print("   -", (r.get("title") or "")[:100], flush=True)
        if len(rows) > 5:
            print(f"   … +{len(rows) - 5} more", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
