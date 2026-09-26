"""Pusher — the big-RAM half of the split-plane design.

Render free (512MB) cannot run a browser, so a stronger host runs THIS script:
it clears Cloudflare once with Camoufox, then scrapes mkvbase over plain HTTP
(~1s per request, no browser) and pushes every result set to the Render API
via POST /sync. Render flips to MKV_SERVE_ONLY=true and serves everything
browser-free in ~50MB RAM. Every pushed row is deduplicated on the receiving
side by id/url/title (LinksIndex), so it is safe to push overlapping data from
multiple pushers.

Two loops, forever:
  recent    poll mkvbase /api/links every MKV_PUSHER_RECENT_S (default 120s) and
            POST when the id-set changes — an idle site costs one scrape, zero traffic.
  searches  replay watched terms when older than MKV_PUSHER_SEARCH_S (default 1h);
            cold-start backlog replays in first-seen order via data/pusher_state.json.

Run anywhere with RAM and a residential IP (home PC, or Android via Termux +
proot-distro Ubuntu — Camoufox ships arm64 Linux builds and a phone IP clears
Cloudflare more easily than a datacenter IP):

  MKV_RENDER_URL=https://<your-service>.onrender.com \
  MKV_SYNC_KEY=<same as Render's env> \
  python -m app.pusher --terms "godzilla,interstellar"
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.client import MkvbaseClient, MkvbaseError  # noqa: E402
from app.engines import make_engine  # noqa: E402

_RECENT_MARK = "_recent_"


class Seen:
    """Persisted pusher state: when each term was last pushed + last recent id-set."""

    def __init__(self, path: str):
        self.path = path
        self.terms: dict[str, float] = {}
        self.recent_ids: set[int] = set()
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
            self.terms = {k: float(v) for k, v in (data.get("terms") or {}).items()}
            self.recent_ids = set(data.get("recent_ids") or [])
        except Exception:
            pass

    def save(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({"terms": self.terms,
                           "recent_ids": sorted(i for i in self.recent_ids if i is not None)[-5000:]},
                          f)
        except Exception:
            pass


class Pusher:
    def __init__(self, render_url: str, sync_key: str, client: MkvbaseClient, state_dir: str):
        self.render = render_url.rstrip("/")
        self.sync_key = sync_key
        self.client = client
        self.seen = Seen(os.path.join(state_dir, "pusher_state.json"))
        self.recent_every = float(os.getenv("MKV_PUSHER_RECENT_S", "120"))
        self.search_every = float(os.getenv("MKV_PUSHER_SEARCH_S", "3600"))
        self.force_recent = os.getenv("MKV_PUSHER_ALWAYS", "").lower() in ("1", "true", "yes")

    # ------------------------------------------------------------------ push
    def _push(self, payload: dict) -> dict:
        req = urllib.request.Request(
            f"{self.render}/sync", data=json.dumps(payload).encode(), method="POST",
            headers={"Content-Type": "application/json",
                     **({"X-Sync-Key": self.sync_key} if self.sync_key else {})})
        with urllib.request.urlopen(req, timeout=60) as r:
            return json.loads(r.read() or b"{}")

    # ------------------------------------------------------------------ recent loop
    def poll_recent_once(self) -> str:
        obj = self.client.recent()
        rows = obj.get("results") or []
        ids = {r.get("id") for r in rows if isinstance(r, dict)}
        if ids and ids == self.seen.recent_ids and not self.force_recent:
            return f"recent: {len(rows)} rows unchanged, skipped POST"
        resp = self._push({"kind": "links", "term": "latest", "count": len(rows),
                           "results": rows})
        self.seen.recent_ids = ids
        self.seen.terms[_RECENT_MARK] = time.time()
        self.seen.save()
        links = resp.get("links") or {}
        return (f"recent: pushed {len(rows)} rows -> new {links.get('new', '?')} "
                f"updated {links.get('updated', '?')} total {links.get('total', '?')}")

    # ------------------------------------------------------------------ search loop
    def add_terms(self, terms: list[str]) -> None:
        changed = False
        for t in terms:
            t = t.strip().lower()
            if t and t not in self.seen.terms:
                self.seen.terms[t] = 0.0  # due immediately (backlog replays oldest-first)
                changed = True
        if changed:
            self.seen.save()

    def tick_searches(self, budget: int = 3) -> list[str]:
        now = time.time()
        due = sorted((t for t, ts in self.seen.terms.items()
                      if t != _RECENT_MARK and now - ts >= self.search_every),
                     key=lambda t: self.seen.terms[t])[:budget]
        done = []
        for term in due:
            try:
                obj = self.client.search(term)
                resp = self._push({"kind": "search", "term": term, **obj})
                links = resp.get("links") or {}
                print(f"[pusher] search {term!r}: {obj.get('count')} rows -> "
                      f"new {links.get('new', '?')} total {links.get('total', '?')}", flush=True)
                self.seen.terms[term] = time.time()
                self.seen.save()
                done.append(term)
            except MkvbaseError as e:
                print(f"[pusher] search {term!r} failed: {str(e)[:160]}", flush=True)
                self.seen.terms[term] = now - self.search_every + 600  # retry in 10 min
            except Exception as e:
                print(f"[pusher] search {term!r} push failed: {type(e).__name__}: {e}", flush=True)
        return done

    # ------------------------------------------------------------------ main loop
    def run(self) -> None:
        print(f"[pusher] render={self.render} recent_every={self.recent_every:.0f}s "
              f"search_every={self.search_every / 60:.0f}min terms={len(self.seen.terms) - 1} "
              f"(first scrape clears Cloudflare — may take a minute)", flush=True)
        next_recent = 0.0
        next_search_tick = 0.0
        while True:
            now = time.monotonic()
            if now >= next_recent:
                try:
                    print(f"[pusher] {self.poll_recent_once()}", flush=True)
                    next_recent = now + self.recent_every
                except Exception as e:
                    print(f"[pusher] recent poll failed: {type(e).__name__}: {str(e)[:160]}",
                          flush=True)
                    next_recent = now + 60  # retry sooner; bootstrap may still be clearing CF
            if now >= next_search_tick:
                self.tick_searches()
                next_search_tick = time.monotonic() + 60
            time.sleep(5)


def main(argv=None) -> None:
    ap = argparse.ArgumentParser(description="Scrape mkvbase on a big-RAM host, push to Render.")
    ap.add_argument("--render-url", default=os.getenv("MKV_RENDER_URL", ""),
                    help="e.g. https://pro-movieapidrive.onrender.com")
    ap.add_argument("--sync-key", default=os.getenv("MKV_SYNC_KEY", ""))
    ap.add_argument("--terms", default=os.getenv("MKV_PUSHER_TERMS", ""),
                    help="comma-separated terms to keep refreshed (added to remembered ones)")
    ap.add_argument("--data-dir", default=os.getenv("MKV_DATA_DIR", "data"))
    args = ap.parse_args(argv)
    if not args.render_url:
        ap.error("--render-url or MKV_RENDER_URL is required")
    client = MkvbaseClient(make_engine(), cache_path=os.path.join(args.data_dir, "pusher"))
    p = Pusher(args.render_url, args.sync_key, client, args.data_dir)
    p.add_terms([t for t in args.terms.split(",") if t.strip()])
    try:
        p.run()
    except KeyboardInterrupt:
        print("\n[pusher] stopped", flush=True)


if __name__ == "__main__":
    main()
