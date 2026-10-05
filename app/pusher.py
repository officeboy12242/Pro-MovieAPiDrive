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
import threading
import time
import urllib.error
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.client import MkvbaseClient, MkvbaseError  # noqa: E402
from app.discovery import Discovery  # noqa: E402
from app.engines import make_engine  # noqa: E402
from app.idgap import start_idgap  # noqa: E402
from app.store import make_index  # noqa: E402

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
    TRENDING_EVERY = float(os.getenv("MKV_PUSHER_TRENDING_S", "1200"))

    def __init__(self, render_url: str, sync_key: str, client: MkvbaseClient, state_dir: str):
        self.render = render_url.rstrip("/")
        self.sync_key = sync_key
        self.client = client
        # Everything the pusher scrapes ALSO lands directly in the durable index
        # (Atlas when MKV_MONGODB_URI/data-mongo_uri.txt is set). Render's /sync
        # remains a serving layer; the vault is Mongo regardless of Render config.
        self.index = make_index(state_dir)
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
    def poll_recent_once(self, force: bool = False) -> str:
        print("[pusher] polling /api/links (recent)…", flush=True)
        t0 = time.time()
        obj = self.client.recent()
        rows = obj.get("results") or []
        ids = {r.get("id") for r in rows if isinstance(r, dict)}
        if ids and ids == self.seen.recent_ids and not (self.force_recent or force):
            return (f"recent: {len(rows)} rows unchanged, skipped POST  "
                    f"({time.time() - t0:.1f}s)")
        # vault first: rows land in Atlas even if the Render push fails (401 etc.)
        self.index.upsert(rows, source="recent")
        # freshness-first crawl: words from the newest uploads jump the discovery
        # queue, so the next searches target the freshest part of the catalog
        disc = getattr(self, "disc", None)
        if disc is not None and rows:
            try:
                seeded = disc.seed_titles(
                    [r.get("title") or "" for r in rows if isinstance(r, dict)],
                    front=True)
                if seeded:
                    print(f"[pusher] freshness: front-queued {seeded} terms from "
                          f"{len(rows)} newest rows", flush=True)
            except Exception:
                pass
        try:
            resp = self._push({"kind": "links", "term": "latest", "count": len(rows),
                               "results": rows})
            links = resp.get("links") or {}
            total = links.get("total")
            if not total:  # tolerate older API without links stats
                try:
                    with urllib.request.urlopen(f"{self.render}/health", timeout=30) as r:
                        total = (json.loads(r.read() or b"{}").get("links") or {}).get("rows")
                except Exception:
                    pass
            push_bit = (f"-> Render new {links.get('new', '?')} "
                        f"updated {links.get('updated', '?')} total "
                        f"{total if total is not None else '?'}")
        except Exception as e:
            push_bit = f"-> Render push skipped ({type(e).__name__}: {str(e)[:60]})"
        self.seen.recent_ids = ids
        self.seen.terms[_RECENT_MARK] = time.time()
        self.seen.save()
        # record the site's newest id for the dashboard's lag monitor
        try:
            smax = max(int(i) for i in ids if i is not None)
        except Exception:
            smax = 0
        if smax:
            self._record_site_max(smax)
        return (f"recent: vaulted {len(rows)} rows {push_bit}  "
                f"({time.time() - t0:.1f}s)")

    def _record_site_max(self, site_max: int) -> None:
        """Persist the site's newest id (seen by the recent watcher) so the
        dashboard can show vault-vs-site lag. Logs when it advances."""
        path = os.path.join(os.path.dirname(self.seen.path) or ".", "site_state.json")
        prev = 0
        try:
            prev = int(json.load(open(path, encoding="utf-8")).get("site_max_id") or 0)
        except Exception:
            pass
        try:
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"site_max_id": site_max, "seen_at": time.time()}, f)
        except Exception:
            pass
        if site_max > prev:
            print(f"[site:max] {site_max} (+{site_max - prev} new on site)", flush=True)

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
                print(f"[pusher] searching {term!r}…", flush=True)
                t0 = time.time()
                obj = self.client.search(term)
                self.index.upsert(obj.get("results") or [], source="search")
                try:
                    resp = self._push({"kind": "search", "term": term, **obj})
                    links = resp.get("links") or {}
                    push_bit = (f"-> Render new {links.get('new', '?')} "
                                f"total {links.get('total', '?')}")
                except Exception as e:
                    push_bit = f"-> Render push skipped ({type(e).__name__})"
                print(f"[pusher] search {term!r}: {obj.get('count')} rows {push_bit}  "
                      f"({time.time() - t0:.1f}s)", flush=True)
                self.seen.terms[term] = time.time()
                self.seen.save()
                done.append(term)
            except MkvbaseError as e:
                print(f"[pusher] search {term!r} failed: {str(e)[:160]}", flush=True)
                self.seen.terms[term] = now - self.search_every + 600  # retry in 10 min
            except Exception as e:
                print(f"[pusher] search {term!r} push failed: {type(e).__name__}: {e}",
                      flush=True)
        return done

    # ------------------------------------------------------------------ discovery
    def start_discovery(self, log=print) -> "Discovery | None":
        """Back-catalog crawler (MKV_DISCOVERY=1 or --discover): searches mkvbase
        term-by-term and merges rows DIRECTLY into the Mongo index (no /sync hop).
        Needs a durable index to make sense; skipped when Mongo is not configured."""
        if os.getenv("MKV_DISCOVERY", "").lower() not in ("1", "true", "yes"):
            return None
        idx = make_index(self.seen.path and os.path.dirname(self.seen.path) or ".")
        if (idx.stats().get("backend") != "mongodb"):
            log("[discovery] DISABLED: MongoDB not configured "
                "(set MKV_MONGODB_URI or data/mongo_uri.txt) — file index would "
                "never reach Render", flush=True)
            return None
        disc = Discovery(self.client, idx, os.path.dirname(self.seen.path) or ".")
        threading.Thread(target=disc.run, kwargs={"log": log}, daemon=True,
                         name="discovery").start()
        return disc

    def _warm_session(self) -> None:
        """Clear Cloudflare up front (short retries). Progress ticks every 10s."""
        print("[pusher] warming session (reuse disk/Mongo if still valid)…", flush=True)
        t0 = time.time()
        stop = threading.Event()

        def _tick():
            while not stop.wait(10):
                eng = getattr(self.client, "_engine", None)
                phase = getattr(eng, "phase", None) if eng else None
                print(f"[pusher] clearing… {time.time() - t0:.0f}s"
                      f"{f' [{phase}]' if phase else ''}", flush=True)

        threading.Thread(target=_tick, daemon=True, name="warm-tick").start()
        try:
            # 3×70s attempts inside ensure_session; total budget ~210s worst case,
            # usually much faster when disk/Mongo/cookiefree hits.
            ok = self.client.ensure_session(timeout_s=210)
            if ok and self.client.session_ready():
                print(f"[pusher] READY in {time.time() - t0:.0f}s — crawling starts now",
                      flush=True)
            else:
                print(f"[pusher] warm finished in {time.time() - t0:.0f}s but session "
                      f"not verified — scrapes will retry clear", flush=True)
        except Exception as e:
            print(f"[pusher] warm FAILED after {time.time() - t0:.0f}s: "
                  f"{type(e).__name__}: {str(e)[:160]}", flush=True)
            print("[pusher] scrapes will keep retrying clear", flush=True)
        finally:
            stop.set()

    # ------------------------------------------------------------------ main loop
    def run(self) -> None:
        print(f"[pusher] render={self.render} recent_every={self.recent_every:.0f}s "
              f"search_every={self.search_every / 60:.0f}min terms={len(self.seen.terms) - 1}",
              flush=True)
        self._warm_session()  # one clear before any crawl threads start
        next_recent = 0.0
        next_search_tick = 0.0
        next_trending = 0.0
        next_heartbeat = 0.0
        next_idgap_status = 0.0
        first_poll = True
        started = time.time()
        self.disc = self.start_discovery()  # no-op unless MKV_DISCOVERY/--discover
        # IdGap miner: id-coverage agents filling the thinnest eras of the vault.
        # Independent lane from Discovery (own state file: data/idgap_state.json).
        self.idgap = None
        if os.getenv("MKV_IDGAP", "true").lower() in ("1", "true", "yes"):
            try:
                self.idgap = start_idgap(self.client, os.path.dirname(self.seen.path) or ".")
            except Exception as e:
                print(f"[idgap] failed to start: {type(e).__name__}: {e}", flush=True)
        while True:
            now = time.monotonic()
            disc = self.disc
            if now >= next_heartbeat:
                up = int(time.time() - started)
                disc_bit = ""
                if disc is not None:
                    agents = getattr(disc, "agents_n", 1)
                    disc_bit = (f" | discovery agents={agents} done={disc.done_terms} "
                                f"queued={len(disc.queued)} rows={disc.found_rows}")
                idgap_bit = ""
                if self.idgap is not None:
                    idgap_bit = f" | idgap {self.idgap.status_line()}"
                print(f"[alive] up {up // 60}m{up % 60:02d}s | "
                      f"session={'ok' if self.client.session_ready() else 'warming'} | "
                      f"watched_terms={max(0, len(self.seen.terms) - 1)}{disc_bit}{idgap_bit}",
                      flush=True)
                next_heartbeat = now + 60
            if self.idgap is not None and now >= next_idgap_status:
                try:
                    cov = self.idgap.coverage()
                    thin = [f"b{b['block_start']}({b['have']})" for b in (cov.get("thin_blocks") or [])[:5]]
                    print(f"[idgap:status] coverage={cov.get('coverage_pct')}% of site ids "
                          f"({cov.get('total_rows', 0):,}/{cov.get('max_id', 0):,}) "
                          f"thin: {' '.join(thin) or 'none'}", flush=True)
                except Exception as e:
                    print(f"[idgap:status] failed: {type(e).__name__}: {e}", flush=True)
                next_idgap_status = now + 1800
            if disc is not None and now >= next_trending:
                # what real users are searching right now -> high-yield crawl seeds
                try:
                    titles = self.client.recent_trending()
                    n = disc.seed_titles(titles)
                    if n:
                        print(f"[pusher] trending: seeded {n} terms from "
                              f"{len(titles)} hot titles", flush=True)
                    next_trending = now + self.TRENDING_EVERY
                except Exception as e:
                    print(f"[pusher] trending seed failed: {type(e).__name__}: {e}",
                          flush=True)
                    next_trending = now + 300
            if now >= next_recent:
                # The first poll always POSTs: a fresh/empty backend (new Mongo
                # collection, wiped /tmp) gets repopulated on pusher start.
                try:
                    print(f"[pusher] {self.poll_recent_once(force=first_poll)}", flush=True)
                    first_poll = False
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
    ap.add_argument("--discover", action="store_true", default=None,
                    help="also crawl the back catalog into Mongo (or MKV_DISCOVERY=1)")
    args = ap.parse_args(argv)
    if not args.render_url:
        ap.error("--render-url or MKV_RENDER_URL is required")
    client = MkvbaseClient(make_engine(), cache_path=os.path.join(args.data_dir, "pusher"))
    p = Pusher(args.render_url, args.sync_key, client, args.data_dir)
    p.add_terms([t for t in args.terms.split(",") if t.strip()])
    if args.discover:
        os.environ["MKV_DISCOVERY"] = "1"
    try:
        p.run()  # run() starts the discovery thread itself when enabled
    except KeyboardInterrupt:
        print("\n[pusher] stopped", flush=True)


if __name__ == "__main__":
    main()
