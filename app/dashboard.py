"""Live vault dashboard v4 — real-time crawler control-room page.

  python -m app.dashboard            (env: MKV_DASHBOARD_PORT, default 8766)

Transport: server-sent events. ONE sampler thread polls Mongo + the crawler
log every 3s, diffs it into a live event stream, and every browser gets the
snapshot pushed instantly - no polling, no refresh. Row-count history persists
to data/vault_history.json so velocity + the 24h chart survive restarts.

Read-only. Safe to run next to the API server and the fleet.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import time
from collections import Counter, deque

from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

DATA_DIR = os.getenv("MKV_DATA_DIR", "data")
PORT = int(os.getenv("MKV_DASHBOARD_PORT", "8766"))
SITE = os.getenv("MKV_BASE_URL", "https://mkvbase.site")
LOG_PATH = os.path.join(DATA_DIR, "pusher.log")
IDGAP_STATE = os.path.join(DATA_DIR, "idgap_state.json")
SITE_STATE = os.path.join(DATA_DIR, "site_state.json")  # site max id (pusher watcher)
HIST_PATH = os.path.join(DATA_DIR, "vault_history.json")
RENDER_URL = os.getenv("MKV_RENDER_URL", "https://pro-movieapidrive.onrender.com")
BLOCK = 25000
TICK_S = 3.0
HIST_EVERY_S = 300.0
HIST_MAX_AGE_S = 8 * 86400.0

app = FastAPI(docs_url=None, redoc_url=None)

# The control-room UI is a real static site now (Tailwind + Alpine, vendored),
# served straight off disk so the Python module stays payload-only.
_UI_DIR = os.path.dirname(os.path.abspath(__file__))
app.mount("/static", StaticFiles(directory=os.path.join(_UI_DIR, "static")), name="static")
_started = time.time()
_latest: dict = {}
_hist: list[list[float]] = []
_events: deque = deque(maxlen=60)
_ok_t: deque = deque(maxlen=600)
_fail_t: deque = deque(maxlen=600)
_log_off = 0
_tick_n = 0
_skew_cache: dict | None = None
_mongo_lock = threading.Lock()
_mongo_col = None
_sampler_started = False


# ------------------------------------------------------------------- mongo
def _mongo_col_cached():
    global _mongo_col
    if _mongo_col is not None:
        return _mongo_col
    with _mongo_lock:
        if _mongo_col is not None:
            return _mongo_col
        uri = (os.getenv("MKV_MONGODB_URI") or "").strip()
        if not uri and os.path.exists(os.path.join(DATA_DIR, "mongo_uri.txt")):
            try:
                uri = open(os.path.join(DATA_DIR, "mongo_uri.txt"),
                           encoding="utf-8").read().strip()
            except Exception:
                uri = ""
        if not uri:
            return None
        try:
            from .store import prefer_mongo_ipv4
            prefer_mongo_ipv4()
            from pymongo import MongoClient
            c = MongoClient(uri, serverSelectionTimeoutMS=4000, socketTimeoutMS=15000)
            c.admin.command("ping")
            _mongo_col = c[os.getenv("MKV_MONGO_DB", "mkvbase")].links
            return _mongo_col
        except Exception:
            return None


# ---------------------------------------------------------------- data bits
def _vault(col=None) -> dict:
    idx = _get_idx()
    if idx is None:
        return {"rows": None, "error": "mongo unreachable"}
    try:
        cols = list(idx.cols())
        rows = sum(c.estimated_document_count() for c in cols)
        get_mx = getattr(idx, "max_id", None)
        mx = get_mx() if get_mx else (cols[0].find_one(sort=[("id", -1)],
                                                       projection={"id": 1}) or {}).get("id")
        out = {"rows": rows, "max_id": mx,
               "coverage_pct": round(100 * rows / mx, 1) if mx else None,
               "shards": len(cols)}
        blocks, thin = [], []
        for c in cols:
            try:
                for r in c.aggregate([
                        {"$match": {"id": {"$ne": None}}},
                        {"$group": {"_id": {"$floor": {"$divide": ["$id", BLOCK]}},
                                    "n": {"$sum": 1}}}]):
                    b = {"block": int(r["_id"]), "n": r["n"]}
                    blocks.append(b)
                    if r["n"] < BLOCK * 0.6:
                        thin.append(b)
            except Exception:
                pass
        blocks.sort(key=lambda x: x["block"])
        thin.sort(key=lambda x: x["n"])
        out["blocks"] = blocks
        out["thin"] = [{"block": b["block"], "have": b["n"]} for b in thin[:5]]
        return out
    except Exception as e:
        return {"rows": None, "error": type(e).__name__}


def _sources(col, n: int = 2000) -> list[dict]:
    counts: dict[str, int] = {}
    try:
        cols = list(globals().get("_dash_idx").cols()) if globals().get("_dash_idx") \
            else ([col] if col is not None else [])
        for c in cols:
            for r in c.find({}, {"_src": 1}).sort("_seq", -1).limit(n):
                src = (r.get("_src") or "unknown").split(":")[0]
                counts[src] = counts.get(src, 0) + 1
    except Exception:
        return []
    total = sum(counts.values()) or 1
    return [{"name": k, "n": v, "pct": round(100 * v / total, 1)}
            for k, v in sorted(counts.items(), key=lambda x: -x[1])]


def _newest(col, n: int = 12) -> list[dict]:
    cols = [col] if col is not None else []
    try:
        extra = list(globals().get("_dash_idx").cols())  # overflow shard too
        for c in extra:
            if c not in cols:
                cols.append(c)
    except Exception:
        pass
    if not cols:
        return []
    try:
        out = []
        seen: set = set()
        for c in cols:
            for r in c.find({}, {"id": 1, "title": 1, "_src": 1, "created_at": 1}
                            ).sort("_seq", -1).limit(n):
                # The same site id can sit in BOTH clusters (written before
                # the overflow shard took over, then re-upserted there). The
                # old merge showed it twice, and Alpine's x-for with
                # :key="x.id" then refuses to render the panel at all.
                key = r.get("id") if r.get("id") is not None else r.get("_id")
                if key in seen:
                    continue
                seen.add(key)
                out.append({"id": r.get("id"), "title": (r.get("title") or "")[:90],
                            "src": (r.get("_src") or "").split(":")[0],
                            "created_at": (r.get("created_at") or "")[:10]})
        out.sort(key=lambda r: r.get("id") or 0, reverse=True)
        return out[:n]
    except Exception:
        return []


def _log_tail_bytes() -> tuple[list[str], float]:
    try:
        age = time.time() - os.path.getmtime(LOG_PATH)
        with open(LOG_PATH, "rb") as f:
            f.seek(max(0, os.path.getsize(LOG_PATH) - 200_000))
            lines = f.read().decode("utf-8", "replace").splitlines()
        return [l[:220] for l in lines], age
    except Exception:
        return [], -1.0


_ALIVE = re.compile(r"up (\d+)m(\d+)s \| session=(\w+)")
_SB_OK = re.compile(r"\[idgap:(a\d+)\] ok '([^']*)'/(\S+) block=(\d+) era=([\d-]*)?: "
                    r"(\d+) rows, (\d+) new, \d+ in-block \([\d.]+s\)\s+cov=([\d.]+)%")
_SB_RETRY = re.compile(r"\[idgap:(a\d+)\] RETRY")


def _scoreboard(lines: list[str], block: int = 25_000) -> dict:
    """Quiet scoreboard: mine the recent log window (~2h of lines) into the
    numbers that matter — pace, yield, jackpots — no scrolling feed."""
    import time as _t
    now = _t.time()
    hours = [0, 0, 0]          # [2h-1h, 1h-0h, live] new-rows buckets
    searches = retries = capped = 0
    cov = None
    last_block = last_era = None
    terms: Counter = Counter()
    jack: dict[str, int] = {}
    cur_age = 2.0  # heartbeat clock: [alive] up Xm -> line age = 2h - X
    for l in lines[-2000:]:     # chronological; heartbeat age carried forward
        hb = _ALIVE.search(l)
        if hb:
            cur_age = max(0.0, 2.0 - int(hb.group(1)) / 60)
        if _SB_RETRY.search(l):
            retries += 1
            continue
        m = _SB_OK.search(l)
        if not m:
            continue
        searches += 1
        _agent, term, kind, blk, _era, rows, new, c = m.groups()
        new, rows = int(new), int(rows)
        if rows >= 50:
            capped += 1
        if cur_age >= 1.0:
            hours[0] += new
        elif cur_age >= 0.05:
            hours[1] += new
        else:
            hours[2] += new
        terms[kind] += 1
        if new > 0:
            jack[term] = max(jack.get(term, 0), new)
        cov, last_block, last_era = float(c), int(blk), _era
    pace1 = hours[1] + hours[2]
    yield1 = round(pace1 / max(searches, 1), 2)
    top = sorted(jack.items(), key=lambda kv: -kv[1])[:5]
    return {"h2": hours[0], "h1": pace1, "per_min": round(pace1 / 60.0, 1),
            "searches": searches, "retries": retries,
            "capped_pct": round(100.0 * capped / searches, 1) if searches else None,
            "yield": yield1 if searches else None,
            "cov": cov, "block": last_block,
            "block_start": last_block * block if last_block is not None else None,
            "era": last_era,
            "top_kind": (terms.most_common(1) or [(None, 0)])[0][0],
            "jackpots": [{"term": t, "new": n} for t, n in top]}
_TICK = re.compile(r"done=(\d+) queued=(\d+).*?rows=(\d+).*?agents=(\d+)")
_LANES = re.compile(r"\[priority=(\d+) day=(\d+) year=(\d+) alpha=(\d+) "
                    r"words=(\d+) series=(\d+) facet=(\d+)\]")


_IDGAP_ON = re.compile(r"\[idgap\] online: agents=(\d+)")
_IDGAP_AGENT = re.compile(r"\[idgap:(a\d+)\] (ok|RETRY)")
_WANTED = ("uptime_min", "idgap_agents", "tick", "lanes")


def _fleet_from_log(lines: list[str]) -> dict:
    out: dict = {}
    # newest-first scan; stop only when EVERY wanted key was seen. The old
    # len(out) >= 3 break fired after {uptime_min, session, tick} and never
    # reached the boot-time '[idgap] online: agents=N' line, so the fleet
    # card permanently showed 0 idgap agents.
    for l in reversed(lines):
        if "uptime_min" not in out and "[alive]" in l:
            m = _ALIVE.search(l)
            if m:
                out["uptime_min"] = int(m.group(1))
                out["session"] = m.group(3)
        if "idgap_agents" not in out:
            m = _IDGAP_ON.search(l)
            if m:
                out["idgap_agents"] = int(m.group(1))
        if "tick" not in out and "[discovery] fleet tick" in l:
            m = _TICK.search(l)
            if m:
                out["tick"] = {"done": int(m.group(1)), "queued": int(m.group(2)),
                               "rows": int(m.group(3)), "agents": int(m.group(4))}
        if "lanes" not in out:
            m = _LANES.search(l)
            if m:
                names = ("priority", "day", "year", "alpha", "words", "series", "facet")
                out["lanes"] = {k: int(v) for k, v in zip(names, m.groups())}
        if all(k in out for k in _WANTED):
            break
    # Live idgap-agent count beats the boot line: the boot-time '[idgap]
    # online: agents=N' line ages out of the log window (or rotates away),
    # while actual per-agent chatter in the recent window reflects reality.
    # Fall back to the boot count when no chatter yet (fresh restart).
    seen = {m.group(1) for l in lines[-500:]
            if (m := _IDGAP_AGENT.search(l))}
    out["idgap_agents"] = len(seen) if seen else out.get("idgap_agents", 0)
    return out


def _dbstats(col) -> dict:
    """Real MongoDB storage numbers: how much of the Atlas space is filled
    and how much remains. fsUsed/fsFree (true cluster quota) when the server
    reports them; otherwise WiredTiger disk usage vs MKV_MONGO_LIMIT_MB
    (default 512 = Atlas free tier)."""
    if col is None:
        return {}
    try:
        db = col.database
        s = db.command("collstats", col.name)
        d = db.command("dbstats")
        out = {
            "disk_mb": round((s.get("storageSize") or 0) / 1e6, 1),
            "idx_mb": round((d.get("indexSize") or
                             sum((v or {}).get("size", 0) for v in
                                 (s.get("indexSizes") or {}).values())) / 1e6, 1),
            "data_mb": round((s.get("size") or 0) / 1e6, 1),
            "avg_b": s.get("avgObjSize"),
            "rows": s.get("count"),
        }
        if d.get("fsUsedSize") is not None:
            used = d["fsUsedSize"] / 1e6
            free = (d.get("fsFreeSize") or 0) / 1e6
            out.update(used_mb=round(used, 1), total_mb=round(used + free, 1))
        else:
            limit = float(os.getenv("MKV_MONGO_LIMIT_MB", "512"))
            used = out["disk_mb"] + out["idx_mb"]
            out.update(used_mb=round(used, 1), total_mb=limit)
        return out
    except Exception:
        return {}


def _site_state() -> dict:
    """Newest id seen on the site (recorded by the pusher's recent watcher)."""
    try:
        d = json.load(open(SITE_STATE, encoding="utf-8"))
        return {"site_max_id": int(d.get("site_max_id") or 0),
                "seen_at": float(d.get("seen_at") or 0)}
    except Exception:
        return {}


def _idgap_state() -> dict:
    try:
        d = json.load(open(IDGAP_STATE, encoding="utf-8"))
        ts = d.get("term_stats") or {}
        return {"term_stats": ts,
                "new_rows": sum(v.get("new", 0) for v in ts.values()),
                "terms": sum(v.get("tries", 0) for v in ts.values())}
    except Exception:
        return {}


def _render_health() -> dict:
    import urllib.request
    try:
        with urllib.request.urlopen(f"{RENDER_URL}/health", timeout=10) as r:
            h = json.loads(r.read() or b"{}")
        up = int(h.get("uptime_s") or 0)
        return {"ok": True, "rows": (h.get("links") or {}).get("rows"),
                "uptime": f"{up // 86400}d {up % 86400 // 3600}h",
                "mem_mb": h.get("mem_mb"), "serve_only": h.get("serve_only")}
    except Exception as e:
        return {"ok": False, "err": type(e).__name__}


def _clock_skew() -> dict | None:
    """Local clock vs the site's own HTTP Date header. Signed search URLs go
    stale when the PC clock drifts - this is the silent crawler killer."""
    import urllib.error
    import urllib.request
    try:
        req = urllib.request.Request(SITE, method="HEAD")
        try:
            resp = urllib.request.urlopen(req, timeout=8)
            d = resp.headers.get("Date")
        except urllib.error.HTTPError as e:   # 403 still carries a Date header
            d = e.headers.get("Date")
        if not d:
            return _skew_cache
        from email.utils import parsedate_to_datetime
        server = parsedate_to_datetime(d).timestamp()
        return {"skew_s": round(time.time() - server), "checked_at": time.time()}
    except Exception:
        return _skew_cache


# ------------------------------------------------------------------ history
def _hist_load() -> None:
    global _hist
    try:
        d = json.load(open(HIST_PATH, encoding="utf-8"))
        cut = time.time() - HIST_MAX_AGE_S
        _hist = [p for p in (d.get("samples") or [])
                 if isinstance(p, list) and len(p) == 2 and p[0] >= cut]
    except Exception:
        _hist = []


def _hist_append(ts: float, rows: int) -> None:
    if _hist and ts - _hist[-1][0] < HIST_EVERY_S:
        return
    _hist.append([ts, rows])
    cut = ts - HIST_MAX_AGE_S
    while _hist and _hist[0][0] < cut:
        _hist.pop(0)
    try:
        tmp = HIST_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({"samples": _hist[-3000:]}, f, separators=(",", ":"))
        os.replace(tmp, HIST_PATH)
    except Exception:
        pass


def _velocity() -> dict:
    out = {"per_min": None, "h1": None, "h24": None}
    if not _hist:
        return out
    now, cur = time.time(), (_hist[-1][1] if _hist else 0)
    for win, key in ((3600, "h1"), (86400, "h24")):
        past = next((r for t, r in _hist if t >= now - win), None)
        if past is not None:
            out[key] = cur - past
    span = min(1800.0, now - _hist[0][0])
    past = next((r for t, r in _hist if t >= now - span), None)
    if past is not None and span >= 120:
        out["per_min"] = round((cur - past) / span * 60, 1)
    return out


# ------------------------------------------------------------ event stream
def _consume_log_lines() -> list[str]:
    global _log_off
    try:
        size = os.path.getsize(LOG_PATH)
        if size < _log_off:
            _log_off = 0
        if size == _log_off:
            return []
        with open(LOG_PATH, "rb") as f:
            f.seek(_log_off)
            data = f.read()
            _log_off = f.tell()
        return [l for l in data.decode("utf-8", "replace").splitlines() if l.strip()]
    except Exception:
        return []


def _classify(l: str) -> tuple[str, str] | None:
    if " ok " in l and l.startswith(("[discovery:", "[idgap:")):
        m = re.search(r"ok '([^']*)'.*?(\d+) new", l)
        if m:
            lane = l.split("]")[0][1:].split(":")[-1]
            kind = "idgap" if l.startswith("[idgap:") else "ok"
            return kind, f"{lane}: '{m.group(1)}' +{m.group(2)} rows"
        return ("ok", l[1:60])
    if "FAIL" in l or "RETRY" in l:
        m = re.search(r"(?:FAIL|RETRY) ([^:]+)", l)
        why = "stale URL / challenge" if "timing window" in l or "no JSON" in l else \
              (l.split(":", 2)[-1][:60] if l.count(":") >= 2 else "")
        return ("fail", f"{(m.group(1) if m else '?').strip()[:40]} — {why.strip()[:60]}")
    if "recent: vaulted" in l:
        m = re.search(r"vaulted (\d+) rows", l)
        return ("push", f"recent watcher vaulted {m.group(1) if m else '?'} rows")
    if "seeded" in l:
        m = re.search(r"seeded (\d+) terms", l)
        return ("info", f"trending seeded {m.group(1) if m else '?'} terms")
    if "READY in" in l:
        return ("info", "crawler READY")
    if "[idgap] online" in l:
        return ("info", "idgap miners online")
    return None


def _drain_events() -> None:
    # Live event feed removed per user preference; log lines still feed the
    # ok/fail health counters the header pill uses.
    for l in _consume_log_lines():
        ev = _classify(l)
        if ev:
            kind, _text = ev
            now = time.time()
            if kind in ("ok", "idgap"):
                _ok_t.append(now)
            elif kind == "fail":
                _fail_t.append(now)


def _rate(deq: deque, win_s: int = 300) -> float:
    now = time.time()
    return round(len([t for t in deq if t > now - win_s]) / (win_s / 60), 1)


# ----------------------------------------------------------------- sampler
def _get_idx():
    """Cached shard-aware MongoIndex (rebuilt only if it dies)."""
    idx = globals().get("_dash_idx")
    if idx is not None:
        try:
            idx.stats()
            return idx
        except Exception:
            globals()["_dash_idx"] = None
    try:
        from app.store import make_index
        idx = make_index(DATA_DIR)
        if idx.stats().get("backend") == "mongodb":
            globals()["_dash_idx"] = idx
            return idx
    except Exception:
        pass
    return None


def _sample_once() -> None:
    global _latest, _tick_n, _skew_cache
    _drain_events()
    idx = _get_idx()
    col = idx._col if idx is not None else None
    lines, log_age = _log_tail_bytes()
    try:
        sb = _scoreboard(lines)
    except Exception:
        sb = None  # scoreboard must never take the whole payload down
    snap = {
        "ts": time.time(),
        "uptime_min": int((time.time() - _started) / 60),
        "vault": _vault(col),
        "fleet": _fleet_from_log(lines),
        "site": _site_state(),
        "idgap": _idgap_state(),
        "newest": _newest(col),
        "log": lines,
        "log_age_s": round(log_age) if log_age >= 0 else -1,
        "scoreboard": sb,
        "health": {"ok_min": _rate(_ok_t), "fail_min": _rate(_fail_t)},
        "clock": _skew_cache,
        "db": _latest.get("db") or {},
        "render": None,
        "sources": None,
    }
    if snap["vault"].get("rows") is not None:
        _hist_append(time.time(), snap["vault"]["rows"])
    if _tick_n % 4 == 0 or not _latest.get("sources"):
        snap["sources"] = _sources(col)
    else:
        snap["sources"] = _latest.get("sources")
    if _tick_n % 4 == 1 or not _latest.get("db"):
        snap["db"] = _dbstats(col)
    else:
        snap["db"] = _latest.get("db")
    if _tick_n % 6 == 0 or not _latest.get("render"):
        snap["render"] = _render_health()
    else:
        snap["render"] = _latest.get("render")
    if _tick_n % 15 == 0 or _skew_cache is None:
        _skew_cache = _clock_skew()
        snap["clock"] = _skew_cache
    snap["velocity"] = _velocity()
    snap["history"] = _hist[-600:]
    _latest = snap


def _sampler() -> None:
    global _tick_n
    while True:
        try:
            _sample_once()
        except Exception:
            pass
        _tick_n += 1
        time.sleep(TICK_S)


def _start_sampler() -> None:
    global _sampler_started
    if _sampler_started:
        return
    _sampler_started = True
    _hist_load()
    threading.Thread(target=_sampler, daemon=True, name="dash-sampler").start()


# ------------------------------------------------------------------- routes
@app.get("/api/stream")
async def stream():
    _start_sampler()
    async def gen():
        last = 0
        while True:
            if _latest and _latest.get("ts") != last:
                last = _latest["ts"]
                yield f"data: {json.dumps(_latest, default=str)}\n\n"
            elif int(time.time()) % 15 == 0:
                yield ": keepalive\n\n"
            await asyncio.sleep(1.0)
    return StreamingResponse(gen(), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache", "X-Accel-Buffering": "no",
        "Connection": "keep-alive"})


@app.get("/api/live")
async def live():
    _start_sampler()
    return JSONResponse(_latest or {"ts": time.time(), "booting": True})


def _sync_key() -> str:
    try:
        return open(os.path.join(DATA_DIR, "sync_key.txt"),
                    encoding="utf-8").read().strip()
    except OSError:
        return ""


def _push_render(term: str, rows: list[dict]) -> str:
    import urllib.request
    payload = {"kind": "search", "term": term, "count": len(rows), "results": rows}
    req = urllib.request.Request(
        f"{RENDER_URL}/sync", data=json.dumps(payload).encode(), method="POST",
        headers={"Content-Type": "application/json",
                 **({"X-Sync-Key": _sync_key()} if _sync_key() else {})})
    with urllib.request.urlopen(req, timeout=60) as r:
        links = (json.loads(r.read() or b"{}").get("links") or {})
    return f"Render new {links.get('new', '?')} total {links.get('total', '?')}"


def _do_manual_search(term: str) -> dict:
    """Crawl the term with the shared Cloudflare session: signed
    searches over plain HTTPS, upserting every row into the index.
    Same flow the pusher's manual-search tick uses.

    session, vault rows (shard-aware), push to Render. Runs in a thread."""
    from app.client import MkvbaseClient, NeedsSession
    from app.store import make_index
    out: dict = {"term": term, "ts": time.time()}
    try:
        client = MkvbaseClient(None)  # browser-free: borrow the fleet's session
        try:
            obj = client.search_http(term)
        except NeedsSession:
            if not client.ensure_session(timeout_s=150):
                raise NeedsSession("could not clear Cloudflare")
            obj = client.search_http(term)
        rows = [r for r in (obj.get("results") or []) if isinstance(r, dict)]
        index = make_index(DATA_DIR)
        new, upd = index.upsert(rows, source="manual")
        try:
            push = _push_render(term, rows)
        except Exception as e:
            push = f"push failed ({type(e).__name__}: {str(e)[:60]})"
        out.update(ok=True, rows=len(rows), new=new, updated=upd, push=push,
                   top=[{"id": r.get("id"), "title": (r.get("title") or "")[:80]}
                        for r in rows[:5]])
        _events.append({"ts": time.time(), "kind": "push",
                        "text": f"manual '{term}' +{new} new, {len(rows)} rows"})
    except Exception as e:
        out.update(ok=False, err=f"{type(e).__name__}: {str(e)[:140]}")
    return out


_search_lock = threading.Lock()


@app.post("/api/search")
async def api_search(term: str = Query(..., min_length=1, max_length=100)):
    if not _search_lock.acquire(blocking=False):
        return JSONResponse({"ok": False, "err": "a search is already running"},
                            status_code=429)
    try:
        res = await asyncio.to_thread(_do_manual_search, term.strip())
        return JSONResponse(res)
    finally:
        _search_lock.release()


@app.get("/")
def index():
    return FileResponse(os.path.join(_UI_DIR, "templates", "index.html"))


def main() -> None:
    import uvicorn
    _start_sampler()
    print(f"[dashboard] http://127.0.0.1:{PORT}  (log: {LOG_PATH})", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
