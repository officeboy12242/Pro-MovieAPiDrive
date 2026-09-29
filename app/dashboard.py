"""Live vault dashboard v3 — real-time crawler control-room page.

  python -m app.dashboard            (env: MKV_DASHBOARD_PORT, default 8766)

Transport: server-sent events. ONE sampler thread polls Mongo + the crawler
log every 3s, diffs it into a live event stream, and every browser gets the
snapshot pushed instantly - no polling, no refresh. Row-count history persists
to data/vault_history.json so velocity + the 24h chart survive restarts.

Cards: vault rows + count-up, coverage gauge, push velocity, crawl health
(ok/fail rates + PC-clock skew vs the site's own HTTP Date - clock drift is
what silently kills signed search URLs), all-block id heatmap, 24h growth
chart, live event feed, newest pushes, fleet lanes, source mix, log tail.

Read-only. Safe to run next to the API server and the fleet.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import time
from collections import deque

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

DATA_DIR = os.getenv("MKV_DATA_DIR", "data")
PORT = int(os.getenv("MKV_DASHBOARD_PORT", "8766"))
SITE = os.getenv("MKV_BASE_URL", "https://mkvbase.site")
LOG_PATH = os.path.join(DATA_DIR, "pusher.log")
IDGAP_STATE = os.path.join(DATA_DIR, "idgap_state.json")
HIST_PATH = os.path.join(DATA_DIR, "vault_history.json")
RENDER_URL = os.getenv("MKV_RENDER_URL", "https://pro-movieapidrive.onrender.com")
BLOCK = 25000
TICK_S = 3.0
HIST_EVERY_S = 300.0
HIST_MAX_AGE_S = 8 * 86400.0

app = FastAPI(docs_url=None, redoc_url=None)
_started = time.time()
_latest: dict = {}
_hist: list[list[float]] = []
_events: deque = deque(maxlen=60)          # {ts, kind, text}
_ok_t: deque = deque(maxlen=600)           # timestamps of ok crawls
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
            from pymongo import MongoClient
            c = MongoClient(uri, serverSelectionTimeoutMS=4000, socketTimeoutMS=15000)
            c.admin.command("ping")
            _mongo_col = c[os.getenv("MKV_MONGO_DB", "mkvbase")].links
            return _mongo_col
        except Exception:
            return None


# ---------------------------------------------------------------- data bits
def _vault(col) -> dict:
    if col is None:
        return {"rows": None, "error": "mongo unreachable"}
    try:
        rows = col.estimated_document_count()
        mx = (col.find_one(sort=[("id", -1)], projection={"id": 1}) or {}).get("id")
        out = {"rows": rows, "max_id": mx,
               "coverage_pct": round(100 * rows / mx, 1) if mx else None}
        blocks, thin = [], []
        try:
            for r in col.aggregate([
                    {"$match": {"id": {"$ne": None}}},
                    {"$group": {"_id": {"$floor": {"$divide": ["$id", BLOCK]}},
                                "n": {"$sum": 1}}}]):
                b = {"block": int(r["_id"]), "n": r["n"]}
                blocks.append(b)
                if r["n"] < BLOCK * 0.6:
                    thin.append(b)
            blocks.sort(key=lambda x: x["block"])
            thin.sort(key=lambda x: x["have"] if "have" in x else x["n"])
            out["blocks"] = blocks
            out["thin"] = [{"block": b["block"], "have": b["n"]} for b in thin[:5]]
        except Exception:
            pass
        return out
    except Exception as e:
        return {"rows": None, "error": type(e).__name__}


def _sources(col, n: int = 2000) -> list[dict]:
    if col is None:
        return []
    counts: dict[str, int] = {}
    try:
        for r in col.find({}, {"_src": 1}).sort("_seq", -1).limit(n):
            src = (r.get("_src") or "unknown").split(":")[0]
            counts[src] = counts.get(src, 0) + 1
    except Exception:
        return []
    total = sum(counts.values()) or 1
    return [{"name": k, "n": v, "pct": round(100 * v / total, 1)}
            for k, v in sorted(counts.items(), key=lambda x: -x[1])]


def _newest(col, n: int = 12) -> list[dict]:
    if col is None:
        return []
    try:
        out = []
        for r in col.find({}, {"id": 1, "title": 1, "_src": 1, "created_at": 1}
                          ).sort("_seq", -1).limit(n):
            out.append({"id": r.get("id"), "title": (r.get("title") or "")[:90],
                        "src": (r.get("_src") or "").split(":")[0],
                        "created_at": (r.get("created_at") or "")[:10]})
        return out
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
_TICK = re.compile(r"done=(\d+) queued=(\d+).*?rows=(\d+).*?agents=(\d+)")
_LANES = re.compile(r"\[priority=(\d+) day=(\d+) year=(\d+) alpha=(\d+) "
                    r"words=(\d+) series=(\d+) facet=(\d+)\]")


def _fleet_from_log(lines: list[str]) -> dict:
    out: dict = {}
    for l in reversed(lines):
        if "uptime_min" not in out and "[alive]" in l:
            m = _ALIVE.search(l)
            if m:
                out["uptime_min"] = int(m.group(1))
                out["session"] = m.group(3)
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
        if len(out) >= 3:
            break
    return out


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
        _skew_cache_local = {"skew_s": round(time.time() - server),
                             "checked_at": time.time()}
        return _skew_cache_local
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
    """Incrementally read only NEW log bytes since the last tick."""
    global _log_off
    try:
        size = os.path.getsize(LOG_PATH)
        if size < _log_off:            # rotated / truncated
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
    for l in _consume_log_lines():
        ev = _classify(l)
        if ev:
            kind, text = ev
            now = time.time()
            _events.append({"ts": now, "kind": kind, "text": text})
            if kind in ("ok", "idgap"):
                _ok_t.append(now)
            elif kind == "fail":
                _fail_t.append(now)


def _rate(deq: deque, win_s: int = 300) -> float:
    now = time.time()
    return round(len([t for t in deq if t > now - win_s]) / (win_s / 60), 1)


# ----------------------------------------------------------------- sampler
def _sample_once() -> None:
    global _latest, _tick_n, _skew_cache
    _drain_events()
    col = _mongo_col_cached()
    lines, log_age = _log_tail_bytes()
    snap = {
        "ts": time.time(),
        "uptime_min": int((time.time() - _started) / 60),
        "vault": _vault(col),
        "fleet": _fleet_from_log(lines),
        "idgap": _idgap_state(),
        "newest": _newest(col),
        "log": lines,
        "log_age_s": round(log_age) if log_age >= 0 else -1,
        "events": list(_events)[-30:],
        "health": {"ok_min": _rate(_ok_t), "fail_min": _rate(_fail_t)},
        "clock": _skew_cache,
        "render": None,
        "sources": None,
    }
    if snap["vault"].get("rows") is not None:
        _hist_append(time.time(), snap["vault"]["rows"])
    if _tick_n % 4 == 0 or not _latest.get("sources"):
        snap["sources"] = _sources(col)
    else:
        snap["sources"] = _latest.get("sources")
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


_HTML = r"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>mkvbase vault — control room</title><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{--bg:#05070d;--card:rgba(15,20,32,.66);--card2:rgba(15,20,32,.9);
--bd:rgba(148,170,220,.10);--bd2:rgba(148,170,220,.22);--fg:#e8eefb;--dim:#7e8aa5;
--grn:#3ddc84;--amb:#ffb454;--red:#ff5c69;--blu:#6ea8ff;--pur:#b48cff;--cyn:#54d6ff;
--grad:linear-gradient(93deg,#6ea8ff,#b48cff 55%,#54d6ff)}
*{box-sizing:border-box;margin:0}
html{scrollbar-color:#2a3346 transparent}
body{background:
 radial-gradient(900px 520px at 85% -8%,rgba(110,140,255,.13),transparent 60%),
 radial-gradient(800px 500px at -10% 105%,rgba(180,140,255,.10),transparent 60%),
 var(--bg);color:var(--fg);
 font:14px/1.5 'Segoe UI',system-ui,-apple-system,Roboto,sans-serif;min-height:100vh}
.num,.lg,td{font-variant-numeric:tabular-nums}
.num{font-family:'Cascadia Code',Consolas,ui-monospace,monospace}
header{position:sticky;top:0;z-index:20;display:flex;align-items:center;gap:12px;
 padding:13px 22px;background:rgba(5,7,13,.78);backdrop-filter:blur(14px);
 border-bottom:1px solid var(--bd)}
.logo{width:26px;height:26px;border-radius:8px;background:var(--grad);
 display:grid;place-items:center;font-weight:900;font-size:13px;color:#05070d}
.brand{font-weight:700;font-size:15px}
.brand small{color:var(--dim);font-weight:400;margin-left:7px;font-size:12px}
.dot{width:8px;height:8px;border-radius:50%;background:var(--grn);animation:p 1.8s infinite}
.dot.warn{background:var(--amb);animation:none}.dot.dead{background:var(--red);animation:none}
@keyframes p{0%{box-shadow:0 0 0 0 rgba(61,220,132,.5)}70%{box-shadow:0 0 0 8px transparent}100%{box-shadow:0 0 0 0 transparent}}
.pill{font-size:10px;font-weight:800;letter-spacing:1.4px;padding:4px 11px;border-radius:99px;
 border:1px solid rgba(61,220,132,.35);color:var(--grn);text-transform:uppercase}
.pill.warn{border-color:rgba(255,180,84,.4);color:var(--amb)}
.pill.dead{border-color:rgba(255,92,105,.45);color:var(--red)}
#hdr-right{margin-left:auto;display:flex;gap:14px;color:var(--dim);font-size:12px;align-items:center}
.badge{display:flex;gap:6px;align-items:center;font-size:11.5px;padding:3px 9px;
 border-radius:8px;border:1px solid var(--bd);background:rgba(15,20,32,.5)}
.badge .num{color:var(--fg)}
main{max-width:1220px;margin:18px auto 0;padding:0 20px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:14px;margin-bottom:14px}
.card{position:relative;background:var(--card);border:1px solid var(--bd);border-radius:16px;
 padding:16px 18px;backdrop-filter:blur(8px);transition:border-color .25s,transform .25s}
.card:hover{border-color:var(--bd2);transform:translateY(-1px)}
.card h3{font-size:10.5px;text-transform:uppercase;letter-spacing:1.3px;color:var(--dim);
 margin-bottom:9px;display:flex;justify-content:space-between;align-items:center;font-weight:600}
.card h3 .tag{text-transform:none;letter-spacing:.2px;font-weight:400;font-size:10.5px}
.v{font-size:31px;font-weight:800;background:var(--grad);-webkit-background-clip:text;
 background-clip:text;color:transparent;line-height:1.05}
.s{font-size:12px;color:var(--dim);margin-top:5px}
.s b{color:var(--fg);font-weight:600}
.g{color:var(--grn)}.a{color:var(--amb)}.b{color:var(--blu)}.p{color:var(--pur)}.r{color:var(--red)}.c{color:var(--cyn)}
.delta{display:inline-block;margin-left:8px;font-size:12.5px;color:var(--grn);opacity:0}
.delta.pop{animation:pop 2.6s ease-out}
@keyframes pop{0%{opacity:0;transform:translateY(7px)}12%{opacity:1;transform:translateY(0)}80%{opacity:1}100%{opacity:0}}
.gauge{display:flex;align-items:center;gap:14px}
.gauge svg{flex:none}
.gauge .ring{transition:stroke-dasharray .9s ease}
.gauge .pct{font-size:24px;font-weight:800;fill:#e8eefb;font-family:'Cascadia Code',Consolas,monospace}
.gauge .sub{fill:var(--dim);font-size:9px}
.full{grid-column:1/-1}
.hm{display:grid;grid-template-columns:repeat(auto-fill,minmax(52px,1fr));gap:7px;margin-top:4px}
.cell{position:relative;border-radius:9px;padding:7px 4px 5px;text-align:center;
 border:1px solid var(--bd);background:rgba(255,255,255,.02);transition:transform .15s}
.cell:hover{transform:scale(1.07);z-index:2}
.cell .pc{font-size:13px;font-weight:700}
.cell .bl{font-size:8.5px;color:var(--dim);letter-spacing:.3px}
.cell.thin{border-color:rgba(255,92,105,.4);box-shadow:0 0 12px rgba(255,92,105,.15)}
.cell.fullb{border-color:rgba(61,220,132,.35)}
.legend{display:flex;gap:14px;margin-top:9px;font-size:10.5px;color:var(--dim);flex-wrap:wrap}
.legend i{display:inline-block;width:9px;height:9px;border-radius:3px;margin-right:5px;vertical-align:-1px}
svg text{fill:var(--dim);font-size:10px;font-family:'Cascadia Code',Consolas,monospace}
table{width:100%;border-collapse:collapse;font-size:12.5px}
td,th{padding:5px 8px;border-bottom:1px solid rgba(148,170,220,.06);text-align:left;white-space:nowrap}
th{color:var(--dim);font-weight:500;text-transform:uppercase;font-size:9.5px;letter-spacing:1px}
td.t{white-space:normal;max-width:0;width:100%;overflow:hidden;text-overflow:ellipsis}
tr.new td{animation:rowin 2.2s ease-out}
@keyframes rowin{0%{background:rgba(61,220,132,.16)}100%{background:transparent}}
.bar{display:flex;height:11px;border-radius:99px;overflow:hidden;margin-top:8px;background:rgba(148,170,220,.07)}
.bar div{height:100%;transition:width .8s ease}
.lg{font-size:11.5px;line-height:1.55;max-height:290px;overflow-y:auto;background:rgba(2,4,9,.65);
 border:1px solid var(--bd);border-radius:11px;padding:10px 12px;
 font-family:'Cascadia Code',Consolas,ui-monospace,monospace}
.lg .d{color:var(--dim)}.lg .e{color:var(--red)}.lg .i{color:var(--grn)}.lg .m{color:var(--blu)}
.feed{max-height:290px;overflow-y:auto;font-size:12.5px}
.ev{display:flex;gap:9px;padding:5px 8px;border-radius:8px;align-items:baseline;
 border-left:2px solid transparent;animation:evin .4s ease}
@keyframes evin{0%{opacity:0;transform:translateX(-6px)}100%{opacity:1;transform:none}}
.ev .ts{color:var(--dim);font-size:10.5px;flex:none;width:60px}
.ev.ok{border-left-color:var(--blu)} .ev.ok .tx b{color:var(--blu)}
.ev.idgap{border-left-color:var(--pur)} .ev.idgap .tx b{color:var(--pur)}
.ev.push{border-left-color:var(--grn)} .ev.push .tx b{color:var(--grn)}
.ev.fail{border-left-color:var(--red);background:rgba(255,92,105,.05)} .ev.fail .tx{color:#ffb3b9}
.ev.info{border-left-color:var(--cyn)} .ev.info .tx{color:var(--cyn)}
.two{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.lanes{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}
.lane{font-size:11px;padding:3px 10px;border-radius:99px;border:1px solid var(--bd);
 color:var(--dim);background:rgba(15,20,32,.5)}
.lane b{color:var(--fg)}
@media(max-width:900px){.two{grid-template-columns:1fr}.v{font-size:26px}
 #hdr-right{display:none}}
footer{color:var(--dim);font-size:11.5px;text-align:center;margin-top:22px}
footer a{color:var(--blu);text-decoration:none}
</style></head><body>
<header>
 <span class="dot"></span>
 <div class="logo">M</div>
 <span class="brand">mkvbase <small>vault · control room</small></span>
 <span class="pill" id="pill">connecting</span>
 <div id="hdr-right">
  <span class="badge" id="clockbadge" title="PC clock vs mkvbase server time (signed URLs die when this drifts)">🖥 clock <span class="num" id="clockv">—</span></span>
  <span class="badge num" id="pageup"></span>
 </div>
</header>
<main>
 <div class="grid">
  <div class="card"><h3>Vault rows <span class="tag">mongo atlas</span></h3>
   <div class="v num" id="rows">—</div><span class="delta num" id="rowsdelta"></span>
   <div class="s" id="rowsub">connecting…</div></div>
  <div class="card"><h3>Site coverage</h3>
   <div class="gauge">
    <svg width="86" height="86" viewBox="0 0 86 86">
     <defs><linearGradient id="gg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#6ea8ff"/><stop offset="1" stop-color="#b48cff"/></linearGradient></defs>
     <circle cx="43" cy="43" r="36" fill="none" stroke="rgba(148,170,220,.12)" stroke-width="8"/>
     <circle class="ring" id="gring" cx="43" cy="43" r="36" fill="none" stroke="url(#gg)"
      stroke-width="8" stroke-linecap="round" stroke-dasharray="0 226"
      transform="rotate(-90 43 43)"/>
     <text class="pct" id="gpct" x="43" y="41" text-anchor="middle">—</text>
     <text class="sub" x="43" y="55" text-anchor="middle">of site ids</text>
    </svg>
    <div class="s" id="covsub"></div></div></div>
  <div class="card"><h3>Push velocity</h3>
   <div class="v num" id="rate">—</div><div class="s" id="ratesub">live rate</div></div>
  <div class="card"><h3>Crawl health <span class="tag">last 5 min</span></h3>
   <div class="v num" id="health">—</div><div class="s" id="healthsub">measuring…</div></div>
 </div>

 <div class="grid full"><div class="card">
  <h3>Id-block heatmap <span class="tag" id="hmtag">25k ids per block · target ≥60% filled</span></h3>
  <div class="hm" id="hm"></div>
  <div class="legend"><span><i style="background:rgba(255,92,105,.75)"></i>&lt;15% full</span>
   <span><i style="background:rgba(255,180,84,.75)"></i>15–40%</span>
   <span><i style="background:rgba(84,214,255,.7)"></i>40–60%</span>
   <span><i style="background:rgba(61,220,132,.8)"></i>≥60% (done)</span></div></div></div>

 <div class="grid full"><div class="card">
  <h3>Vault growth — 24h <span class="tag num" id="chartsum"></span></h3>
  <svg id="chart" viewBox="0 0 600 132" width="100%" height="132" preserveAspectRatio="none">
   <defs><linearGradient id="gr" x1="0" y1="0" x2="0" y2="1">
    <stop offset="0" stop-color="#6ea8ff" stop-opacity=".4"/>
    <stop offset="1" stop-color="#6ea8ff" stop-opacity="0"/></linearGradient></defs>
  </svg></div></div>

 <div class="two">
  <div class="card"><h3>Live events <span class="tag" id="evtag"></span></h3>
   <div class="feed" id="feed"><div class="ev info"><span class="ts num">—</span><span class="tx">connecting…</span></div></div></div>
  <div class="card"><h3>Newest pushes → mongo</h3>
   <table><thead><tr><th>id</th><th>title</th><th>src</th></tr></thead>
   <tbody id="rowsfeed"><tr><td colspan=3 class="s">connecting…</td></tr></tbody></table></div>
 </div>

 <div class="two" style="margin-top:14px">
  <div class="card"><h3>Fleet <span class="tag" id="fleettag"></span></h3>
   <div class="v" id="fleetv">—</div><div class="s" id="fleetsub"></div>
   <div class="lanes" id="lanes"></div>
   <h3 style="margin-top:14px">Push sources <span class="tag" id="srcleg"></span></h3>
   <div class="bar" id="srcbar"></div>
   <div class="s" id="rendersub" style="margin-top:9px"></div></div>
  <div class="card"><h3>Crawler log</h3><div class="lg" id="log"></div></div>
 </div>
</main>
<footer>streamed live over server-sent events · read-only ·
 <a href="/api/live">/api/live</a> · stop/start via STOP-all-crawlers.cmd / START-auto-everything.cmd</footer>
<script>
const $=id=>document.getElementById(id);
const fmt=n=>n==null?'—':Math.round(n).toLocaleString();
const kk=n=>n>=1000?(n/1000).toFixed(n>=10000?0:1)+'k':n;
let cur={},prevRows=null,lastFeedId=null,prevLog='',prevHm='',lastOkSeen=0;
/* count-up tween */
function tween(id,fmtFn,target){const el=$(id);const from=(cur[id]===undefined?target:cur[id]);cur[id]=target;
 if(from===target){el.textContent=fmtFn(target);return}
 const t0=performance.now(),D=550;
 (function fr(t){const p=Math.min(1,(t-t0)/D),e=1-Math.pow(1-p,3);
  el.textContent=fmtFn(from+(target-from)*e);if(p<1)requestAnimationFrame(fr)})(t0);}
function cls(l){if(/\bFAIL\b|\berror\b|Traceback/i.test(l))return'e';
 if(/\[alive\]|ok |READY|online|seeded/.test(l))return'i';
 if(/^\[discovery/.test(l))return'm';return'd';}
function chart(h){const s=$('chart');const base=s.innerHTML.split('<path')[0];
 if(!h||h.length<2){$('chartsum').textContent='collecting history…';s.innerHTML=base;return}
 const now=h[h.length-1][0],cut=now-86400e3;const pts=h.filter(p=>p[0]>=cut);
 if(pts.length<2){$('chartsum').textContent='collecting history…';s.innerHTML=base;return}
 const vs=pts.map(p=>p[1]),mn=Math.min(...vs),mx=Math.max(...vs);
 const W=600,H=132,pad=6,n=pts.length;
 const X=i=>i/(n-1)*W,Y=v=>H-pad-14-(v-mn)/((mx-mn)||1)*(H-2*pad-14);
 let d='M'+X(0).toFixed(1)+' '+Y(vs[0]).toFixed(1);
 for(let i=1;i<n;i++)d+=' L'+X(i).toFixed(1)+' '+Y(vs[i]).toFixed(1);
 const g=(t,x,y,anchor)=>`<text x="${x}" y="${y}"${anchor?` text-anchor="${anchor}"`:''}>${t}</text>`;
 s.innerHTML=base+`<path d="${d} L${W} ${H} L0 ${H} Z" fill="url(#gr)"/>`+
  `<path d="${d}" fill="none" stroke="#6ea8ff" stroke-width="1.7"/>`+
  g(fmt(mn),4,H-2)+g(fmt(mx),4,10)+g('24h ago',2,H-2)+g('now',W-24,H-2);
 const gain=vs[n-1]-vs[0];
 $('chartsum').textContent=gain>0?`+${fmt(gain)} rows / 24h`:'';}
function heatmap(blocks){if(!blocks)return;const ser=JSON.stringify(blocks);
 if(ser===prevHm)return;prevHm=ser;
 $('hm').innerHTML=blocks.map(b=>{const p=Math.round(100*b.n/25000);
  const col=p>=60?'rgba(61,220,132,.8)':p>=40?'rgba(84,214,255,.7)':
            p>=15?'rgba(255,180,84,.75)':'rgba(255,92,105,.75)';
  return `<div class="cell${p<60?(p<15?' thin':''):' fullb'}" title="ids ${fmt(b.block*25000)}–${fmt((b.block+1)*25000)} · ${fmt(b.n)}/25,000 rows (${p}%)">`+
   `<div class="pc" style="color:${col}">${p}%</div><div class="bl num">${b.block*25}k</div></div>`}).join('');}
function events(evs){if(!evs||!evs.length)return;
 $('feed').innerHTML=evs.slice().reverse().map(e=>{
  const t=new Date(e.ts*1000).toTimeString().slice(0,8);
  const tx=e.text.replace(/</g,'&lt;').replace(/'([^']*)' \+(\d+)/,"'<b>$1</b>' <b>+$2</b>");
  return `<div class="ev ${e.kind}"><span class="ts num">${t}</span><span class="tx">${tx}</span></div>`}).join('');
 $('evtag').textContent=evs.length+' recent';}
function render(d){
 const ok=d.log_age_s!=null&&d.log_age_s>=0&&d.log_age_s<90;
 const dotEl=document.querySelector('.dot');
 if(dotEl)dotEl.className='dot'+(ok?'':(d.log_age_s<0?' dead':' warn'));
 const pill=$('pill'),h=d.health||{};
 if(h.ok_min===0&&h.fail_min>0){pill.textContent='crawls failing';pill.className='pill dead'}
 else if(!ok){pill.textContent='stale';pill.className='pill warn'}
 else{pill.textContent='live';pill.className='pill'}
 $('pageup').textContent=`page ${d.uptime_min||0}m · log ${d.log_age_s<0?'missing':d.log_age_s+'s'}`;
 const v=d.vault||{};
 if(v.rows!=null){tween('rows',fmt,v.rows);
  if(prevRows!=null&&v.rows>prevRows){const el=$('rowsdelta');
   el.textContent='+'+fmt(v.rows-prevRows);el.classList.remove('pop');void el.offsetWidth;el.classList.add('pop');}
  prevRows=v.rows;$('rowsub').innerHTML=`<b>${fmt(v.max_id)}</b> newest site id · ${fmt((v.max_id||0)-v.rows)} not yet mined`;}
 else $('rowsub').textContent=v.error||'';
 if(v.coverage_pct!=null){const p=v.coverage_pct;
  $('gring').style.strokeDasharray=`${p*2.26} 226`;
  $('gpct').textContent=p+'%';
  $('covsub').innerHTML='thinnest: '+(v.thin||[]).map(b=>`b${b.block*25}k(${fmt(b.have)})`).join(' ');}
 const vel=d.velocity||{};
 if(vel.per_min!=null)tween('rate',x=>'+'+(+x).toFixed(1),vel.per_min);
 $('ratesub').innerHTML=`rows/min · <b class="g">+${fmt(vel.h1)}</b> hour · <b class="b">+${fmt(vel.h24)}</b> 24h`;
 if(h.ok_min!=null||h.fail_min!=null){tween('health',x=>fmt(x),(h.ok_min||0));
  $('healthsub').innerHTML=`<b class="g">${h.ok_min||0}</b> ok/min · <b class="${(h.fail_min||0)>0?'r':'g'}">${h.fail_min||0}</b> fail/min`+
   ((h.ok_min===0&&h.fail_min>0)?' · <b class="r">crawl blocked!</b>':'');}
 const ck=d.clock;
 if(ck){const s=ck.skew_s,a=Math.abs(s);
  $('clockv').textContent=(s>0?'+':'')+s+'s';
  $('clockv').className='num '+(a>120?'r':a>30?'a':'g');
  $('clockbadge').title=a>30?'Clock drift! Signed search URLs will be REJECTED — resync Windows time':'PC clock vs mkvbase server time';}
 heatmap(v.blocks);
 chart(d.history);
 events(d.events);
 const f=d.fleet||{};
 if(f.tick){$('fleetv').innerHTML=`${f.tick.agents} <span class="g">agents</span>`;
  $('fleetsub').innerHTML=`done <b>${fmt(f.tick.done)}</b> · queued <b>${fmt(f.tick.queued)}</b> · rows mined <b>${fmt(f.tick.rows)}</b>`;
  $('fleettag').textContent='crawler uptime '+(f.uptime_min||0)+'m · session '+(f.session||'?');}
 const lanes=f.lanes||{};
 $('lanes').innerHTML=Object.entries(lanes).map(([k,x])=>
  `<span class="lane">${k} <b>${kk(x)}</b></span>`).join('')||'<span class="lane">waiting for fleet tick…</span>';
 const t=d.idgap||{};
 $('fleetsub').innerHTML+=` · idgap <b class="p">+${fmt(t.new_rows)}</b>`;
 const cols=['#3ddc84','#6ea8ff','#b48cff','#ffb454','#ff5c69','#54d6ff'];
 $('srcbar').innerHTML=(d.sources||[]).map((s,i)=>
  `<div style="width:${s.pct}%;background:${cols[i%6]}" title="${s.name}: ${fmt(s.n)}"></div>`).join('');
 $('srcleg').textContent=(d.sources||[]).map(s=>`${s.name} ${s.pct}%`).join(' · ');
 $('rowsfeed').innerHTML=(d.newest||[]).map(x=>
  `<tr${x.id!==lastFeedId&&lastFeedId!=null&&x.id>lastFeedId?' class="new"':''}>`+
  `<td class="num">${x.id??''}</td><td class="t">${(x.title||'').replace(/</g,'&lt;')}</td>`+
  `<td class="${x.src==='idgap'?'p':'b'}">${x.src||''}</td></tr>`).join('')
  ||'<tr><td colspan=3 class=s>empty</td></tr>';
 if(d.newest&&d.newest[0])lastFeedId=d.newest[0].id;
 const r=d.render;
 if(r){$('rendersub').innerHTML=r.ok?
  `render serve: <b>${fmt(r.rows)}</b> rows · up <b>${r.uptime}</b> · ${r.mem_mb} MB`:
  `render serve: <b class="r">unreachable</b>`;}
 const lg=(d.log||[]).map(l=>`<div class="${cls(l)}">${l.replace(/</g,'&lt;')}</div>`).join('');
 if(lg!==prevLog){$('log').innerHTML=lg;prevLog=lg;$('log').scrollTop=$('log').scrollHeight;}}
const es=new EventSource('/api/stream');
es.onmessage=e=>{try{render(JSON.parse(e.data))}catch(_){}};
es.onopen=()=>{$('pill').textContent='live';$('pill').className='pill'};
es.onerror=()=>{$('pill').textContent='reconnecting…';$('pill').className='pill warn';
 const de=document.querySelector('.dot');if(de)de.className='dot warn'};
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return _HTML


def main() -> None:
    import uvicorn
    _start_sampler()
    print(f"[dashboard] http://127.0.0.1:{PORT}  (log: {LOG_PATH})", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
