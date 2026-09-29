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
from collections import deque

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

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
            thin.sort(key=lambda x: x["n"])
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
        "site": _site_state(),
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
:root{--bg:#05070c;--panel:#0b101a;--panel2:#0d1320;--line:rgba(154,172,207,.10);
--line2:rgba(154,172,207,.22);--txt:#eaeff8;--dim:#8a95ab;--faint:#5d6679;
--grn:#34d399;--amb:#fbbf24;--red:#f87171;--blu:#60a5fa;--pur:#a78bfa;--cyn:#22d3ee;
--mono:'Cascadia Code',ui-monospace,'SF Mono',Consolas,monospace}
*{box-sizing:border-box;margin:0}
html{scrollbar-color:#273043 transparent}
body{background:radial-gradient(1000px 600px at 88% -12%,rgba(96,140,255,.08),transparent 62%),
 var(--bg);color:var(--txt);font:14px/1.5 'Segoe UI',system-ui,-apple-system,Roboto,sans-serif;
 min-height:100vh;-webkit-font-smoothing:antialiased}
.num{font-family:var(--mono);font-variant-numeric:tabular-nums;letter-spacing:-.3px}
header{position:sticky;top:0;z-index:20;display:flex;align-items:center;gap:11px;
 padding:0 22px;height:54px;background:rgba(5,7,12,.82);backdrop-filter:blur(14px);
 border-bottom:1px solid var(--line)}
.logo{width:26px;height:26px;border-radius:7px;background:linear-gradient(135deg,#60a5fa,#a78bfa);
 display:grid;place-items:center;font-weight:800;font-size:13px;color:#05070c;flex:none}
.brand{font-weight:650;font-size:14.5px}
.brand small{color:var(--dim);font-weight:400;margin-left:7px;font-size:12px}
.dot{width:8px;height:8px;border-radius:50%;background:var(--grn);flex:none;
 animation:p 2s infinite}
.dot.warn{background:var(--amb);animation:none}.dot.dead{background:var(--red);animation:none}
@keyframes p{0%{box-shadow:0 0 0 0 rgba(52,211,153,.45)}70%{box-shadow:0 0 0 7px transparent}
100%{box-shadow:0 0 0 0 transparent}}
.pill{font-size:10px;font-weight:700;letter-spacing:1.3px;padding:3px 10px;border-radius:99px;
 border:1px solid rgba(52,211,153,.35);color:var(--grn);text-transform:uppercase}
.pill.warn{border-color:rgba(251,191,36,.4);color:var(--amb)}
.pill.dead{border-color:rgba(248,113,113,.45);color:var(--red)}
#hdr-right{margin-left:auto;display:flex;gap:10px;color:var(--dim);font-size:12px;align-items:center}
.badge{display:flex;gap:6px;align-items:center;font-size:11.5px;padding:3px 10px;
 border-radius:8px;border:1px solid var(--line);background:rgba(11,16,26,.6)}
main{max-width:1240px;margin:0 auto;padding:18px 20px 30px}
.grid{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:14px}
@media(max-width:1080px){.grid{grid-template-columns:repeat(2,1fr)}}
@media(max-width:620px){.grid{grid-template-columns:1fr}}
.card{background:linear-gradient(180deg,var(--panel2),var(--panel));border:1px solid var(--line);
 border-radius:14px;padding:16px 18px 14px;min-width:0}
.card h3{font-size:10.5px;text-transform:uppercase;letter-spacing:1.2px;color:var(--dim);
 margin-bottom:10px;font-weight:600;display:flex;justify-content:space-between;gap:8px}
.card h3 .tag{text-transform:none;letter-spacing:.1px;font-weight:400;font-size:10.5px;
 color:var(--faint);text-align:right}
.v{font-size:29px;font-weight:650;color:var(--txt);line-height:1.05;letter-spacing:-.8px}
.v small{font-size:15px;font-weight:500;color:var(--dim);letter-spacing:0;margin-left:2px}
.s{font-size:12px;color:var(--dim);margin-top:6px;line-height:1.5}
.s b{color:var(--txt);font-weight:600}
.g{color:var(--grn)}.a{color:var(--amb)}.b{color:var(--blu)}.p{color:var(--pur)}.r{color:var(--red)}.c{color:var(--cyn)}
.chips{display:flex;flex-wrap:wrap;gap:5px;margin-top:8px}
.chip{font-size:11px;padding:2px 8px;border-radius:6px;border:1px solid var(--line);
 color:var(--dim);background:rgba(154,172,207,.04)}
.chip b{color:var(--txt);font-weight:600}
.delta{display:inline-block;margin-left:8px;font-size:12px;color:var(--grn);opacity:0;
 transform:translateY(4px)}
.delta.pop{animation:pop 2.6s ease-out}
@keyframes pop{0%{opacity:0;transform:translateY(5px)}15%{opacity:1;transform:none}
80%{opacity:1}100%{opacity:0}}
.statrow{display:flex;align-items:center;gap:8px;margin-top:7px;font-size:12px;color:var(--dim)}
.statrow .lab{width:64px;flex:none}
.statrow .bar{flex:1;height:4px;border-radius:99px;background:rgba(154,172,207,.1);overflow:hidden}
.statrow .bar i{display:block;height:100%;border-radius:99px;transition:width .7s ease}
.statrow .val{width:70px;flex:none;text-align:right;color:var(--txt);font-weight:600}
.full{grid-column:1/-1}
/* gauge */
.gwrap{display:flex;align-items:center;gap:16px}
.gwrap svg{flex:none}
.gpct{font-family:var(--mono);font-size:20px;font-weight:700;fill:var(--txt);letter-spacing:-.5px}
.gsub{fill:var(--dim);font-size:9px;letter-spacing:.6px;text-transform:uppercase}
.ring{transition:stroke-dasharray .9s ease}
/* heatmap */
.hmwrap{position:relative}
.hm{position:relative;height:120px;display:flex;align-items:flex-end;gap:5px;
 margin-top:6px;margin-right:44px}
.hline{position:absolute;left:0;right:0;border-top:1px dashed rgba(154,172,207,.13)}
.hline span{position:absolute;right:-40px;top:-6px;font-size:9.5px;color:var(--faint);
 font-family:var(--mono)}
.htarget{position:absolute;left:0;right:0;border-top:1px dashed rgba(251,191,36,.5);z-index:2}
.htarget span{position:absolute;right:-40px;top:-6px;font-size:9.5px;color:var(--amb);
 font-family:var(--mono)}
.hcol{flex:1;min-width:14px;max-width:44px;height:100%;display:flex;flex-direction:column;
 justify-content:flex-end;position:relative;z-index:1}
.hfill{width:100%;border-radius:5px 5px 1px 1px;opacity:.92;min-height:3px;
 transition:height .8s ease}
.hfill:hover{opacity:1}
.hlabels{display:flex;gap:5px;margin-top:5px;margin-right:44px}
.hlabels span{flex:1;min-width:14px;max-width:44px;text-align:center;font-size:9px;
 color:var(--faint);font-family:var(--mono)}
/* chart */
.chartwrap{position:relative;padding-bottom:20px}
.chartwrap svg{display:block;width:100%;height:170px}
.ylab{position:absolute;right:0;font-size:10px;color:var(--faint);font-family:var(--mono);
 transform:translateY(-50%);background:var(--panel2);padding:1px 4px;border-radius:4px}
.xlab{position:absolute;left:0;right:0;bottom:0;display:flex;justify-content:space-between;
 font-size:10px;color:var(--faint);font-family:var(--mono)}
/* feeds */
.two{display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px}
@media(max-width:900px){.two{grid-template-columns:1fr}}
table{width:100%;border-collapse:collapse;font-size:12.5px}
td,th{padding:5px 8px;border-bottom:1px solid rgba(154,172,207,.06);text-align:left;
 white-space:nowrap}
th{color:var(--faint);font-weight:500;text-transform:uppercase;font-size:9.5px;letter-spacing:1px}
td.t{white-space:normal;max-width:0;width:100%;overflow:hidden;text-overflow:ellipsis;color:#c6d0e2}
tr.new td{animation:rowin 2.2s ease-out}
@keyframes rowin{0%{background:rgba(52,211,153,.14)}100%{background:transparent}}
.feed{max-height:288px;overflow-y:auto;font-size:12.5px}
.ev{display:flex;gap:9px;padding:4px 8px;border-radius:7px;align-items:baseline;
 border-left:2px solid transparent}
.ev .ts{color:var(--faint);font-size:10.5px;flex:none;width:52px;font-family:var(--mono)}
.ev .tx{color:#c6d0e2;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ev.ok{border-left-color:var(--blu)} .ev.ok .tx b{color:var(--blu);font-weight:600}
.ev.idgap{border-left-color:var(--pur)} .ev.idgap .tx b{color:var(--pur);font-weight:600}
.ev.push{border-left-color:var(--grn)} .ev.push .tx{color:var(--grn)}
.ev.fail{border-left-color:var(--red);background:rgba(248,113,113,.06)}
.ev.fail .tx{color:#fda4ab}
.ev.info{border-left-color:var(--cyn)} .ev.info .tx{color:var(--cyn)}
.lg{font-size:11.5px;line-height:1.55;max-height:288px;overflow-y:auto;
 background:rgba(2,4,9,.6);border:1px solid var(--line);border-radius:10px;
 padding:9px 12px;font-family:var(--mono)}
.lg .d{color:var(--faint)}.lg .e{color:var(--red)}.lg .i{color:var(--grn)}.lg .m{color:var(--blu)}
.lanes{display:flex;flex-wrap:wrap;gap:5px;margin-top:8px}
.lane{font-size:11px;padding:2px 9px;border-radius:99px;border:1px solid var(--line);
 color:var(--dim);background:rgba(154,172,207,.04)}
.lane b{color:var(--txt);font-weight:600}
.meter{display:flex;height:10px;border-radius:99px;overflow:hidden;margin-top:9px;
 background:rgba(154,172,207,.08)}
.meter div{height:100%;transition:width .8s ease}
footer{color:var(--faint);font-size:11.5px;text-align:center;margin-top:8px}
footer a{color:var(--blu);text-decoration:none}
@media(max-width:620px){.v{font-size:24px}#hdr-right{display:none}}
</style></head><body>
<header>
 <span class="dot"></span>
 <div class="logo">M</div>
 <span class="brand">mkvbase <small>control room</small></span>
 <span class="pill" id="pill">connecting</span>
 <div id="hdr-right">
  <span class="badge" id="clockbadge" title="PC clock vs mkvbase server time — drift kills signed search URLs">clock <span class="num" id="clockv">—</span></span>
  <span class="badge num" id="pageup"></span>
 </div>
</header>
<main>
 <div class="grid">
  <div class="card"><h3>Vault rows <span class="tag">mongo atlas</span></h3>
   <div class="v num" id="rows">—</div><span class="delta num" id="rowsdelta"></span>
   <div class="chips" id="rowchips"></div></div>
  <div class="card"><h3>Site coverage</h3>
   <div class="gwrap">
    <svg width="108" height="108" viewBox="0 0 120 120">
     <defs><linearGradient id="gg" x1="0" y1="0" x2="1" y2="1">
      <stop offset="0" stop-color="#60a5fa"/><stop offset="1" stop-color="#a78bfa"/></linearGradient></defs>
     <circle cx="60" cy="60" r="48" fill="none" stroke="rgba(154,172,207,.12)" stroke-width="10"/>
     <circle class="ring" id="gring" cx="60" cy="60" r="48" fill="none" stroke="url(#gg)"
      stroke-width="10" stroke-linecap="round" stroke-dasharray="0 302"
      transform="rotate(-90 60 60)"/>
     <text class="gpct" id="gpct" x="60" y="58" text-anchor="middle">—</text>
     <text class="gsub" x="60" y="76" text-anchor="middle">of ids</text>
    </svg>
    <div style="min-width:0">
     <div class="s" id="covsub"></div>
     <div class="chips" id="covchips"></div></div></div></div>
  <div class="card"><h3>Push velocity</h3>
   <div class="v num" id="rate">—<small> rows/min</small></div>
   <div class="statrow"><span class="lab">last hour</span><span class="bar"><i id="barh1" style="background:var(--blu)"></i></span><span class="val num" id="vh1">—</span></div>
   <div class="statrow"><span class="lab">24 hours</span><span class="bar"><i id="barh24" style="background:var(--pur)"></i></span><span class="val num" id="vh24">—</span></div>
   <div class="s" id="ratesub"></div></div>
  <div class="card"><h3>Crawl health <span class="tag">per min · 5m window</span></h3>
   <div class="v num" id="health">—<small> ok/min</small></div>
   <div class="statrow"><span class="lab">success</span><span class="bar"><i id="barok" style="background:var(--grn)"></i></span><span class="val num" id="vok">—</span></div>
   <div class="statrow"><span class="lab">failure</span><span class="bar"><i id="barfail" style="background:var(--red)"></i></span><span class="val num" id="vfail">—</span></div>
   <div class="s" id="healthsub"></div></div>
 </div>

 <div class="card full">
  <h3>Id-block fill rate <span class="tag">each bar = 25,000 site ids · dashed line = 60% target</span></h3>
  <div class="hmwrap">
   <div class="hm" id="hm"></div>
   <div class="hlabels" id="hmlabels"></div></div>
  <div class="chips" style="margin-top:10px">
   <span class="chip"><i style="display:inline-block;width:8px;height:8px;border-radius:2px;background:var(--red);margin-right:5px"></i>&lt;15% full</span>
   <span class="chip"><i style="display:inline-block;width:8px;height:8px;border-radius:2px;background:var(--amb);margin-right:5px"></i>15–40%</span>
   <span class="chip"><i style="display:inline-block;width:8px;height:8px;border-radius:2px;background:var(--cyn);margin-right:5px"></i>40–60%</span>
   <span class="chip"><i style="display:inline-block;width:8px;height:8px;border-radius:2px;background:var(--grn);margin-right:5px"></i>≥60% done</span>
   <span class="chip" id="hmthin" style="display:none"></span></div></div>

 <div class="card full">
  <h3>Vault growth — 24h <span class="tag num" id="chartsum"></span></h3>
  <div class="chartwrap">
   <div class="ylab" style="top:6px" id="ylmax">—</div>
   <div class="ylab" style="top:50%" id="ylmid">—</div>
   <div class="ylab" style="bottom:22px" id="ylmin">—</div>
   <svg id="chart" viewBox="0 0 620 150" preserveAspectRatio="none">
    <defs><linearGradient id="gr" x1="0" y1="0" x2="0" y2="1">
     <stop offset="0" stop-color="#60a5fa" stop-opacity=".32"/>
     <stop offset="1" stop-color="#60a5fa" stop-opacity="0"/></linearGradient></defs>
   </svg>
   <div class="xlab"><span>24h ago</span><span>now</span></div></div></div>

 <div class="two">
  <div class="card"><h3>Live events <span class="tag" id="evtag"></span></h3>
   <div class="feed" id="feed"></div></div>
  <div class="card"><h3>Newest pushes → mongo</h3>
   <div style="max-height:288px;overflow-y:auto">
   <table><thead><tr><th>id</th><th>title</th><th>src</th></tr></thead>
   <tbody id="rowsfeed"></tbody></table></div></div>
 </div>

 <div class="two">
  <div class="card"><h3>Fleet <span class="tag" id="fleettag"></span></h3>
   <div class="v" id="fleetv">—</div>
   <div class="s" id="fleetsub"></div>
   <div class="lanes" id="lanes"></div>
   <h3 style="margin-top:16px">Push sources <span class="tag" id="srcleg"></span></h3>
   <div class="meter" id="srcbar"></div>
   <div class="s" id="rendersub" style="margin-top:10px"></div></div>
  <div class="card"><h3>Crawler log</h3><div class="lg" id="log"></div></div>
 </div>
</main>
<footer>streamed live over server-sent events · read-only · <a href="/api/live">/api/live</a> ·
 stop/start via STOP-all-crawlers.cmd / START-auto-everything.cmd</footer>
<script>
const $=id=>document.getElementById(id);
const fmt=n=>n==null?'—':Math.round(n).toLocaleString('en-US');
let prevRows=null,lastFeedId=null,prevLog='',prevHm='',tweenSt={};
function tween(id,set,target,dec=0){const el=$(id);
 const from=(tweenSt[id]===undefined?target:tweenSt[id]);tweenSt[id]=target;
 if(Math.abs(from-target)<1e-9){set(el,target);return}
 const t0=performance.now(),D=500;
 (function fr(t){const p=Math.min(1,(t-t0)/D),e=1-Math.pow(1-p,3);
  set(el,from+(target-from)*e);if(p<1)requestAnimationFrame(fr)})(t0);}
const setNum=(el,x)=>el.firstChild?el.firstChild.nodeValue=fmt(x):el.textContent=fmt(x);
function cls(l){if(/\bFAIL\b|\berror\b|Traceback/i.test(l))return'e';
 if(/\[alive\]|ok |READY|online|seeded/.test(l))return'i';
 if(/^\[discovery/.test(l))return'm';return'd';}
function chart(h){const s=$('chart');const base=s.innerHTML.split('<path')[0];
 if(!h||h.length<2){$('chartsum').textContent='collecting history…';s.innerHTML=base;return}
 const now=h[h.length-1][0],cut=now-86400e3;const pts=h.filter(p=>p[0]>=cut);
 if(pts.length<2){$('chartsum').textContent='collecting history…';s.innerHTML=base;return}
 const vs=pts.map(p=>p[1]);
 let mn=Math.min(...vs),mx=Math.max(...vs);const span=(mx-mn)||1;mn-=span*.06;mx+=span*.06;
 const L=2,R=612,T=10,B=140,n=pts.length;
 const X=i=>L+(i/(n-1))*(R-L),Y=v=>T+(1-(v-mn)/(mx-mn))*(B-T);
 let d='M'+X(0).toFixed(1)+' '+Y(vs[0]).toFixed(1);
 for(let i=1;i<n;i++)d+=' L'+X(i).toFixed(1)+' '+Y(vs[i]).toFixed(1);
 const grid=y=>`<line x1="${L}" y1="${y}" x2="${R}" y2="${y}" stroke="rgba(154,172,207,.09)" stroke-dasharray="3 4"/>`;
 s.innerHTML=base+grid(T)+grid((T+B)/2)+grid(B)+
  `<path d="${d} L${R} ${B} L${L} ${B} Z" fill="url(#gr)"/>`+
  `<path d="${d}" fill="none" stroke="#60a5fa" stroke-width="1.6"/>`+
  `<circle cx="${X(n-1)}" cy="${Y(vs[n-1])}" r="3" fill="#60a5fa"/>`+
  `<circle cx="${X(n-1)}" cy="${Y(vs[n-1])}" r="7" fill="#60a5fa" opacity=".22"/>`;
 $('ylmax').textContent=fmt(mx);$('ylmid').textContent=fmt((mx+mn)/2);$('ylmin').textContent=fmt(mn);
 const gain=vs[n-1]-vs[0];
 $('chartsum').textContent=gain>0?`+${fmt(gain)} rows / 24h · now ${fmt(vs[n-1])}`:
  `now ${fmt(vs[n-1])}`;}
function heatmap(blocks){if(!blocks)return;const ser=JSON.stringify(blocks);
 if(ser===prevHm)return;prevHm=ser;
 const band=p=>p>=60?'var(--grn)':p>=40?'var(--cyn)':p>=15?'var(--amb)':'var(--red)';
 $('hm').innerHTML=
  `<div class="hline" style="bottom:25%"><span>25</span></div>`+
  `<div class="hline" style="bottom:50%"><span>50</span></div>`+
  `<div class="hline" style="bottom:75%"><span>75</span></div>`+
  `<div class="htarget" style="bottom:60%"><span>60</span></div>`+
  blocks.map(b=>{const p=Math.min(100,Math.round(100*b.n/25000));
   return `<div class="hcol" title="ids ${fmt(b.block*25000)}–${fmt((b.block+1)*25000-1)} · ${fmt(b.n)}/25,000 (${p}%)">`+
    `<div class="hfill" style="height:${p}%;background:${band(p)}"></div></div>`}).join('');
 $('hlabels').innerHTML=blocks.map(b=>`<span>${b.block*25}k</span>`).join('');
 const thin=(blocks.map(b=>({b,p:Math.round(100*b.n/25000)})).filter(x=>x.p<60)
  .sort((a,b)=>a.p-b.p).slice(0,3));
 const ht=$('hmthin');
 if(thin.length){ht.style.display='';
  ht.innerHTML='thinnest: '+thin.map(x=>`<b class="r">${x.b.block*25}k · ${x.p}%</b>`).join(' · ')}
 else ht.style.display='none';}
function events(evs){if(!evs)return;
 const now=Date.now()/1000;
 $('feed').innerHTML=evs.slice().reverse().map(e=>{
  const rel=now-e.ts,rs=rel<60?`${Math.round(rel)}s`:`${Math.floor(rel/60)}m`;
  const tx=e.text.replace(/</g,'&lt;').replace(/'([^']*)' \+(\d+)/,"'<b>$1</b>' <b>+$2</b>");
  return `<div class="ev ${e.kind}"><span class="ts">${rs}</span><span class="tx">${tx}</span></div>`}).join('');
 $('evtag').textContent=evs.length+' recent';}
function render(d){
 const ok=d.log_age_s!=null&&d.log_age_s>=0&&d.log_age_s<90;
 const dotEl=document.querySelector('.dot');
 if(dotEl)dotEl.className='dot'+(ok?'':(d.log_age_s<0?' dead':' warn'));
 const pill=$('pill'),h=d.health||{ok_min:0,fail_min:0};
 if(h.ok_min===0&&h.fail_min>0){pill.textContent='crawls failing';pill.className='pill dead'}
 else if(!ok){pill.textContent='stale';pill.className='pill warn'}
 else{pill.textContent='live';pill.className='pill'}
 $('pageup').textContent=`page ${d.uptime_min||0}m · log ${d.log_age_s<0?'missing':d.log_age_s+'s'}`;
 const v=d.vault||{};
 if(v.rows!=null){tween('rows',setNum,v.rows);
  if(prevRows!=null&&v.rows>prevRows){const el=$('rowsdelta');
   el.textContent='+'+fmt(v.rows-prevRows);el.classList.remove('pop');void el.offsetWidth;el.classList.add('pop');}
  prevRows=v.rows;
  const st=d.site||{};const smax=st.site_max_id||v.max_id;
  const lag=Math.max(0,smax-v.max_id);
  const lagCls=lag>60?'r':lag>15?'a':'g';
  const polled=st.seen_at?Math.round(Date.now()/1000-st.seen_at):null;
  $('rowchips').innerHTML=`<span class="chip" title="newest id in the vault">vault id <b>${fmt(v.max_id)}</b></span>`+
   `<span class="chip" title="newest id seen on mkvbase.site${polled!=null?` (polled ${polled}s ago)`:''}">site id <b>${fmt(st.site_max_id||null)}</b></span>`+
   `<span class="chip" title="uploads on the site the vault has not caught yet">lag <b class="${lagCls}">${fmt(lag)}</b></span>`+
   `<span class="chip">not yet mined <b class="a">${fmt(smax-v.rows)}</b></span>`;}
 else $('rowchips').innerHTML=`<span class="chip r">${v.error||''}</span>`;
 if(v.coverage_pct!=null){const p=v.coverage_pct;
  $('gring').style.strokeDasharray=`${(p/100*301.59).toFixed(1)} 302`;
  $('gpct').textContent=p.toFixed(1)+'%';
  $('covsub').innerHTML='of all site ids';
  $('covchips').innerHTML=(v.thin||[]).slice(0,4).map(b=>
   `<span class="chip">b${b.block*25}k <b>${fmt(b.have)}</b></span>`).join('');}
 const vel=d.velocity||{per_min:null,h1:null,h24:null};
 if(vel.per_min!=null)tween('rate',(el,x)=>{el.innerHTML=(x>=0?'+':'')+
  (+x).toFixed(1)+'<small> rows/min</small>'},vel.per_min);
 const mx=Math.max(vel.h1||0,vel.h24||0,1);
 $('barh1').style.width=Math.min(100,(vel.h1||0)/mx*100)+'%';
 $('barh24').style.width=Math.min(100,(vel.h24||0)/mx*100)+'%';
 $('vh1').textContent='+'+fmt(vel.h1);$('vh24').textContent='+'+fmt(vel.h24);
 $('ratesub').textContent=vel.per_min==null?'building history (5-min samples)…':'rolling 30-min average';
 const okm=h.ok_min||0,fm=h.fail_min||0,den=Math.max(okm+fm,1);
 tween('health',setNum,okm);
 $('barok').style.width=(okm/den*100)+'%';$('barfail').style.width=(fm/den*100)+'%';
 $('vok').textContent=String(okm);$('vfail').textContent=String(fm);
 $('healthsub').innerHTML=(okm===0&&fm>0)?'<b class="r">all crawls failing — check clock badge / session</b>':
  (fm>0?`<span class="a">${fm} failing — usually transient challenges</span>`:'all lanes healthy');
 const ck=d.clock;
 if(ck){const s=ck.skew_s,a=Math.abs(s);
  $('clockv').textContent=(s>0?'+':'')+s+'s';
  $('clockv').style.color=a>120?'var(--red)':a>30?'var(--amb)':'var(--grn)';}
 heatmap(v.blocks);
 chart(d.history);
 events(d.events);
 const f=d.fleet||{};
 if(f.tick){$('fleetv').innerHTML=`${f.tick.agents} <span class="g">agents</span>`;
  $('fleetsub').innerHTML=`done <b>${fmt(f.tick.done)}</b> · queued <b>${fmt(f.tick.queued)}</b> · rows mined <b>${fmt(f.tick.rows)}</b>`;
  $('fleettag').textContent=`crawler up ${(f.uptime_min||0)}m · session ${f.session||'?'}`;}
 const lanes=f.lanes||{};
 $('lanes').innerHTML=Object.entries(lanes).map(([k,x])=>
  `<span class="lane">${k} <b>${x>=1000?(x/1000).toFixed(1)+'k':x}</b></span>`).join('');
 const t=d.idgap||{};
 if(t.terms)$('fleetsub').innerHTML+=` · idgap <b class="p">+${fmt(t.new_rows)}</b>`;
 const cols=['#34d399','#60a5fa','#a78bfa','#fbbf24','#f87171','#22d3ee'];
 $('srcbar').innerHTML=(d.sources||[]).map((s,i)=>
  `<div style="width:${s.pct}%;background:${cols[i%6]}" title="${s.name}: ${fmt(s.n)} (${s.pct}%)"></div>`).join('');
 $('srcleg').textContent=(d.sources||[]).map(s=>`${s.name} ${s.pct}%`).join(' · ');
 $('rowsfeed').innerHTML=(d.newest||[]).map(x=>
  `<tr${x.id!==lastFeedId&&lastFeedId!=null&&x.id>lastFeedId?' class="new"':''}>`+
  `<td class="num">${x.id??''}</td><td class="t">${(x.title||'').replace(/</g,'&lt;')}</td>`+
  `<td class="${x.src==='idgap'?'p':'b'}">${x.src||''}</td></tr>`).join('');
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
