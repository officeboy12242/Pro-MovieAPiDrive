"""Live vault dashboard — real-time page showing the crawler fleet working:
rows pushed to Mongo, 24h growth chart, per-lane activity, live feed of newest
pushes, and the crawler log.

  python -m app.dashboard            (env: MKV_DASHBOARD_PORT, default 8766)

Transport: server-sent events (/api/stream). ONE sampler thread polls Mongo +
log every 3s and publishes a shared snapshot; every connected browser gets it
pushed instantly - no polling, no refresh. History of row counts is persisted
to data/vault_history.json so velocity and the chart survive restarts.

Read-only. Safe to run next to the API server and the fleet.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import threading
import time

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

DATA_DIR = os.getenv("MKV_DATA_DIR", "data")
PORT = int(os.getenv("MKV_DASHBOARD_PORT", "8766"))
LOG_PATH = os.path.join(DATA_DIR, "pusher.log")
IDGAP_STATE = os.path.join(DATA_DIR, "idgap_state.json")
HIST_PATH = os.path.join(DATA_DIR, "vault_history.json")
RENDER_URL = os.getenv("MKV_RENDER_URL", "https://pro-movieapidrive.onrender.com")
BLOCK = 25000  # idgap block size (matches app.idgap default)
TICK_S = 3.0
HIST_EVERY_S = 300.0          # persist one history point every 5 min
HIST_MAX_AGE_S = 8 * 86400.0  # keep ~8 days of history

app = FastAPI(docs_url=None, redoc_url=None)
_started = time.time()
_latest: dict = {}
_hist: list[list[float]] = []  # [ts, rows] persisted samples
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
        thin = []
        try:
            for r in col.aggregate([
                    {"$match": {"id": {"$ne": None}}},
                    {"$group": {"_id": {"$floor": {"$divide": ["$id", BLOCK]}},
                                "n": {"$sum": 1}}}]):
                if r["n"] < BLOCK * 0.6:
                    thin.append({"block": int(r["_id"]), "have": r["n"]})
            thin.sort(key=lambda x: x["have"])
            out["thin"] = thin[:5]
        except Exception:
            pass
        return out
    except Exception as e:
        return {"rows": None, "error": type(e).__name__}


def _sources(col, n: int = 2000) -> list[dict]:
    """Push-source mix over the newest n rows (_src prefix before ':')."""
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


def _newest(col, n: int = 14) -> list[dict]:
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


def _log_tail(n: int = 60) -> tuple[list[str], float]:
    try:
        age = time.time() - os.path.getmtime(LOG_PATH)
        with open(LOG_PATH, "rb") as f:
            f.seek(max(0, os.path.getsize(LOG_PATH) - 200_000))
            lines = f.read().decode("utf-8", "replace").splitlines()
        return [l[:220] for l in lines[-n:]], age
    except Exception:
        return [], -1


_ALIVE = re.compile(r"up (\d+)m(\d+)s \| session=(\w+)")
_TICK = re.compile(r"done=(\d+) queued=(\d+).*?rows=(\d+).*?agents=(\d+)")


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
        if len(out) >= 2:
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


# ----------------------------------------------------------------- history
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
    """Append one persisted sample (throttled to HIST_EVERY_S) + save."""
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
    """rows/min over the last 30 min, plus hourly/daily deltas from history."""
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


# ----------------------------------------------------------------- sampler
def _sample_once(n_tick: int) -> None:
    global _latest
    col = _mongo_col_cached()
    lines, log_age = _log_tail()
    snap = {
        "ts": time.time(),
        "uptime_min": int((time.time() - _started) / 60),
        "vault": _vault(col),
        "fleet": _fleet_from_log(lines),
        "idgap": _idgap_state(),
        "newest": _newest(col),
        "log": lines,
        "log_age_s": round(log_age) if log_age >= 0 else -1,
        "render": None,  # filled every 6th tick below
    }
    if snap["vault"].get("rows") is not None:
        _hist_append(time.time(), snap["vault"]["rows"])
    if n_tick % 4 == 0:
        snap["sources"] = _sources(col)
    else:
        snap["sources"] = _latest.get("sources") or []
    if n_tick % 6 == 0:
        snap["render"] = _render_health()
    else:
        snap["render"] = _latest.get("render")
    snap["velocity"] = _velocity()
    snap["history"] = _hist[-600:]
    _latest = snap


def _sampler() -> None:
    n = 0
    while True:
        try:
            _sample_once(n)
        except Exception:
            pass
        n += 1
        time.sleep(TICK_S)


def _start_sampler() -> None:
    global _sampler_started
    if _sampler_started:
        return
    _sampler_started = True
    _hist_load()
    threading.Thread(target=_sampler, daemon=True, name="dash-sampler").start()


# Sampler starts lazily via _start_sampler() from main() and the routes
# (idempotent); this FastAPI version removed add_event_handler/startup hooks.


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
<title>mkvbase vault — live</title><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{--bg0:#07090f;--bg1:#0d1117;--card:rgba(255,255,255,.035);--bd:rgba(240,246,252,.09);
--fg:#e6edf3;--dim:#7d8590;--grn:#3fb950;--amb:#d29922;--red:#f85149;
--blu:#58a6ff;--pur:#bc8cff;--grad:linear-gradient(92deg,#4f9cff,#9d6bff)}
*{box-sizing:border-box;margin:0}
html{scrollbar-color:#30363d var(--bg0)}
body{background:radial-gradient(1200px 700px at 80% -10%,rgba(88,166,255,.09),transparent),
 var(--bg0);color:var(--fg);font:14px/1.5 -apple-system,'Segoe UI',system-ui,Roboto,sans-serif;
 padding:0 0 30px;min-height:100vh}
.num{font-family:ui-monospace,Consolas,monospace;font-variant-numeric:tabular-nums}
header{position:sticky;top:0;z-index:9;display:flex;align-items:center;gap:12px;
 padding:14px 22px;background:rgba(7,9,15,.8);backdrop-filter:blur(10px);
 border-bottom:1px solid var(--bd)}
.brand{font-size:16px;font-weight:700;letter-spacing:.2px}
.brand small{color:var(--dim);font-weight:400;margin-left:8px;font-size:12px}
.dot{width:9px;height:9px;border-radius:50%;background:var(--grn);
 box-shadow:0 0 0 0 rgba(63,185,80,.6);animation:p 1.8s infinite}
.dot.warn{background:var(--amb);animation:none;box-shadow:none}
.dot.dead{background:var(--red);animation:none;box-shadow:none}
@keyframes p{0%{box-shadow:0 0 0 0 rgba(63,185,80,.5)}70%{box-shadow:0 0 0 8px rgba(63,185,80,0)}
100%{box-shadow:0 0 0 0 rgba(63,185,80,0)}}
.pill{font-size:10.5px;font-weight:700;letter-spacing:1.2px;padding:4px 10px;border-radius:99px;
 border:1px solid var(--bd);color:var(--grn);text-transform:uppercase}
.pill.warn{color:var(--amb)} .pill.dead{color:var(--red)}
#hdr-right{margin-left:auto;color:var(--dim);font-size:12px}
main{max-width:1180px;margin:20px auto 0;padding:0 20px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(215px,1fr));gap:14px;margin-bottom:14px}
.card{background:var(--card);border:1px solid var(--bd);border-radius:14px;padding:16px 18px;
 backdrop-filter:blur(6px);transition:border-color .2s}
.card:hover{border-color:rgba(88,166,255,.35)}
.card h3{font-size:10.5px;text-transform:uppercase;letter-spacing:1.1px;color:var(--dim);
 margin-bottom:8px;display:flex;justify-content:space-between;align-items:center}
.v{font-size:30px;font-weight:800;background:var(--grad);-webkit-background-clip:text;
 background-clip:text;color:transparent;line-height:1.1}
.s{font-size:12px;color:var(--dim);margin-top:5px}
.s b{color:var(--fg);font-weight:600}
.g{color:var(--grn)}.a{color:var(--amb)}.b{color:var(--blu)}.p{color:var(--pur)}.r{color:var(--red)}
.delta{display:inline-block;margin-left:8px;font-size:13px;color:var(--grn);opacity:0}
.delta.pop{animation:pop 2.5s ease-out}
@keyframes pop{0%{opacity:0;transform:translateY(6px)}12%{opacity:1;transform:translateY(0)}
80%{opacity:1}100%{opacity:0}}
.covbar{height:5px;border-radius:99px;background:rgba(240,246,252,.08);margin-top:10px;overflow:hidden}
.covbar i{display:block;height:100%;width:0;background:var(--grad);border-radius:99px;
 transition:width .8s ease}
.chartcard{grid-column:1/-1}
svg text{fill:var(--dim);font-size:10px}
table{width:100%;border-collapse:collapse;font-size:12.5px}
td,th{padding:5px 8px;border-bottom:1px solid rgba(240,246,252,.06);text-align:left;white-space:nowrap}
th{color:var(--dim);font-weight:500;text-transform:uppercase;font-size:10px;letter-spacing:.8px}
td.t{white-space:normal;max-width:0;width:100%;overflow:hidden;text-overflow:ellipsis}
tr.new td{animation:rowin 2s ease-out}
@keyframes rowin{0%{background:rgba(63,185,80,.14)}100%{background:transparent}}
.bar{display:flex;height:12px;border-radius:99px;overflow:hidden;margin-top:8px;background:rgba(240,246,252,.06)}
.bar div{height:100%;transition:width .8s ease}
.lg{font-size:11.5px;line-height:1.55;max-height:300px;overflow-y:auto;background:rgba(1,4,9,.7);
 border:1px solid var(--bd);border-radius:10px;padding:10px 12px}
.lg .d{color:var(--dim)}.lg .e{color:var(--red)}.lg .i{color:var(--grn)}.lg .m{color:var(--blu)}
.two{display:grid;grid-template-columns:1.05fr .95fr;gap:14px}
@media(max-width:880px){.two{grid-template-columns:1fr}.v{font-size:26px}}
.chips{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px}
.chip{font-size:11px;padding:2px 9px;border-radius:99px;border:1px solid var(--bd);color:var(--dim)}
.chip b{color:var(--fg)}
footer{color:var(--dim);font-size:11.5px;text-align:center;margin-top:22px}
footer a{color:var(--blu);text-decoration:none}
</style></head><body>
<header>
 <span class="dot" id="dot"></span>
 <span class="brand">mkvbase <small>vault · live crawler</small></span>
 <span class="pill" id="pill">connecting</span>
 <span id="hdr-right" class="num"></span>
</header>
<main>
 <div class="grid">
  <div class="card"><h3>Vault rows <span style="text-transform:none;letter-spacing:0">mongo atlas</span></h3>
   <div class="v num" id="rows">—</div><span class="delta num" id="rowsdelta"></span>
   <div class="s" id="rowsub">connecting…</div><div class="covbar"><i id="covbar2"></i></div></div>
  <div class="card"><h3>Site id coverage</h3>
   <div class="v num" id="cov">—</div><div class="s" id="covsub"></div></div>
  <div class="card"><h3>Push velocity</h3>
   <div class="v num" id="rate">—</div><div class="s" id="ratesub">live rate</div></div>
  <div class="card"><h3>Discovery fleet</h3>
   <div class="v" id="disc">—</div><div class="s" id="discsub"></div></div>
  <div class="card"><h3>Idgap miner</h3>
   <div class="v num" id="idg">—</div><div class="s" id="idgsub"></div>
   <div class="chips" id="idgchips"></div></div>
  <div class="card"><h3>Render (serve)</h3>
   <div class="v num" id="render">—</div><div class="s" id="rendersub"></div></div>
 </div>

 <div class="grid chartcard"><div class="card">
  <h3>Vault growth — last 24h <span id="chartsum" class="num" style="text-transform:none;letter-spacing:0"></span></h3>
  <svg id="chart" viewBox="0 0 600 130" width="100%" height="130" preserveAspectRatio="none">
   <defs><linearGradient id="gr" x1="0" y1="0" x2="0" y2="1">
    <stop offset="0" stop-color="#58a6ff" stop-opacity=".45"/>
    <stop offset="1" stop-color="#58a6ff" stop-opacity="0"/></linearGradient></defs>
  </svg></div></div>

 <div class="grid chartcard"><div class="card">
  <h3>Push sources <span style="text-transform:none;letter-spacing:0" id="srcleg"></span></h3>
  <div class="bar" id="srcbar"></div></div></div>

 <div class="two">
  <div class="card"><h3>Newest pushes → mongo</h3>
   <table><thead><tr><th>id</th><th>title</th><th>src</th></tr></thead>
   <tbody id="feed"><tr><td colspan=3 class="s">connecting…</td></tr></tbody></table></div>
  <div class="card"><h3>Crawler log</h3><div class="lg num" id="log"></div></div>
 </div>
</main>
<footer>streamed over server-sent events — no refresh needed · read-only ·
 stop/start via STOP-all-crawlers.cmd / START-auto-everything.cmd · <a href="/api/live">/api/live</a></footer>
<script>
const $=id=>document.getElementById(id);
let prevRows=null,lastFeedId=null,prevLog='';
const fmt=n=>n==null?'—':Number(n).toLocaleString();
function cls(l){if(/\bFAIL\b|\berror\b|Traceback/i.test(l))return'e';
 if(/\[alive\]|ok |READY|online|seeded/.test(l))return'i';
 if(/^\[discovery/.test(l))return'm';return'd';}
function chart(h){const s=$('chart');
 if(!h||h.length<2){$('chartsum').textContent='collecting history…';
  s.innerHTML=s.innerHTML.split('<path')[0];return}
 const now=h[h.length-1][0],cut=now-86400e3;
 const pts=h.filter(p=>p[0]>=cut); if(pts.length<2){$('chartsum').textContent='collecting history…';return}
 const vs=pts.map(p=>p[1]),mn=Math.min(...vs),mx=Math.max(...vs);
 const W=600,H=130,pad=4,n=pts.length;
 const X=i=>i/(n-1)*W, Y=v=>H-pad-(v-mn)/((mx-mn)||1)*(H-2*pad-14);
 let d='M'+X(0).toFixed(1)+' '+Y(vs[0]).toFixed(1);
 for(let i=1;i<n;i++)d+=' L'+X(i).toFixed(1)+' '+Y(vs[i]).toFixed(1);
 const area=d+` L${W} ${H} L0 ${H} Z`;
 const g=(t,x,y)=>`<text x="${x}" y="${y}">${t}</text>`;
 s.innerHTML=s.innerHTML.split('<path')[0]+
  `<path d="${area}" fill="url(#gr)"/><path d="${d}" fill="none" stroke="#58a6ff" stroke-width="1.6"/>`+
  g(fmt(mn),4,H-pad-2)+g(fmt(mx),4,10)+g('24h ago',2,H-1)+g('now',W-26,H-1);
 const gain=vs[n-1]-vs[0];
 $('chartsum').textContent=gain>0?`+${fmt(gain)} rows in 24h`:'';}
function render(d){
 const ok=d.log_age_s!=null&&d.log_age_s>=0&&d.log_age_s<90;
 $('dot').className='dot'+(ok?'':(d.log_age_s<0?' dead':' warn'));
 const pill=$('pill');
 if(ok){pill.textContent='live';pill.className='pill'}
 else{pill.textContent='crawler stale';pill.className='pill warn'}
 $('hdr-right').textContent=`page ${d.uptime_min||0}m · log `+
  (d.log_age_s<0?'missing':d.log_age_s+'s ago');
 const v=d.vault||{};
 if(v.rows!=null){$('rows').textContent=fmt(v.rows);
  if(prevRows!=null&&v.rows>prevRows){const el=$('rowsdelta');
   el.textContent='+'+fmt(v.rows-prevRows);el.classList.remove('pop');
   void el.offsetWidth;el.classList.add('pop');}
  prevRows=v.rows;$('rowsub').innerHTML='<b>'+fmt(v.max_id)+'</b> site max id';}
 else $('rowsub').textContent=v.error||'';
 if(v.coverage_pct!=null){$('cov').textContent=v.coverage_pct+'%';
  $('covbar2').style.width=Math.min(100,v.coverage_pct)+'%';
  $('covsub').textContent='of site ids · thinnest: '+
   (v.thin||[]).map(b=>`b${b.block*25000}(${fmt(b.have)})`).join(' ');}
 const vel=d.velocity||{};
 if(vel.per_min!=null){$('rate').textContent='+'+vel.per_min;
  $('ratesub').innerHTML=`rows/min · <b>+${fmt(vel.h1)}</b> last hour · <b>+${fmt(vel.h24)}</b> 24h`;}
 else $('ratesub').textContent='building history (one point per 5 min)…';
 const f=d.fleet||{};
 if(f.tick){$('disc').innerHTML=`${f.tick.agents} <span class="g">agents</span>`;
  $('discsub').innerHTML=`done <b>${fmt(f.tick.done)}</b> · queued <b>${fmt(f.tick.queued)}</b> · rows <b>${fmt(f.tick.rows)}</b>`;}
 else $('disc').textContent='—';
 const t=d.idgap||{};
 if(t.terms){$('idg').textContent='+'+fmt(t.new_rows);
  $('idgsub').innerHTML=`<b>${fmt(t.terms)}</b> terms searched`;
  $('idgchips').innerHTML=Object.entries(t.term_stats||{})
   .sort((a,b)=>b[1].new-a[1].new).slice(0,5)
   .map(([k,x])=>`<span class="chip">${k.replace('era-','')} <b>+${fmt(x.new)}</b></span>`).join('');}
 else $('idg').textContent='—';
 const r=d.render;
 if(r){if(r.ok){$('render').textContent=fmt(r.rows);
   $('rendersub').innerHTML=`up <b>${r.uptime}</b> · ${r.mem_mb} MB · serve_only=${r.serve_only}`;}
  else $('render').textContent='unreachable';}
 const cols=['#3fb950','#58a6ff','#bc8cff','#d29922','#f85149','#39c5cf'];
 $('srcbar').innerHTML=(d.sources||[]).map((s,i)=>
  `<div style="width:${s.pct}%;background:${cols[i%6]}" title="${s.name}: ${fmt(s.n)}"></div>`).join('');
 $('srcleg').textContent=(d.sources||[]).map(s=>`${s.name} ${s.pct}%`).join(' · ');
 $('feed').innerHTML=(d.newest||[]).map(x=>
  `<tr${x.id!==lastFeedId&&lastFeedId!=null&&x.id>lastFeedId?' class="new"':''}>`+
  `<td class="num">${x.id??''}</td><td class="t">${(x.title||'').replace(/</g,'&lt;')}</td>`+
  `<td class="${x.src==='idgap'?'p':'b'}">${x.src||''}</td></tr>`).join('')
  ||'<tr><td colspan=3 class=s>empty</td></tr>';
 if(d.newest&&d.newest[0])lastFeedId=d.newest[0].id;
 const lg=(d.log||[]).map(l=>`<div class="${cls(l)}">${l.replace(/</g,'&lt;')}</div>`).join('');
 if(lg!==prevLog){$('log').innerHTML=lg;prevLog=lg;
  $('log').scrollTop=$('log').scrollHeight;}}
chart(null);
const es=new EventSource('/api/stream');
es.onmessage=e=>{try{render(JSON.parse(e.data))}catch(_){}};
es.onopen=()=>{$('pill').textContent='live';$('pill').className='pill'};
es.onerror=()=>{$('pill').textContent='reconnecting…';$('pill').className='pill warn';
 $('dot').className='dot warn'};
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
