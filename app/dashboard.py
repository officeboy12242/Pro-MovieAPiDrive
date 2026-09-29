"""Live vault dashboard — one page showing the crawler fleet working in real
time: rows pushed to Mongo, velocity, per-lane activity, live feed of newest
pushes, and the crawler log.

  python -m app.dashboard            (env: MKV_DASHBOARD_PORT, default 8766)

Read-only: opens Mongo in read mode and tails data/pusher.log. Safe to run
next to the API server and the fleet.
"""

from __future__ import annotations

import json
import os
import re
import threading
import time
from collections import deque

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

DATA_DIR = os.getenv("MKV_DATA_DIR", "data")
PORT = int(os.getenv("MKV_DASHBOARD_PORT", "8766"))
LOG_PATH = os.path.join(DATA_DIR, "pusher.log")
IDGAP_STATE = os.path.join(DATA_DIR, "idgap_state.json")
BLOCK = 25000  # idgap block size (matches app.idgap default)

app = FastAPI(docs_url=None, redoc_url=None)
_started = time.time()
_lock = threading.Lock()
_history: deque[tuple[float, int]] = deque(maxlen=720)  # (ts, total rows)
_last_sample = 0.0


def _mongo_col():
    uri = (os.getenv("MKV_MONGODB_URI") or "").strip()
    if not uri and os.path.exists(os.path.join(DATA_DIR, "mongo_uri.txt")):
        uri = open(os.path.join(DATA_DIR, "mongo_uri.txt"),
                   encoding="utf-8").read().strip()
    if not uri:
        return None
    try:
        from pymongo import MongoClient
        c = MongoClient(uri, serverSelectionTimeoutMS=4000, socketTimeoutMS=15000)
        c.admin.command("ping")
        return c[os.getenv("MKV_MONGO_DB", "mkvbase")].links
    except Exception:
        return None


# ----------------------------------------------------------------- data bits
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


def _newest(col, n: int = 15) -> list[dict]:
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


def _log_tail(n: int = 50) -> tuple[list[str], float]:
    try:
        age = time.time() - os.path.getmtime(LOG_PATH)
        with open(LOG_PATH, "rb") as f:
            f.seek(max(0, os.path.getsize(LOG_PATH) - 200_000))
            lines = f.read().decode("utf-8", "replace").splitlines()
        return [l[:200] for l in lines[-n:]], age
    except Exception:
        return [], -1


_ALIVE = re.compile(r"up (\d+)m(\d+)s \| session=(\w+)")
_TICK = re.compile(r"done=(\d+) queued=(\d+).*?rows=(\d+).*?agents=(\d+)")


def _fleet_from_log(lines: list[str]) -> dict:
    out: dict = {}
    for l in reversed(lines):
        if "out" not in out and "[alive]" in l:
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
        return {"term_stats": {k: v for k, v in ts.items()},
                "new_rows": sum(v.get("new", 0) for v in ts.values()),
                "terms": sum(v.get("tries", 0) for v in ts.values())}
    except Exception:
        return {}


# ------------------------------------------------------------------ api/html
@app.get("/api/live")
def live():
    global _last_sample
    col = _mongo_col()
    vault = _vault(col)
    now = time.time()
    with _lock:
        if now - _last_sample >= 4 and vault.get("rows") is not None:
            _history.append((now, vault["rows"]))
            _last_sample = now
        hist = list(_history)
    lines, log_age = _log_tail()
    rate = {"per_min": None, "since_open": 0}
    if len(hist) >= 2:
        (t0, r0), (t1, r1) = hist[0], hist[-1]
        if t1 > t0:
            rate["per_min"] = round((r1 - r0) / (t1 - t0) * 60, 1)
            rate["since_open"] = r1 - r0
    idg = _idgap_state()
    return JSONResponse({
        "ts": now, "uptime_min": int((now - _started) / 60),
        "vault": vault, "rate": rate,
        "history": [[t, r] for t, r in hist[-180:]],
        "fleet": _fleet_from_log(lines),
        "idgap": idg,
        "sources": _sources(col),
        "newest": _newest(col),
        "log": lines, "log_age_s": round(log_age),
    })


_HTML = """<!doctype html><html><head><meta charset="utf-8">
<title>mkvbase vault — live</title><meta name="viewport" content="width=device-width,initial-scale=1">
<style>
:root{--bg:#0d1117;--card:#161b22;--bd:#21262d;--fg:#c9d1d9;--dim:#8b949e;
--grn:#3fb950;--amb:#d29922;--red:#f85149;--blu:#58a6ff;--pur:#bc8cff}
*{box-sizing:border-box;margin:0}
body{background:var(--bg);color:var(--fg);font:14px/1.45 ui-monospace,Consolas,monospace;padding:18px}
h1{font-size:17px;display:flex;align-items:center;gap:9px}
.dot{width:9px;height:9px;border-radius:50%;background:var(--grn);animation:p 1.6s infinite}
.dot.stale{background:var(--amb);animation:none}.dot.dead{background:var(--red);animation:none}
@keyframes p{0%,100%{opacity:1}50%{opacity:.25}}
.big{font-size:42px;font-weight:700;color:var(--blu);letter-spacing:-1px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;margin:14px 0}
.card{background:var(--card);border:1px solid var(--bd);border-radius:8px;padding:12px 14px}
.card h3{font-size:11px;text-transform:uppercase;color:var(--dim);margin-bottom:6px}
.card .v{font-size:22px;font-weight:600}
.card .s{font-size:12px;color:var(--dim);margin-top:3px}
.g{color:var(--grn)}.a{color:var(--amb)}.b{color:var(--blu)}.p{color:var(--pur)}
table{width:100%;border-collapse:collapse;font-size:12.5px}
td,th{padding:4px 8px;border-bottom:1px solid var(--bd);text-align:left;white-space:nowrap}
th{color:var(--dim);font-weight:400;text-transform:uppercase;font-size:10.5px}
td.t{white-space:normal;max-width:0;width:100%;overflow:hidden;text-overflow:ellipsis}
.bar{display:flex;height:14px;border-radius:4px;overflow:hidden;margin-top:6px}
.bar div{height:100%}
.lg{font-size:11.5px;line-height:1.5;max-height:260px;overflow-y:auto;background:#010409;
border:1px solid var(--bd);border-radius:8px;padding:8px 10px}
.lg .d{color:var(--dim)}.lg .e{color:var(--red)}.lg .i{color:var(--grn)}.lg .m{color:var(--blu)}
.two{display:grid;grid-template-columns:1fr 1fr;gap:12px}
@media(max-width:900px){.two{grid-template-columns:1fr}}
footer{color:var(--dim);font-size:11px;margin-top:14px}
</style></head><body>
<h1><span class="dot" id="dot"></span> mkvbase vault — live <span style="color:var(--dim);font-size:12px" id="up"></span></h1>

<div class="grid">
 <div class="card"><h3>vault rows (mongo)</h3><div class="big" id="rows">—</div>
  <div class="s" id="rowsub">loading…</div></div>
 <div class="card"><h3>site id coverage</h3><div class="big" id="cov">—</div>
  <div class="s" id="covsub"></div></div>
 <div class="card"><h3>push velocity</h3><div class="big" id="rate">—</div>
  <div class="s" id="ratesub"></div><svg id="spark" width="100%" height="34" preserveAspectRatio="none"></svg></div>
 <div class="card"><h3>discovery fleet</h3><div class="v" id="disc">—</div><div class="s" id="discsub"></div></div>
 <div class="card"><h3>idgap miner</h3><div class="v" id="idg">—</div><div class="s" id="idgsub"></div></div>
</div>

<div class="grid"><div class="card" style="grid-column:1/-1"><h3>push sources (newest 2000 rows)</h3>
 <div class="bar" id="srcbar"></div><div class="s" id="srcleg"></div></div></div>

<div class="two">
 <div class="card"><h3>newest pushes → mongo</h3>
  <table><thead><tr><th>id</th><th>title</th><th>src</th></tr></thead>
  <tbody id="feed"><tr><td colspan=3 class="s">loading…</td></tr></tbody></table></div>
 <div class="card"><h3>crawler log (tail)</h3><div class="lg" id="log"></div></div>
</div>
<footer>auto-refresh 5s · page is read-only · stop/start via STOP-all-crawlers.cmd / START-auto-everything.cmd</footer>
<script>
const $=id=>document.getElementById(id);
let prevRows=null;
function cls(l){if(/\bFAIL\b|\berror\b|Traceback/i.test(l))return'e';
 if(/\[alive\]|ok |READY|online|seeded/.test(l))return'i';
 if(/^\[discovery/.test(l))return'm';return'd';}
function spark(h){const s=$('spark');if(h.length<2){s.innerHTML='';return}
 const vs=h.map(p=>p[1]),mn=Math.min(...vs),mx=Math.max(...vs)||1;
 const w=300,ht=34,pts=vs.map((v,i)=>`${(i/(vs.length-1)*w).toFixed(1)},${(ht-2-(v-mn)/(mx-mn||1)*(ht-4)).toFixed(1)}`).join(' ');
 s.innerHTML=`<polyline points="${pts}" fill="none" stroke="var(--blu)" stroke-width="1.5"/>`;}
async function tick(){try{
 const d=await(await fetch('/api/live')).json();
 const alive=d.log_age_s>=0&&d.log_age_s<90;
 $('dot').className='dot'+(alive?'':(d.log_age_s<0?' dead':' stale'));
 $('up').textContent='· dashboard up '+d.uptime_min+'m · log '+
   (d.log_age_s<0?'missing':d.log_age_s+'s ago');
 if(d.vault.rows!=null){$('rows').textContent=d.vault.rows.toLocaleString();
  $('rowsub').textContent=(prevRows!=null&&d.vault.rows>prevRows?
   '+'+(d.vault.rows-prevRows)+' since last poll · ':'')+ (d.rate.since_open>0?
   '+'+d.rate.since_open.toLocaleString()+' since page open':'waiting…');
  prevRows=d.vault.rows;}else $('rowsub').textContent=d.vault.error||'';
 if(d.vault.coverage_pct!=null){$('cov').textContent=d.vault.coverage_pct+'%';
  $('covsub').textContent='site max id '+ (d.vault.max_id||0).toLocaleString()+
   ' · thinnest: '+(d.vault.thin||[]).map(b=>'b'+(b.block*25000)+'('+b.have+')').join(' ')||'';}
 if(d.rate.per_min!=null){$('rate').textContent='+'+d.rate.per_min+'/min';
  $('ratesub').textContent='live rows/min (since page open)';}
 spark(d.history);
 const f=d.fleet||{};
 if(f.tick){$('disc').innerHTML=f.tick.agents+' agents <span class=g>active</span>';
  $('discsub').textContent='done '+f.tick.done.toLocaleString()+' · queued '+
   f.tick.queued.toLocaleString()+' · rows '+f.tick.rows.toLocaleString();}
 else $('disc').textContent='—';
 const t=d.idgap||{};
 if(t.terms){$('idg').innerHTML=(d.vault.coverage_pct||'?')+'% <span class=p>coverage</span>';
  $('idgsub').textContent=t.terms+' terms → '+t.new_rows.toLocaleString()+' new rows · '+
   Object.entries(t.term_stats||{}).map(([k,v])=>k.split('-')[1]+':'+v.new).join(' ');}
 else $('idg').textContent='—';
 const cols={};
 (d.sources||[]).forEach((s,i)=>{cols[s.name]=['#3fb950','#58a6ff','#bc8cff','#d29922','#f85149'][i%5];});
 $('srcbar').innerHTML=(d.sources||[]).map(s=>
  `<div style="width:${s.pct}%;background:${cols[s.name]}" title="${s.name}: ${s.n}"></div>`).join('');
 $('srcleg').textContent=(d.sources||[]).map(s=>
  `${s.name} ${s.pct}% (${s.n.toLocaleString()})`).join('   ·   ');
 $('feed').innerHTML=(d.newest||[]).map(r=>
  `<tr><td>${r.id??''}</td><td class="t">${(r.title||'').replace(/</g,'&lt;')}</td>`+
  `<td class="${r.src==='idgap'?'p':'b'}">${r.src||''}</td></tr>`).join('')||'<tr><td colspan=3 class=s>empty</td></tr>';
 $('log').innerHTML=(d.log||[]).map(l=>
  `<div class="${cls(l)}">${l.replace(/</g,'&lt;')}</div>`).join('');
 $('log').scrollTop=$('log').scrollHeight;
}catch(e){$('dot').className='dot dead'}}
tick();setInterval(tick,5000);
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return _HTML


def main() -> None:
    import uvicorn
    print(f"[dashboard] http://127.0.0.1:{PORT}  (log: {LOG_PATH})", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
