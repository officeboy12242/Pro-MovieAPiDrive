"""mkvbase-cf-api — Cloudflare-proof search API for mkvbase.site.

Speed model:
  cold start        engine launch + CF clearance            (~40-70s, once)
  cached term       TTL cache hit                           (<5ms)
  warm in-page      signed URL fetched in-page, no nav      (~0.3-1s)
  warm nav          signed URL full navigation              (~2-5s)
"""
from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .client import MkvbaseClient, MkvbaseError
from .engines import make_engine
from .keepalive import start_keepalive
from .store import Store

app = FastAPI(title="mkvbase-cf-api", version="1.2.0")
start_keepalive()

# Ingest key for split-plane sync (POST /sync). Required only when MKV_SYNC_KEY is set.
_SYNC_KEY = os.getenv("MKV_SYNC_KEY", "")
# Serve-only mode (Render free): /search never live-scrapes, only serves synced results.
_SERVE_ONLY = os.getenv("MKV_SERVE_ONLY", "").lower() in ("1", "true", "yes")

_client: MkvbaseClient | None = None
_engine_obj = None
# Playwright/Camoufox sync objects are thread-affine: create AND use them on one
# dedicated worker thread, no matter which threadpool thread handles the request.
_engine_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="engine")

# ---------------------------------------------------------------- TTL result cache
_CACHE_TTL_S = float(os.getenv("MKV_CACHE_TTL", "300"))
_lock = threading.Lock()
_cache: dict[str, tuple[float, dict]] = {}
_cache_hits = 0
_cache_misses = 0


def _cache_get(term: str) -> tuple[dict, float] | None:
    global _cache_hits
    with _lock:
        hit = _cache.get(term.lower())
        if hit and time.time() - hit[0] < _CACHE_TTL_S:
            _cache_hits += 1
            return hit[1], time.time() - hit[0]
    return None


def _cache_put(term: str, obj: dict) -> None:
    with _lock:
        _cache[term.lower()] = (time.time(), obj)


def _run_on_engine(fn, *args, **kwargs):
    return _engine_pool.submit(fn, *args, **kwargs).result()


def _get_client() -> MkvbaseClient:
    global _client, _engine_obj
    if _client is None:
        engine = _run_on_engine(make_engine)
        _engine_obj = engine
        data_dir = os.getenv("MKV_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
        _client = MkvbaseClient(engine, cache_path=os.path.join(data_dir, "search"))
    return _client


# ---------------------------------------------------------------- pre-warm
def _prewarm() -> None:
    """Launch engine + clear CF before the first user request hits."""
    try:
        client = _get_client()
        _run_on_engine(client.recent)
        print("[prewarm] session warm", flush=True)
    except Exception as e:
        print(f"[prewarm] skipped: {e}", flush=True)


@app.on_event("startup")
def _startup() -> None:
    if os.getenv("MKV_PREWARM", "true").lower() in ("1", "true", "yes"):
        threading.Thread(target=_prewarm, daemon=True, name="prewarm").start()


@app.get("/health")
def health():
    sess = None
    if _client is not None and _client._session is not None:
        sess = {"cookies": len(_client._session.cookies),
                "mkv_session": _client._session.has_mkv_session(),
                "cf_clearance": bool(_client._session.cookies.get("cf_clearance"))}
    return {"ok": True, "engine": _engine_obj.name if _engine_obj else os.getenv("MKV_ENGINE", "auto"),
            "client_engine": _client.engine_name if _client else "not-init",
            "session": sess,
            "bootstrap_timeout_s": _client._bootstrap_timeout if _client else None,
            "cache": {"entries": len(_cache), "hits": _cache_hits, "misses": _cache_misses,
                      "ttl_s": _CACHE_TTL_S},
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


@app.post("/sync")
def sync(payload: dict, x_sync_key: str | None = None):
    """Split-plane ingest: a scraper elsewhere (home PC) pushes full result objects.
    Body: {"term": str, "results": [...], "count": int, ...} — the same shape
    client.search() produces. Guarded by MKV_SYNC_KEY when set."""
    if _SYNC_KEY and x_sync_key != _SYNC_KEY:
        raise HTTPException(status_code=401, detail="bad sync key")
    term = (payload.get("term") or payload.get("_term") or "").strip()
    if not term:
        raise HTTPException(status_code=400, detail="missing term")
    obj = dict(payload)
    obj["_source"] = "sync"
    obj["_synced_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    _cache_put(term, obj)  # serves /search instantly; lives as long as the process
    if obj.get("results"):
        Store(os.getenv("MKV_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
              ).record("search", term, obj)
    return {"ok": True, "term": term, "count": obj.get("count"),
            "cached": True, "cache_entries": len(_cache)}


@app.get("/search")
def search(term: str = Query(..., min_length=1, max_length=100),
           refresh: bool = Query(False, description="bypass TTL cache and force a live scrape"),
           save: bool = True):
    t0 = time.time()
    cached = None if refresh else _cache_get(term)
    if cached is not None:
        obj, age = cached
        return {
            "term": term, "count": obj.get("count"), "difficulty": obj.get("difficulty"),
            "engine": obj.get("_engine"), "cached": True, "age_s": round(age, 1),
            "took_ms": int((time.time() - t0) * 1000),
            "results": obj.get("results", []),
        }
    global _cache_misses
    _cache_misses += 1
    client = _get_client()
    obj = None
    last_err: Exception | None = None
    for attempt in range(2):  # one automatic retry absorbs transient site flakes
        try:
            obj = _run_on_engine(client.search, term)
            break
        except MkvbaseError as e:
            last_err = e
            time.sleep(1.5)
        except Exception as e:
            last_err = e
            break
    if obj is None:
        if isinstance(last_err, MkvbaseError):
            raise HTTPException(status_code=502, detail=str(last_err))
        raise HTTPException(status_code=500, detail=f"{type(last_err).__name__}: {last_err}")
    _cache_put(term, obj)
    path = Store(os.getenv("MKV_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
                 ).record("search", term, obj) if save else None
    return {
        "term": term, "count": obj.get("count"), "difficulty": obj.get("difficulty"),
        "engine": obj.get("_engine"), "mode": obj.get("_mode"), "cached": False,
        "took_ms": int((time.time() - t0) * 1000), "saved_to": path,
        "results": obj.get("results", []),
    }


@app.get("/recent")
def recent(save: bool = True):
    client = _get_client()
    try:
        obj = _run_on_engine(client.recent)
    except MkvbaseError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
    if save:
        Store(os.getenv("MKV_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
              ).record("recent", "latest", obj)
    return obj


@app.get("/saved")
def saved():
    data_dir = os.getenv("MKV_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
    return {"files": Store(data_dir).list_saved()}


@app.get("/saved/{filename}")
def saved_file(filename: str):
    data_dir = os.getenv("MKV_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
    obj = Store(data_dir).load(filename)
    if obj is None:
        raise HTTPException(status_code=404, detail="not found")
    return JSONResponse(obj)


@app.post("/cache/clear")
def cache_clear():
    with _lock:
        _cache.clear()
    return {"ok": True, "cleared": True}
