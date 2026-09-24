"""mkvbase-cf-api — Cloudflare-proof search API for mkvbase.site.

Engines: DrissionPage (real Chrome via CDP) and Camoufox (anti-detect Firefox).
Both clear the managed challenge by real top-level navigation; the mkv_* session
cookies are then harvested and the search URL is signed locally (XOR + PoW + HMAC).
"""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .client import MkvbaseClient, MkvbaseError
from .engines import make_engine
from .keepalive import start_keepalive
from .store import Store

app = FastAPI(title="mkvbase-cf-api", version="1.0.0")
start_keepalive()

_client: MkvbaseClient | None = None
_engine_obj = None
# Playwright/Camoufox sync objects are thread-affine: create AND use them on one
# dedicated worker thread, no matter which threadpool thread handles the request.
_engine_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="engine")


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


@app.get("/health")
def health():
    return {"ok": True, "engine": _engine_obj.name if _engine_obj else os.getenv("MKV_ENGINE", "auto"),
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}


@app.get("/search")
def search(term: str = Query(..., min_length=1, max_length=100),
           engine: str | None = Query(None, description="drissionpage|camoufox (overrides MKV_ENGINE for this call only if server restarted)"),
           save: bool = True):
    client = _get_client()
    t0 = time.time()
    try:
        obj = _run_on_engine(client.search, term)
    except MkvbaseError as e:
        raise HTTPException(status_code=502, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}")
    path = Store(os.getenv("MKV_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
                 ).record("search", term, obj) if save else None
    return {
        "term": term,
        "count": obj.get("count"),
        "difficulty": obj.get("difficulty"),
        "engine": obj.get("_engine"),
        "took_ms": int((time.time() - t0) * 1000),
        "saved_to": path,
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
