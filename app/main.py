"""mkvbase-cf-api — Cloudflare-proof search API for mkvbase.site.

Request path never waits on a browser:
  cached term       TTL cache hit                                   (<5ms)
  warm              signed locally, fetched over plain HTTP         (~1s)
  no session yet    the background warmer clears Cloudflare; the request waits at
                    most MKV_REQUEST_WAIT seconds, then serves the last known result
                    (stale=true) or answers 503 + Retry-After with the reason.

The warmer is the only thing that launches a browser. After a clearance it keeps
the session alive over plain HTTP and closes the browser (see client.py).
"""
from __future__ import annotations

import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeout

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse

from .client import MkvbaseClient, MkvbaseError, NeedsSession
from .engines import make_engine
from .keepalive import start_keepalive
from .store import Store

app = FastAPI(title="mkvbase-cf-api", version="1.3.0")
start_keepalive()

# Ingest key for split-plane sync (POST /sync). Required only when MKV_SYNC_KEY is set.
_SYNC_KEY = os.getenv("MKV_SYNC_KEY", "")
# Serve-only mode: /search and /recent never live-scrape, only serve synced/saved results.
_SERVE_ONLY = os.getenv("MKV_SERVE_ONLY", "").lower() in ("1", "true", "yes")
_DATA_DIR = os.getenv("MKV_DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "data"))
# Longest a request waits for the warmer before answering from stale data or 503.
_REQUEST_WAIT_S = float(os.getenv("MKV_REQUEST_WAIT", "25"))

_client: MkvbaseClient | None = None
_client_lock = threading.Lock()
# Playwright/Camoufox sync objects are thread-affine: every browser call runs on
# this one worker thread. The plain-HTTP path never touches it.
_engine_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="engine")

# ---------------------------------------------------------------- TTL result cache
_CACHE_TTL_S = float(os.getenv("MKV_CACHE_TTL", "300"))
_lock = threading.Lock()
_cache: dict[str, tuple[float, dict]] = {}
_cache_hits = 0
_cache_misses = 0


def _cache_get(term: str, max_age: float | None = _CACHE_TTL_S) -> tuple[dict, float] | None:
    """Fresh hit within max_age; max_age=None returns any age (stale fallback)."""
    global _cache_hits
    with _lock:
        hit = _cache.get(term.lower())
        if hit and (max_age is None or time.time() - hit[0] < max_age):
            if max_age is not None:
                _cache_hits += 1
            return hit[1], time.time() - hit[0]
    return None


def _cache_put(term: str, obj: dict) -> None:
    with _lock:
        _cache[term.lower()] = (time.time(), obj)


def _run_on_engine(fn, *args, timeout: float | None = None, **kwargs):
    return _engine_pool.submit(fn, *args, **kwargs).result(timeout=timeout)


def _get_client() -> MkvbaseClient:
    global _client
    with _client_lock:
        if _client is None:
            # engine construction is lazy (no browser yet), so any thread may build it
            _client = MkvbaseClient(make_engine(), cache_path=os.path.join(_DATA_DIR, "search"))
    return _client


# ---------------------------------------------------------------- background warmer
class Warmer:
    """Owns Cloudflare clearance. Requests kick it and wait briefly; they never
    run a browser bootstrap themselves. Failures back off 30s -> 10 min."""

    def __init__(self):
        self.state = "idle"  # idle | warming | ready | failed
        self.attempts = 0
        self.fails = 0
        self.last_error: str | None = None
        self.last_ok: float | None = None
        self.last_took_s: float | None = None
        self.next_retry_at: float | None = None
        self._started = 0.0
        self._kick = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None

    @property
    def http_ok(self) -> bool | None:
        return _client.http_verified if _client else None

    def start(self) -> None:
        if self._thread is None and not _SERVE_ONLY:
            self._thread = threading.Thread(target=self._loop, daemon=True, name="warmer")
            self._thread.start()

    def kick(self) -> None:
        """Ask for a usable session. Ignored while backing off after a failure."""
        self.start()
        self._ready.clear()
        self._kick.set()

    def wait_ready(self, timeout: float) -> bool:
        return self._ready.wait(timeout)

    def _loop(self) -> None:
        while True:
            if self.state == "failed":
                time.sleep(max(0.0, self.next_retry_at - time.time()))
            else:
                self._kick.wait()
            self._kick.clear()
            self._attempt()

    def _attempt(self) -> None:
        client = _get_client()
        if client.session_ready():
            self.state = "ready"
            self._ready.set()
            return
        self.state = "warming"
        self._started = time.time()
        self.attempts += 1
        try:
            _run_on_engine(client.ensure_session)
            self.state, self.fails, self.last_error = "ready", 0, None
            self.last_ok = time.time()
            self._ready.set()
            print(f"[warmer] session ready in {time.time() - self._started:.1f}s "
                  f"(plain HTTP {'ok' if client.http_verified else 'blocked, browser mode'})", flush=True)
        except Exception as e:
            self.fails += 1
            self.state = "failed"
            self.last_error = f"{type(e).__name__}: {e}"[:400]
            self.next_retry_at = time.time() + min(600, 30 * 2 ** (self.fails - 1))
            print(f"[warmer] attempt {self.attempts} failed: {self.last_error}", flush=True)
        finally:
            self.last_took_s = round(time.time() - self._started, 1)

    def retry_after_s(self) -> int:
        now = time.time()
        if self.state == "warming":
            return int(max(5, 60 - (now - self._started)))
        if self.state == "failed" and self.next_retry_at:
            return int(max(5, self.next_retry_at - now + 60))
        return 5

    def status(self) -> dict:
        return {"state": self.state, "attempts": self.attempts, "last_error": self.last_error,
                "last_ok_ago_s": round(time.time() - self.last_ok) if self.last_ok else None,
                "last_took_s": self.last_took_s, "retry_after_s": self.retry_after_s(),
                "plain_http_ok": self.http_ok}


_warmer = Warmer()


@app.on_event("startup")
def _startup() -> None:
    if _SERVE_ONLY:
        return
    _warmer.start()
    if os.getenv("MKV_PREWARM", "true").lower() in ("1", "true", "yes"):
        _warmer.kick()


# ---------------------------------------------------------------- live fetch + fallbacks
def _live(fetch_http, fetch_browser):
    """Plain HTTP on the request thread. No session -> wake the warmer, wait briefly,
    try once more. Hosts where only the browser passes go through the engine thread."""
    for attempt in range(2):
        try:
            if _warmer.http_ok is False:
                return _run_on_engine(fetch_browser, timeout=_REQUEST_WAIT_S)
            return fetch_http()
        except NeedsSession:
            _warmer.kick()
            if attempt or not _warmer.wait_ready(_REQUEST_WAIT_S):
                raise
        except MkvbaseError:
            if attempt:
                raise
            time.sleep(1.5)  # one automatic retry absorbs transient site flakes


def _stale(kind: str, term: str) -> tuple[dict, float] | None:
    hit = _cache_get(term, max_age=None) if kind == "search" else None
    if hit:
        return hit
    return Store(_DATA_DIR).load_record(kind, term)


def _unavailable(kind: str, term: str, err: Exception) -> tuple[dict, float]:
    """Last known result if there is one, else a fast, explained 503/502."""
    stale = _stale(kind, term)
    if stale is not None:
        return stale
    if isinstance(err, (NeedsSession, FutureTimeout)):
        retry = _warmer.retry_after_s()
        raise HTTPException(status_code=503, headers={"Retry-After": str(retry)}, detail={
            "error": "warming up: Cloudflare clearance not ready yet" if _warmer.state != "failed"
            else "Cloudflare clearance failing on this host",
            "retry_after_s": retry, "warmer": _warmer.status()})
    if isinstance(err, MkvbaseError):
        raise HTTPException(status_code=502, detail=str(err))
    raise HTTPException(status_code=500, detail=f"{type(err).__name__}: {err}")


def _mem_mb() -> float | None:
    """Container memory in use (cgroup v2, then v1). None when not in a container."""
    for p in ("/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory/memory.usage_in_bytes"):
        try:
            with open(p) as f:
                return round(int(f.read()) / 2 ** 20, 1)
        except Exception:
            continue
    return None


# ---------------------------------------------------------------- routes
@app.get("/health")
def health():
    sess = None
    if _client is not None and _client._session is not None:
        sess = {"cookies": len(_client._session.cookies),
                "mkv_session": _client._session.has_mkv_session(),
                "cf_clearance": bool(_client._session.cookies.get("cf_clearance")),
                "challenge_ttl_s": round(_client.challenge_ttl_s())}
    return {"ok": True, "engine": os.getenv("MKV_ENGINE", "auto"),
            "client_engine": _client.engine_name if _client else "not-init",
            "session": sess,
            "warmer": _warmer.status(),
            "browser_open": _client.browser_open if _client else False,
            "mem_mb": _mem_mb(),
            "proxy": bool(os.getenv("MKV_PROXY")),
            "serve_only": _SERVE_ONLY,
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
        Store(_DATA_DIR).record("search", term, obj)
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
    if _SERVE_ONLY:
        stale = _stale("search", term)
        if stale is None:
            raise HTTPException(status_code=404, detail="term not synced (serve-only mode)")
        obj, age = stale
        return {"term": term, "count": obj.get("count"), "difficulty": obj.get("difficulty"),
                "engine": obj.get("_engine"), "cached": True, "age_s": round(age, 1),
                "took_ms": int((time.time() - t0) * 1000), "results": obj.get("results", [])}
    global _cache_misses
    _cache_misses += 1
    client = _get_client()
    try:
        obj = _live(lambda: client.search_http(term), lambda: client.search(term))
    except Exception as e:
        obj, age = _unavailable("search", term, e)
        return {"term": term, "count": obj.get("count"), "difficulty": obj.get("difficulty"),
                "engine": obj.get("_engine"), "cached": True, "stale": True, "age_s": round(age, 1),
                "reason": str(e)[:200], "warmer": _warmer.state,
                "took_ms": int((time.time() - t0) * 1000), "results": obj.get("results", [])}
    _cache_put(term, obj)
    path = Store(_DATA_DIR).record("search", term, obj) if save else None
    return {
        "term": term, "count": obj.get("count"), "difficulty": obj.get("difficulty"),
        "engine": obj.get("_engine"), "mode": obj.get("_mode"), "cached": False,
        "took_ms": int((time.time() - t0) * 1000), "saved_to": path,
        "results": obj.get("results", []),
    }


@app.get("/recent")
def recent(save: bool = True):
    if _SERVE_ONLY:
        stale = _stale("recent", "latest")
        if stale is None:
            raise HTTPException(status_code=404, detail="no recent snapshot (serve-only mode)")
        return {**stale[0], "stale": True, "age_s": round(stale[1], 1)}
    client = _get_client()
    try:
        obj = _live(client.recent_http, client.recent)
    except Exception as e:
        obj, age = _unavailable("recent", "latest", e)
        return {**obj, "stale": True, "age_s": round(age, 1), "reason": str(e)[:200]}
    if save:
        Store(_DATA_DIR).record("recent", "latest", obj)
    return obj


@app.get("/saved")
def saved():
    return {"files": Store(_DATA_DIR).list_saved()}


@app.get("/saved/{filename}")
def saved_file(filename: str):
    obj = Store(_DATA_DIR).load(filename)
    if obj is None:
        raise HTTPException(status_code=404, detail="not found")
    return JSONResponse(obj)


@app.post("/cache/clear")
def cache_clear():
    with _lock:
        _cache.clear()
    return {"ok": True, "cleared": True}
