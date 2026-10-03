"""MkvbaseClient — orchestrates engine + protocol to deliver search/recent results.

Two tiers:
  plain HTTP   search_http()/recent_http(): signed locally, fetched over curl_cffi with
               the cleared cookies + browser-identical TLS. ~1s, no browser, safe to call
               from any thread. Raises NeedsSession when there is nothing to fetch with.
  full chain   search()/recent() (engine thread only): plain HTTP first, then browser
               clearance, then browser fetch (inpage -> nav -> settle-then-sign).

Session lifecycle:
  - One browser clearance yields cf_clearance + mkv_* cookies (persisted to session.json).
  - mkvbase re-issues every mkv_* cookie on each response (mkv_challenge expiry rolls
    forward ~30 min). Absorbing them keeps the session alive over plain HTTP, so the
    browser is only needed again when Cloudflare's cf_clearance itself dies (403).
  - After a clearance whose plain-HTTP path is verified, the browser is closed
    (MKV_RELEASE_BROWSER, default on) so a 512MB host is not holding Firefox in RAM.

Owner allowlist (MKV_ORIGIN_KEY): the site owner adds a Cloudflare rule that skips
the challenge for requests carrying this secret header. Every request then sends
it, and a session is bootstrapped from bare /api/links over plain HTTP: no browser.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

from .engines.base import BaseEngine, Session
from .protocol import build_search_url

BASE = "https://mkvbase.site"
_IMPERSONATE = ("firefox133", "firefox", "chrome131", None)
_DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0"


class MkvbaseError(RuntimeError):
    pass


class NeedsSession(MkvbaseError):
    """Plain HTTP cannot proceed: no clearance yet, or Cloudflare clearance died."""


class _PrioritySemaphore:
    """A threading.Semaphore whose waiters wake in priority order (lower
    value first) instead of FIFO. Keeps the 2-slot HTTP budget pointed at
    the highest-yield lanes: the recents watcher never queues behind bulk
    discovery, and idgap seed searches beat low-priority probes.

    Priority alone is NOT starvation-safe, and that is not hypothetical: idgap
    runs 8 agents that never sleep between successful searches, so its waiters
    keep the heap non-empty and every released token goes to lane 1. Discovery
    (lanes 2/3) then waits on ev.wait() forever -- measured live: 3 completed
    searches in 17 minutes, one step logging 142.0s, all of it queue wait,
    while idgap ran 84 searches/min. So a waiter's effective priority AGES:
    every AGING_S of waiting it gains one level, which guarantees any lane
    reaches the front within (levels x AGING_S) no matter how loud the
    higher-priority lanes are. Re-heapified on release (waiters are ~12).
    """

    AGING_S = float(os.getenv("MKV_HTTP_AGING_S", "20"))

    def __init__(self, value: int = 1):
        import heapq
        self._heapq = heapq
        self._cond = threading.Condition()
        self._tokens = value
        self._seq = 0
        # heap of (effective_priority, seq, event, arrived_at, base_priority)
        self._waiters: list = []

    def _effective(self, arrived_at: float, base_priority: int, now: float) -> int:
        return base_priority - int((now - arrived_at) / self.AGING_S)

    def acquire(self, priority: int = 3, timeout: float | None = None) -> bool:
        with self._cond:
            if self._tokens > 0 and not self._waiters:
                self._tokens -= 1
                return True
            ev = threading.Event()
            self._seq += 1
            now = time.time()
            self._heapq.heappush(
                self._waiters,
                (self._effective(now, priority, now), self._seq, ev, now, priority))
        # wait OUTSIDE the condition lock or release() can never run
        ok = ev.wait(timeout) if timeout else ev.wait()
        if not ok:  # timed out: remove ourselves so the token is not lost
            with self._cond:
                self._waiters = [w for w in self._waiters if w[2] is not ev]
                self._heapq.heapify(self._waiters)
        return ok

    def release(self) -> None:
        with self._cond:
            if self._waiters:
                now = time.time()
                self._waiters = [
                    (self._effective(arrived, base, now), seq, ev, arrived, base)
                    for (_eff, seq, ev, arrived, base) in self._waiters]
                self._heapq.heapify(self._waiters)
                _, _, ev, _, _ = self._heapq.heappop(self._waiters)
                ev.set()
            else:
                self._tokens += 1

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()
        return False


class MkvbaseClient:
    def __init__(self, engine: BaseEngine | None, base: str = BASE, cache_path: str | None = None):
        self._engine = engine  # created lazily on the engine thread when first needed
        self.base = base
        self.cache_path = cache_path
        self._session: Session | None = None
        self._lock = threading.RLock()  # guards _session (HTTP path runs on request threads)
        self._impersonate: str | None = None  # sticky winner once one passes
        self._bootstrap_timeout = int(os.getenv("MKV_BOOTSTRAP_TIMEOUT", "90"))
        self._proxy = os.getenv("MKV_PROXY") or None  # cf_clearance is IP-bound: browser + HTTP share it
        self._origin_key = os.getenv("MKV_ORIGIN_KEY") or None  # owner's Cloudflare skip-rule secret
        self._origin_header = os.getenv("MKV_ORIGIN_HEADER", "X-Mkv-Key")
        self._release_browser = os.getenv("MKV_RELEASE_BROWSER", "true").lower() in ("1", "true", "yes")
        self.http_verified: bool | None = None  # did plain HTTP pass after the last clearance?
        # cf_clearance values that just got a 403 — never re-borrow these from Mongo/disk
        self._rejected_cf: set[str] = set()
        # Playwright/Camoufox sync objects are thread-affine and refuse a running asyncio
        # loop: every browser call hops to this one worker (pusher + discovery + API).
        self._engine_pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="mkv-eng")
        self._engine_tid: int | None = None
        # Multi-agent stampede kills cf_clearance: one clear at a time, few concurrent GETs.
        self._clear_lock = threading.Lock()
        self._http_sem = _PrioritySemaphore(
            max(1, int(os.getenv("MKV_HTTP_CONCURRENCY", "2"))))
        self._http_gap_s = float(os.getenv("MKV_HTTP_GAP_S", "0.4"))
        self._last_http_at = 0.0
        self._pace_lock = threading.Lock()
        self._load_persisted_session()

    @staticmethod
    def _lane_priority() -> int:
        """Slot-queue priority by calling lane (lower wakes first):
        0 recents watcher/sync -> 1 idgap miners -> 2 discovery priority
        lane -> 3 everything else. Derives from the thread name, so callers
        need no changes."""
        n = threading.current_thread().name
        if n == "MainThread" or n.startswith("mkv-"):
            return 0
        if n.startswith("idgap"):
            return 1
        if "-priority" in n:
            return 2
        return 3

    def _pace_http(self) -> None:
        """Small gap between plain-HTTP calls so CF does not rotate clearance."""
        with self._pace_lock:
            now = time.time()
            wait = self._http_gap_s - (now - self._last_http_at)
            if wait > 0:
                time.sleep(wait)
            self._last_http_at = time.time()

    def _on_engine(self, fn, *args, timeout: float | None = None, **kwargs):
        if self._engine_tid is not None and threading.get_ident() == self._engine_tid:
            return fn(*args, **kwargs)

        def run():
            self._engine_tid = threading.get_ident()
            return fn(*args, **kwargs)

        return self._engine_pool.submit(run).result(timeout=timeout)

    # ------------------------------------------------------------------ engine (lazy)
    @property
    def engine(self) -> BaseEngine:
        if self._engine is None:
            from .engines import make_engine
            self._engine = make_engine()
        return self._engine

    @property
    def engine_name(self) -> str:
        return self._engine.name if self._engine else "not-started"

    @property
    def browser_open(self) -> bool:
        return bool(self._engine and self._engine.is_open())

    def release_browser(self) -> None:
        """Close the browser; it relaunches lazily if needed again."""
        if self._engine is not None and self._engine.is_open():
            self._on_engine(self._engine.close)

    # ------------------------------------------------------------------ session persistence
    def _session_file(self) -> str | None:
        if not self.cache_path:
            return None
        return os.path.join(os.path.dirname(self.cache_path), "session.json")

    def _load_persisted_session(self) -> None:
        sf = self._session_file()
        if not sf or not os.path.exists(sf):
            pass
        else:
            try:
                with open(sf, encoding="utf-8") as f:
                    data = json.load(f)
                s = Session(cookies=data.get("cookies", {}), user_agent=data.get("user_agent", ""))
                if s.has_mkv_session():
                    self._session = s
            except Exception:
                pass
        if self._session is None:
            self._load_shared_session()

    # ------------------------------------------------------------------ shared session (Mongo)
    def _mongo_sessions(self):
        """sessions collection in the shared vault, or None when Mongo is not set.
        Browser-capable hosts (PC) publish their CF-cleared session here; headless
        hosts without a working browser (Termux phone, tiny VPS) borrow it and run
        fully browser-free."""
        try:
            from .store import _mongo_uri
            uri = _mongo_uri()
        except Exception:
            uri = None
        if not uri:
            return None
        try:
            from pymongo import MongoClient
            return MongoClient(uri, serverSelectionTimeoutMS=8000, socketTimeoutMS=20000,
                               maxPoolSize=2)[os.getenv("MKV_MONGO_DB", "mkvbase")].sessions
        except Exception:
            return None

    def _cf_rejected(self, cookies: dict) -> bool:
        cf = (cookies or {}).get("cf_clearance") or ""
        return bool(cf and cf in self._rejected_cf)

    def _load_shared_session(self) -> bool:
        col = self._mongo_sessions()
        if col is None:
            return False
        try:
            doc = col.find_one({"_id": "mkvbase"})
        except Exception:
            return False
        if not doc or not doc.get("cookies"):
            return False
        if self._cf_rejected(doc["cookies"]):
            return False
        s = Session(cookies=doc["cookies"], user_agent=doc.get("user_agent", ""))
        if s.has_mkv_session():
            self._session = s
            return True
        return False

    def _refresh_from_shared(self) -> bool:
        """Pick up a session another host pushed (fresher cf_clearance)."""
        col = self._mongo_sessions()
        if col is None:
            return False
        try:
            doc = col.find_one({"_id": "mkvbase"})
        except Exception:
            return False
        if not doc or not doc.get("cookies"):
            return False
        if self._cf_rejected(doc["cookies"]):
            return False
        s = Session(cookies=doc["cookies"], user_agent=doc.get("user_agent", ""))
        if not s.has_mkv_session():
            return False
        with self._lock:
            if self._session is not None and self._session.cookies == s.cookies:
                return False  # already holding this exact session
            self._session = s
        self._persist_session()
        return True

    def _persist_session(self) -> None:
        sf = self._session_file()
        if sf and self._session is not None:
            try:
                os.makedirs(os.path.dirname(sf), exist_ok=True)
                with open(sf, "w", encoding="utf-8") as f:
                    json.dump({"cookies": self._session.cookies,
                               "user_agent": self._session.user_agent}, f)
            except Exception:
                pass
        # share with other hosts (phone crawls browser-free off this), throttled
        col = self._mongo_sessions()
        if col is not None and self._session is not None:
            now = time.time()
            if now - getattr(self, "_mongo_shared_at", 0.0) > 300:
                try:
                    col.update_one({"_id": "mkvbase"},
                                   {"$set": {"cookies": self._session.cookies,
                                             "user_agent": self._session.user_agent,
                                             "ts": now}}, upsert=True)
                    self._mongo_shared_at = now
                except Exception:
                    pass

    # ------------------------------------------------------------------ session state
    def _challenge_expired(self) -> bool:
        """mkv_challenge embeds an expiry_ms timestamp — stale means re-issue needed."""
        return self.challenge_ttl_s() <= 30  # 30s safety margin

    def challenge_ttl_s(self) -> float:
        try:
            ch = urllib.parse.unquote(self._session.cookies.get("mkv_challenge", ""))
            return int(ch.split(":")[2]) / 1000 - time.time()
        except Exception:
            return 0.0

    def session_ready(self) -> bool:
        # Cookiefree-friendly IPs (phone/residential) often have mkv_* with no
        # cf_clearance; that is enough. CF-strict hosts get a 403 and re-clear.
        with self._lock:
            s = self._session
            return bool(s and s.has_mkv_session() and not self._challenge_expired()
                        and not self._cf_rejected(s.cookies))

    def _absorb(self, s: Session, cookies) -> None:
        """Merge Set-Cookie values from a plain-HTTP response into the live session."""
        with self._lock:
            if self._session is not s:  # a fresh clearance replaced it meanwhile
                return
            for k, v in cookies:
                if v:
                    s.cookies[k] = v
            self._persist_session()

    def _drop_session(self, s: Session, *, cf_dead: bool = False) -> None:
        with self._lock:
            if self._session is s:
                self._session = None
        if not cf_dead:
            return
        cf = (s.cookies or {}).get("cf_clearance") or ""
        if cf:
            self._rejected_cf.add(cf)
            if len(self._rejected_cf) > 8:  # ponytail: bounded set; drop oldest-ish extras
                self._rejected_cf = set(list(self._rejected_cf)[-4:])
        # wipe shared copy so other hosts do not keep re-adopting the corpse
        col = self._mongo_sessions()
        if col is not None and cf:
            try:
                doc = col.find_one({"_id": "mkvbase"}, {"cookies.cf_clearance": 1})
                if doc and (doc.get("cookies") or {}).get("cf_clearance") == cf:
                    col.delete_one({"_id": "mkvbase"})
            except Exception:
                pass
        # and the on-disk session.json so the next start does not reload it
        sf = self._session_file()
        if sf and os.path.exists(sf):
            try:
                os.remove(sf)
            except Exception:
                pass

    def _bootstrap(self, timeout_s: int | None = None) -> Session:
        """Full browser clearance on the bare endpoint (always on the engine thread)."""
        return self._on_engine(self._bootstrap_locked, timeout_s)

    def _bootstrap_locked(self, timeout_s: int | None = None) -> Session:
        s = self.engine.get_session(f"{self.base}/api/links", timeout_s=timeout_s or self._bootstrap_timeout)
        if not s.has_mkv_session():
            raise MkvbaseError(f"Cloudflare did not clear: no mkv_* cookies after {timeout_s or self._bootstrap_timeout}s "
                               f"(got cookies: {sorted(s.cookies)})")
        with self._lock:
            self._session = s
            self._persist_session()
        return s

    def _bootstrap_nokey(self) -> bool:
        """Residential/mobile IPs are often never challenged by Cloudflare: a bare
        /api/links over curl_cffi then sets every mkv_* cookie with no browser at
        all. Costs one request; harmless (403/challenge page) when the IP IS
        challenged. This is what makes a phone fully PC-independent."""
        try:
            from curl_cffi import requests as cffi
        except ImportError:
            return False
        try:
            r = cffi.get(f"{self.base}/api/links",
                         headers={"User-Agent": _DEFAULT_UA, "Accept": "*/*",
                                  "Accept-Language": "en-US,en;q=0.9",
                                  "X-Requested-With": "XMLHttpRequest",
                                  "Referer": f"{self.base}/"},
                         timeout=25, impersonate="firefox133", allow_redirects=True)
        except Exception:
            return False
        if r.status_code != 200:
            return False
        try:
            cookies = {str(k): str(v) for k, v in r.cookies.items() if v}
        except Exception:
            cookies = {}
        s = Session(cookies=cookies, user_agent=_DEFAULT_UA)
        if not s.has_mkv_session():
            return False
        with self._lock:
            self._session = s
        self._persist_session()
        return True

    def ensure_session(self, timeout_s: int | None = None) -> bool:
        """Make the session usable, cheapest way first.

        Single-flight: many discovery agents used to call this at once, thrash the
        one browser worker, and burn a still-valid cf_clearance. Waiters reuse the
        winner's session instead of clearing again.
        """
        # cheap unlocked peek — avoid queueing on the clear lock when already good
        if self.session_ready() and self.http_verified is not False:
            try:
                if self._renew_http():
                    self.http_verified = True
                    return True
            except NeedsSession:
                pass
        with self._clear_lock:
            return self._ensure_session_locked(timeout_s)

    def _ensure_session_locked(self, timeout_s: int | None = None) -> bool:
        # re-check: another agent may have cleared while we waited for the lock
        if self.session_ready():
            try:
                if self._renew_http():
                    self.http_verified = True
                    return True
            except NeedsSession:
                pass
        if self._bootstrap_http() or self._bootstrap_nokey():
            self.http_verified = True
            return True
        if self._refresh_from_shared() and self._renew_http() and self.session_ready():
            self.http_verified = True
            return True
        per = int(os.getenv("MKV_CLEAR_ATTEMPT_S", "70"))
        attempts = int(os.getenv("MKV_CLEAR_ATTEMPTS", "3"))
        budget = int(timeout_s or self._bootstrap_timeout)
        last_err: Exception | None = None
        used = 0
        for i in range(max(1, attempts)):
            if used >= budget:
                break
            slice_s = min(per, budget - used)
            t0 = time.time()
            try:
                self._bootstrap(slice_s)
                if self._renew_http() and self.session_ready():
                    self.http_verified = True
                    if self._release_browser:
                        self.release_browser()
                    return True
                last_err = MkvbaseError("browser cleared but plain HTTP still 403")
            except Exception as e:
                last_err = e
            used += max(1, int(time.time() - t0))
            if self._release_browser:
                self.release_browser()
            time.sleep(2)
        if last_err:
            raise last_err
        self.http_verified = False
        return False

    def _bootstrap_http(self) -> bool:
        """With the owner's skip-rule header, bare /api/links issues mkv_* cookies
        over plain HTTP, so no browser clearance is needed at all."""
        if not self._origin_key:
            return False
        s = Session(cookies={}, user_agent=_DEFAULT_UA)
        with self._lock:
            self._session = s
        try:
            self._http_request(f"{self.base}/api/links", timeout_s=20)
        except MkvbaseError:
            self._drop_session(s)
            return False
        if not self.session_ready():
            self._drop_session(s)
            return False
        return True

    def _renew_http(self) -> bool:
        """Bare /api/links re-issues every mkv_* cookie. Absorbing them resets the
        challenge expiry without a browser, as long as cf_clearance is alive."""
        try:
            self._http_request(f"{self.base}/api/links", timeout_s=20)
        except MkvbaseError:
            return False
        return self.session_ready()

    # ------------------------------------------------------------------ plain HTTP
    def _http_request(self, url: str, timeout_s: int = 25) -> str:
        """GET with the cleared cookies and a browser-identical TLS fingerprint.
        Verified live: impersonate='firefox133' + cookies captured from a browser
        clearance passes Cloudflare with plain HTTPS (no browser) -> 200 JSON."""
        with self._lock:
            s = self._session
            if s is None or not s.has_mkv_session():
                raise NeedsSession("no session yet")
            cookies, ua = dict(s.cookies), s.user_agent
        try:
            from curl_cffi import requests as cffi
        except ImportError:
            raise NeedsSession("curl_cffi not installed")
        order = [self._impersonate] + [i for i in _IMPERSONATE if i != self._impersonate] \
            if self._impersonate else list(_IMPERSONATE)
        blocked = 0
        last = "no attempt"
        for impersonate in order:
            kwargs = dict(headers={
                "User-Agent": ua or "Mozilla/5.0",
                "X-Requested-With": "XMLHttpRequest",
                "Accept": "*/*",
                "Referer": f"{self.base}/",
                **({self._origin_header: self._origin_key} if self._origin_key else {}),
            }, cookies=cookies, timeout=timeout_s)
            if impersonate:
                kwargs["impersonate"] = impersonate
            if self._proxy:
                kwargs["proxy"] = self._proxy
            try:
                r = cffi.get(url, **kwargs)
            except Exception as e:
                last = f"{type(e).__name__}: {e}"
                continue
            if r.status_code == 200 and r.text.strip():
                low = r.text[:800].lower()
                # CF sometimes returns 200 + challenge HTML; treat as dead clearance
                if ("just a moment" in low or "cf-browser-verification" in low
                        or "attention required" in low):
                    blocked += 1
                    last = "HTTP 200 challenge-html"
                    continue
                self._impersonate = impersonate
                self._absorb(s, r.cookies.items())
                self.http_verified = True
                return r.text
            if r.status_code == 403:
                blocked += 1
            last = f"HTTP {r.status_code}"
        if blocked:
            self._drop_session(s, cf_dead=True)
            self.http_verified = False
            raise NeedsSession("Cloudflare clearance expired (403/challenge)")
        raise MkvbaseError(f"plain-HTTP fetch failed: {last}")

    def _fetch_http(self, make_url) -> dict:
        if not self.session_ready() and not self._renew_http():
            raise NeedsSession("session missing or expired")
        with self._lock:
            ck = dict(self._session.cookies)
        self._pace_http()
        text = self._http_request(make_url(ck))
        obj = self._extract_json(text)
        if obj is None:
            snippet = re.sub(r"\s+", " ", text)[:200]
            # bare HTML with no results almost always means CF interstitial
            if "<html" in (text or "")[:200].lower():
                with self._lock:
                    s = self._session
                if s is not None:
                    self._drop_session(s, cf_dead=True)
                self.http_verified = False
                raise NeedsSession("Cloudflare challenge HTML (no JSON)")
            raise MkvbaseError(f"no JSON with 'results' in response: {snippet!r}")
        return obj

    # ------------------------------------------------------------------ parsing
    def _extract_json(self, text: str) -> dict | None:
        if not text:
            return None
        start = text.find("{")
        if start == -1:
            return None
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    chunk = text[start:i + 1]
                    try:
                        obj = json.loads(chunk)
                    except Exception:
                        continue
                    if isinstance(obj, dict) and "results" in obj:
                        return obj
        return None

    # ------------------------------------------------------------------ browser fallback
    def _fetch_browser(self, make_url, timeout_s: int) -> tuple[dict, str]:
        """Browser fallback when plain HTTP does not pass on this host."""
        return self._on_engine(self._fetch_browser_locked, make_url, timeout_s)

    def _fetch_browser_locked(self, make_url, timeout_s: int) -> tuple[dict, str]:
        body = ""
        with self._lock:
            ck = dict(self._session.cookies) if self._session else None
        if ck is None:
            self._bootstrap_locked(timeout_s)  # already on engine thread
            ck = dict(self._session.cookies)
        # 1) in-page fetch from the cleared page
        try:
            obj = self._extract_json(self.engine.inpage_fetch(make_url(ck), timeout_s=min(timeout_s, 20)) or "")
            if obj is not None:
                return obj, "inpage"
        except Exception:
            pass
        # 2) full top-level navigation to the signed URL, then 3) re-clear and re-sign once
        for mode in ("nav", "nav2"):
            try:
                if mode == "nav2":
                    self._bootstrap_locked(timeout_s)
                    ck = dict(self._session.cookies)
                body, session = self.engine.fetch(make_url(ck), timeout_s=timeout_s)
                with self._lock:
                    self._session = session
                    self._persist_session()
                obj = self._extract_json(body)
                if obj is not None:
                    return obj, mode
            except Exception:
                pass
        snippet = re.sub(r"\s+", " ", (body or ""))[:200]
        raise MkvbaseError(f"no JSON with 'results' in response: {snippet!r}")

    def _fetch(self, make_url, timeout_s: int) -> tuple[dict, str]:
        """Plain HTTP, then clearance + HTTP, then browser fetch. Safe from any thread.

        HTTP concurrency is capped so 10 discovery agents cannot stampede CF.
        """
        self._http_sem.acquire(self._lane_priority())
        try:
            try:
                return self._fetch_http(make_url), "http"
            except NeedsSession:
                pass
            except MkvbaseError:
                pass
            try:
                if self.ensure_session(timeout_s):
                    return self._fetch_http(make_url), "http"
            except NeedsSession:
                pass
            except MkvbaseError:
                pass
            return self._fetch_browser(make_url, timeout_s)
        finally:
            self._http_sem.release()

    # ------------------------------------------------------------------ API
    def _search_url(self, term: str, ent: int):
        return lambda ck: build_search_url(self.base, term, ck["mkv_client_key"], ck.get("mkv_seq", "1"),
                                           ck["mkv_challenge"], ent=ent)

    def _recent_url(self, ck) -> str:
        return f"{self.base}/api/links"

    def recent_trending(self) -> list[str]:
        """GET /api/trending (found in the site's own bundles): titles users are
        searching right now. No signing needed; empty list on any failure."""
        try:
            text = self._http_request(f"{self.base}/api/trending", timeout_s=15)
        except (NeedsSession, MkvbaseError):
            return []
        try:
            obj = json.loads(text[text.find("{"):text.rfind("}") + 1])
            return [str(t).strip() for t in (obj.get("trending") or []) if str(t).strip()]
        except Exception:
            return []

    def recent_http(self) -> dict:
        """Any thread. Browser-free; raises NeedsSession when no clearance is usable."""
        return self._fetch_http(self._recent_url)

    def recent(self, timeout_s: int = 90) -> dict:
        return self._fetch(self._recent_url, timeout_s)[0]

    def search_http(self, term: str, ent: int = 10) -> dict:
        """Any thread. Browser-free; raises NeedsSession when no clearance is usable."""
        return self._finish(self._fetch_http(self._search_url(term, ent)), term, "http")

    def search(self, term: str, ent: int = 10, timeout_s: int = 120) -> dict:
        obj, mode = self._fetch(self._search_url(term, ent), timeout_s)
        return self._finish(obj, term, mode)

    def _finish(self, obj: dict, term: str, mode: str) -> dict:
        obj["_term"] = term
        obj["_engine"] = self.engine_name
        obj["_mode"] = mode
        obj["_scraped_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        if self.cache_path:
            self._cache(obj, term)
        return obj

    # ------------------------------------------------------------------ cache
    def _cache(self, obj: dict, term: str) -> None:
        try:
            if not self.cache_path:
                return
            os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
            safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", term.lower())[:60]
            with open(f"{os.path.splitext(self.cache_path)[0]}_{safe}.json", "w",
                      encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
