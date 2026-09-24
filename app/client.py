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

from .engines.base import BaseEngine, Session
from .protocol import build_search_url

BASE = "https://mkvbase.site"
_IMPERSONATE = ("firefox133", "firefox", "chrome131", None)
_DEFAULT_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:133.0) Gecko/20100101 Firefox/133.0"


class MkvbaseError(RuntimeError):
    pass


class NeedsSession(MkvbaseError):
    """Plain HTTP cannot proceed: no clearance yet, or Cloudflare clearance died."""


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
        self._load_persisted_session()

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
        """Engine thread only. Close the browser; it relaunches lazily if needed again."""
        if self._engine is not None and self._engine.is_open():
            self._engine.close()

    # ------------------------------------------------------------------ session persistence
    def _session_file(self) -> str | None:
        if not self.cache_path:
            return None
        return os.path.join(os.path.dirname(self.cache_path), "session.json")

    def _load_persisted_session(self) -> None:
        sf = self._session_file()
        if not sf or not os.path.exists(sf):
            return
        try:
            with open(sf, encoding="utf-8") as f:
                data = json.load(f)
            s = Session(cookies=data.get("cookies", {}), user_agent=data.get("user_agent", ""))
            if s.has_mkv_session():
                self._session = s
        except Exception:
            pass

    def _persist_session(self) -> None:
        sf = self._session_file()
        if not sf or self._session is None:
            return
        try:
            os.makedirs(os.path.dirname(sf), exist_ok=True)
            with open(sf, "w", encoding="utf-8") as f:
                json.dump({"cookies": self._session.cookies,
                           "user_agent": self._session.user_agent}, f)
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
        with self._lock:
            s = self._session
            return bool(s and s.has_mkv_session() and (self._origin_key or s.cookies.get("cf_clearance"))
                        and not self._challenge_expired())

    def _absorb(self, s: Session, cookies) -> None:
        """Merge Set-Cookie values from a plain-HTTP response into the live session."""
        with self._lock:
            if self._session is not s:  # a fresh clearance replaced it meanwhile
                return
            for k, v in cookies:
                if v:
                    s.cookies[k] = v
            self._persist_session()

    def _drop_session(self, s: Session) -> None:
        with self._lock:
            if self._session is s:
                self._session = None

    def _bootstrap(self, timeout_s: int | None = None) -> Session:
        """Engine thread only. Full browser clearance on the bare endpoint."""
        s = self.engine.get_session(f"{self.base}/api/links", timeout_s=timeout_s or self._bootstrap_timeout)
        if not s.has_mkv_session():
            raise MkvbaseError(f"Cloudflare did not clear: no mkv_* cookies after {timeout_s or self._bootstrap_timeout}s "
                               f"(got cookies: {sorted(s.cookies)})")
        with self._lock:
            self._session = s
            self._persist_session()
        return s

    def ensure_session(self, timeout_s: int | None = None) -> bool:
        """Engine thread only. Make the session usable, cheapest way first:
        already valid -> renew mkv_* over plain HTTP -> browser clearance.
        Returns whether the plain-HTTP path works (False = only the browser does)."""
        if self.session_ready() or self._renew_http() or self._bootstrap_http():
            self.http_verified = True
            return True
        try:
            self._bootstrap(timeout_s)
        except Exception:
            if self._release_browser:
                self.release_browser()  # do not sit on ~300MB of Firefox between retries
            raise
        self.http_verified = self._renew_http()
        if self.http_verified and self._release_browser:
            self.release_browser()
        return self.http_verified

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
            if s is None or not (self._origin_key or s.cookies.get("cf_clearance")):
                raise NeedsSession("no Cloudflare clearance yet")
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
                self._impersonate = impersonate
                self._absorb(s, r.cookies.items())
                return r.text
            if r.status_code == 403:
                blocked += 1
            last = f"HTTP {r.status_code}"
        if blocked == len(order):  # every fingerprint refused: clearance dead (or skip rule not matching)
            self._drop_session(s)
            raise NeedsSession("Cloudflare clearance expired (403)")
        raise MkvbaseError(f"plain-HTTP fetch failed: {last}")

    def _fetch_http(self, make_url) -> dict:
        if not self.session_ready() and not self._renew_http():
            raise NeedsSession("session missing or expired")
        with self._lock:
            ck = dict(self._session.cookies)
        text = self._http_request(make_url(ck))
        obj = self._extract_json(text)
        if obj is None:
            snippet = re.sub(r"\s+", " ", text)[:200]
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
        """Engine thread only. Used when plain HTTP does not pass on this host."""
        body = ""
        with self._lock:
            ck = dict(self._session.cookies) if self._session else None
        if ck is None:
            self._bootstrap(timeout_s)
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
                    self._bootstrap(timeout_s)
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
        """Engine thread only. Plain HTTP, then clearance + HTTP, then browser fetch."""
        try:
            return self._fetch_http(make_url), "http"
        except NeedsSession:
            if self.ensure_session(timeout_s):
                return self._fetch_http(make_url), "http"
        except MkvbaseError:
            pass
        return self._fetch_browser(make_url, timeout_s)

    # ------------------------------------------------------------------ API
    def _search_url(self, term: str, ent: int):
        return lambda ck: build_search_url(self.base, term, ck["mkv_client_key"], ck.get("mkv_seq", "1"),
                                           ck["mkv_challenge"], ent=ent)

    def _recent_url(self, ck) -> str:
        return f"{self.base}/api/links"

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
