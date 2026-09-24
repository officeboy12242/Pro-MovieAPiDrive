"""MkvbaseClient — orchestrates engine + protocol to deliver search/recent results.

Fetch modes (tried in order):
  http     plain HTTPS with captured cookies + browser-identical TLS (curl_cffi).
           After ONE Cloudflare clearance this is all that's needed: ~0.3s, no browser.
  inpage   in-page fetch from the cleared page (mkvbase's own client pattern). ~0.5s.
  nav      full top-level navigation to the signed URL. ~2-5s.
  nav2     settle-then-sign: re-clear on bare endpoint, then one more navigation.

Bootstrap clears CF once and persists cookies+UA to disk (session.json), so a
restarted process can keep searching over plain HTTP without launching a browser.
"""
from __future__ import annotations

import json
import os
import re
import time
import urllib.parse

from .engines.base import BaseEngine, Session
from .protocol import build_search_url

BASE = "https://mkvbase.site"


class MkvbaseError(RuntimeError):
    pass


class MkvbaseClient:
    def __init__(self, engine: BaseEngine | None, base: str = BASE, cache_path: str | None = None):
        self._engine = engine  # created lazily on the engine thread when first needed
        self.base = base
        self.cache_path = cache_path
        self._session: Session | None = None
        self._last_mode = "none"
        self._bootstrap_timeout = int(os.getenv("MKV_BOOTSTRAP_TIMEOUT", "90"))
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

    # ------------------------------------------------------------------ core
    def _bootstrap(self, force: bool = False, timeout_s: int | None = None):
        if timeout_s is None:
            timeout_s = self._bootstrap_timeout
        if self._session is not None and not force and self._session.has_mkv_session():
            if not self._challenge_expired():
                return self._session
        self._session = self.engine.get_session(f"{self.base}/api/links", timeout_s=timeout_s)
        if not self._session.has_mkv_session():
            raise MkvbaseError(
                f"no mkv_* cookies after clearance (got: {sorted(self._session.cookies)})")
        self._persist_session()
        return self._session

    def _challenge_expired(self) -> bool:
        """mkv_challenge embeds an expiry_ms timestamp — re-bootstrap only when stale."""
        try:
            ch = urllib.parse.unquote(self._session.cookies.get("mkv_challenge", ""))
            expiry_ms = int(ch.split(":")[2])
            return time.time() * 1000 > expiry_ms - 30_000  # 30s safety margin
        except Exception:
            return True

    # ------------------------------------------------------------------ plain HTTP fast path
    def _http_get(self, url: str, timeout_s: int = 25) -> str | None:
        """GET with the cleared cookies and a browser-identical TLS fingerprint.
        Verified live: impersonate='firefox133' + cookies captured from the Camoufox
        clearance passes Cloudflare with plain HTTPS (no browser) -> 200 JSON."""
        if self._session is None or not self._session.cookies.get("cf_clearance"):
            return None
        try:
            from curl_cffi import requests as cffi
        except ImportError:
            return None
        for impersonate in ("firefox133", "firefox", "chrome131", None):
            try:
                kwargs = dict(headers={
                    "User-Agent": self._session.user_agent or "Mozilla/5.0",
                    "X-Requested-With": "XMLHttpRequest",
                    "Accept": "*/*",
                    "Referer": f"{self.base}/",
                }, cookies=self._session.cookies, timeout=timeout_s)
                if impersonate:
                    kwargs["impersonate"] = impersonate
                r = cffi.get(url, **kwargs)
                if r.status_code == 200 and r.text.strip():
                    return r.text
                if r.status_code == 403:  # clearance died -> force re-bootstrap next time
                    self._session = None
                    return None
            except Exception:
                continue
        return None

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

    def _get_session_or_raise(self, timeout_s: int | None):
        try:
            return self._bootstrap(timeout_s=timeout_s)
        except MkvbaseError:
            raise
        except Exception as e:
            raise MkvbaseError(f"engine failure during bootstrap: {type(e).__name__}: {e}")

    def _fetch_json(self, url: str, timeout_s: int = 90) -> dict:
        # 1) plain HTTP with cleared cookies — cheapest, no browser
        body = self._http_get(url, timeout_s=min(timeout_s, 25))
        obj = self._extract_json(body)
        if obj is not None:
            self._last_mode = "http"
            return obj
        if self._session is None:
            self._get_session_or_raise(timeout_s)
        # 2) in-page fetch from the cleared page
        try:
            body = self.engine.inpage_fetch(url, timeout_s=min(timeout_s, 20))
            obj = self._extract_json(body or "")
            if obj is not None:
                self._last_mode = "inpage"
                return obj
        except Exception:
            pass
        self._last_mode = "nav"
        # 3) full top-level navigation to the signed URL
        try:
            body, session = self.engine.fetch(url, timeout_s=timeout_s)
            self._session = session
            self._persist_session()
            obj = self._extract_json(body)
            if obj is not None:
                return obj
        except Exception:
            obj = None
        # 4) settle-then-sign: re-clear on bare endpoint, then one more navigation
        try:
            self._bootstrap(force=True, timeout_s=timeout_s)
            body, session = self.engine.fetch(url, timeout_s=timeout_s)
            self._session = session
            self._persist_session()
            obj = self._extract_json(body)
        except Exception:
            obj = None
        if obj is None:
            snippet = re.sub(r"\s+", " ", (body or ""))[:200]
            raise MkvbaseError(f"no JSON with 'results' in response: {snippet!r}")
        return obj

    # ------------------------------------------------------------------ API
    def recent(self, timeout_s: int = 90) -> dict:
        if self._session is None or self._challenge_expired():
            self._get_session_or_raise(timeout_s)
        return self._fetch_json(f"{self.base}/api/links", timeout_s=timeout_s)

    def search(self, term: str, ent: int = 10, timeout_s: int = 120) -> dict:
        if self._session is None or self._challenge_expired():
            self._get_session_or_raise(timeout_s)
        seq = self._session.cookies.get("mkv_seq", "1")
        key = self._session.cookies["mkv_client_key"]
        challenge = self._session.cookies["mkv_challenge"]
        url = build_search_url(self.base, term, key, seq, challenge, ent=ent)
        obj = self._fetch_json(url, timeout_s=timeout_s)
        obj["_term"] = term
        obj["_engine"] = self.engine_name
        obj["_mode"] = self._last_mode
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
