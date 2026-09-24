"""MkvbaseClient — orchestrates engine + protocol to deliver search/recent results.

Two fetch modes:
  FAST  in-page fetch from the already-cleared page (mkvbase's own client does
        exactly this; needs X-Requested-With header). ~0.3-0.8s per search.
  SLOW  full top-level navigation to the signed URL (~2-5s, session stays warm).

Bootstrap: top-level GET /api/links clears CF and sets mkv_* cookies.
Fallback:  settle-then-sign (re-clear, retry), then slow path, then re-bootstrap.
"""
from __future__ import annotations

import json
import re
import time
import urllib.parse

from .engines.base import BaseEngine
from .protocol import build_search_url, parse_challenge

BASE = "https://mkvbase.site"


class MkvbaseError(RuntimeError):
    pass


class MkvbaseClient:
    def __init__(self, engine: BaseEngine, base: str = BASE, cache_path: str | None = None):
        self.engine = engine
        self.base = base
        self.cache_path = cache_path
        self._session = None
        self._last_mode = "slow"

    # ------------------------------------------------------------------ core
    def _bootstrap(self, force: bool = False, timeout_s: int = 90):
        if self._session is not None and not force and self._session.has_mkv_session():
            if not self._challenge_expired():
                return self._session
        self._session = self.engine.get_session(f"{self.base}/api/links", timeout_s=timeout_s)
        if not self._session.has_mkv_session():
            raise MkvbaseError(
                f"no mkv_* cookies after clearance (got: {sorted(self._session.cookies)})")
        return self._session

    def _challenge_expired(self) -> bool:
        """mkv_challenge embeds an expiry_ms timestamp — re-bootstrap only when stale."""
        try:
            ch = urllib.parse.unquote(self._session.cookies.get("mkv_challenge", ""))
            expiry_ms = int(ch.split(":")[2])
            return time.time() * 1000 > expiry_ms - 30_000  # 30s safety margin
        except Exception:
            return True  # can't tell -> safest to refresh

    def _extract_json(self, text: str) -> dict | None:
        """Find the first JSON object with a 'results' key in page text."""
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

    def _get_session_or_raise(self, timeout_s: int):
        try:
            return self._bootstrap(timeout_s=timeout_s)
        except MkvbaseError:
            raise
        except Exception as e:
            raise MkvbaseError(f"engine failure during bootstrap: {type(e).__name__}: {e}")

    def _fetch_json(self, url: str, timeout_s: int = 90) -> dict:
        # FAST path: in-page fetch, no navigation
        try:
            body = self.engine.inpage_fetch(url, timeout_s=min(timeout_s, 20))
            obj = self._extract_json(body or "")
            if obj is not None:
                self._last_mode = "fast"
                return obj
        except Exception:
            pass
        self._last_mode = "slow"
        # SLOW path: full navigation to the signed URL
        try:
            body, session = self.engine.fetch(url, timeout_s=timeout_s)
            self._session = session
            obj = self._extract_json(body)
            if obj is not None:
                return obj
        except Exception:
            obj = None
        # settle-then-sign: re-clear on bare endpoint, then one more slow shot
        try:
            self._bootstrap(force=True, timeout_s=timeout_s)
            body, session = self.engine.fetch(url, timeout_s=timeout_s)
            self._session = session
            obj = self._extract_json(body)
        except Exception:
            obj = None
        if obj is None:
            snippet = re.sub(r"\s+", " ", (body or ""))[:200]
            raise MkvbaseError(f"no JSON with 'results' in response: {snippet!r}")
        return obj

    # ------------------------------------------------------------------ API
    def recent(self, timeout_s: int = 90) -> dict:
        self._get_session_or_raise(timeout_s)
        return self._fetch_json(f"{self.base}/api/links", timeout_s=timeout_s)

    def search(self, term: str, ent: int = 10, timeout_s: int = 120) -> dict:
        session = self._get_session_or_raise(timeout_s)
        seq = session.cookies.get("mkv_seq", "1")
        key = session.cookies["mkv_client_key"]
        challenge = session.cookies["mkv_challenge"]
        url = build_search_url(self.base, term, key, seq, challenge, ent=ent)
        obj = self._fetch_json(url, timeout_s=timeout_s)
        obj["_term"] = term
        obj["_engine"] = self.engine.name
        obj["_mode"] = "inpage" if self._last_mode == "fast" else "nav"
        obj["_scraped_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        if self.cache_path:
            self._cache(obj, term)
        return obj

    # ------------------------------------------------------------------ cache
    def _cache(self, obj: dict, term: str) -> None:
        try:
            import os
            os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
            safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", term.lower())[:60]
            with open(f"{os.path.splitext(self.cache_path)[0]}_{safe}.json", "w",
                      encoding="utf-8") as f:
                json.dump(obj, f, ensure_ascii=False, indent=2)
        except Exception:
            pass
