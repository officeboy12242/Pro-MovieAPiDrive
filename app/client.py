"""MkvbaseClient — orchestrates engine + protocol to deliver search/recent results.

Recipe (validated live):
  1. engine.get_session(GET /api/links)      -> CF clears on top-level nav,
                                                cookies mkv_client_key/challenge/seq set
  2. protocol.build_search_url(...)          -> XOR + PoW + HMAC, all local
  3. engine.fetch(signed_url)                -> JSON renders top-level
  4. parse, cache, return

If the signed URL still hits the interstitial (rare), we settle back on bare
/api/links once and immediately retry the signed URL (the settle-then-sign trick).
"""
from __future__ import annotations

import json
import re
import time

from .engines.base import BaseEngine
from .protocol import build_search_url

BASE = "https://mkvbase.site"


class MkvbaseError(RuntimeError):
    pass


class MkvbaseClient:
    def __init__(self, engine: BaseEngine, base: str = BASE, cache_path: str | None = None):
        self.engine = engine
        self.base = base
        self.cache_path = cache_path
        self._session = None

    # ------------------------------------------------------------------ core
    def _bootstrap(self, force: bool = False, timeout_s: int = 90):
        if self._session is not None and not force and self._session.has_mkv_session():
            return self._session
        self._session = self.engine.get_session(f"{self.base}/api/links", timeout_s=timeout_s)
        if not self._session.has_mkv_session():
            raise MkvbaseError(
                f"no mkv_* cookies after clearance (got: {sorted(self._session.cookies)})")
        return self._session

    def _extract_json(self, text: str) -> dict | None:
        """Find the first JSON object with a 'results' key in page text."""
        if not text:
            return None
        start = text.find("{")
        if start == -1:
            return None
        # scan for a balanced JSON object containing "results"
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

    def _fetch_json(self, url: str, timeout_s: int = 90) -> dict:
        body, session = self.engine.fetch(url, timeout_s=timeout_s)
        self._session = session
        obj = self._extract_json(body)
        if obj is None:
            # settle-then-sign retry: re-clear on bare endpoint, then one more shot
            self._bootstrap(force=True, timeout_s=timeout_s)
            body, session = self.engine.fetch(url, timeout_s=timeout_s)
            self._session = session
            obj = self._extract_json(body)
        if obj is None:
            snippet = re.sub(r"\s+", " ", (body or ""))[:200]
            raise MkvbaseError(f"no JSON with 'results' in response: {snippet!r}")
        return obj

    # ------------------------------------------------------------------ API
    def recent(self, timeout_s: int = 90) -> dict:
        self._bootstrap(timeout_s=timeout_s)
        return self._fetch_json(f"{self.base}/api/links", timeout_s=timeout_s)

    def search(self, term: str, ent: int = 10, timeout_s: int = 120) -> dict:
        session = self._bootstrap(timeout_s=timeout_s)
        seq = session.cookies.get("mkv_seq", "1")
        key = session.cookies["mkv_client_key"]
        challenge = session.cookies["mkv_challenge"]
        url = build_search_url(self.base, term, key, seq, challenge, ent=ent)
        obj = self._fetch_json(url, timeout_s=timeout_s)
        obj["_term"] = term
        obj["_engine"] = self.engine.name
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
            pass  # cache is best-effort
