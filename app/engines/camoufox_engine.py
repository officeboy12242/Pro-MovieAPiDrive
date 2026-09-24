"""Camoufox engine — Firefox fork with humanized fingerprints (anti-detect).

Camoufox is a patched Firefox designed to defeat fingerprinting; its Python API
(camoufox.sync_api / Camoufox) launches it with a realistic fingerprint. The
managed challenge usually auto-clears on plain page.goto; when it doesn't,
click_in_page can click the Turnstile widget as a fallback.

NOTE: sync Playwright objects are bound to their creating thread. The FastAPI
layer must route all engine calls through a single-worker executor (see main.py).
"""
from __future__ import annotations

import time

from .base import BaseEngine, Session


def _looks_like_challenge(html: str) -> bool:
    t = (html or "")[:1200].lower()
    return ("just a moment" in t or "attention required" in t
            or "performing security verification" in t or "challenge-platform" in t)


class CamoufoxEngine(BaseEngine):
    name = "camoufox"

    def __init__(self, headless: bool = True, humanize: bool = True):
        self.headless = headless
        self.humanize = humanize
        self._cm = None
        self._pw = None
        self._ctx = None
        self._page = None

    def _ensure(self):
        if self._page is not None:
            return self._page
        from camoufox.sync_api import Camoufox
        kwargs = {"headless": self.headless}
        if self.humanize:
            kwargs["humanize"] = True  # human-like cursor/mouse behavior
        self._cm = Camoufox(**kwargs)
        self._pw = self._cm.__enter__()
        self._ctx = self._pw.new_context()
        self._page = self._ctx.new_page()
        return self._page

    def _cookies(self) -> dict[str, str]:
        return {c["name"]: c["value"] for c in self._ctx.cookies()}

    def _wait_through_challenge(self, timeout_s: int, want_json: bool = True) -> str:
        """Poll until real content renders. Handles the observed sequence:
        'Just a moment...' -> Turnstile auto-solve (cf_clearance issued) ->
        automatic reload -> target page (JSON starts with '{')."""
        page = self._ensure()
        deadline = time.time() + timeout_s
        reloaded = False
        html = ""
        while time.time() < deadline:
            time.sleep(1.5)
            try:
                html = page.content()
            except Exception:
                html = ""
            if html.strip() and not _looks_like_challenge(html[:1200]):
                if not want_json or "{" in html[:300]:
                    return html
            cleared = any(c["name"] == "cf_clearance" for c in self._ctx.cookies())
            if cleared and not reloaded and time.time() + 4 < deadline:
                # clearance landed but page hasn't re-rendered yet -> nudge one reload
                try:
                    page.reload(wait_until="domcontentloaded", timeout=30000)
                    reloaded = True
                    continue
                except Exception:
                    pass
            # last resort: try clicking the Turnstile checkbox after 20s of stuck
            if _looks_like_challenge(html[:1200]) and timeout_s - (deadline - time.time()) > 20:
                self._click_turnstile()
        return html

    def _click_turnstile(self) -> bool:
        try:
            page = self._page
            page.wait_for_selector("iframe[src*='challenges.cloudflare.com']", timeout=3000)
            box = page.frame_locator("iframe[src*='challenges.cloudflare.com']")
            box.locator("input[type='checkbox'], .ctp-checkbox-label").first.click(timeout=3000)
            time.sleep(3)
            return True
        except Exception:
            return False

    def get_session(self, url: str, timeout_s: int = 90) -> Session:
        page = self._ensure()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
        self._wait_through_challenge(timeout_s, want_json=False)
        return Session(cookies=self._cookies(), user_agent=page.evaluate("navigator.userAgent"))

    def fetch(self, url: str, timeout_s: int = 90) -> tuple[str, Session]:
        page = self._ensure()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
        html = self._wait_through_challenge(timeout_s, want_json=True)
        return html, Session(cookies=self._cookies(), user_agent=page.evaluate("navigator.userAgent"))

    def close(self) -> None:
        try:
            if self._cm is not None:
                self._cm.__exit__(None, None, None)
        except Exception:
            pass
        self._cm = self._pw = self._ctx = self._page = None
