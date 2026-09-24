"""DrissionPage engine — drives a real Chrome via CDP with human-like fingerprints.

DrissionPage's ChromiumPage is a plain Chrome started with --remote-debugging-port
controlled over CDP. It does NOT set navigator.webdriver automation leaks the way
Selenium/Playwright do, and its requests are indistinguishable from a user typing
in the address bar -> Cloudflare managed challenge auto-clears on top-level
navigation in most cases (exactly what we observed with our Freebuff tab).
"""
from __future__ import annotations

import time

from .base import BaseEngine, Session


class DrissionPageEngine(BaseEngine):
    name = "drissionpage"

    def __init__(self, headless: bool = False, user_data_dir: str | None = None,
                 binary_path: str | None = None):
        self.headless = headless
        self.user_data_dir = user_data_dir
        self.binary_path = binary_path
        self._page = None

    def _ensure(self):
        if self._page is not None:
            return self._page
        from DrissionPage import ChromiumOptions, ChromiumPage
        co = ChromiumOptions()
        if self.headless:
            co.headless()
        if self.user_data_dir:
            co.set_user_data_path(self.user_data_dir)
        if self.binary_path:
            co.set_browser_path(self.binary_path)
        # flags that help with CF + stability
        for arg in ("--no-first-run", "--no-default-browser-check", "--disable-blink-features=AutomationControlled"):
            co.set_argument(arg)
        self._page = ChromiumPage(co)
        return self._page

    @staticmethod
    def _looks_like_challenge(text: str) -> bool:
        t = (text or "")[:800].lower()
        return ("just a moment" in t or "attention required" in t
                or "performing security verification" in t or "challenge-platform" in t)

    def get_session(self, url: str, timeout_s: int = 90) -> Session:
        page = self._ensure()
        page.get(url)
        deadline = time.time() + timeout_s
        last = ""
        while time.time() < deadline:
            time.sleep(1.5)
            try:
                last = page.html[:2000]
            except Exception:
                last = ""
            if not self._looks_like_challenge(last) and last.strip():
                break
        cookies = {c.get("name", ""): c.get("value", "") for c in page.cookies()}
        return Session(cookies=cookies, user_agent=page.user_agent)

    def fetch(self, url: str, timeout_s: int = 90) -> tuple[str, Session]:
        page = self._ensure()
        page.get(url)
        deadline = time.time() + timeout_s
        body = ""
        while time.time() < deadline:
            time.sleep(1.0)
            try:
                body = page.html
            except Exception:
                body = ""
            # JSON API responses render as plain text in body; wait until non-empty
            # and not an interstitial
            if body.strip() and not self._looks_like_challenge(body[:2000]):
                break
        cookies = {c.get("name", ""): c.get("value", "") for c in page.cookies()}
        return body, Session(cookies=cookies, user_agent=page.user_agent)

    def close(self) -> None:
        try:
            if self._page is not None:
                self._page.quit()
        except Exception:
            pass
        self._page = None
