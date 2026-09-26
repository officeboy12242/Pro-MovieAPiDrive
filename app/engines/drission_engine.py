"""DrissionPage engine — drives a real Chrome via CDP with human-like fingerprints.

DrissionPage's ChromiumPage is a plain Chrome started with --remote-debugging-port
controlled over CDP. It does NOT set navigator.webdriver automation leaks the way
Selenium/Playwright do, so requests look like a user typing in the address bar.
"""
from __future__ import annotations

import time

from .base import BaseEngine, Session, check_launch_ram, looks_like_challenge


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
        check_launch_ram()
        from DrissionPage import ChromiumOptions, ChromiumPage
        co = ChromiumOptions()
        if self.headless:
            co.headless()
        if self.user_data_dir:
            co.set_user_data_path(self.user_data_dir)
        if self.binary_path:
            co.set_browser_path(self.binary_path)
        for arg in ("--no-first-run", "--no-default-browser-check",
                    "--disable-blink-features=AutomationControlled"):
            co.set_argument(arg)
        self._page = ChromiumPage(co)
        return self._page

    def get_session(self, url: str, timeout_s: int = 90) -> Session:
        page = self._ensure()
        page.get(url)
        deadline = time.time() + timeout_s
        last = ""
        while time.time() < deadline:
            time.sleep(0.3)
            try:
                last = page.html[:2000]
            except Exception:
                last = ""
            if last.strip() and not looks_like_challenge(last):
                break
        cookies = {c.get("name", ""): c.get("value", "") for c in page.cookies()}
        return Session(cookies=cookies, user_agent=page.user_agent)

    def fetch(self, url: str, timeout_s: int = 90) -> tuple[str, Session]:
        page = self._ensure()
        page.get(url)
        deadline = time.time() + timeout_s
        body = ""
        while time.time() < deadline:
            time.sleep(0.25)
            try:
                body = page.html
            except Exception:
                body = ""
            if body.strip() and not looks_like_challenge(body[:2000]):
                break
        cookies = {c.get("name", ""): c.get("value", "") for c in page.cookies()}
        return body, Session(cookies=cookies, user_agent=page.user_agent)

    def inpage_fetch(self, url: str, timeout_s: int = 30) -> str | None:
        """In-page fetch() from the cleared page — mkvbase's own client does this.
        Requires X-Requested-With header (server checks it)."""
        page = self._ensure()
        js = """
        return fetch(arguments[0], {headers: {'X-Requested-With': 'XMLHttpRequest'},
            credentials: 'include'})
            .then(r => r.text())
        """
        try:
            out = page.run_js(js, url)
            return out if isinstance(out, str) and out.strip() else None
        except Exception:
            return None

    def close(self) -> None:
        try:
            if self._page is not None:
                self._page.quit()
        except Exception:
            pass
        self._page = None
