"""Camoufox engine — Firefox fork with humanized fingerprints (anti-detect).

Sync Playwright objects are thread-affine; the FastAPI layer routes all engine
calls through a single-worker executor (see app/main.py).
"""
from __future__ import annotations

import time
import urllib.parse

from .base import BaseEngine, Session, looks_like_challenge


class CamoufoxEngine(BaseEngine):
    name = "camoufox"

    def __init__(self, headless: bool = True, humanize: bool = True, proxy: str | None = None):
        self.headless = headless
        self.humanize = humanize
        self.proxy = proxy  # e.g. http://user:pass@host:port (residential, when the host IP is CF-blocked)
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
            kwargs["humanize"] = True
        if self.proxy:
            p = urllib.parse.urlsplit(self.proxy)
            kwargs["proxy"] = {"server": f"{p.scheme}://{p.hostname}:{p.port}",
                               "username": urllib.parse.unquote(p.username or ""),
                               "password": urllib.parse.unquote(p.password or "")}
            kwargs["geoip"] = True  # timezone/locale follow the proxy IP, not the host
        self._cm = Camoufox(**kwargs)
        self._pw = self._cm.__enter__()
        self._ctx = self._pw.new_context()
        self._page = self._ctx.new_page()
        return self._page

    def _cookies(self) -> dict[str, str]:
        return {c["name"]: c["value"] for c in self._ctx.cookies()}

    def _wait_through_challenge(self, timeout_s: int, want_json: bool = False) -> str:
        """Poll for real content. Handles: challenge -> Turnstile auto-solve ->
        reload -> target page. Optional response capture is armed separately."""
        page = self._ensure()
        deadline = time.time() + timeout_s
        reloaded = False
        html = ""
        while time.time() < deadline:
            time.sleep(0.3)
            try:
                html = page.content()
            except Exception:
                html = ""
            if html.strip() and not looks_like_challenge(html[:1200]):
                if not want_json or "{" in html[:300]:
                    return html
            cleared = any(c["name"] == "cf_clearance" for c in self._ctx.cookies())
            if cleared and not reloaded and time.time() + 4 < deadline:
                try:
                    page.reload(wait_until="domcontentloaded", timeout=30000)
                    reloaded = True
                    continue
                except Exception:
                    pass
            if looks_like_challenge(html[:1200]) and timeout_s - (deadline - time.time()) > 20:
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

    # ------------------------------------------------------------------ fast path
    def _arm_capture(self):
        """Install a JS hook capturing in-page fetch() responses (same-window XHR,
        like mkvbase's own client). Runs on the engine thread only."""
        self._page.evaluate(
            "window.__mkv_resps = [];"
            "if (!window.__mkv_hooked) {"
            "  const of = window.fetch;"
            "  window.fetch = function(...a) {"
            "    return of.apply(this, a).then(r => {"
            "      try { const cl = r.clone(); cl.text().then(t => window.__mkv_resps.push({u: r.url, b: t})); } catch (e) {}"
            "      return r;"
            "    });"
            "  };"
            "  window.__mkv_hooked = true;"
            "}"
        )

    def _pop_capture(self) -> list:
        try:
            return self._page.evaluate("(() => { const r = window.__mkv_resps || []; window.__mkv_resps = []; return r; })()") or []
        except Exception:
            return []

    def inpage_fetch(self, url: str, timeout_s: int = 30) -> str | None:
        """In-page fetch from the cleared page with the XHR header the server
        requires; returns the response body text or None."""
        try:
            page = self._ensure()
            out = page.evaluate(
                "u => fetch(u, {headers: {'X-Requested-With': 'XMLHttpRequest'},"
                "credentials: 'include'}).then(r => r.text())", url)
            return out if isinstance(out, str) and out.strip() else None
        except Exception:
            return None

    def close(self) -> None:
        try:
            if self._cm is not None:
                self._cm.__exit__(None, None, None)
        except Exception:
            pass
        self._cm = self._pw = self._ctx = self._page = None
