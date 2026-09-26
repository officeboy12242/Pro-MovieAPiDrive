"""Camoufox engine — Firefox fork with humanized fingerprints (anti-detect).

Sync Playwright objects are thread-affine; the FastAPI layer routes all engine
calls through a single-worker executor (see app/main.py).
"""
from __future__ import annotations

import os
import time
import urllib.parse

from .base import BaseEngine, Session, check_launch_ram, looks_like_challenge

# Firefox single-process mode (MOZ_FORCE_DISABLE_E10S) makes Playwright's new_page()
# hang forever (verified: launch + new_context fine, new_page never returns). Keep
# multi-process mode and trim it to one content process instead to save RAM.
_DROP_ENV = ("MOZ_FORCE_DISABLE_E10S",)
_LEAN_PREFS = {
    "dom.ipc.processCount": 1,
    "dom.ipc.processCount.webIsolated": 1,
    "dom.ipc.processPrelaunch.enabled": False,
    "fission.autostart": False,
    "network.process.enabled": False,
    "media.rdd-process.enabled": False,
    "media.utility-process.enabled": False,
    "browser.sessionhistory.max_total_viewers": 0,
    "browser.cache.memory.capacity": 16384,
}


class CamoufoxEngine(BaseEngine):
    name = "camoufox"

    def __init__(self, headless: bool = True, humanize: bool = True, proxy: str | None = None):
        # No X server (Termux/proot, ssh without X): a headed launch would die with
        # "no DISPLAY environment variable specified" -> auto-force headless.
        if not headless and os.name == "posix" and not os.environ.get("DISPLAY"):
            headless = True
        self.headless = headless
        self.humanize = humanize
        self.proxy = proxy
        self._cm = None
        self._pw = None
        self._ctx = None
        self._page = None
        self.phase = "idle"
        self.phase_since = time.time()

    def _set_phase(self, phase: str) -> None:
        self.phase, self.phase_since = phase, time.time()

    def _ensure(self):
        if self._page is not None:
            return self._page
        check_launch_ram()
        from camoufox.sync_api import Camoufox
        kwargs = {"headless": self.headless,
                  "env": {k: v for k, v in os.environ.items() if k not in _DROP_ENV}}
        # lean prefs save RAM on Render but hurt Turnstile on phone — off unless asked
        if os.getenv("MKV_LEAN_BROWSER", "false").lower() in ("1", "true", "yes"):
            from camoufox import DefaultAddons
            kwargs["firefox_user_prefs"] = dict(_LEAN_PREFS)
            kwargs["exclude_addons"] = [DefaultAddons.UBO]
        if self.humanize:
            kwargs["humanize"] = True
        if self.proxy:
            p = urllib.parse.urlsplit(self.proxy)
            kwargs["proxy"] = {"server": f"{p.scheme}://{p.hostname}:{p.port}",
                               "username": urllib.parse.unquote(p.username or ""),
                               "password": urllib.parse.unquote(p.password or "")}
            kwargs["geoip"] = True
        self._set_phase("launch")
        self._cm = Camoufox(**kwargs)
        self._pw = self._cm.__enter__()
        self._set_phase("new_context")
        self._ctx = self._pw.new_context()
        self._set_phase("new_page")
        self._page = self._ctx.new_page()
        return self._page

    def _cookies(self) -> dict[str, str]:
        return {c["name"]: c["value"] for c in self._ctx.cookies()}

    def _has_cf(self) -> bool:
        return "cf_clearance" in self._cookies()

    def _has_mkv(self) -> bool:
        c = self._cookies()
        return all(k in c for k in ("mkv_client_key", "mkv_challenge", "mkv_seq"))

    def _click_turnstile(self) -> bool:
        try:
            page = self._page
            page.wait_for_selector("iframe[src*='challenges.cloudflare.com']", timeout=2000)
            box = page.frame_locator("iframe[src*='challenges.cloudflare.com']")
            box.locator("input[type='checkbox'], .ctp-checkbox-label").first.click(timeout=2000)
            return True
        except Exception:
            return False

    def _goto(self, url: str, timeout_ms: int = 45000) -> None:
        page = self._ensure()
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        except Exception:
            pass

    def get_session(self, url: str, timeout_s: int = 70) -> Session:
        """Straight to the API URL. Success = mkv_* cookies present (and ideally
        cf_clearance). Fail-fast: click Turnstile often, harvest as soon as CF clears."""
        page = self._ensure()
        deadline = time.time() + timeout_s
        self._set_phase("goto")
        self._goto(url, timeout_ms=min(45000, int(timeout_s * 1000)))
        last_click = 0.0
        harvested = False
        while time.time() < deadline:
            ck = self._cookies()
            has_cf = "cf_clearance" in ck
            has_mkv = all(k in ck for k in ("mkv_client_key", "mkv_challenge", "mkv_seq"))
            self._set_phase(f"clear cf={int(has_cf)} mkv={int(has_mkv)}")
            if has_mkv and has_cf:
                self._set_phase("done")
                return Session(cookies=ck, user_agent=page.evaluate("navigator.userAgent"))
            if has_mkv and not has_cf:
                # cookiefree / already past CF — good enough for this host
                self._set_phase("done")
                return Session(cookies=ck, user_agent=page.evaluate("navigator.userAgent"))
            if has_cf and not has_mkv and not harvested:
                harvested = True
                self._set_phase("harvest")
                self._goto(url, timeout_ms=20000)
                time.sleep(0.8)
                continue
            # poke Turnstile every 3s
            if time.time() - last_click >= 3:
                try:
                    html = page.content()[:1200]
                except Exception:
                    html = ""
                if looks_like_challenge(html) or not has_cf:
                    self._click_turnstile()
                last_click = time.time()
            time.sleep(0.25)
        self._set_phase("done")
        return Session(cookies=self._cookies(),
                       user_agent=page.evaluate("navigator.userAgent"))

    def fetch(self, url: str, timeout_s: int = 90) -> tuple[str, Session]:
        page = self._ensure()
        self._goto(url, timeout_ms=timeout_s * 1000)
        deadline = time.time() + timeout_s
        html = ""
        last_click = 0.0
        while time.time() < deadline:
            try:
                html = page.content()
            except Exception:
                html = ""
            if html.strip() and not looks_like_challenge(html[:1200]) and "{" in html[:300]:
                break
            if time.time() - last_click >= 3:
                self._click_turnstile()
                last_click = time.time()
            time.sleep(0.3)
        return html, Session(cookies=self._cookies(),
                             user_agent=page.evaluate("navigator.userAgent"))

    def inpage_fetch(self, url: str, timeout_s: int = 30) -> str | None:
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
        self._set_phase("closed")
