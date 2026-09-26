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
    "dom.ipc.processPrelaunch.enabled": False,  # no spare pre-launched content process
    "fission.autostart": False,
    "network.process.enabled": False,  # socket/RDD/utility work folded into the parent
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
        self.proxy = proxy  # e.g. http://user:pass@host:port (residential, when the host IP is CF-blocked)
        self._cm = None
        self._pw = None
        self._ctx = None
        self._page = None
        self.phase = "idle"  # launch -> new_context -> new_page -> goto -> challenge -> done
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
        if os.getenv("MKV_LEAN_BROWSER", "true").lower() in ("1", "true", "yes"):
            from camoufox import DefaultAddons
            kwargs["firefox_user_prefs"] = dict(_LEAN_PREFS)
            kwargs["exclude_addons"] = [DefaultAddons.UBO]  # ad blocker not needed to pass CF
        if self.humanize:
            kwargs["humanize"] = True
        if self.proxy:
            p = urllib.parse.urlsplit(self.proxy)
            kwargs["proxy"] = {"server": f"{p.scheme}://{p.hostname}:{p.port}",
                               "username": urllib.parse.unquote(p.username or ""),
                               "password": urllib.parse.unquote(p.password or "")}
            kwargs["geoip"] = True  # timezone/locale follow the proxy IP, not the host
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

    def _wait_through_challenge(self, timeout_s: int, want_json: bool = False,
                                need_mkv: bool = False) -> str:
        """Poll until real content (and optionally mkv_* cookies). Do NOT treat
        'HTML no longer looks like a challenge' as success — CF often leaves a
        blank/success shell with only cf_chl_* cookies and no clearance."""
        page = self._ensure()
        deadline = time.time() + timeout_s
        reloaded = False
        last_click = 0.0
        html = ""
        while time.time() < deadline:
            time.sleep(0.4)
            try:
                html = page.content()
            except Exception:
                html = ""
            ck = self._cookies()
            has_cf = "cf_clearance" in ck
            has_mkv = all(k in ck for k in ("mkv_client_key", "mkv_challenge", "mkv_seq"))
            self._set_phase(f"challenge cf={int(has_cf)} mkv={int(has_mkv)}")
            if need_mkv and has_mkv:
                return html
            if not need_mkv and html.strip() and not looks_like_challenge(html[:1200]):
                if not want_json or "{" in html[:300]:
                    return html
            if has_cf and not reloaded and time.time() + 4 < deadline:
                try:
                    page.reload(wait_until="domcontentloaded", timeout=30000)
                    reloaded = True
                    continue
                except Exception:
                    pass
            # poke Turnstile every ~8s (auto-solve often needs a click under Xvfb)
            if time.time() - last_click > 8 and (
                    looks_like_challenge(html[:1200]) or not has_cf):
                if self._click_turnstile():
                    last_click = time.time()
        return html

    def _click_turnstile(self) -> bool:
        try:
            page = self._page
            page.wait_for_selector("iframe[src*='challenges.cloudflare.com']", timeout=3000)
            box = page.frame_locator("iframe[src*='challenges.cloudflare.com']")
            box.locator("input[type='checkbox'], .ctp-checkbox-label").first.click(timeout=3000)
            time.sleep(2)
            return True
        except Exception:
            return False

    def get_session(self, url: str, timeout_s: int = 90) -> Session:
        """Navigate until mkv_* cookies exist. Site root first (Turnstile clears
        more reliably on HTML), then the API URL to harvest mkv_* cookies."""
        page = self._ensure()
        deadline = time.time() + timeout_s
        base = url.split("/api/")[0] if "/api/" in url else url.rstrip("/")
        # 1) clear CF on the HTML front door
        self._set_phase("goto")
        try:
            page.goto(base + "/", wait_until="domcontentloaded",
                      timeout=min(90, timeout_s) * 1000)
        except Exception:
            pass
        self._set_phase("challenge")
        remain = max(15, int(deadline - time.time()))
        self._wait_through_challenge(remain, need_mkv=False)
        # 2) hit the API endpoint so Set-Cookie issues mkv_*
        if not self._has_mkv() and time.time() < deadline:
            self._set_phase("goto-api")
            try:
                page.goto(url, wait_until="domcontentloaded",
                          timeout=min(60, max(10, int(deadline - time.time()))) * 1000)
            except Exception:
                pass
            remain = max(10, int(deadline - time.time()))
            self._wait_through_challenge(remain, need_mkv=True)
        # 3) one more reload if we have cf_clearance but still no mkv_*
        if self._has_cf() and not self._has_mkv() and time.time() < deadline:
            try:
                page.goto(url, wait_until="domcontentloaded", timeout=30000)
                time.sleep(2)
            except Exception:
                pass
        self._set_phase("done")
        return Session(cookies=self._cookies(), user_agent=page.evaluate("navigator.userAgent"))

    def fetch(self, url: str, timeout_s: int = 90) -> tuple[str, Session]:
        page = self._ensure()
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_s * 1000)
        html = self._wait_through_challenge(timeout_s, want_json=True, need_mkv=False)
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
        self._set_phase("closed")
