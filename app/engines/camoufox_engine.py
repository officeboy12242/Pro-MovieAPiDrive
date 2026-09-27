"""Camoufox engine — Firefox fork with humanized fingerprints (anti-detect).

Turnstile lives in a *closed* Shadow DOM. Plain iframe selectors never see it.
Camoufox unlocks that DOM only when launched with:
  config={'forceScopeAccess': True}, disable_coop=True
(same requirements as camoufox-captcha).
"""
from __future__ import annotations

import os
import time
import urllib.parse

from .base import BaseEngine, Session, check_launch_ram, looks_like_challenge

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

# Collect Cloudflare challenge iframes from Camoufox-unlocked shadow roots.
_CF_IFRAME_JS = """
() => {
  const out = [];
  function walk(node) {
    if (!node) return;
    const sr = node.shadowRootUnl || node.shadowRoot;
    if (sr) {
      for (const iframe of sr.querySelectorAll('iframe')) {
        const src = iframe.getAttribute('src') || '';
        if (src.includes('challenges.cloudflare.com')) out.push(iframe);
      }
      walk(sr);
    }
    if (node.querySelectorAll) {
      for (const el of node.querySelectorAll('*')) {
        if (el.shadowRootUnl || el.shadowRoot) walk(el);
      }
    }
  }
  walk(document);
  return out;
}
"""


class CamoufoxEngine(BaseEngine):
    name = "camoufox"

    def __init__(self, headless: bool = True, humanize: bool = True, proxy: str | None = None):
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
        kwargs = {
            "headless": self.headless,
            "humanize": True if self.humanize else False,
            # REQUIRED for Turnstile: unlock closed shadow DOM (shadowRootUnl)
            "config": {"forceScopeAccess": True},
            "disable_coop": True,
            "i_know_what_im_doing": True,  # required with disable_coop
            "window": (1280, 720),
            "env": {k: v for k, v in os.environ.items() if k not in _DROP_ENV},
        }
        if os.getenv("MKV_LEAN_BROWSER", "false").lower() in ("1", "true", "yes"):
            from camoufox import DefaultAddons
            kwargs["firefox_user_prefs"] = dict(_LEAN_PREFS)
            kwargs["exclude_addons"] = [DefaultAddons.UBO]
        if self.proxy:
            p = urllib.parse.urlsplit(self.proxy)
            kwargs["proxy"] = {"server": f"{p.scheme}://{p.hostname}:{p.port}",
                               "username": urllib.parse.unquote(p.username or ""),
                               "password": urllib.parse.unquote(p.password or "")}
            kwargs["geoip"] = True
        elif os.getenv("MKV_GEOIP", "false").lower() in ("1", "true", "yes"):
            kwargs["geoip"] = True
        self._set_phase("launch")
        self._cm = Camoufox(**kwargs)
        self._pw = self._cm.__enter__()
        self._set_phase("new_context")
        self._ctx = self._pw.new_context()
        self._set_phase("new_page")
        self._page = self._ctx.new_page()
        self._page.set_viewport_size({"width": 1280, "height": 720})
        return self._page

    def _cookies(self) -> dict[str, str]:
        return {c["name"]: c["value"] for c in self._ctx.cookies()}

    def _has_cf(self) -> bool:
        return "cf_clearance" in self._cookies()

    def _has_mkv(self) -> bool:
        c = self._cookies()
        return all(k in c for k in ("mkv_client_key", "mkv_challenge", "mkv_seq"))

    def _goto(self, url: str, timeout_ms: int = 45000) -> None:
        try:
            self._ensure().goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        except Exception:
            pass

    def _click_cf_challenge(self) -> bool:
        """Click the Turnstile/interstitial checkbox inside unlocked shadow DOM."""
        page = self._ensure()
        try:
            handle = page.evaluate_handle(_CF_IFRAME_JS)
            props = handle.get_properties()
            for prop in props.values():
                el = prop.as_element()
                if not el:
                    continue
                try:
                    frame = el.content_frame()
                except Exception:
                    frame = None
                if frame is None:
                    continue
                # checkbox is itself inside the iframe's shadow tree
                try:
                    box_handle = frame.evaluate_handle(
                        """() => {
                          const roots = [];
                          function walk(n) {
                            if (!n) return;
                            const sr = n.shadowRootUnl || n.shadowRoot;
                            if (sr) { roots.push(sr); walk(sr); }
                            if (n.querySelectorAll)
                              for (const el of n.querySelectorAll('*'))
                                if (el.shadowRootUnl || el.shadowRoot) walk(el);
                          }
                          walk(document);
                          for (const r of roots) {
                            const c = r.querySelector('input[type=checkbox]');
                            if (c) return c;
                          }
                          return document.querySelector('input[type=checkbox]');
                        }"""
                    )
                    checkbox = box_handle.as_element()
                    if checkbox and checkbox.is_visible():
                        checkbox.click(timeout=3000)
                        self._set_phase("clicked-turnstile")
                        return True
                except Exception:
                    # fallback: try normal frame locator
                    try:
                        frame.locator("input[type=checkbox]").first.click(timeout=2000)
                        self._set_phase("clicked-turnstile")
                        return True
                    except Exception:
                        continue
        except Exception:
            pass
        # last-resort: open-shadow / top-level iframe selector
        try:
            page.frame_locator("iframe[src*='challenges.cloudflare.com']").locator(
                "input[type=checkbox]").first.click(timeout=2000)
            self._set_phase("clicked-turnstile")
            return True
        except Exception:
            return False

    def get_session(self, url: str, timeout_s: int = 70) -> Session:
        """Prefer the async camoufox-captcha worker (shadow-DOM Turnstile). Falls
        back to in-process sync clear if the worker is unavailable."""
        s = self._get_session_via_worker(url, timeout_s)
        if s is not None:
            return s
        return self._get_session_sync(url, timeout_s)

    def _get_session_via_worker(self, url: str, timeout_s: int) -> Session | None:
        import json
        import subprocess
        import sys
        self._set_phase("worker-clear")
        # Close any sync browser first — Playwright sync+async in one process fights.
        self.close()
        env = dict(os.environ)
        env.setdefault("MKV_GEOIP", "true")
        cmd = [sys.executable, "-m", "app.cf_clear_worker",
               "--url", url, "--timeout", str(timeout_s),
               "--headless", "true" if self.headless else "false"]
        try:
            proc = subprocess.run(
                cmd, capture_output=True, text=True, timeout=timeout_s + 60, env=env,
                cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        except Exception as e:
            self._set_phase(f"worker-err:{type(e).__name__}")
            return None
        line = (proc.stdout or "").strip().splitlines()
        line = line[-1] if line else ""
        if not line:
            err = ((proc.stderr or "") + (proc.stdout or ""))[-300:]
            self._set_phase(f"worker-empty:{err[:80]}")
            return None
        try:
            data = json.loads(line)
        except Exception:
            self._set_phase("worker-bad-json")
            return None
        cookies = data.get("cookies") or {}
        self._set_phase(
            f"worker cf={int('cf_clearance' in cookies)} "
            f"mkv={int(all(k in cookies for k in ('mkv_client_key','mkv_challenge','mkv_seq')))} "
            f"solved={data.get('solved')}")
        if not data.get("ok") and not all(k in cookies for k in ("mkv_client_key", "mkv_challenge", "mkv_seq")):
            # still return cookies so caller can see what we got in the error
            return Session(cookies=cookies, user_agent=data.get("user_agent") or "")
        return Session(cookies=cookies, user_agent=data.get("user_agent") or "")

    def _get_session_sync(self, url: str, timeout_s: int = 70) -> Session:
        """Homepage → shadow-DOM click → harvest mkv_* (fallback path)."""
        page = self._ensure()
        deadline = time.time() + timeout_s
        base = "https://mkvbase.site"
        if "/api/" in url:
            base = url.split("/api/")[0]

        self._set_phase("goto-home")
        self._goto(base + "/", timeout_ms=min(45000, int(timeout_s * 1000)))

        last_click = 0.0
        harvested = False
        while time.time() < deadline:
            has_cf, has_mkv = self._has_cf(), self._has_mkv()
            self._set_phase(f"clear cf={int(has_cf)} mkv={int(has_mkv)}")
            if has_mkv:
                self._set_phase("done")
                return Session(cookies=self._cookies(),
                               user_agent=page.evaluate("navigator.userAgent"))
            if has_cf and not harvested:
                harvested = True
                self._set_phase("harvest-api")
                self._goto(url, timeout_ms=25000)
                time.sleep(1.0)
                continue
            if time.time() - last_click >= 4:
                try:
                    html = page.content()[:1500]
                except Exception:
                    html = ""
                if looks_like_challenge(html) or not has_cf:
                    self._click_cf_challenge()
                last_click = time.time()
            time.sleep(0.3)

        if self._has_cf() and not self._has_mkv():
            self._goto(url, timeout_ms=20000)
            time.sleep(1.0)
        self._set_phase("done")
        return Session(cookies=self._cookies(),
                       user_agent=page.evaluate("navigator.userAgent"))

    def fetch(self, url: str, timeout_s: int = 90) -> tuple[str, Session]:
        page = self._ensure()
        self._goto(url, timeout_ms=timeout_s * 1000)
        deadline = time.time() + timeout_s
        html, last_click = "", 0.0
        while time.time() < deadline:
            try:
                html = page.content()
            except Exception:
                html = ""
            if html.strip() and not looks_like_challenge(html[:1200]) and "{" in html[:300]:
                break
            if self._has_mkv() and "{" in (html or "")[:300]:
                break
            if time.time() - last_click >= 4:
                self._click_cf_challenge()
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
