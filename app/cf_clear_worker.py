"""Cloudflare clearance worker for Termux.

mkvbase serves a managed \"Just a moment...\" challenge (no Turnstile checkbox).
Camoufox under proot/Xvfb is fingerprint-blocked (clicks=0 forever). Real
Chromium via nodriver headful under Xvfb is what clears this site.

  # install once:
  bash deploy/phone-setup.sh

  # test:
  bash deploy/phone-start.sh   # wraps xvfb
  # or:
  xvfb-run -a -s \"-screen 0 1280x720x24\" \\
    .venv/bin/python -m app.cf_clear_worker --timeout 120
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import sys
import time

API = "https://mkvbase.site/api/links"
_MKV = ("mkv_client_key", "mkv_challenge", "mkv_seq")


def _mkv_ok(ck: dict) -> bool:
    return all(k in ck for k in _MKV)


def _find_chromium() -> str | None:
    env = os.getenv("MKV_CHROME_PATH")
    if env and os.path.isfile(env):
        return env
    for name in ("chromium", "chromium-browser", "google-chrome", "google-chrome-stable", "chrome"):
        p = shutil.which(name)
        if p:
            return p
    for p in ("/usr/bin/chromium", "/usr/bin/chromium-browser",
              "/usr/bin/google-chrome", "/snap/bin/chromium"):
        if os.path.isfile(p):
            return p
    return None


async def clear_nodriver(url: str, timeout_s: int, headless: bool) -> dict:
    import nodriver as uc

    exe = _find_chromium()
    if not exe:
        return {"ok": False, "error": "chromium not installed — run: bash deploy/phone-setup.sh",
                "engine": "nodriver", "cookies": {}}

    if not headless and os.name == "posix" and not os.environ.get("DISPLAY"):
        headless = True

    t0 = time.time()
    browser = await uc.start(
        headless=headless,
        browser_executable_path=exe,
        sandbox=False,
        browser_args=[
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--window-size=1280,720",
            "--disable-blink-features=AutomationControlled",
        ],
    )
    try:
        tab = await browser.get(url)
        deadline = t0 + timeout_s
        last_title = ""
        while time.time() < deadline:
            try:
                raw = await browser.cookies.get_all()
                ck = {c.name: c.value for c in raw if getattr(c, "value", None)}
            except Exception:
                ck = {}
            try:
                last_title = await tab.evaluate("document.title") or ""
            except Exception:
                pass

            if _mkv_ok(ck):
                try:
                    ua = await tab.evaluate("navigator.userAgent")
                except Exception:
                    ua = ""
                return {
                    "ok": True,
                    "engine": "nodriver",
                    "cookies": ck,
                    "user_agent": ua,
                    "took_s": round(time.time() - t0, 1),
                    "title": last_title[:120],
                    "cookie_names": sorted(ck),
                    "cf": "cf_clearance" in ck,
                    "chrome": exe,
                }

            # managed challenge: wait; occasionally reload if stuck >45s with no cf
            elapsed = time.time() - t0
            if elapsed > 45 and "cf_clearance" not in ck and int(elapsed) % 20 < 2:
                try:
                    await tab.reload()
                except Exception:
                    pass
            await tab.sleep(1.0)

        try:
            raw = await browser.cookies.get_all()
            ck = {c.name: c.value for c in raw if getattr(c, "value", None)}
        except Exception:
            ck = {}
        try:
            html = await tab.get_content()
            html_hint = (html or "")[:180].replace("\n", " ")
        except Exception:
            html_hint = ""
        return {
            "ok": False,
            "engine": "nodriver",
            "cookies": ck,
            "took_s": round(time.time() - t0, 1),
            "title": last_title[:120],
            "cookie_names": sorted(ck),
            "html_hint": html_hint,
            "chrome": exe,
            "error": "timeout without mkv_* cookies",
        }
    finally:
        try:
            browser.stop()
        except Exception:
            pass


def clear_camoufox(url: str, timeout_s: int, headless: bool) -> dict:
    """Fallback only — usually fingerprint-blocked on Termux for managed CF."""
    from camoufox.sync_api import Camoufox

    if not headless and os.name == "posix" and not os.environ.get("DISPLAY"):
        headless = True
    t0 = time.time()
    kwargs = dict(
        headless=headless,
        humanize=True,
        geoip=False,
        window=(1280, 720),
        config={"forceScopeAccess": True},
        disable_coop=True,
        i_know_what_im_doing=True,
    )
    with Camoufox(**kwargs) as browser:
        page = browser.new_page()
        page.set_viewport_size({"width": 1280, "height": 720})
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
        except Exception as e:
            return {"ok": False, "engine": "camoufox", "error": f"goto: {e}", "cookies": {}}
        page.wait_for_timeout(5000)
        deadline = t0 + timeout_s
        while time.time() < deadline:
            ck = {c["name"]: c["value"] for c in page.context.cookies() if c.get("value")}
            if _mkv_ok(ck):
                return {
                    "ok": True, "engine": "camoufox", "cookies": ck,
                    "user_agent": page.evaluate("navigator.userAgent"),
                    "took_s": round(time.time() - t0, 1),
                    "title": (page.title() or "")[:120],
                    "cookie_names": sorted(ck), "cf": "cf_clearance" in ck,
                }
            if "cf_clearance" in ck and not _mkv_ok(ck):
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(2000)
                except Exception:
                    pass
            page.wait_for_timeout(1000)
        ck = {c["name"]: c["value"] for c in page.context.cookies() if c.get("value")}
        return {
            "ok": False, "engine": "camoufox", "cookies": ck,
            "took_s": round(time.time() - t0, 1),
            "title": (page.title() or "")[:120],
            "cookie_names": sorted(ck),
            "html_hint": (page.content() or "")[:180].replace("\n", " "),
            "error": "timeout without mkv_* cookies",
        }


def clear(url: str, timeout_s: int, headless: bool) -> dict:
    engine = (os.getenv("MKV_CLEAR_ENGINE") or "auto").lower()
    # Prefer nodriver/chromium — Camoufox is blocked on this CF for Termux.
    if engine in ("auto", "nodriver"):
        try:
            result = asyncio.run(clear_nodriver(url, timeout_s, headless))
            if result.get("ok") or engine == "nodriver":
                return result
            # auto: fall through to camoufox only if chromium missing
            if "chromium not installed" not in (result.get("error") or ""):
                return result
        except Exception as e:
            if engine == "nodriver":
                return {"ok": False, "engine": "nodriver",
                        "error": f"{type(e).__name__}: {e}", "cookies": {}}
    try:
        return clear_camoufox(url, timeout_s, headless)
    except Exception as e:
        return {"ok": False, "engine": "camoufox",
                "error": f"{type(e).__name__}: {e}", "cookies": {}}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.getenv("MKV_CLEAR_URL", API))
    ap.add_argument("--timeout", type=int, default=int(os.getenv("MKV_CLEAR_ATTEMPT_S", "120")))
    ap.add_argument("--headless", default=os.getenv("MKV_HEADLESS", "false"))
    args = ap.parse_args(argv)
    headless = str(args.headless).lower() in ("1", "true", "yes")
    result = clear(args.url, args.timeout, headless)
    sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")
    sys.stdout.flush()
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
