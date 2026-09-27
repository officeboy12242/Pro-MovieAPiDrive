"""Cloudflare clearance worker for Termux.

mkvbase uses a managed \"Just a moment...\" challenge. Camoufox is fingerprint-
blocked under proot/Xvfb. Real Chromium via nodriver is required.

Chromium source (first match wins):
  1) MKV_CHROME_PATH
  2) Playwright-bundled Chromium  (~/.cache/ms-playwright/...)
  3) system chromium / chromium-browser

Install once:
  bash deploy/phone-setup.sh

Test:
  xvfb-run -a -s \"-screen 0 1280x720x24\" \\
    env MKV_HEADLESS=false MKV_CLEAR_ENGINE=nodriver \\
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
from pathlib import Path

API = "https://mkvbase.site/api/links"
_MKV = ("mkv_client_key", "mkv_challenge", "mkv_seq")


def _mkv_ok(ck: dict) -> bool:
    return all(k in ck for k in _MKV)


def _find_playwright_chrome() -> str | None:
    """Locate Playwright-downloaded Chromium (works in proot; no snap)."""
    roots = []
    env = os.getenv("PLAYWRIGHT_BROWSERS_PATH")
    if env:
        roots.append(Path(env))
    roots += [
        Path.home() / ".cache" / "ms-playwright",
        Path(os.environ.get("XDG_CACHE_HOME", Path.home() / ".cache")) / "ms-playwright",
    ]
    # also next to the venv if playwright put it there
    try:
        import playwright
        roots.append(Path(playwright.__file__).resolve().parent / "driver" / "package" / ".local-browsers")
    except Exception:
        pass
    candidates: list[Path] = []
    for root in roots:
        if not root.is_dir():
            continue
        for pat in ("chromium-*/chrome-linux*/chrome",
                    "chromium_headless_shell-*/chrome-linux*/headless_shell",
                    "chromium-*/chrome-linux/chrome"):
            candidates.extend(root.glob(pat))
    # prefer full chrome over headless_shell
    for c in sorted(candidates, key=lambda p: ("headless" in str(p).lower(), str(p)), reverse=False):
        if c.is_file() and os.access(c, os.X_OK):
            return str(c)
    return None


def _find_chromium() -> str | None:
    env = os.getenv("MKV_CHROME_PATH")
    if env and os.path.isfile(env):
        return env
    pw = _find_playwright_chrome()
    if pw:
        return pw
    for name in ("chromium", "chromium-browser", "google-chrome", "google-chrome-stable", "chrome"):
        p = shutil.which(name)
        if p:
            # snap stub in proot is useless
            try:
                if os.path.islink(p) and "snap" in os.readlink(p):
                    continue
            except Exception:
                pass
            return p
    for p in ("/usr/bin/chromium", "/usr/bin/chromium-browser",
              "/usr/bin/google-chrome"):
        if os.path.isfile(p):
            return p
    return None


async def clear_nodriver(url: str, timeout_s: int, headless: bool) -> dict:
    try:
        import nodriver as uc
    except ImportError:
        return {"ok": False, "engine": "nodriver", "cookies": {},
                "error": "nodriver not installed — run: .venv/bin/pip install nodriver"}

    exe = _find_chromium()
    if not exe:
        return {
            "ok": False, "engine": "nodriver", "cookies": {},
            "error": ("no Chromium found. Run: bash deploy/phone-setup.sh "
                      "(installs Playwright Chromium for proot)"),
        }

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
                    "ok": True, "engine": "nodriver", "cookies": ck,
                    "user_agent": ua, "took_s": round(time.time() - t0, 1),
                    "title": last_title[:120], "cookie_names": sorted(ck),
                    "cf": "cf_clearance" in ck, "chrome": exe,
                }

            elapsed = time.time() - t0
            if elapsed > 40 and "cf_clearance" not in ck and int(elapsed) % 25 < 2:
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
            "ok": False, "engine": "nodriver", "cookies": ck,
            "took_s": round(time.time() - t0, 1), "title": last_title[:120],
            "cookie_names": sorted(ck), "html_hint": html_hint, "chrome": exe,
            "error": "timeout without mkv_* cookies",
        }
    finally:
        try:
            browser.stop()
        except Exception:
            pass


def clear(url: str, timeout_s: int, headless: bool) -> dict:
    engine = (os.getenv("MKV_CLEAR_ENGINE") or "nodriver").lower()
    # Never silently burn 120s on Camoufox — it cannot clear this CF on Termux.
    if engine == "camoufox":
        return {"ok": False, "engine": "camoufox", "cookies": {},
                "error": "Camoufox cannot clear mkvbase CF on Termux. Use MKV_CLEAR_ENGINE=nodriver"}
    try:
        return asyncio.run(clear_nodriver(url, timeout_s, headless))
    except Exception as e:
        return {"ok": False, "engine": "nodriver", "cookies": {},
                "error": f"{type(e).__name__}: {e}"}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.getenv("MKV_CLEAR_URL", API))
    ap.add_argument("--timeout", type=int, default=int(os.getenv("MKV_CLEAR_ATTEMPT_S", "120")))
    ap.add_argument("--headless", default=os.getenv("MKV_HEADLESS", "false"))
    args = ap.parse_args(argv)
    headless = str(args.headless).lower() in ("1", "true", "yes")
    # diagnose chrome path up front on stderr so Termux users see it
    chrome = _find_chromium()
    print(f"[cf_clear] chrome={chrome or 'NOT FOUND'} engine={os.getenv('MKV_CLEAR_ENGINE', 'nodriver')}",
          file=sys.stderr, flush=True)
    result = clear(args.url, args.timeout, headless)
    sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")
    sys.stdout.flush()
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
