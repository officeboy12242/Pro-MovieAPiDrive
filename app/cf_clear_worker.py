"""One-shot Cloudflare clearance worker (async Camoufox + camoufox-captcha).

Run alone:
  .venv/bin/python -m app.cf_clear_worker
  .venv/bin/python -m app.cf_clear_worker --url https://mkvbase.site/api/links --timeout 90

Prints one JSON line to stdout: {"ok": true, "cookies": {...}, "user_agent": "..."}
Designed to run in a fresh process so sync Playwright never fights the async solver.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys


async def clear(url: str, timeout_s: int, headless: bool) -> dict:
    from camoufox.async_api import AsyncCamoufox

    base = url.split("/api/")[0] if "/api/" in url else url.rstrip("/")
    home = base.rstrip("/") + "/"
    deadline = asyncio.get_event_loop().time() + timeout_s

    kwargs = dict(
        headless=headless,
        humanize=True,
        geoip=os.getenv("MKV_GEOIP", "true").lower() in ("1", "true", "yes"),
        window=(1280, 720),
        config={"forceScopeAccess": True},
        disable_coop=True,
        i_know_what_im_doing=True,  # required with disable_coop; silences LeakWarning
    )
    proxy = os.getenv("MKV_PROXY") or None
    if proxy:
        from urllib.parse import urlsplit, unquote
        p = urlsplit(proxy)
        kwargs["proxy"] = {
            "server": f"{p.scheme}://{p.hostname}:{p.port}",
            "username": unquote(p.username or ""),
            "password": unquote(p.password or ""),
        }
        kwargs["geoip"] = True

    async with AsyncCamoufox(**kwargs) as browser:
        page = await browser.new_page()
        await page.set_viewport_size({"width": 1280, "height": 720})
        try:
            await page.goto(home, wait_until="domcontentloaded", timeout=45000)
        except Exception as e:
            return {"ok": False, "error": f"goto-home: {type(e).__name__}: {e}", "cookies": {}}

        # Prefer the dedicated solver (shadow-DOM Turnstile/interstitial).
        solved = False
        try:
            from camoufox_captcha import solve_captcha
            remain = max(20, int(deadline - asyncio.get_event_loop().time()))
            # interstitial on homepage, short retries
            solved = await asyncio.wait_for(
                solve_captcha(
                    page,
                    captcha_type="cloudflare",
                    challenge_type="interstitial",
                    solve_attempts=3,
                    solve_click_delay=4,
                    wait_checkbox_attempts=8,
                    wait_checkbox_delay=3,
                    checkbox_click_attempts=3,
                    attempt_delay=3,
                ),
                timeout=remain,
            )
        except Exception as e:
            solved = False
            solve_err = f"{type(e).__name__}: {e}"
        else:
            solve_err = None

        # Poll for cf_clearance
        cookies: dict[str, str] = {}
        while asyncio.get_event_loop().time() < deadline:
            raw = await page.context.cookies()
            cookies = {c["name"]: c["value"] for c in raw if c.get("value")}
            if "cf_clearance" in cookies:
                break
            await asyncio.sleep(0.4)

        # Harvest mkv_* from the API URL
        if "cf_clearance" in cookies or solved:
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                await asyncio.sleep(1.5)
            except Exception:
                pass
            raw = await page.context.cookies()
            cookies = {c["name"]: c["value"] for c in raw if c.get("value")}

        ua = await page.evaluate("navigator.userAgent")
        mkv_ok = all(k in cookies for k in ("mkv_client_key", "mkv_challenge", "mkv_seq"))
        cf_ok = "cf_clearance" in cookies
        title = ""
        try:
            title = await page.title()
        except Exception:
            pass
        return {
            "ok": bool(mkv_ok and (cf_ok or solved)),
            "cookies": cookies,
            "user_agent": ua,
            "solved": solved,
            "solve_err": solve_err,
            "title": title[:120],
            "cookie_names": sorted(cookies),
        }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.getenv("MKV_CLEAR_URL", "https://mkvbase.site/api/links"))
    ap.add_argument("--timeout", type=int, default=int(os.getenv("MKV_CLEAR_ATTEMPT_S", "75")))
    ap.add_argument("--headless", default=os.getenv("MKV_HEADLESS", "false"))
    args = ap.parse_args(argv)
    headless = str(args.headless).lower() in ("1", "true", "yes")
    # No DISPLAY -> force headless to avoid crash
    if not headless and os.name == "posix" and not os.environ.get("DISPLAY"):
        headless = True
    try:
        result = asyncio.run(clear(args.url, args.timeout, headless))
    except Exception as e:
        result = {"ok": False, "error": f"{type(e).__name__}: {e}", "cookies": {}}
    sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")
    sys.stdout.flush()
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
