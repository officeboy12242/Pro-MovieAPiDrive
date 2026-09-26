"""One-shot Cloudflare clearance for Termux (sync Camoufox).

Phone doctor already cleared CF once with sync Camoufox on this host.
This is that path as a fresh process — no async captcha lib, no geoip hang.

  xvfb-run -a -s "-screen 0 1280x720x24" \\
    MKV_HEADLESS=false .venv/bin/python -m app.cf_clear_worker --timeout 120
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time

API = "https://mkvbase.site/api/links"

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


def _ck(ctx) -> dict[str, str]:
    return {c["name"]: c["value"] for c in ctx.cookies() if c.get("value")}


def _mkv(ck: dict) -> bool:
    return all(k in ck for k in ("mkv_client_key", "mkv_challenge", "mkv_seq"))


def _challenge(html: str) -> bool:
    t = (html or "")[:2000].lower()
    return any(s in t for s in (
        "just a moment", "attention required", "challenge-platform",
        "turnstile", "checking your browser", "cf-browser-verification"))


def _click(page) -> bool:
    try:
        handle = page.evaluate_handle(_CF_IFRAME_JS)
        for prop in handle.get_properties().values():
            el = prop.as_element()
            if not el:
                continue
            try:
                frame = el.content_frame()
            except Exception:
                continue
            if frame is None:
                continue
            try:
                box = frame.evaluate_handle(
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
                    }""")
                el2 = box.as_element()
                if el2:
                    el2.click(timeout=3000)
                    return True
            except Exception:
                try:
                    frame.locator("input[type=checkbox]").first.click(timeout=2000)
                    return True
                except Exception:
                    continue
    except Exception:
        pass
    try:
        page.frame_locator("iframe[src*='challenges.cloudflare.com']").locator(
            "input[type=checkbox]").first.click(timeout=2000)
        return True
    except Exception:
        return False


def clear(url: str, timeout_s: int, headless: bool) -> dict:
    from camoufox.sync_api import Camoufox

    if not headless and os.name == "posix" and not os.environ.get("DISPLAY"):
        headless = True

    # geoip=True can hang/stall page load on Termux (MaxMind / network). Off unless asked.
    use_geoip = os.getenv("MKV_GEOIP", "false").lower() in ("1", "true", "yes")
    kwargs = dict(
        headless=headless,
        humanize=True,
        geoip=use_geoip,
        window=(1280, 720),
        config={"forceScopeAccess": True},
        disable_coop=True,
        i_know_what_im_doing=True,
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

    t0 = time.time()
    clicks = 0
    with Camoufox(**kwargs) as browser:
        page = browser.new_page()
        page.set_viewport_size({"width": 1280, "height": 720})
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
        except Exception as e:
            return {"ok": False, "error": f"goto: {type(e).__name__}: {e}", "cookies": {}}

        # CF challenge JS needs wall-clock time after domcontentloaded
        page.wait_for_timeout(5000)
        deadline = t0 + timeout_s
        last_click = 0.0

        while time.time() < deadline:
            ck = _ck(page.context)
            if _mkv(ck):
                return {
                    "ok": True,
                    "cookies": ck,
                    "user_agent": page.evaluate("navigator.userAgent"),
                    "took_s": round(time.time() - t0, 1),
                    "clicks": clicks,
                    "title": (page.title() or "")[:120],
                    "cookie_names": sorted(ck),
                    "cf": "cf_clearance" in ck,
                }

            if "cf_clearance" in ck:
                # clearance without mkv_* → re-hit API to harvest site cookies
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    page.wait_for_timeout(2000)
                except Exception:
                    pass
                ck = _ck(page.context)
                if _mkv(ck):
                    return {
                        "ok": True,
                        "cookies": ck,
                        "user_agent": page.evaluate("navigator.userAgent"),
                        "took_s": round(time.time() - t0, 1),
                        "clicks": clicks,
                        "title": (page.title() or "")[:120],
                        "cookie_names": sorted(ck),
                        "cf": True,
                    }

            if time.time() - last_click >= 3.5:
                try:
                    html = page.content()
                except Exception:
                    html = ""
                if _challenge(html) or "cf_clearance" not in ck:
                    if _click(page):
                        clicks += 1
                        page.wait_for_timeout(3000)
                last_click = time.time()
            page.wait_for_timeout(500)

        ck = _ck(page.context)
        try:
            html = page.content()[:200].replace("\n", " ")
            title = page.title()
        except Exception:
            html, title = "", ""
        return {
            "ok": False,
            "cookies": ck,
            "user_agent": page.evaluate("navigator.userAgent"),
            "took_s": round(time.time() - t0, 1),
            "clicks": clicks,
            "title": (title or "")[:120],
            "cookie_names": sorted(ck),
            "html_hint": html,
            "error": "timeout without mkv_* cookies",
        }


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.getenv("MKV_CLEAR_URL", API))
    ap.add_argument("--timeout", type=int, default=int(os.getenv("MKV_CLEAR_ATTEMPT_S", "120")))
    ap.add_argument("--headless", default=os.getenv("MKV_HEADLESS", "false"))
    args = ap.parse_args(argv)
    headless = str(args.headless).lower() in ("1", "true", "yes")
    try:
        result = clear(args.url, args.timeout, headless)
    except Exception as e:
        result = {"ok": False, "error": f"{type(e).__name__}: {e}", "cookies": {}}
    sys.stdout.write(json.dumps(result, ensure_ascii=False) + "\n")
    sys.stdout.flush()
    return 0 if result.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
