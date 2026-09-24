"""Standalone test v2 of the curl_cffi plain-HTTP fast path.

v1 result: first signed search 200 OK in 1.4s; subsequent requests 403.
Hypothesis: server rotates mkv_seq/mkv_challenge via Set-Cookie after each
search (replay protection) -> must reuse a cookie JAR and re-read cookies
before signing each request. This version:
  - uses curl_cffi Session (persistent cookie jar, auto Set-Cookie)
  - re-reads mkv_seq/challenge from the jar before every signing
  - tries seq increment if the jar value alone is not enough
  - prints 403 body sniff (CF challenge vs app rejection)
"""
import json
import sys
import time

sys.path.insert(0, ".")
from app.protocol import build_search_url, xor_encode_hex, solve_pow, parse_challenge, search_signature
from app.engines.camoufox_engine import CamoufoxEngine
from curl_cffi import requests as cffi

BASE = "https://mkvbase.site"


def extract_results(text: str):
    if not text:
        return None
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    for i in range(start, len(text)):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                try:
                    obj = json.loads(text[start:i + 1])
                except Exception:
                    continue
                if isinstance(obj, dict) and "results" in obj:
                    return obj
    return None


def sniff(r):
    t = (r.text or "")[:150].replace("\n", " ")
    tag = "CF-challenge" if ("just a moment" in t.lower() or "challenge" in t.lower()) else "app-error"
    return f"{r.status_code} {tag}: {t[:100]!r}"


def jar_cookies(jar):
    return {c.name: c.value for c in jar.cookies.jar}


def signed_url(term, jar):
    ck = jar_cookies(jar)
    seq = ck.get("mkv_seq", "1")
    return build_search_url(BASE, term, ck["mkv_client_key"], seq, ck["mkv_challenge"])


def main():
    term = sys.argv[1] if len(sys.argv) > 1 else "godzilla"

    t0 = time.time()
    eng = CamoufoxEngine(headless=True, humanize=True)
    sess = eng.get_session(f"{BASE}/api/links", timeout_s=120)
    print(f"[1] bootstrap: {time.time() - t0:.1f}s | cf_clearance={'cf_clearance' in sess.cookies} | "
          f"mkv={sorted(k for k in sess.cookies if k.startswith('mkv_'))}")
    assert sess.has_mkv_session(), "no mkv session"
    eng.close()  # browser no longer needed at all!

    jar = cffi.Session(impersonate="firefox")
    for k, v in sess.cookies.items():
        jar.cookies.set(k, v, domain="mkvbase.site")

    def cffi_get(url, label):
        r = jar.get(url, headers={
            "User-Agent": sess.user_agent or "Mozilla/5.0",
            "X-Requested-With": "XMLHttpRequest",
            "Accept": "*/*",
            "Referer": f"{BASE}/",
        }, timeout=30)
        obj = extract_results(r.text)
        print(f"    {label}: HTTP {r.status_code} -> {'count=' + str(obj.get('count')) if obj else sniff(r)}")
        time.sleep(2)  # be gentle
        return obj

    # [2] search #1
    url = signed_url(term, jar)
    obj = cffi_get(url, f"search('{term}') #1")
    print(f"    mkv_seq now: {jar_cookies(jar).get('mkv_seq')} (was 1)")

    # [3] search #2 — cookies re-read from jar (post-rotation)
    url2 = signed_url(term + " hindi", jar)
    obj2 = cffi_get(url2, "search #2 (jar cookies)")

    # [4] search #3 — explicit seq+1 variant if jar seq unchanged
    ck = jar_cookies(jar)
    try_seq = str(int(ck.get("mkv_seq", "1")) + 1)
    salt, diff = parse_challenge(ck["mkv_challenge"])
    t_ms = int(time.time() * 1000)
    q = xor_encode_hex(term + " 2", t_ms)
    nonce = solve_pow(salt, diff, q)
    sig = search_signature(ck["mkv_client_key"], q, t_ms, try_seq, nonce, 10)
    url3 = f"{BASE}/api/links?q={q}&t={t_ms}&seq={try_seq}&pow={nonce}&ent=10&sig={sig}"
    obj3 = cffi_get(url3, f"search #3 (seq+1={try_seq})")

    # [5] recent
    obj4 = cffi_get(f"{BASE}/api/links", "recent")

    eng = None
    ok = obj is not None and obj2 is not None
    print("RESULT:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
