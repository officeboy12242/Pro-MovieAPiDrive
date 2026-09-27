"""Phone doctor - run INSIDE proot Ubuntu at ~/mkv:

  .venv/bin/python _phone_doctor.py

Prints exactly what works and what is broken on this phone, step by step.
Read-only except harmless temp writes (and one optional short browser probe).
"""
from __future__ import annotations

import os
import subprocess
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ".")

# load ~/mkv.env the same way phone-start does
if os.path.isfile(os.path.expanduser("~/mkv.env")):
    for ln in open(os.path.expanduser("~/mkv.env"), encoding="utf-8"):
        ln = ln.strip()
        if not ln or ln.startswith("#") or "=" not in ln:
            continue
        k, _, v = ln.partition("=")
        os.environ.setdefault(k.strip(), v.strip())

print("=" * 26, "1. env keys", "=" * 26)
mongo = os.environ.get("MKV_MONGODB_URI", "")
sync = os.environ.get("MKV_SYNC_KEY", "")
print("MKV_MONGODB_URI set:", bool(mongo) and "PASTE" not in mongo)
print("MKV_SYNC_KEY set:", bool(sync) and "PASTE" not in sync)

print("=" * 26, "2. Mongo vault", "=" * 26)
try:
    from pymongo import MongoClient
    col = MongoClient(mongo, serverSelectionTimeoutMS=10000)["mkvbase"].links
    print("Atlas reachable, rows:", col.estimated_document_count())
except Exception as e:
    print("MONGO FAIL:", type(e).__name__, str(e)[:150])

print("=" * 26, "3. no-browser session bootstrap (THE key test)", "=" * 26)
nokey_ok = False
try:
    from app.client import MkvbaseClient
    c = MkvbaseClient(None, cache_path=None)
    nokey_ok = c._bootstrap_nokey()
    print("no-browser bootstrap:", "SUCCESS - phone needs NO browser at all" if nokey_ok
          else "not on this IP (CF challenged) - browser still needed here")
    if nokey_ok:
        print("   challenge_ttl_s:", round(c.challenge_ttl_s()))
except Exception as e:
    print("client import/bootstrap error:", type(e).__name__, str(e)[:150])

print("=" * 26, "4. camoufox binary (only matters if step 3 failed)", "=" * 26)
path = None
try:
    from camoufox.pkgman import launch_path
    path = launch_path()
    print("browser path:", path)
    env2 = dict(os.environ)
    env2.update({"MOZ_DISABLE_CONTENT_SANDBOX": "1", "MOZ_DISABLE_RDD_SANDBOX": "1",
                 "MOZ_DISABLE_SOCKET_PROCESS_SANDBOX": "1", "MOZ_DISABLE_GMP_SANDBOX": "1"})
    r = subprocess.run([path, "--headless", "--version"], capture_output=True,
                       text=True, timeout=60, env=env2)
    out = ((r.stdout or "") + (r.stderr or "")).strip()
    print("binary --headless --version exit:", r.returncode)
    print("output:", out[:400].replace("\n", " | "))
    for lib in ("libX11", "libgtk", "libdbus", "libgbm", "error", "cannot"):
        if lib.lower() in out.lower():
            print("   mentions:", lib)
except FileNotFoundError:
    print("BROWSER MISSING - run: .venv/bin/python -m camoufox fetch")
except Exception as e:
    print("binary test error:", type(e).__name__, str(e)[:200])
    if "not installed" in str(e).lower() or "CamoufoxNotInstalled" in type(e).__name__:
        print("FIX: .venv/bin/python -m camoufox fetch")

print("=" * 26, "5. one-shot browser clearance (only if step 3 failed)", "=" * 26)
if nokey_ok:
    print("skipped - no-browser path already works")
else:
    try:
        from app.client import MkvbaseClient
        from app.engines import make_engine
        os.environ.setdefault("MKV_HEADLESS", "true")
        for k, v in (("MOZ_DISABLE_CONTENT_SANDBOX", "1"), ("MOZ_DISABLE_RDD_SANDBOX", "1"),
                     ("MOZ_DISABLE_SOCKET_PROCESS_SANDBOX", "1"), ("MOZ_DISABLE_GMP_SANDBOX", "1")):
            os.environ.setdefault(k, v)
        c2 = MkvbaseClient(make_engine(), cache_path=None)
        print("launching Camoufox (may take 1-2 min)...")
        ok = c2.ensure_session(timeout_s=120)
        print("ensure_session:", "SUCCESS plain-HTTP ok" if ok
              else "browser cleared but plain HTTP still blocked")
        print("session_ready:", c2.session_ready(), "cf_clearance:",
              bool((c2._session.cookies if c2._session else {}).get("cf_clearance")))
        c2.release_browser()
    except Exception as e:
        print("BROWSER CLEAR FAIL:", type(e).__name__, str(e)[:250])

print("=" * 26, "verdict", "=" * 26)
print("If step 3 SUCCESS: bash deploy/phone-start.sh  (no browser needed).")
print("If step 3 failed and step 5 SUCCESS: same start command - crawler will work.")
print("If step 4/5 failed: paste this whole output back.")
