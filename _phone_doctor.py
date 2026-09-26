"""Phone doctor - run INSIDE proot Ubuntu at ~/mkv:

  .venv/bin/python _phone_doctor.py

Prints exactly what works and what is broken on this phone, step by step.
Read-only except harmless temp writes.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ".")

print("=" * 26, "1. env keys", "=" * 26)
env = {}
for line in ("MKV_MONGODB_URI", "MKV_SYNC_KEY", "MKV_DATA_DIR", "MKV_DISCOVERY"):
    env[line] = os.environ.get(line, "")
if not env["MKV_MONGODB_URI"]:
    try:
        env["MKV_MONGODB_URI"] = open(os.path.expanduser("~/mkv.env")).read()
    except Exception:
        pass
print("MKV_MONGODB_URI set:", bool(env["MKV_MONGODB_URI"])
      and "PASTE" not in env["MKV_MONGODB_URI"])
print("MKV_SYNC_KEY set:", bool(env["MKV_SYNC_KEY"]))

print("=" * 26, "2. Mongo vault", "=" * 26)
try:
    from pymongo import MongoClient
    col = MongoClient(env["MKV_MONGODB_URI"], serverSelectionTimeoutMS=10000)["mkvbase"].links
    print("Atlas reachable, rows:", col.estimated_document_count())
except Exception as e:
    print("MONGO FAIL:", type(e).__name__, str(e)[:150])

print("=" * 26, "3. no-browser session bootstrap (THE key test)", "=" * 26)
try:
    from app.client import MkvbaseClient
    c = MkvbaseClient(None, cache_path=None)
    ok = c._bootstrap_nokey()
    print("no-browser bootstrap:", "SUCCESS - phone needs NO browser at all" if ok
          else "not on this IP (CF challenged) - browser still needed here")
    if ok:
        print("   challenge_ttl_s:", round(c.challenge_ttl_s()))
except Exception as e:
    print("client import/bootstrap error:", type(e).__name__, str(e)[:150])

print("=" * 26, "4. camoufox binary (only matters if step 3 failed)", "=" * 26)
try:
    from camoufox import launcher
    path = launcher.get_path("camoufox")
    print("browser path:", path)
    env2 = dict(os.environ)
    env2.update({"MOZ_DISABLE_CONTENT_SANDBOX": "1", "MOZ_DISABLE_RDD_SANDBOX": "1",
                 "MOZ_DISABLE_SOCKET_PROCESS_SANDBOX": "1", "MOZ_DISABLE_GMP_SANDBOX": "1"})
    r = subprocess.run([path, "--headless", "--version"], capture_output=True,
                       text=True, timeout=60, env=env2)
    out = ((r.stdout or "") + (r.stderr or "")).strip()
    print("binary --headless --version exit:", r.returncode)
    print("output:", out[:400].replace("\n", " | "))
    for lib in ("libX11", "libgtk", "libdbus", "libgbm"):
        if lib.lower() in out.lower():
            print("   mentions:", lib)
except FileNotFoundError:
    print("BROWSER MISSING - run: .venv/bin/python -m camoufox fetch")
except Exception as e:
    print("binary test error:", type(e).__name__, str(e)[:200])

print("=" * 26, "verdict", "=" * 26)
print("If step 3 says SUCCESS: just run  bash deploy/phone-start.sh  and the")
print("phone crawls with NO browser and NO PC. If step 3 failed, paste steps 3+4")
print("output back to me.")
