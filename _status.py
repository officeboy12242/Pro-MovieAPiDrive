"""Vault status details for CHECK-status.cmd — log heartbeat, Render health,
DB storage stats. Read-only, browser-free, safe beside the pusher."""
import json
import os
import sys
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ROOT = os.path.dirname(os.path.abspath(__file__))
LOG = os.path.join(ROOT, "data", "pusher.log")
RENDER = os.getenv("MKV_RENDER_URL", "https://pro-movieapidrive.onrender.com")


def human(n) -> str:
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} GB"


def ago(seconds: float) -> str:
    s = int(max(0, seconds))
    if s < 90:
        return f"{s}s ago"
    m = s // 60
    if m < 90:
        return f"{m} min ago"
    h = m // 60
    if h < 36:
        return f"{h}h {m % 60}m ago"
    return f"{h // 24}d {h % 24}h ago"


def section_log() -> None:
    print("~ crawl log (data\\pusher.log)")
    try:
        mt = os.path.getmtime(LOG)
        age = time.time() - mt
        verdict = "RUNNING (log is fresh)" if age < 300 else \
            "STOPPED (log is stale)"
        print(f"  last write  : {ago(age)}  ->  {verdict}")
        tail = []
        try:
            with open(LOG, "r", encoding="utf-8", errors="replace") as f:
                tail = f.readlines()[-4:]
        except OSError:
            pass
        for ln in tail:
            print("  | " + ln.rstrip()[:150])
    except OSError:
        print("  no log file yet (fleet never started on this PC)")


def section_render() -> None:
    print("~ vault API (Render)")
    try:
        with urllib.request.urlopen(f"{RENDER}/health", timeout=30) as r:
            h = json.loads(r.read() or b"{}")
    except Exception as e:
        print(f"  UNREACHABLE: {type(e).__name__}: {str(e)[:80]}")
        return
    links = h.get("links") or {}
    up = int(h.get("uptime_s") or 0)
    print(f"  service     : UP {ago(-up) if False else f'{up // 86400}d {up % 86400 // 3600}h'}"
          f"  |  serve_only={h.get('serve_only')}  mem={h.get('mem_mb')} MB")
    print(f"  vault rows  : {links.get('rows', 0):,}  (served {links.get('served', 0)} queries)")


def section_db() -> None:
    print("~ mongo storage (what the vault consumes)")
    try:
        from app.store import _mongo_uri
        uri = _mongo_uri()
        if not uri:
            print("  no Mongo URI configured (data/mongo_uri.txt)")
            return
        from pymongo import MongoClient
        db_name = os.getenv("MKV_MONGO_DB", "mkvbase")
        cli = MongoClient(uri, serverSelectionTimeoutMS=8000, socketTimeoutMS=20000)
        db = cli[db_name]
        s = db.command("dbstats")
        links = db.command("collstats", "links")
        n = links.get("count", 0)
        print(f"  database    : {db_name}  |  total {human(s.get('dataSize', 0))} data, "
              f"{human(s.get('storageSize', 0))} on disk")
        print(f"  links rows  : {n:,}  |  {human(links.get('storageSize', 0))} collection, "
              f"avg {human(links.get('avgObjSize', 0))}/row")
        free, total = s.get("fsFreeSize", 0), s.get("fsTotalSize", 0)
        if total:
            print(f"  atlas space : {human(free)} free of {human(total)} "
                  f"({100 * free / total:.1f}% free)")
    except Exception as e:
        print(f"  DB stats unavailable: {type(e).__name__}: {str(e)[:80]}")


if __name__ == "__main__":
    section_log()
    print()
    section_render()
    print()
    section_db()
