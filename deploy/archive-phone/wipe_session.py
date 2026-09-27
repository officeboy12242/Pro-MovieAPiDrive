"""Wipe cached CF sessions so the next warm must clear fresh.

  .venv/bin/python -m app.wipe_session
"""
from __future__ import annotations

import glob
import os
import sys


def main() -> int:
    # load ~/mkv.env if present
    env_path = os.path.expanduser("~/mkv.env")
    if os.path.isfile(env_path):
        for ln in open(env_path, encoding="utf-8"):
            ln = ln.strip()
            if not ln or ln.startswith("#") or "=" not in ln:
                continue
            k, _, v = ln.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

    removed = []
    roots = [
        os.getenv("MKV_DATA_DIR") or "",
        os.path.expanduser("~/mkvdata"),
        "data",
        os.path.join(os.path.dirname(__file__), "..", "data"),
    ]
    for root in roots:
        if not root:
            continue
        for p in glob.glob(os.path.join(root, "**/session.json"), recursive=True):
            try:
                os.remove(p)
                removed.append(p)
            except Exception as e:
                print(f"skip {p}: {e}")
        p = os.path.join(root, "session.json")
        if os.path.isfile(p):
            try:
                os.remove(p)
                removed.append(p)
            except Exception as e:
                print(f"skip {p}: {e}")

    mongo_deleted = 0
    try:
        from app.store import _mongo_uri
        uri = _mongo_uri()
        if uri:
            from pymongo import MongoClient
            r = MongoClient(uri, serverSelectionTimeoutMS=10000)["mkvbase"].sessions.delete_one(
                {"_id": "mkvbase"})
            mongo_deleted = r.deleted_count
    except Exception as e:
        print(f"mongo wipe skipped: {type(e).__name__}: {e}")

    print(f"removed_files={removed}")
    print(f"mongo_sessions_deleted={mongo_deleted}")
    print("OK — next start will clear Cloudflare fresh")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
