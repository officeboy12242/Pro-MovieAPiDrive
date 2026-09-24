"""Tiny JSONL result store — every successful search/recent is appended and snapshotted."""
from __future__ import annotations

import json
import os
import threading
import time


class Store:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)
        self.history_path = os.path.join(data_dir, "history.jsonl")
        self._lock = threading.Lock()

    def _path(self, kind: str, term: str) -> str:
        safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in term.lower())[:60] or "recent"
        return os.path.join(self.data_dir, f"{kind}_{safe}.json")

    def record(self, kind: str, term: str, payload: dict) -> str | None:
        out_path = self._path(kind, term)
        try:
            with self._lock:
                with open(self.history_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps({
                        "ts": os.path.getmtime(self.history_path) if os.path.exists(self.history_path) else 0,
                        "kind": kind, "term": term, "count": payload.get("count"),
                    }, ensure_ascii=False) + "\n")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            return out_path
        except Exception:
            return None

    def list_saved(self) -> list[dict]:
        out = []
        try:
            for fn in sorted(os.listdir(self.data_dir)):
                if fn.endswith(".json"):
                    p = os.path.join(self.data_dir, fn)
                    out.append({"file": fn, "size": os.path.getsize(p),
                                "modified": os.path.getmtime(p)})
        except Exception:
            pass
        return out

    def load_record(self, kind: str, term: str) -> tuple[dict, float] | None:
        """Last saved result for (kind, term) and its age in seconds, if any."""
        p = self._path(kind, term)
        try:
            with open(p, encoding="utf-8") as f:
                obj = json.load(f)
            return (obj, time.time() - os.path.getmtime(p)) if obj.get("results") else None
        except Exception:
            return None

    def load(self, filename: str) -> dict | None:
        if "/" in filename or "\\" in filename or not filename.endswith(".json"):
            return None  # path traversal guard
        try:
            with open(os.path.join(self.data_dir, filename), encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None
