"""Tiny JSONL result store — every successful search/recent is appended and snapshotted.

Also owns LinksIndex: a persistent, id-deduplicated index of every link row ever
seen (recent pages + search rows merged), compacted when it grows past a limit.
"""
from __future__ import annotations

import json
import os
import threading
import time


# Keys identifying a row across scrape shapes (id is authoritative when present).
_KEYS = ("id", "url", "title")


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


class LinksIndex:
    """Deduplicated, persistent index of every link row ever seen.

    Rows are keyed by their first present identifier (id -> url -> title), so a
    row posted twice — by different pushers, or in both /recent and /search —
    updates in place instead of duplicating. Sits in memory (dict), persisted to
    links_index.json on every change, compacted when MAX_ROWS is exceeded.
    """

    MAX_ROWS = int(os.getenv("MKV_LINKS_MAX", "20000"))

    def __init__(self, data_dir: str):
        self.path = os.path.join(data_dir, "links_index.json")
        self._lock = threading.Lock()
        self._rows: dict[str, dict] = {}
        self._order: list[str] = []  # insertion order, oldest first
        self._hits = 0
        self._loads()

    # ------------------------------------------------------------------ row key
    @staticmethod
    def _key(row: dict) -> str | None:
        for k in _KEYS:
            v = row.get(k)
            if v not in (None, ""):
                return f"{k}:{str(v).strip().lower()}"
        return None

    # ------------------------------------------------------------------ write
    def upsert(self, rows: list[dict], source: str = "") -> tuple[int, int]:
        """Merge rows; returns (rows_new, rows_updated). Thread-safe."""
        new = updated = 0
        with self._lock:
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                key = self._key(row)
                if key is None:
                    continue
                clean = {k: row[k] for k in ("id", "title", "url", "created_at", "status")
                         if row.get(k) is not None}
                if source:
                    clean["_src"] = source
                if key in self._rows:
                    merged = {**self._rows[key], **clean}
                    if merged != self._rows[key]:
                        updated += 1
                    self._rows[key] = merged
                else:
                    self._rows[key] = clean
                    self._order.append(key)
                    new += 1
            self._enforce_cap_locked()
            self._persist_locked()
        return new, updated

    def _enforce_cap_locked(self) -> None:
        if len(self._rows) <= self.MAX_ROWS:
            return
        drop = set(self._order[:len(self._rows) - self.MAX_ROWS])
        self._order = [k for k in self._order if k not in drop]
        self._rows = {k: v for k, v in self._rows.items() if k not in drop}

    def _persist_locked(self) -> None:
        try:
            tmp = self.path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump({"rows": [self._rows[k] for k in self._order]}, f,
                          ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, self.path)
        except Exception:
            pass

    def _loads(self) -> None:
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
            for row in data.get("rows", []):
                key = self._key(row)
                if key and key not in self._rows:
                    self._rows[key] = row
                    self._order.append(key)
        except Exception:
            pass

    # ------------------------------------------------------------------ read
    def recent(self, limit: int = 50, q: str | None = None) -> dict:
        """Newest-first rows (file order is insertion order), optionally filtered
        by a case-insensitive substring on title/url."""
        with self._lock:
            self._hits += 1
            rows = list(self._rows.values())
        rows.reverse()
        if q:
            q = q.lower()
            rows = [r for r in rows if q in (r.get("title") or "").lower()
                    or q in (r.get("url") or "").lower()]
        return {"count": len(rows), "results": rows[:max(0, limit)]}

    def stats(self) -> dict:
        with self._lock:
            return {"rows": len(self._rows), "max_rows": self.MAX_ROWS,
                    "file": os.path.basename(self.path), "served": self._hits}
