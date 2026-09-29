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


def _site_tokens(q: str) -> list[str]:
    """mkvbase site search semantics (validated live against the signed search API):
    the query is split on whitespace and EVERY token must match the row TITLE as a
    whole token (substring inside a token does not count: 'paathirathi' finds 0 rows
    while 'paathirathri' finds 6). Matching is token-anywhere/any-order AND —
    'zee5 paathirathri' == 'paathirathri zee5' == 'paathirathri  web dl'. URLs are
    NOT searched: 'gdflix.dev' matches rows whose url is gdflix.dev yet returns 0.
    Returns [] only for a whitespace-only query (which the site treats as no filter)."""
    return [t for t in (q or "").split() if t]


def _site_title_match(title: str, tokens: list[str]) -> bool:
    if not tokens:
        return True
    title_tokens = (title or "").lower().split()
    lowered = [t.lower() for t in tokens]
    return all(tok in title_tokens for tok in lowered)


def _row_key(row: dict) -> str | None:
    """Stable dedup key for a link row: id -> url -> title."""
    for k in _KEYS:
        v = row.get(k)
        if v not in (None, ""):
            return f"{k}:{str(v).strip().lower()}"
    return None


def _mongo_uri() -> str | None:
    """MongoDB connection string: MKV_MONGODB_URI env, else data/mongo_uri.txt
    (gitignored) for local runs. None when not configured -> file-backed index."""
    uri = os.getenv("MKV_MONGODB_URI", "")
    if not uri:
        try:
            p = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                             "data", "mongo_uri.txt")
            uri = open(p, encoding="utf-8").read().strip()
        except OSError:
            return None
    return uri or None


def make_index(data_dir: str):
    """Links index backend selector. MKV_INDEX=mongo (or a Mongo URI present)
    -> MongoDB Atlas, durable across redeploys and sleeps; otherwise the
    file-backed LinksIndex in data_dir. Both satisfy the same interface:
    upsert(rows, source) -> (new, updated), recent(limit, q) -> {count, results},
    stats() -> {rows, ...}."""
    if os.getenv("MKV_INDEX", "").lower() == "file":
        return LinksIndex(data_dir)
    uri = _mongo_uri()
    if uri:
        try:
            idx = MongoIndex(uri)
            idx.stats()  # fail fast at boot, fall back to file if unreachable
            print("[index] MongoDB backend (durable across redeploys)", flush=True)
            return idx
        except Exception as e:
            print(f"[index] MongoDB unreachable ({type(e).__name__}: {e}); "
                  "falling back to file index", flush=True)
    return LinksIndex(data_dir)


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
        exactly like the mkvbase site search: whitespace tokens, ALL must appear
        as whole tokens in the title (case-insensitive)."""
        with self._lock:
            self._hits += 1
            rows = list(self._rows.values())
        rows.reverse()
        tokens = _site_tokens(q)
        if tokens:
            rows = [r for r in rows if _site_title_match(r.get("title") or "", tokens)]
        return {"count": len(rows), "results": rows[:max(0, limit)]}

    def stats(self) -> dict:
        with self._lock:
            return {"rows": len(self._rows), "max_rows": self.MAX_ROWS,
                    "file": os.path.basename(self.path), "served": self._hits}


class MongoIndex:
    """Links index backed by MongoDB Atlas — durable across Render redeploys,
    sleeps and restarts. One document per link row, deduplicated on _id (the
    row key: id/url/title) via upserts; queries are single-index sorts.

    Search semantics = the mkvbase site's own: every whitespace token of the
    query must appear as a whole token in the row TITLE (case-insensitive).
    Implemented with a `title_tokens` multikey array + $all (index-backed, no
    fetch window), which is exactly AND-over-tokens.

    db/collection: MKV_MONGO_DB (default mkvbase) / links.
    """

    def __init__(self, uri: str, db: str | None = None):
        from pymongo import DESCENDING, MongoClient
        self._DESC = DESCENDING
        self._client = MongoClient(uri, serverSelectionTimeoutMS=8000,
                                   socketTimeoutMS=20000, maxPoolSize=8)
        self._col = self._client[db or os.getenv("MKV_MONGO_DB", "mkvbase")].links
        self._col.create_index("_seq")  # first-seen order, for newest-first paging
        self._col.create_index("title_tokens")  # whole-token AND search
        self._col.create_index("id")  # idgap block math + max-id lookups
        self._backfill_title_tokens()
        self._hits = 0

    def _backfill_title_tokens(self) -> None:
        """One-time migration: rows written before title_tokens existed get the
        array computed server-side ($split of $title). No-op when current."""
        try:
            if self._col.count_documents({"title_tokens": {"$exists": False}},
                                         limit=1) == 0:
                return
            self._col.update_many(
                {"title_tokens": {"$exists": False}},
                [{"$set": {"title_tokens": {
                    "$map": {"input": {"$split": [
                        {"$toLower": {"$ifNull": ["$title", ""]}}, " "]},
                    "as": "t", "in": "$$t"}}}}],
            )
            print("[index] backfilled title_tokens for site-style search", flush=True)
        except Exception:
            pass

    @staticmethod
    def _doc(row: dict, source: str, seq: float) -> dict:
        # NOTE: no _id here — it is immutable in $set; upsert inserts take it
        # from the equality filter instead.
        doc = {k: row[k] for k in ("id", "title", "url", "created_at", "status")
               if row.get(k) is not None}
        if source:
            doc["_src"] = source
        doc["_seq"] = seq
        # site-style search: whole tokens of the title, lowercased
        doc["title_tokens"] = list({t for t in str(doc.get("title") or "").lower().split() if t})
        return doc

    def upsert(self, rows: list[dict], source: str = "") -> tuple[int, int]:
        """Merge rows by key; returns (rows_new, rows_updated). One bulk op per call."""
        from pymongo import UpdateOne
        ops, new, updated, now = [], 0, 0, time.time()
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            key = _row_key(row)
            if key is None:
                continue
            ops.append(UpdateOne(
                {"_id": key},
                [{"$set": {**self._doc(row, source, now),
                           "_seq": {"$ifNull": ["$_seq", now]},  # keep first-seen order
                           "_upd": now}}],
                upsert=True))
        if not ops:
            return 0, 0
        try:
            res = self._col.bulk_write(ops, ordered=False)
            new, updated = res.upserted_count, res.modified_count
        except Exception:
            # bulk_write counts can be off with retries; make the return exact
            new, updated = 0, 0
            for op in ops:
                before = self._col.find_one({"_id": op._filter["_id"]}, {"_id": 1})
                res = self._col.update_one(op._filter, op._document[0], upsert=True)
                if res.upserted_count:
                    new += 1
                elif before is not None and res.modified_count:
                    updated += 1
        return new, updated

    def recent(self, limit: int = 50, q: str | None = None) -> dict:
        """Newest-discovered-first rows, filtered exactly like the mkvbase site
        search: whitespace tokens, ALL must appear as whole tokens in the title.
        Backed by the title_tokens multikey index ($all) — exact at any scale,
        no fetch window."""
        self._hits += 1
        tokens = list({t.lower() for t in _site_tokens(q)})
        query = {"title_tokens": {"$all": tokens}} if tokens else {}
        total = self._col.count_documents(query)
        rows = list(self._col.find(query, {"_id": 0, "_seq": 0, "_upd": 0,
                                           "title_tokens": 0})
                    .sort("_seq", self._DESC).limit(max(0, limit)))
        return {"count": total, "results": rows}

    def titles_on_created_day(self, day: str, limit: int = 300) -> list[str]:
        """Titles whose created_at falls on YYYY-MM-DD (site upload day)."""
        day = (day or "")[:10]
        if len(day) < 10:
            return []
        try:
            cur = self._col.find(
                {"created_at": {"$regex": f"^{day}"}},
                {"title": 1, "_id": 0},
            ).limit(max(0, limit))
            return [str(r["title"]).strip() for r in cur if r.get("title")]
        except Exception:
            return []

    def stats(self) -> dict:
        return {"rows": self._col.estimated_document_count(),
                "backend": "mongodb", "db": self._col.database.name,
                "served": self._hits}
