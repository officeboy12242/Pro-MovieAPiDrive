"""Tiny JSONL result store — every successful search/recent is appended and snapshotted.

Also owns LinksIndex: a persistent, id-deduplicated index of every link row ever
seen (recent pages + search rows merged), compacted when it grows past a limit.
"""
from __future__ import annotations

import json
import os
import re
import socket
import threading
import time


# Keys identifying a row across scrape shapes (id is authoritative when present).
_KEYS = ("id", "url", "title")


# --- Mongo reachability guard ------------------------------------------
# On DNS64/NAT64 networks the resolver answers Atlas' A records with a
# synthesized AAAA in the well-known NAT64 prefix 64:ff9b::/96. If the local
# NAT64 gateway is absent or black-holing, pymongo dials those addresses,
# every server stays `Unknown`, and the driver dies with
# "No replica set members found yet" -- even though plain IPv4 to the same
# host:27017 connects fine. Symptom: [index] MongoDB unreachable, then
# [idgap]/[discovery] DISABLED (no durable index, so nothing reaches Render).
# Dropping the synthesized records makes the driver use the real A record.
_NAT64_PREFIX = b"\x00\xff\x9b"  # 64:ff9b::/96 first 3 bytes
_ipv4_only_installed = False


def _is_nat64(addr) -> bool:
    """True for an IPv6 address inside the NAT64 prefix 64:ff9b::/96.

    getaddrinfo hands back AF_INET6 addresses as strings on Windows, so match the
    text form; the packed 16-byte form is handled as a fallback.
    """
    if isinstance(addr, str):
        parts = addr.split(":")
        return len(parts) >= 3 and parts[0] == "64" and parts[1] == "ff9b" \
            and parts[2] in ("", "0")
    try:
        raw = bytes(addr)[:16]
    except (TypeError, ValueError):
        return False
    return len(raw) == 16 and raw[:3] == _NAT64_PREFIX


def prefer_mongo_ipv4(force: bool | None = None) -> bool:
    """Install a process-wide getaddrinfo filter that hides NAT64 addresses.

    Idempotent and safe to call from every Mongo entry point. `force` overrides
    the MKV_MONGO_IPV4_ONLY env var ("0"/"off"/"false" disables the filter,
    "1"/"on"/"true" drops ALL IPv6 answers, unset = auto: only NAT64 records).
    Returns True when the filter is in place.
    """
    global _ipv4_only_installed
    if _ipv4_only_installed:
        return True
    if force is None:
        raw = (os.getenv("MKV_MONGO_IPV4_ONLY") or "").strip().lower()
        if raw in ("0", "off", "false", "no"):
            return False
        force = raw in ("1", "on", "true", "yes")
    real = socket.getaddrinfo

    def _filtered(host, port, *args, **kwargs):
        try:
            got = real(host, port, *args, **kwargs)
        except Exception:
            raise  # never mask a DNS failure with an UnboundLocalError on `got`
        kept = [r for r in got
                if r[0] != socket.AF_INET6 or (not force and not _is_nat64(r[4][0]))]
        return kept or got

    socket.getaddrinfo = _filtered
    _ipv4_only_installed = True
    print("[mongo] resolver guard: %s" % ("IPv4-only" if force else "NAT64-filtered"),
          flush=True)
    return True


_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def _site_tokens(q: str) -> list[str]:
    """mkvbase site search semantics (validated live against the signed
    search API): every token of the query must appear in the row TITLE
    (case-insensitive), token-anywhere/any-order — 'zee5 paathirathri' ==
    'paathirathri zee5'. URLs are NOT searched: 'gdflix.dev' matches rows
    whose url is gdflix.dev yet returns 0. Returns [] only for a
    whitespace-only query (which the site treats as no filter).

    Punctuation is a token separator on the site, so each word is indexed
    two ways: split on every non-alphanumeric run AND with the punctuation
    removed. 'Kuroko's' therefore indexes as {kuroko, s, kurokos}, which
    makes 'Kuroko', 'Kurokos' and "Kuroko's" all find it — the site's own
    behaviour. Without the joined form a bare 'Kuroko' query matched only
    4 of 65 Kuroko rows in the index (measured 2026-10-06)."""
    t = (q or "").lower().strip()
    if not t:
        return []
    toks: set[str] = set()
    for piece in _NON_ALNUM.split(t):
        if piece:
            toks.add(piece)
    for word in t.split():
        joined = _NON_ALNUM.sub("", word)
        if joined:
            toks.add(joined)
    return sorted(toks)


def _site_title_match(title: str, tokens: list[str]) -> bool:
    if not tokens:
        return True
    title_tokens = _site_tokens(title)
    return all(tok in title_tokens for tok in tokens)


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
        """Newest site pushes first (created_at is the site upload time,
        insertion order the tiebreak), optionally filtered exactly like
        the mkvbase site search: every query token must appear in the
        title, case-insensitive, punctuation as a separator."""
        with self._lock:
            self._hits += 1
            rows = list(self._rows.values())
        rows.reverse()
        tokens = _site_tokens(q)
        if tokens:
            rows = [r for r in rows if _site_title_match(r.get("title") or "", tokens)]
        # Newest site pushes first (created_at is the site's upload time);
        # insertion order is the tiebreak. Stable sort keeps it for ties.
        rows.sort(key=lambda r: str(r.get("created_at") or ""), reverse=True)
        return {"count": len(rows), "results": rows[:max(0, limit)]}

    def stats(self) -> dict:
        with self._lock:
            return {"rows": len(self._rows), "max_rows": self.MAX_ROWS,
                    "file": os.path.basename(self.path), "served": self._hits}


class MongoIndex:
    """Links index backed by MongoDB Atlas — durable across Render redeploys,
    sleeps and restarts. One document per link row, deduplicated on _id (the
    row key: id/url/title) via upserts; queries are single-index sorts.

    Search semantics = the mkvbase site's own: every token of the
    query must appear in the row TITLE (case-insensitive), with
    punctuation treated as a separator (see _site_tokens).
    Implemented with a `title_tokens` multikey array + $all (index-backed,
    no fetch window), which is exactly AND-over-tokens.

    db/collection: MKV_MONGO_DB (default mkvbase) / links.
    """

    def __init__(self, uri: str, db: str | None = None):
        from pymongo import DESCENDING, MongoClient
        prefer_mongo_ipv4()
        self._DESC = DESCENDING
        self._client = MongoClient(uri, serverSelectionTimeoutMS=8000,
                                   socketTimeoutMS=20000, maxPoolSize=8)
        self._col = self._client[db or os.getenv("MKV_MONGO_DB", "mkvbase")].links
        self._col.create_index("_seq")  # first-seen order, for newest-first paging
        self._col.create_index("title_tokens")  # whole-token AND search
        self._col.create_index("id")  # idgap block math + max-id lookups
        # /recent orders by the site's own upload time; index it so the
        # unfiltered newest-first scan doesn't sort 385k docs in memory.
        self._col.create_index([("created_at", -1)])
        self._backfill_title_tokens()
        self._hits = 0
        # --- overflow shard (cluster B) -------------------------------
        # When the primary quota fills, new rows (site ids >= overflow min)
        # route to a second Atlas cluster; reads merge both. Rows without a
        # numeric id always stay on the primary.
        self._col2 = None
        self._overflow_min = int(os.getenv("MKV_MONGO_OVERFLOW_MIN_ID", "700000"))
        uri2 = (os.getenv("MKV_MONGODB_URI2") or "").strip()
        if not uri2:
            p2 = os.path.join(os.getenv("MKV_DATA_DIR", "data"), "mongo_uri2.txt")
            try:
                uri2 = open(p2, encoding="utf-8").read().strip()
            except Exception:
                uri2 = ""
        if uri2 and self._overflow_min > 0:
            try:
                self._client2 = MongoClient(uri2, serverSelectionTimeoutMS=8000,
                                            socketTimeoutMS=20000, maxPoolSize=8)
                self._col2 = self._client2[db or os.getenv("MKV_MONGO_DB", "mkvbase")].links
                self._col2.create_index("_seq")
                self._col2.create_index("title_tokens")
                self._col2.create_index("id")
                self._col2.create_index([("created_at", -1)])
                self._col2.estimated_document_count()  # fail fast if unreachable
                print(f"[index] overflow shard armed: ids >= {self._overflow_min} "
                      f"-> secondary cluster", flush=True)
            except Exception as e:
                print(f"[index] overflow shard unavailable ({type(e).__name__}); "
                      f"primary only", flush=True)
                self._col2 = None
        # after the shard is armed: the v2 retokenize covers every shard
        self._retokenize()

    # ---------------------------------------------------------- shard utils
    def cols(self) -> list:
        """All shards to read from (primary first)."""
        return [self._col] if self._col2 is None else [self._col, self._col2]

    def col_for_id(self, rid):
        """Shard that owns a given site id."""
        if self._col2 is None:
            return self._col
        try:
            return self._col2 if int(rid) >= self._overflow_min else self._col
        except Exception:
            return self._col

    def max_id(self):
        """Newest site id across all shards."""
        mx = 0
        for c in self.cols():
            v = (c.find_one(sort=[("id", -1)], projection={"id": 1}) or {}).get("id")
            if v:
                mx = max(mx, int(v))
        return mx or None

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

    def _retokenize(self) -> None:
        """One-time migration (v2), run in a daemon thread.

        title_tokens used to be whitespace-split, so a title like
        "Kuroko's Basketball ..." indexed the single token "kuroko's"
        and a search for 'Kuroko' could never match it — the index
        answered 4 rows where the site serves 65 (measured 2026-10-06).
        Recomputes the array with the shared _site_tokens tokenizer for
        every row whose token set actually changes (rows with
        punctuation in the title; ~half the index). Idempotent and
        guarded by a meta version stamp, so restarts are cheap and a
        concurrent crawl upsert writes the same value either way."""
        try:
            meta = self._col.database["meta"]
            if int((meta.find_one({"_id": "title_tokens_v"}) or {}).get("v", 0) or 0) >= 2:
                return
        except Exception:
            return

        def run() -> None:
            from pymongo import UpdateOne
            shards = [self._col]
            if getattr(self, "_col2", None) is not None:
                shards.append(self._col2)
            changed = 0
            try:
                for col in shards:
                    ops: list = []
                    for doc in col.find({}, {"_id": 1, "title": 1}).batch_size(5000):
                        title = doc.get("title")
                        if title is None:
                            continue
                        toks = _site_tokens(str(title))
                        if set(toks) != set(doc.get("title_tokens") or []):
                            ops.append(UpdateOne(
                                {"_id": doc["_id"]},
                                {"$set": {"title_tokens": toks}}))
                        if len(ops) >= 2000:
                            col.bulk_write(ops, ordered=False)
                            changed += len(ops)
                            ops = []
                    if ops:
                        col.bulk_write(ops, ordered=False)
                        changed += len(ops)
                meta.update_one({"_id": "title_tokens_v"},
                                {"$set": {"v": 2}}, upsert=True)
                print(f"[index] retokenized {changed} rows: "
                      "apostrophe/punctuation-safe title search", flush=True)
            except Exception as e:
                print(f"[index] retokenize deferred: "
                      f"{type(e).__name__}: {e}", flush=True)

        threading.Thread(target=run, daemon=True, name="retokenize").start()

    @staticmethod
    def _doc(row: dict, source: str, seq: float) -> dict:
        # NOTE: no _id here — it is immutable in $set; upsert inserts take it
        # from the equality filter instead.
        doc = {k: row[k] for k in ("id", "title", "url", "created_at", "status")
               if row.get(k) is not None}
        if source:
            doc["_src"] = source
        doc["_seq"] = seq
        # site-style search: every punctuation-separated token of the
        # title, lowercased, plus the punctuation-joined form (see
        # _site_tokens). Must match the query tokenizer exactly.
        doc["title_tokens"] = _site_tokens(str(doc.get("title") or ""))
        return doc

    def upsert(self, rows: list[dict], source: str = "") -> tuple[int, int]:
        """Merge rows by key; returns (rows_new, rows_updated). Rows are routed
        per site id when an overflow shard is armed (ids >= overflow min go to
        the secondary cluster); one bulk op per target shard."""
        from pymongo import UpdateOne
        buckets: dict = {}
        now = time.time()
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            key = _row_key(row)
            if key is None:
                continue
            col = self.col_for_id(row.get("id"))
            buckets.setdefault(id(col), (col, []))[1].append(UpdateOne(
                {"_id": key},
                [{"$set": {**self._doc(row, source, now),
                           "_seq": {"$ifNull": ["$_seq", now]},  # keep first-seen order
                           "_upd": now}}],
                upsert=True))
        new = updated = 0
        for col, ops in buckets.values():
            try:
                res = col.bulk_write(ops, ordered=False)
                new, updated = new + res.upserted_count, updated + res.modified_count
            except Exception:
                # bulk_write counts can be off with retries; make the return exact
                for op in ops:
                    before = col.find_one({"_id": op._filter["_id"]}, {"_id": 1})
                    res = col.update_one(op._filter, op._document[0], upsert=True)
                    if res.upserted_count:
                        new += 1
                    elif before is not None and res.modified_count:
                        updated += 1
        return new, updated
    def recent(self, limit: int = 50, q: str | None = None) -> dict:
        """Newest site pushes first (created_at), filtered exactly like the
        mkvbase site search: every query token must appear in the title,
        with punctuation as a separator. Backed by the title_tokens multikey
        index ($all) — exact at any scale, no fetch window."""
        self._hits += 1
        tokens = list({t.lower() for t in _site_tokens(q)})
        query = {"title_tokens": {"$all": tokens}} if tokens else {}
        proj = {"_id": 0, "_upd": 0, "title_tokens": 0}
        total, merged = 0, []
        for col in self.cols():
            total += col.count_documents(query)
            # Newest site pushes first: created_at is the site's own upload
            # timestamp. _seq (first-discovered) is the tiebreak, so rows the
            # crawler re-discovered today do not leapfrog fresh uploads.
            merged.extend(col.find(query, proj)
                          .sort([("created_at", self._DESC), ("_seq", self._DESC)])
                          .limit(max(0, limit)))
        merged.sort(key=lambda r: (str(r.get("created_at") or ""),
                                   r.get("_seq") or 0), reverse=True)
        # the same site id can live in both clusters (written before the
        # overflow shard took over, then re-upserted there); serve it once.
        # The total stays the cross-shard count (slight overcount from
        # that overlap), since deduping a count would need a full scan.
        seen, unique = set(), []
        for r in merged:
            key = _row_key(r)
            if key is None or key not in seen:
                seen.add(key)
                unique.append(r)
        rows = unique[:max(0, limit)]
        for r in rows:
            r.pop("_seq", None)
        return {"count": total, "results": rows}

    def titles_on_created_day(self, day: str, limit: int = 300) -> list[str]:
        """Titles whose created_at falls on YYYY-MM-DD (site upload day)."""
        day = (day or "")[:10]
        if len(day) < 10:
            return []
        try:
            out = []
            for col in self.cols():
                cur = col.find(
                    {"created_at": {"$regex": f"^{day}"}},
                    {"title": 1, "_id": 0},
                ).limit(max(0, limit) - len(out))
                out.extend(str(r["title"]).strip() for r in cur if r.get("title"))
                if len(out) >= limit:
                    break
            return out
        except Exception:
            return []

    def stats(self) -> dict:
        out = {"rows": sum(c.estimated_document_count() for c in self.cols()),
               "backend": "mongodb", "db": self._col.database.name,
               "served": self._hits}
        if self._col2 is not None:
            out["shards"] = 2
            out["overflow_min_id"] = self._overflow_min
            out["overflow_rows"] = self._col2.estimated_document_count()
        return out
