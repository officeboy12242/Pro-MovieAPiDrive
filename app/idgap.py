"""IdGap miner — id-coverage-aware crawling agents for mkvbase.

Why: mkvbase hands out a sequential integer id per upload (newest /api/links
rows are perfectly consecutive), so the vault's id histogram is an honest
coverage meter: the site's total uploads ~= max id seen, and blocks of ids
with few stored rows are the eras we are missing.

What: background agents (MKV_IDGAP_AGENTS, default 2) that
  1. MEASURE id coverage per MKV_IDGAP_BLOCK-sized block (default 25000)
  2. Pick the thinnest blocks and map ids -> upload dates (via rows we hold)
  3. Turn those eras into targeted search terms (year, year+month, OTT facets,
     letter+year slices) and run them through the SAME client as discovery,
     so results merge into the SAME deduplicated vault (no duplicate rows can
     ever be created — Mongo keys rows by id:url:title)

Design: does NOT touch Discovery's queue/state. Fully independent lane,
observable under its own name in logs: [idgap:a1/block]. Status dump goes to
data/idgap_state.json so you can SEE it is running and what it is doing.

Run inside the pusher (default on when Mongo is configured): the pusher starts
it next to Discovery. Or standalone:  python -m app.idgap
"""
from __future__ import annotations

import json
import os
import random
import threading
import time
from collections import Counter
from datetime import date, timedelta

from .client import MkvbaseClient
from .store import make_index

_OTTS = ("zee5", "amzn", "nf", "hotstar", "sonyliv", "hoichoi", "aha",
         "mx player", "sun nxt", "prime")
_QUALS = ("1080p", "720p", "480p", "2160p", "10bit", "hevc")
_LANGS = ("hindi", "tamil", "telugu", "malayalam", "kannada", "english", "multi")
_ALPHA = "abcdefghijklmnopqrstuvwxyz"
import re as _re

_HEAD_WORD = _re.compile(r"[a-z0-9]+")
_YEAR_IN_TITLE = _re.compile(r"\b((?:19|20)\d{2})\b")
_SERIES_MARK = _re.compile(r"\b(?:s\d{1,2}\s*e?\s*\d{0,3}|season\s*\d{1,2})", _re.I)
_NOISE = _re.compile(
    r"gdflix|hubcloud|hdrip|camrip|dvdscr|www\.|\.com|\.in\b|downloaded",
    _re.I)


def _title_head(title: str) -> str | None:
    """Clean searchable head of a row title: cut at year/season, keep the words
    before it ('Paathirathri 2025 2160p ZEE5 WEB DL...' -> 'paathirathri').
    Sibling uploads of the same film/series share this head, and a search with
    <50 total matches returns them ALL — the one lever that reaches old ids."""
    t = (title or "").strip().lower()
    t = _re.sub(r"^[a-z0-9 ]{2,15}\s*\|\s*", "", t)  # 'GDFlix | ' prefix
    cuts = [m.start() for m in (_YEAR_IN_TITLE.search(t), _SERIES_MARK.search(t)) if m]
    if cuts:
        t = t[:min(cuts)]
    words = [w for w in _HEAD_WORD.findall(t) if len(w) >= 2 and not _NOISE.search(w)]
    if len(words) >= 2:
        return " ".join(words[:5])
    return words[0] if words else None


class IdGapMiner:
    """Targets the thinnest id-blocks with era-appropriate search terms."""

    def __init__(self, client: MkvbaseClient, index, state_dir: str):
        self.client = client
        self.index = index
        self.state_path = os.path.join(state_dir, "idgap_state.json")
        self.block = max(5000, int(os.getenv("MKV_IDGAP_BLOCK", "25000")))
        self.agents_n = max(1, int(os.getenv("MKV_IDGAP_AGENTS", "2")))
        self.gap_s = float(os.getenv("MKV_IDGAP_GAP_S", "12"))
        # A block counts as full when it holds >= FULL_PCT of its id-space
        # (ids ~= uploads, so a fully-crawled 25k block would hold ~25k rows).
        self.full_pct = float(os.getenv("MKV_IDGAP_BLOCK_FULL_PCT", "0.6"))
        self.round_every_s = float(os.getenv("MKV_IDGAP_ROUND_S", str(6 * 3600)))
        # A searched term may be retried after this long (was 20h; that starved
        # thin blocks whose whole seed list had been tried). Default 3h.
        self.term_ttl_s = float(os.getenv("MKV_IDGAP_TERM_TTL_S", str(3 * 3600)))
        # runtime state (persisted so restarts do not redo finished blocks)
        self.done_blocks: set[int] = set()
        self.searched_terms: dict[str, float] = {}
        self.term_stats: dict[str, dict] = {}  # kind -> {tries, hits_in_block, new}
        self.round_started = 0.0
        self.stats = {"rounds": 0, "terms_done": 0, "rows_new": 0,
                      "coverage_last": None, "plan": [], "picked_block": None,
                      "picked_era": None}
        self._load()
        self._lock = threading.Lock()
        self._seed_cache: dict[int, list[str]] = {}  # block -> unsearched heads

    # ------------------------------------------------------------ state
    def _load(self) -> None:
        try:
            with open(self.state_path, encoding="utf-8") as f:
                d = json.load(f)
            self.done_blocks = {int(b) for b in d.get("done_blocks", [])}
            self.searched_terms = {k: float(v) for k, v in
                                   (d.get("searched_terms") or {}).items()}
            self.term_stats = d.get("term_stats") or {}
            self.round_started = float(d.get("round_started") or 0)
            self.stats.update(d.get("stats") or {})
        except Exception:
            pass

    def save(self) -> None:
        with self._lock:
            try:
                os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
                with open(self.state_path, "w", encoding="utf-8") as f:
                    json.dump({"done_blocks": sorted(self.done_blocks)[-500:],
                               "searched_terms": self.searched_terms,
                               "term_stats": self.term_stats,
                               "round_started": self.round_started,
                               "stats": self.stats}, f)
            except Exception:
                pass

    # ------------------------------------------------------------ coverage
    def _col(self):
        return getattr(self.index, "_col", None)

    def coverage(self) -> dict:
        """Id-block coverage of the vault + id->date map from stored rows."""
        col = self._col()
        if col is None:
            return {}
        # shard-aware when the index has an overflow cluster
        cols = list(getattr(self.index, "cols", lambda: [col])())
        get_mx = getattr(self.index, "max_id", None)
        mx = (get_mx() if get_mx else
              (col.find_one(sort=[("id", -1)]) or {}).get("id"))
        if not mx:
            return {}
        b = self.block
        # Server-side aggregation: one result row per block instead of
        # streaming every vault row through Python (149k+ rows would blow
        # the 20s socket timeout — this returns ~25 rows in milliseconds).
        counts: Counter = Counter()
        id_dates: dict[int, str] = {}
        for c in cols:
            for r in c.aggregate([
                    {"$match": {"id": {"$ne": None}}},
                    {"$group": {
                        "_id": {"$floor": {"$divide": ["$id", b]}},
                        "n": {"$sum": 1},
                        "d": {"$max": "$created_at"},  # newest row dates the era
                    }}]):
                blk = int(r["_id"])
                counts[blk] += r["n"]
                d = str(r.get("d") or "")[:10]
                if d:
                    prev = id_dates.get(blk)
                    if not prev or d > prev:
                        id_dates[blk] = d
        total = sum(counts.values())
        plan = []
        full_n = int(self.block * self.full_pct)
        for blk in range(0, mx // b + 1):
            n = counts.get(blk, 0)
            if n >= full_n:
                continue  # actually full - nothing to mine
            # NOTE: done_blocks no longer excludes a thin block. "Term plan
            # exhausted" used to mark blocks done at 9-20% fill and idle the
            # whole miner. Fill is the only exit now; the term TTL paces retries.
            plan.append({"block": blk, "have": n, "fill_pct": round(100 * n / b, 1),
                         "block_start": blk * b,
                         "era_date": self._era_date(blk, id_dates)})
        plan.sort(key=lambda x: (x["have"], -x["block"]))
        cov = round(100 * total / max(1, mx), 1)
        with self._lock:
            self.stats["coverage_last"] = cov
            self.stats["plan"] = plan[:8]
        return {"total_rows": total, "max_id": mx, "coverage_pct": cov,
                "thin_blocks": plan}

    def _era_date(self, blk: int, id_dates: dict[int, str]) -> str:
        """Best-known date for a block: nearest measured neighbor block."""
        for off in range(1, 12):
            for nb in (blk - off, blk + off):
                if nb in id_dates:
                    return id_dates[nb]
        return ""

    # ------------------------------------------------------------ seeds
    def _seeds_for_block(self, blk: int, n: int = 6) -> list[str]:
        """Title-heads of rows we already hold in the thin block. Searching an
        exact head pulls that title's SIBLING uploads (other episodes/qualities
        uploaded around the same time) — the highest-yield way to fill a block,
        because sub-50 searches return everything the site has for it."""
        col = self._col()
        if col is None:
            return []
        heads = self._seed_cache.get(blk)
        if heads is None:
            heads = []
            for r in col.find({"id": {"$gte": blk * self.block,
                                      "$lt": (blk + 1) * self.block}},
                              {"title": 1}).sort("id", -1).limit(800):
                h = _title_head(r.get("title") or "")
                if h and len(h) >= 3:
                    heads.append(h)
            seen: set[str] = set()
            heads = [h for h in heads if not (h in seen or seen.add(h))]
            self._seed_cache[blk] = heads
        fresh = [h for h in heads if h not in self.searched_terms]
        return fresh[:n]

    # ------------------------------------------------------------ term plan
    def _term_kind(self, term: str) -> str:
        if _re.fullmatch(r"(?:19|20)\d{2}(?: [a-z]{3})?", term):
            return "era-year"
        if any(f" {o}" in f" {term}" or term.startswith(o) for o in _OTTS):
            return "era-ott"
        if any(f" {q}" in f" {term}" for q in _QUALS):
            return "era-quality"
        if any(f" {l}" in f" {term}" for l in _LANGS):
            return "era-language"
        return "seed-head"

    def _terms_for_era(self, era: str) -> list[str]:
        """Era (date str) -> targeted search terms. Lean: every term must earn
        its request, deduped against this miner's own history."""
        out: list[str] = []
        try:
            d = date.fromisoformat(era)
        except Exception:
            d = None
        years = []
        if d:
            years = [str(d.year), str(d.year - 1)]
        else:
            years = ["2025", "2026"]
        for y in years:
            out.append(y)
            for m in ("jan", "feb", "mar", "apr", "may", "jun",
                      "jul", "aug", "sep", "oct", "nov", "dec"):
                out.append(f"{y} {m}")
            for ot in _OTTS:
                out.append(f"{y} {ot}")
            for q in _QUALS:
                out.append(f"{y} {q}")
            for l in _LANGS:
                out.append(f"{y} {l}")
        for a in _ALPHA:
            out.append(f"{a} {years[0]}")
        return out

    def _next_terms(self, n: int, agent: str = "a1") -> tuple[int | None, str, list[str]]:
        cov = self.coverage()
        if not cov or not cov["thin_blocks"]:
            return None, "", []
        now = time.time()
        # Walk the 5 thinnest blocks and pick the first with seed terms that
        # were not searched recently (TTL gate), so one exhausted block cannot
        # stall the miner. With 800 seeds/block there is nearly always work.
        min_gap = float(os.getenv("MKV_IDGAP_SEED_MIN_GAP_S", "1200"))  # 20 min
        best: tuple | None = None  # (last_searched_ts, blk, era, seeds)
        for blk_info in cov["thin_blocks"][:5]:
            blk = blk_info["block"]
            era = blk_info.get("era_date") or ""
            seeds = self._seeds_for_block(blk)
            fresh = [s for s in seeds
                     if now - self.searched_terms.get(s, 0) > self.term_ttl_s]
            if fresh:
                off = int(agent[1:]) if agent[1:].isdigit() else 0
                rot = fresh[off % len(fresh):] + fresh[:off % len(fresh)]
                with self._lock:
                    self.stats["picked_block"] = blk_info["block_start"]
                    self.stats["picked_era"] = era
                return blk, era, [f"seed:{s}" for s in rot[:n]]
            # remember the block whose most-recently-searched seed is OLDEST
            lru = min((self.searched_terms.get(s, 0) for s in seeds), default=0)
            if best is None or lru < best[0]:
                best = (lru, blk, era, seeds)
        # nothing fully fresh: reuse the block with the STALEST seeds once its
        # youngest seed is >= min_gap old (prevents hammering the same terms)
        if best and now - best[0] >= min_gap:
            _, blk, era, seeds = best
            seeds = sorted(seeds, key=lambda s: self.searched_terms.get(s, 0))
            off = int(agent[1:]) if agent[1:].isdigit() else 0
            rot = seeds[off % len(seeds):] + seeds[:off % len(seeds)]
            with self._lock:
                self.stats["picked_block"] = blk * self.block
                self.stats["picked_era"] = era
            return blk, era, [f"seed:{s}" for s in rot[:n]]
        # last resort: era terms for the thinnest block
        blk_info = cov["thin_blocks"][0]
        blk, era = blk_info["block"], blk_info.get("era_date") or ""
        pool = self._terms_for_era(era)
        fresh = [t for t in pool
                 if now - self.searched_terms.get(t, 0) > self.term_ttl_s]
        random.shuffle(fresh)
        with self._lock:
            self.stats["picked_block"] = blk_info["block_start"]
            self.stats["picked_era"] = era
        return blk, era, fresh[:n]

    # ------------------------------------------------------------ loop
    def step(self, agent: str = "a1") -> dict:
        # seeds first (multi-block aware), then era terms as fallback
        blk, era, terms = self._next_terms(3, agent)
        if not terms:
            return {"status": "idle", "agent": agent}
        raw = terms[0]
        term = raw[5:] if raw.startswith("seed:") else raw
        kind = self._term_kind(term)
        t0 = time.time()
        try:
            obj = self.client.search(term)
        except Exception as e:
            self.searched_terms[term] = time.time() - 18 * 3600  # retry sooner
            self.save()
            return {"status": "retry", "term": term, "agent": agent,
                    "err": f"{type(e).__name__}: {str(e)[:100]}"}
        rows = [r for r in (obj.get("results") or []) if isinstance(r, dict)]
        new, _upd = self.index.upsert(rows, source=f"idgap:{agent}:b{blk}")
        in_block = sum(1 for r in rows
                       if blk is not None and blk * self.block <= (r.get("id") or 0)
                       < (blk + 1) * self.block)
        st = self.term_stats.setdefault(kind, {"tries": 0, "in_block": 0, "new": 0})
        st["tries"] += 1
        st["in_block"] += in_block
        st["new"] += new
        with self._lock:
            self.stats["terms_done"] += 1
            self.stats["rows_new"] += new
        self.searched_terms[term] = time.time()
        self._seed_cache.pop(blk, None)  # refresh seeds after block work
        # a block is DONE only when it is actually full (fill_pct >= target);
        # term exhaustion never retires a block any more.
        self.save()
        return {"status": "ok", "term": term, "kind": kind, "agent": agent,
                "block": blk, "era": era, "rows": len(rows), "new": new,
                "in_block": in_block, "took_s": round(time.time() - t0, 1)}

    def run(self, log=print, stop: threading.Event | None = None) -> None:
        stop = stop or threading.Event()

        def agent_loop(idx: int) -> None:
            agent = f"a{idx}"
            while not stop.is_set():
                try:
                    info = self.step(agent)
                    if info.get("status") == "ok":
                        log(f"[idgap:{agent}] ok {info['term']!r}/{info.get('kind')} "
                            f"block={info['block']} era={info['era']}: {info['rows']} rows, "
                            f"{info['new']} new, {info.get('in_block', 0)} in-block "
                            f"({info['took_s']}s)  cov={self.stats.get('coverage_last')}%",
                            flush=True)
                    elif info.get("status") == "retry":
                        log(f"[idgap:{agent}] RETRY {info.get('term')}: "
                            f"{info.get('err')}", flush=True)
                        stop.wait(self.gap_s * 2)
                    else:
                        # idle only when EVERY thin block lacks fresh terms;
                        # short sleep so new seeds (fresh crawls) get picked up fast
                        stop.wait(45)
                except Exception as e:
                    log(f"[idgap:{agent}] error {type(e).__name__}: {str(e)[:120]}",
                        flush=True)
                    stop.wait(30)

        threads = [threading.Thread(target=agent_loop, args=(i,), daemon=True,
                                    name=f"idgap-{i}") for i in range(1, self.agents_n + 1)]
        for t in threads:
            t.start()

    def status_line(self) -> str:
        with self._lock:
            s = self.stats
            kinds = " ".join(f"{k}:{v['tries']}t/{v['new']}n" for k, v in
                             sorted(self.term_stats.items()))
            return (f"rounds={s['rounds']} terms={s['terms_done']} "
                    f"new_rows={s['rows_new']} cov={s.get('coverage_last')}% "
                    f"block={s.get('picked_block')} era={s.get('picked_era')} {kinds}")


def start_idgap(client: MkvbaseClient, state_dir: str, log=print) -> IdGapMiner | None:
    """Start the miner when a durable index exists; None otherwise (file index
    would never reach Render). Called by the pusher at startup."""
    idx = make_index(state_dir)
    if idx.stats().get("backend") != "mongodb":
        log("[idgap] DISABLED: MongoDB not configured", flush=True)
        return None
    miner = IdGapMiner(client, idx, state_dir)
    miner.run(log=log)
    log(f"[idgap] online: agents={miner.agents_n} block={miner.block} "
        f"gap={miner.gap_s:.0f}s", flush=True)
    return miner


if __name__ == "__main__":
    # standalone: python -m app.idgap
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.getenv("MKV_DATA_DIR", "data"))
    args = ap.parse_args()
    from .client import MkvbaseClient
    from .engines import make_engine
    cli = MkvbaseClient(make_engine(), cache_path=os.path.join(args.data_dir, "pusher"))
    m = start_idgap(cli, args.data_dir)
    if m:
        cov = m.coverage()
        print("coverage now:", cov.get("coverage_pct"), "% | thin blocks:",
              [b["block_start"] for b in (cov.get("thin_blocks") or [])[:6]])
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            pass
