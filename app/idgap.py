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
from collections import Counter, deque
from datetime import date, timedelta

from .client import MkvbaseClient
from .store import make_index

_OTTS = ("zee5", "amzn", "nf", "hotstar", "sonyliv", "hoichoi", "aha",
         "mx player", "sun nxt", "prime")
_QUALS = ("1080p", "720p", "480p", "2160p", "10bit", "hevc")
_LANGS = ("hindi", "tamil", "telugu", "malayalam", "kannada", "english", "multi")
_ALPHA = "abcdefghijklmnopqrstuvwxyz"
# Facet-spill: a bare title-head search returns the site's NEWEST 50 matches
# (already vaulted) — appending a quality/format facet reaches the SAME
# title's OLDER/different-dimension rows ranked below the cap.
# ORDER = measured yield (new rows per search; tools/idgap_stats.py leaderboard),
# best first, so each agent's rotation front-loads the earners.
# web-dl / dvdscr REMOVED: 290+ tries each, 0 rows ever returned by the site.
_SPILL_QUALS = ("esub", "720p", "480p", "aac", "10bit", "2160p",
                "1080p", "bluray", "hevc hd", "hevc", "webrip", "hdrip",
                "bdrip")
_SPILL_EXTRAS = ("zip", "s01", "e01", "hindi", "mkv", "dual audio",
                 "tamil", "complete", "pack", "telugu")
# Episode fanout: every other season/episode of a vaulted series is an
# UNSEARCHED sub-50 slice — the season axis, like facets on the quality axis.
_EP_SUFFIXES = tuple([f"s{n:02d}" for n in range(1, 8)]
                     + [f"e{n:02d}" for n in range(1, 13)])
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

    # Class-level defaults so _next_terms() stays usable when the object is
    # built without __init__ (the offline unit tests do exactly that).
    # __init__ overrides these from the environment.
    spill_every = 3
    blocks_scan = 10
    _pick = 0

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
        self._series_cache: list[tuple[str, int]] | None = None
        self._fresh_cache: list[tuple[str, int]] | None = None
        self._fresh_ts = 0.0
        # cap-spill reaction: a 50-row (capped) result proves the head is hot;
        # its other facet/episode slices get queued ahead of normal rotation.
        self._spill_queue: deque = deque(maxlen=80)
        # Phase 1 ration: serve the cap-spill path only every Nth pick. Measured
        # new rows per search: seed-head 0.50, seed-ep 0.25, seed-spill 0.10.
        # Cap-spill ran first and unconditionally, so the fleet's term supply
        # went to the weakest lane (4653 tries vs seed-head's 238) and the
        # higher-yield lanes were only ever reached when spill ran dry.
        self.spill_every = max(1, int(os.getenv("MKV_IDGAP_SPILL_EVERY", "3")))
        # How many thin blocks one call may consider when picking a head.
        self.blocks_scan = max(1, int(os.getenv("MKV_IDGAP_BLOCKS_SCAN", "10")))
        self._pick = 0

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
    def _seeds_for_block(self, blk: int, n: int = 6,
                         fresh_only: bool = True) -> list[str]:
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
        if not fresh_only:
            return heads[:800]
        fresh = [h for h in heads if h not in self.searched_terms]
        return fresh[:24]

    def _series_heads(self) -> list[tuple[str, int]]:
        """(head, block) of vaulted rows whose raw title carries a season mark
        (S01E01 / season 2) — episode fanout mines their OTHER seasons, which
        were never searched by the facet rotation."""
        if self._series_cache is not None:
            return self._series_cache
        col = self._col()
        if col is None:
            return []
        heads: list[tuple[str, int]] = []
        seen: set[str] = set()
        for r in col.find({"title": {"$type": "string"}}, {"title": 1}) \
                .sort("id", -1).limit(4000):
            title = r.get("title") or ""
            h = _title_head(title)
            if h and len(h) >= 3 and h not in seen and _SERIES_MARK.search(title):
                seen.add(h)
                heads.append((h, (r.get("id") or 0) // self.block))
        self._series_cache = heads
        return heads

    def _fresh_heads(self) -> list[tuple[str, int]]:
        """(head, block) of the NEWEST vault pushes (20-min cache). Their
        sibling uploads from the same window are the most likely unvaulted
        rows any head search can still catch."""
        now = time.time()
        if self._fresh_cache is not None and now - self._fresh_ts < 1200:
            return self._fresh_cache
        col = self._col()
        if col is None:
            return []
        heads: list[tuple[str, int]] = []
        seen: set[str] = set()
        for r in col.find({}, {"title": 1}).sort("id", -1).limit(250):
            h = _title_head(r.get("title") or "")
            if h and len(h) >= 3 and h not in seen:
                seen.add(h)
                heads.append((h, (r.get("id") or 0) // self.block))
        self._fresh_cache, self._fresh_ts = heads, now
        return heads

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
        m = _re.match(r"a(\d+)", agent)
        off = int(m.group(1)) if m else 0
        # Rotating gate so the fresh-head and episode-fanout lanes below are
        # actually reached instead of losing every pick to cap-spill.
        with self._lock:
            self._pick += 1
            allow_spill = (self._pick % self.spill_every == 0)
        # CAP-SPILL REACTION: slices queued when a search hit the 50-row site
        # cap. Rationed -- see self.spill_every.
        if allow_spill:
            for _ in range(4):
                with self._lock:
                    if not self._spill_queue:
                        break
                    term = self._spill_queue.popleft()
                    if now - self.searched_terms.get(term, 0.0) <= self.term_ttl_s:
                        continue
                    self.searched_terms[term] = now
                    self.stats["picked_block"] = self.stats.get("picked_block")
                    return None, "", [f"seed:{term}"]
        # FRESH-HEAD SWEEP first: heads of the newest vault pushes (20-min
        # cache). Bare head catches uploads since the vault push; head+top-
        # facet catches that head's pre-push siblings ranked below the cap.
        fh = self._fresh_heads()
        if fh:
            head, fblk = fh[(off + int(now) // 90) % len(fh)]
            sfx = ("", "esub", "720p", "480p")[(off + int(now) // 90) % 4]
            term = f"{head} {sfx}".strip()
            with self._lock:
                if now - self.searched_terms.get(term, 0.0) > self.term_ttl_s:
                    self.searched_terms[term] = now
                    self.stats["picked_block"] = fblk * self.block
                    self.stats["picked_era"] = ""
                    return fblk, "", [f"seed:{term}"]
        # EPISODE FANOUT: vaulted series heads x season/episode suffixes —
        # every OTHER season of a show is an unsearched sub-50 slice.
        series = self._series_heads()
        if series:
            rot = series[off % len(series):] + series[:off % len(series)]
            with self._lock:
                for i, (show, sblk) in enumerate(rot[:200]):
                    term = f"{show} {_EP_SUFFIXES[(off + i) % len(_EP_SUFFIXES)]}"
                    if now - self.searched_terms.get(term, 0.0) <= self.term_ttl_s:
                        continue
                    self.searched_terms[term] = now
                    self.stats["picked_block"] = sblk * self.block
                    self.stats["picked_era"] = ""
                    return sblk, "", [f"seed:{term}"]
        # Head x facet slices are the PRIMARY term space. A bare title-head
        # search returns only the site's NEWEST 50 matches for that title
        # (already vaulted -> 0 new forever), while head+facet slices reach
        # the SAME title's older rows ranked below the cap — the only lever
        # that actually fills a thin block. Universe: ~800 heads x 23 facets.
        facets = _SPILL_QUALS + _SPILL_EXTRAS
        for blk_info in cov["thin_blocks"][:self.blocks_scan]:
            blk = blk_info["block"]
            era = blk_info.get("era_date") or ""
            heads = self._seeds_for_block(blk, fresh_only=False)
            if not heads:
                continue
            rot = heads[off % len(heads):] + heads[:off % len(heads)]
            with self._lock:
                for i, head in enumerate(rot):
                    term = f"{head} {facets[(off + i) % len(facets)]}"
                    if now - self.searched_terms.get(term, 0.0) <= self.term_ttl_s:
                        continue  # searched or claimed recently
                    self.searched_terms[term] = now  # atomic claim: no stampede
                    self.stats["picked_block"] = blk * self.block
                    self.stats["picked_era"] = era
                    return blk, era, [f"seed:{term}"]
        # LRU valve: every slice in the 5 thinnest blocks is burned. Re-search
        # the block whose freshest head is the OLDEST (most stale block), one
        # bare head per call — new sibling uploads since the last try are the
        # only rows a bare head can still catch.
        best: tuple | None = None  # (block_freshness_ts, blk, era, head)
        for blk_info in cov["thin_blocks"][:self.blocks_scan]:
            blk = blk_info["block"]
            era = blk_info.get("era_date") or ""
            heads = self._seeds_for_block(blk, fresh_only=False)
            if not heads:
                continue
            freshest = max(self.searched_terms.get(h, 0.0) for h in heads)
            if best is None or freshest < best[0]:
                best = (freshest, blk, era, heads[(off + int(now)) % len(heads)])
        if best is not None:
            _, blk, era, head = best
            # Claim LRU re-picks: previously unclaimed, two agents could fire
            # the identical bare head at once (seen live: 'sayonee' x2). A
            # 10-min claim window spreads agents across heads; when every
            # head of the stale block is claimed, agents idle one cycle.
            with self._lock:
                if now - self.searched_terms.get(head, 0.0) <= 600:
                    return None, "", []  # another agent just took this head
                self.searched_terms[head] = now
                self.stats["picked_block"] = blk * self.block
                self.stats["picked_era"] = era
            return blk, era, [f"seed:{head}"]
        # fall back to era terms for the thinnest block (no seeds at all)
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
        kind = ("seed-spill" if term.endswith(_SPILL_QUALS + _SPILL_EXTRAS)
                else ("seed-ep" if term.endswith(_EP_SUFFIXES)
                      else self._term_kind(term)))
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
        # cap-spill reaction: a capped (50-row) result proves the head is
        # hot. Queue its top facet slices ahead of normal rotation (bounded
        # deque; TTL-dedup happens at pop time).
        if len(rows) >= 50:
            head = term.rsplit(" ", 1)[0] if " " in term else term
            with self._lock:
                for f in _SPILL_QUALS[:6] + ("zip", "s01"):
                    t2 = f"{head} {f}"
                    if t2 != term and t2 not in self._spill_queue:
                        self._spill_queue.append(t2)
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
