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
#
# Yield leaderboard over 29,371 measured searches (new rows per search):
#   1080p 0.68  esub 0.55  hindi 0.42  720p 0.38  mkv 0.33  season/episode
#   0.35  zip 0.21  2160p 0.21  hevc 0.18  bluray 0.17  aac 0.16  tamil 0.14
#   hevc-hd 0.12  10bit 0.11
#   --- dropped, all measured below 0.10 new rows/search ---
#   dual-audio 0.093  webrip 0.083  480p 0.071  hdrip 0.026  telugu 0.021
#   complete 0.019  pack 0.017  bdrip 0.006
# Those eight ate 8,598 searches — 29% of all traffic — for 361 new rows
# (5.6%). Dropping them redirects that budget to lanes yielding ~0.28.
_SPILL_QUALS = ("esub", "720p", "aac", "10bit", "2160p",
                "1080p", "bluray", "hevc hd", "hevc")
_SPILL_EXTRAS = ("zip", "s01", "e01", "hindi", "mkv", "tamil")
# The cap-spill reaction fires when a search returns the full 50 rows, which
# proves the head is hot. Queue that head's best-yielding slices, not just
# the first N of the rotation.
_SPILL_REACTION = ("esub", "1080p", "hindi", "720p", "mkv", "zip")
# Episode fanout: every other season/episode of a vaulted series is an
# UNSEARCHED sub-50 slice — the season axis, like facets on the quality axis.
_EP_SUFFIXES = tuple([f"s{n:02d}" for n in range(1, 8)]
                     + [f"e{n:02d}" for n in range(1, 13)])
# Season-slice channel (the successor to token/phrase sweeps, both measured
# dry: tokens 174 eligible left, phrases 0.00 new/search). Any query generated
# FROM our own titles is exhausted, because this vault was itself built by
# sweeping those titles. What still pays is a NEW query shape over titles we
# hold: OTHER seasons / episode numbers / season+facet slices of the same
# show. Measured live 2026-10-04: 'prison break s02'-style other-season
# slices 2.38 new/search (re-measured 2026-10-04 at 3.11 over 18 probes);
# probing past a show's held range ('simpsons s37')
# returns 0 rows (nothing to surface), and bare 'show sXXeYY' episode probes
# mostly return 0 rows (the site lacks those uploads), so season slices lead
# and episode slices are the bounded tail of the pool.
_SLICE_POOL_CAP = 40000     # bounded pool: season slices first, episodes fill
_SLICE_SEASON_CAP = 32000   # ...of which at most this many are season slices
_SLICE_QUEUE_CAP = 500      # bounded self-feed queue (mirrors _spill_queue)
_SLICE_SEASON_HI = 40       # per-show season expansion bound (junk guard)
_SLICE_EP_HI = 12           # per-season episode expansion bound
# Facet-qualified season slices are ONLY minted from capped (>=50-row)
# results via the self-feed queue, never upfront in the pool. Rationale
# (site semantics): a query with <=50 total matches returns its COMPLETE
# set, so an uncapped bare slice already surfaced everything; its facet
# sub-slices are subsets (0 new, pure waste). Only a capped bare slice
# (50 rows) hides rows below the cap, so only capped shows earn facet
# sub-slices. Measured 2026-10-05: faceted-upfront yielded 0.337/search
# decaying to 0.00 in mined regions with 5x 0-row waste per barren season;
# bare-first with capped-triggered faceting restores hit rate.
_SLICE_FACETS = ("hindi", "1080p", "720p", "esub", "mkv")
import re as _re

_HEAD_WORD = _re.compile(r"[a-z0-9]+")
_YEAR_IN_TITLE = _re.compile(r"\b((?:19|20)\d{2})\b")
_SERIES_MARK = _re.compile(r"\b(?:s\d{1,2}\s*e?\s*\d{0,3}|season\s*\d{1,2})", _re.I)
_NOISE = _re.compile(
    r"gdflix|hubcloud|hdrip|camrip|dvdscr|www\.|\.com|\.in\b|downloaded",
    _re.I)
# Junk head filter for the season-slice pool: HTML-entity fragments and
# site-tag prefixes that slipped into canonical heads ('grey 039 s anatomy').
_SLICE_JUNK = _re.compile(r"039|titancloud|gdflix|^www\.|\.com\b", _re.I)

# ---------------------------------------------------------------------------
# Token sweep: the one lever that reaches ids OLDER than the newest-50 window.
#
# mkvbase search always returns the site's newest 50 matches for a query, so a
# query with more than 50 total matches can never surface an old row. A query
# with <= 50 total matches returns its COMPLETE match set. Measured on the live
# site over three independent random samples (n=24-30 each): 7.1, 8.0 and 10.0
# NEW rows per search, 0% duplicate share, and every recovered row was an
# INTERIOR id (the missing middle of the id space) -- against 0.02-0.27/search
# for every lane this miner already runs. Measured again through THIS pool
# (23,097 tokens, random sample n=20): 5.30 new rows/search, 7/20 capped.
# Lower than the probes because the pool is broader (len 3-24, punctuation and
# numeric-suffix junk included); still ~29x the idgap lane.
#
# The vault is 44% full (244k of 547k ids) and the 303k gaps are scattered as
# runs of 1-9 ids, so no id range is contiguous enough to walk. This sweep is
# the only strategy that reaches them: it searches tokens the fleet already
# holds in title_tokens but has never sent to the site.
_TOKEN_MIN_FREQ = 2     # freq 1 is typos/random strings (measured: no yield)
_TOKEN_MAX_FREQ = 50    # <= 50 total matches => the search returns ALL of them
_TOKEN_MIN_LEN = 4
_TOKEN_MAX_LEN = 24
_TOKEN_POOL_TTL = 6 * 3600
_FRONTIER_CAP = 500     # bounded queue of tokens found only in recovered rows
_TOKEN_SPLIT = _re.compile(r"[^a-z0-9]+")
# Phrase sweep (the successor channel). The single-token pool is measured
# exhausted: after 29,187 tokens searched there are 174 eligible left, most of
# them typo spam ('crspskmhd', 'e026'). Adjacent pairs of RARE words are the
# next space: a phrase of two rare words matches few rows, and a query matching
# <= 50 rows returns its COMPLETE match set -- the only complete-enumeration
# primitive this site offers. Built client-side: grouping ~2.6M pairs server
# side exceeds the Atlas 100MB $group limit and allowDiskUse is refused here.
_PHRASE_TTL = 6 * 3600
_PHRASE_CAP = 20000       # bounded phrase pool
_PHRASE_FRONTIER_CAP = 500
_PHRASE_WORD = _re.compile(r"^[a-z0-9]{4,}$")
# Very common English words match far more than 50 rows on the site, so they
# can never surface an unseen row -- and the frontier mints them constantly
# (measured live: 'you' and 'know' both returned 50 rows / 0 new). They are
# legal terms for the pool (rare titles use them) but must not consume the
# frontier, whose whole value is that its tokens were unseen by construction.
_TOKEN_COMMON = frozenset((
    "you", "your", "yours", "know", "like", "just", "that", "this", "these",
    "those", "with", "from", "have", "here", "there", "where", "when",
    "what", "which", "while", "will", "would", "could", "should", "does",
    "done", "been", "being", "were", "was", "are", "the", "and", "for",
    "but", "not", "all", "any", "can", "her", "his", "its", "our", "out",
    "she", "him", "how", "who", "why", "too", "very", "into", "over",
    "only", "also", "then", "than", "them", "they", "their", "some",
    "more", "most", "much", "many", "one", "two", "new", "old", "get",
    "got", "make", "made", "take", "took", "come", "came", "see", "saw",
    "way", "day", "night", "life", "time", "year", "man", "woman",
    "boy", "girl", "king", "love", "war", "world", "home", "house",
    "part", "last", "first", "next", "back", "down", "off", "own",
    "about", "after", "again", "against", "before", "between", "during",
    "under", "never", "every", "each", "both", "because", "through",
    "story", "full", "true", "best", "good", "great", "little", "long",
    "red", "blue", "black", "white", "dark", "light", "fire", "blood",
))
# Facet/format vocabulary is already covered by the facet and cap-spill lanes;
# re-searching it here would just re-burn the same capped queries.
_RE_QUALITY_RE = _re.compile(r"(?:^|[^a-z])(?:bdrip|dvdrip|hdrip|webdl|webrip|4k|1080p|720p|2160p|x264|x265|hevc|aac|ac3|truehd|dts)")

_TOKEN_STOP = frozenset((
    "www", "com", "net", "org", "http", "https",
    "mkv", "mp4", "avi", "mov", "srt", "sub", "subs", "esub", "msub",
    "1080p", "720p", "480p", "2160p", "1080i", "uhd", "hdr", "sdr",
    "hevc", "x264", "x265", "aac", "ac3", "ddp", "dts", "truehd",
    "bluray", "brrip", "webrip", "webdl", "hdrip", "dvdrip", "hdtv",
    "hindi", "tamil", "telugu", "malayalam", "kannada", "english",
    "multi", "dual", "audio", "season", "episode", "complete", "pack",
    "zip", "rar", "movie", "movies", "series", "ep", "eps", "vol",
))


def _has_brackets(title: str) -> bool:
    """True when a row title is wrapped in a release-site bracket group like
    '[HubCloud]'. Those bracketed groups carry the site's own quality/identity
    annotation and must never become search terms."""
    return "[" in title or "]" in title


def _terms_of(title: str) -> list[str]:
    """Searchable tokens of a raw row title (fully lowercase alpha, len 4+).

    Filters out junk that pollutes the sweep with capped, zero-yield queries,
    including quality/format suffixes (hdrip, dvdrip, x264, ...), format labels
    (web, dl, mkv, ...), 2-3 letter abbreviations, and bracketed release sites
    (the site mangles title text into bracketed groups). All of these match
    far more than 50 rows on the site and can never surface an unseen one.
    """
    t = (title or "").lower()
    if len(t) < _TOKEN_MIN_LEN:
        return []
    out = []
    for piece in _TOKEN_SPLIT.split(t):
        # A real searchable English word is fully lowercase alpha, len 4+,
        # and carries no quality/format code as a piece.
        if (piece.isalpha() and len(piece) >= 4 and len(piece) <= _TOKEN_MAX_LEN
                and piece not in _TOKEN_STOP and not _RE_QUALITY_RE.search(piece)
                and not _has_brackets(title)):
            out.append(piece)
    return out


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
    # Token sweep (see _TOKEN_* above). Class defaults keep _next_terms()
    # usable when __init__ is skipped (the offline unit tests do that).
    token_every = 1
    token_batch = 400
    _token_pool: list[str] = []
    _token_ts = 0.0
    _token_cursor = 0
    _token_pick = 0
    _token_frontier: deque = deque(maxlen=_FRONTIER_CAP)
    # Season-slice channel defaults (see __init__ for the live values).
    slice_batch = 400
    _slice_pool: list = []
    _slice_ts = 0.0
    _slice_cursor = 0
    _slice_queue: deque = deque(maxlen=_SLICE_QUEUE_CAP)

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
        # Token sweep: serve it every Nth pick (default every pick -- measured
        # 7-10 new rows/search vs 0.02-0.27 for every other lane here).
        self.token_every = max(1, int(os.getenv("MKV_IDGAP_TOKEN_EVERY", "1")))
        self.token_batch = max(10, int(os.getenv("MKV_IDGAP_TOKEN_BATCH", "400")))
        self._token_pool = []
        self._token_ts = 0.0
        self._token_cursor = 0
        self._token_pick = 0
        self._token_frontier = deque(maxlen=_FRONTIER_CAP)
        # Phrase sweep state. _phrase_pool is filled by a background thread so
        # no search ever waits on the 35s build.
        self._phrase_pool: list[str] = []
        self._phrase_ts = 0.0
        self._phrase_cursor = 0
        self._phrase_ttl = float(os.getenv("MKV_IDGAP_PHRASE_TTL_S", str(_PHRASE_TTL)))
        self.phrase_batch = max(10, int(os.getenv("MKV_IDGAP_PHRASE_BATCH", "400")))
        self._phrase_frontier: deque = deque(maxlen=_PHRASE_FRONTIER_CAP)
        # Season-slice channel state. _slice_pool is [(term, kind)] built by a
        # background thread (full vault scan, ~1min); _slice_queue is the
        # self-feed: facet-on-season + newly seen seasons minted from capped
        # slice results, mirroring the _spill_queue/cap-spill pattern.
        self._slice_pool: list[tuple[str, str]] = []
        self._slice_ts = 0.0
        self._slice_cursor = 0
        self.slice_batch = max(10, int(os.getenv("MKV_IDGAP_SLICE_BATCH", "400")))
        self._slice_queue: deque = deque(maxlen=_SLICE_QUEUE_CAP)

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
            self._token_cursor = int(d.get("token_cursor") or 0)
            self._token_frontier = deque(d.get("token_frontier") or [],
                                         maxlen=_FRONTIER_CAP)
            self._phrase_cursor = int(d.get("phrase_cursor") or 0)
            self._phrase_frontier = deque(d.get("phrase_frontier") or [],
                                          maxlen=_PHRASE_FRONTIER_CAP)
            self._slice_cursor = int(d.get("slice_cursor") or 0)
            self._slice_queue = deque(
                [(t, k) for t, k in (d.get("slice_queue") or []) if t and k],
                maxlen=_SLICE_QUEUE_CAP)
        except FileNotFoundError:
            pass
        except Exception as e:
            # A half-written state file used to be swallowed here, so after any
            # forced kill (the 2-hour recycle task) the miner silently forgot
            # every term it had ever searched and re-swept the whole pool for
            # zero rows -- measured 0.02 new rows/search vs 2.2 before the wipe.
            # Quarantine the bad file and say so instead.
            try:
                bad = f"{self.state_path}.corrupt-{int(time.time())}"
                os.replace(self.state_path, bad)
                print(f"[idgap] state file unreadable ({type(e).__name__}: {e}); "
                      f"quarantined to {bad} and starting with an empty history",
                      flush=True)
            except Exception:
                pass

    def save(self) -> None:
        with self._lock:
            try:
                os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
                # Prune expired searched_terms (older than TTL) before dump.
                # Rationale: the file had grown to 102k entries / 4.2MB,
                # rewritten after EVERY step, holding the lock during dump.
                # Pruning keeps saves fast and lets the pool rebuild re-include
                # expired terms for TTL re-sweep (new uploads since last search).
                # TTL check (now - ts > ttl) treats pruned terms as retryable,
                # which is exactly the intended TTL semantics. Atomicity and
                # corrupt-quarantine below are unchanged.
                try:
                    ttl = float(self.term_ttl_s)
                except Exception:
                    ttl = 3 * 3600
                now = time.time()
                st = self.searched_terms
                if len(st) > 20000:
                    self.searched_terms = {
                        k: v for k, v in st.items() if now - float(v) <= ttl
                    }
                payload = {"done_blocks": sorted(self.done_blocks)[-500:],
                           "searched_terms": self.searched_terms,
                           "term_stats": self.term_stats,
                           "round_started": self.round_started,
                           "stats": self.stats,
                           "token_cursor": self._token_cursor,
                           "token_frontier": list(self._token_frontier),
                           "phrase_cursor": self._phrase_cursor,
                           "phrase_frontier": list(self._phrase_frontier),
                           "slice_cursor": self._slice_cursor,
                           "slice_queue": list(self._slice_queue)}
                # Atomic: dump to a sibling temp file, then swap. A kill during
                # the dump used to leave half a JSON that _load() could not
                # read -- i.e. a silent wipe of the whole search history.
                tmp = f"{self.state_path}.tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(payload, f)
                os.replace(tmp, self.state_path)
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

    # ------------------------------------------------------------ token sweep
    def _token_pool_build(self) -> list[str]:
        """Tokens we already hold but have NEVER sent to the site, best first.

        Only tokens whose vault frequency is <= _TOKEN_MAX_FREQ can pay off: the
        site returns the newest 50 matches, so a token matching >50 rows can
        never surface one we are missing. Frequency is also the cost signal --
        a token matching ~40 rows is a much cheaper complete enumeration than
        one matching 4 -- so the pool is ordered by descending frequency.

        The aggregation is server-side and group-only (~38k groups, ~1-2s);
        socketTimeoutMS is 20s, and the pool is cached for _TOKEN_POOL_TTL.
        """
        now = time.time()
        # Cache first: this is called on every pick, and the aggregation below
        # is the most expensive query the miner runs.
        if self._token_pool and now - self._token_ts < _TOKEN_POOL_TTL:
            return self._token_pool
        idx = getattr(self, "index", None)
        if idx is None:
            return []
        cols = list(getattr(idx, "cols", lambda: [self._col()])())
        cols = [c for c in cols if c is not None]
        if not cols:
            return []
        freq: Counter = Counter()
        for c in cols:
            for r in c.aggregate([
                    {"$match": {"title_tokens": {"$exists": True}}},
                    {"$unwind": "$title_tokens"},
                    {"$group": {"_id": "$title_tokens", "n": {"$sum": 1}}}]):
                t = r.get("_id")
                if t:
                    freq[t] += r["n"]
        with self._lock:
            searched = self.searched_terms
            fresh = [t for t, n in freq.items()
                     if (_TOKEN_MIN_FREQ <= n <= _TOKEN_MAX_FREQ
                         and _TOKEN_MIN_LEN <= len(t) <= _TOKEN_MAX_LEN
                         and not t.isdigit() and t not in _TOKEN_STOP
                         and t not in searched)]
        fresh.sort(key=lambda t: -freq[t])
        self._token_pool = fresh
        self._token_ts = now
        return fresh

    # ------------------------------------------------------------ phrase sweep
    def _build_phrase_pool(self) -> int:
        """Rebuild the phrase pool from the vault. Returns phrases kept.

        Two streaming passes, no server-side $group: pass 1 counts clean word
        frequencies, pass 2 counts adjacent pairs of RARE words (2..50 rows).
        A pair past 50 is frozen -- it can no longer be a complete
        enumeration, and freezing keeps the counter small. Measured: 330k rows
        per pass in ~35s -> ~11k phrases.

        The word count is done client-side on purpose. The equivalent $group
        timed out (20s socketTimeout) on the Atlas shard when the per-pick
        coverage query was running alongside it; two cursor passes never exceed
        a per-batch socket timeout.
        """
        idx = getattr(self, "index", None)
        if idx is None:
            return 0
        cols = [c for c in getattr(idx, "cols", lambda: [])() if c is not None]
        if not cols:
            return 0

        def _scan(rare: set[str] | None = None):
            """Yield adjacent clean-word pairs (filtered by `rare` if given)."""
            for c in cols:
                for r in c.find({"title_tokens": {"$exists": True}},
                                {"title_tokens": 1, "_id": 0}).batch_size(2000):
                    seq = [w for w in (r.get("title_tokens") or [])
                           if isinstance(w, str) and _PHRASE_WORD.match(w)]
                    if rare is not None:
                        seq = [w for w in seq if w in rare]
                    for a, b in zip(seq, seq[1:]):
                        yield a + " " + b

        freq: Counter = Counter()
        for w in (p.split(" ", 1)[0] for p in _scan()):
            freq[w] += 1
        rare = {w for w, n in freq.items()
                if _TOKEN_MIN_FREQ <= n <= _TOKEN_MAX_FREQ
                and w not in _TOKEN_STOP}
        pairs: Counter = Counter()
        for p in _scan(rare):
            if pairs.get(p, 0) >= _TOKEN_MAX_FREQ:
                continue
            pairs[p] += 1
        with self._lock:
            searched = self.searched_terms
        fresh = [p for p, n in pairs.items()
                 if n >= _TOKEN_MIN_FREQ and p not in searched]
        fresh.sort(key=lambda p: -pairs[p])
        fresh = fresh[:_PHRASE_CAP]
        self._phrase_pool = fresh
        self._phrase_ts = time.time()
        return len(fresh)

    def _start_phrase_builder(self, log=print) -> None:
        """Build the phrase pool off-thread (~70s) and refresh it every TTL."""
        def _loop() -> None:
            time.sleep(20.0)  # let the first picks settle: the shard is busiest
            #                          right after startup and coverage() is running
            while True:
                t0 = time.time()
                try:
                    n = self._build_phrase_pool()
                    log(f"[idgap] phrase pool: {n} two-word phrases ready "
                        f"({time.time() - t0:.0f}s)", flush=True)
                    time.sleep(max(600.0, self._phrase_ttl / 2))
                except Exception as e:
                    log(f"[idgap] phrase pool build failed: {type(e).__name__}: "
                        f"{str(e)[:120]}", flush=True)
                    time.sleep(120.0)  # transient shard contention: retry soon
        threading.Thread(target=_loop, daemon=True, name="idgap-phrase").start()

    def _claim_token(self) -> tuple[str, str] | None:
        """Claim one sweep term: (term, kind) or None when every lane is dry.

        Order is measured supply, not history. The single-token pool is measured
        exhausted (174 eligible left, mostly typo spam), so the phrase pool --
        12k fresh two-word keys -- outranks both frontiers. Serving it before
        the frontiers matters: the token frontier holds up to 500 minted keys
        and measures ~0.19 new rows/search, which at 22 searches/min would
        starve a 12k pool for the better part of a day.

        Token pool, then phrase pool, then the two frontiers last. The phrase
        pool is built by its own thread and only read here, so a search never
        pays for the build.
        """
        now = time.time()
        with self._lock:
            self._token_pick += 1
        pool = self._token_pool_build()
        if pool:
            with self._lock:
                n = len(pool)
                for i in range(self._token_cursor,
                               self._token_cursor + self.token_batch):
                    t = pool[i % n]
                    if now - self.searched_terms.get(t, 0.0) > self.term_ttl_s:
                        self._token_cursor = i + 1
                        self.searched_terms[t] = now
                        return t, "token-sweep"
        phrases = self._phrase_pool
        if phrases:
            with self._lock:
                n2 = len(phrases)
                for i in range(self._phrase_cursor,
                               self._phrase_cursor + self.phrase_batch):
                    p = phrases[i % n2]
                    if now - self.searched_terms.get(p, 0.0) > self.term_ttl_s:
                        self._phrase_cursor = i + 1
                        self.searched_terms[p] = now
                        return p, "phrase-sweep"
        with self._lock:
            while self._token_frontier:
                t = self._token_frontier.popleft()
                if now - self.searched_terms.get(t, 0.0) <= self.term_ttl_s:
                    continue  # raced or already done
                self.searched_terms[t] = now
                return t, "token-frontier"
        with self._lock:
            while self._phrase_frontier:
                p = self._phrase_frontier.popleft()
                if now - self.searched_terms.get(p, 0.0) <= self.term_ttl_s:
                    continue
                self.searched_terms[p] = now
                return p, "phrase-frontier"
        return None

    def _drain_token_hits(self, rows: list[dict]) -> None:
        """Mint search keys from rows the sweep just recovered.

        Every token here was absent from the whole vault, so it is by
        construction a query the fleet has never run. Bounded queue; the TTL
        check happens at pop time.
        """
        minted = 0
        minted_phrases = 0
        with self._lock:
            for r in rows:
                terms = _terms_of(r.get("title") or "")
                for t in terms:
                    if (t in _TOKEN_STOP or t in _TOKEN_COMMON
                            or t in self.searched_terms
                            or len(self._token_frontier) >= _FRONTIER_CAP):
                        continue
                    if _TOKEN_MIN_LEN <= len(t) <= _TOKEN_MAX_LEN:
                        self._token_frontier.append(t)
                        minted += 1
                # ...and their adjacent pairs, so a recovered row feeds the
                # phrase channel too and the sweep keeps generating its own
                # supply even after the static phrase pool is used up.
                seq = [t for t in terms
                       if _PHRASE_WORD.match(t) and t not in _TOKEN_STOP]
                for a, b in zip(seq, seq[1:]):
                    p = a + " " + b
                    if p in self.searched_terms:
                        continue
                    if len(self._phrase_frontier) >= _PHRASE_FRONTIER_CAP:
                        break
                    self._phrase_frontier.append(p)
                    minted_phrases += 1
        if minted:
            self.stats["token_minted"] = self.stats.get("token_minted", 0) + minted
        if minted_phrases:
            self.stats["phrase_minted"] = (
                self.stats.get("phrase_minted", 0) + minted_phrases)

    # ------------------------------------------------------------ season-slice channel
    def _build_slice_pool(self) -> int:
        """Expand every vaulted series head across its seasons + a bounded
        episode range. Returns slices kept.

        One streaming pass over titles: heads come from the same series-head
        path (_series_heads normalisation) with per-season expansion via
        discovery's _seasons_in/_episodes_in. A bare head search returns only
        the site's NEWEST 50 matches, but 'show s02' is a DIFFERENT query
        whose <=50 match set (or newest-50 window over a smaller set) reaches
        rows the bare head can never surface. Season slices lead the pool
        (measured 3.11 new/search); episode slices are the bounded tail.
        Single-season shows first: their other seasons are the proven shape.
        """
        from .discovery import (_canonical_show, _episodes_in, _seasons_in,
                                _title_head as _show_head)
        idx = getattr(self, "index", None)
        if idx is None:
            return 0
        cols = [c for c in getattr(idx, "cols", lambda: [])() if c is not None]
        if not cols:
            return 0
        shows: dict[str, set[int]] = {}
        epmax: dict[tuple[str, int], int] = {}
        for c in cols:
            cur = c.find({"title": {"$type": "string"}},
                         {"title": 1, "_id": 0}).batch_size(2000)
            for r in cur:
                t = r.get("title") or ""
                if not _SERIES_MARK.search(t):
                    continue
                h = _show_head(t)
                if not h:
                    continue
                h = _canonical_show(h)
                if not h or len(h) < 4 or _SLICE_JUNK.search(h):
                    continue
                shows.setdefault(h, set()).update(_seasons_in(t))
                for sn, ep in _episodes_in(t):
                    if 1 <= ep <= 60:
                        k = (h, sn)
                        if ep > epmax.get(k, 0):
                            epmax[k] = ep
        with self._lock:
            searched = self.searched_terms
        multi: list[tuple[str, str]] = []   # >=2 held seasons: proven catalog depth
        single: list[tuple[str, str]] = []  # 1 held season: s01 first (scam-1992 shape)
        eps: list[tuple[str, str]] = []
        for h in sorted(shows):
            ss = shows[h]
            if not ss:
                continue
            # Single-season shows: seasons past max+1 are proven barren
            # ('simpsons s37'-style probes: 0 rows), and the padded s04-s06
            # range burns searches for nothing. Keep max+1 plus one pad.
            if len(ss) >= 2:
                hi = min(max(max(ss), 5) + 1, _SLICE_SEASON_HI)
            else:
                hi = min(max(ss) + 2, 4)
            dest = multi if len(ss) >= 2 else single
            for s in range(1, hi + 1):
                t = f"{h} s{s:02d}"
                if t not in searched:
                    dest.append((t, "slice-season"))
                # NOTE: no faceted-upfront here. Facet sub-slices are minted
                # ONLY from capped (>=50-row) results via _drain_slice_hits:
                # uncapped bare slices already returned their complete set,
                # so upfront faceting is 5x 0-row waste per barren season.
        # s01 of every single-season show outranks their s05: lower seasons
        # exist more often, and unsearched s01 slices measured 16 new/search.
        single.sort(key=lambda tk: (int(tk[0].rsplit(" ", 1)[-1][1:]), tk[0]))
        # Bare seasons first: highest hit rate (3.11/search prime). Facets
        # arrive via the capped-triggered self-feed queue, not the pool.
        season = multi + single
        for h in sorted(shows, key=lambda h: (-len(shows[h]), h)):
            for s in sorted(shows[h]):
                top = min(max(epmax.get((h, s), 6), 6), _SLICE_EP_HI)
                for e in range(1, top + 1):
                    t = f"{h} s{s:02d}e{e:02d}"
                    if t not in searched:
                        eps.append((t, "slice-ep"))
        kept_season = season[:_SLICE_SEASON_CAP]
        kept_eps = eps[:max(0, _SLICE_POOL_CAP - len(kept_season))]
        self._slice_pool = kept_season + kept_eps
        self._slice_ts = time.time()
        return len(self._slice_pool)

    def _start_slice_builder(self, log=print) -> None:
        """Build the slice pool off-thread and refresh it every TTL."""
        def _loop() -> None:
            time.sleep(10.0)  # coverage() owns the shard right after startup
            while True:
                t0 = time.time()
                try:
                    n = self._build_slice_pool()
                    log(f"[idgap] slice pool: {n} season/episode slices ready "
                        f"({time.time() - t0:.0f}s)", flush=True)
                    time.sleep(max(900.0, self.term_ttl_s / 2))
                except Exception as e:
                    log(f"[idgap] slice pool build failed: {type(e).__name__}: "
                        f"{str(e)[:120]}", flush=True)
                    time.sleep(120.0)
        threading.Thread(target=_loop, daemon=True, name="idgap-slice").start()

    def _claim_slice(self) -> tuple[str, str] | None:
        """Claim one season/episode slice: (term, kind) or None when dry.

        Pool first (cursor + TTL claim, so two agents never take the same
        slice), then the self-feed queue minted from capped slice results.

        NOTE: the queue must stay SECOND. _SPILL_REACTION contains 's01',
        so a capped slice mints 'show s01 s01', which caps and mints
        'show s01 s01 s01' -- a degenerate chain that returns 0 new.
        Draining the queue first was measured on 2026-10-05: 475 slice
        tries, 0 new rows, vault frozen for 15 min. Pool-first is the
        known-good order; the queue is only a fallback when the pool's
        local batch is TTL-locked.
        """
        now = time.time()
        pool = self._slice_pool
        if pool:
            with self._lock:
                n = len(pool)
                for i in range(self._slice_cursor,
                               self._slice_cursor + self.slice_batch):
                    term, kind = pool[i % n]
                    if now - self.searched_terms.get(term, 0.0) > self.term_ttl_s:
                        self._slice_cursor = i + 1
                        self.searched_terms[term] = now
                        return term, kind
        with self._lock:
            while self._slice_queue:
                term, kind = self._slice_queue.popleft()
                if now - self.searched_terms.get(term, 0.0) <= self.term_ttl_s:
                    continue  # raced or already done
                self.searched_terms[term] = now
                return term, kind
        return None

    def _drain_slice_hits(self, rows: list[dict], term: str) -> None:
        """Feed the slice channel from its own results (bounded queue).

        A capped season slice proves older rows exist below the 50-window:
        mint its facet sub-slices. Rows naming seasons the pool never
        expanded to become new season slices. TTL-dedup happens at pop time.
        """
        from .discovery import _seasons_in
        minted = 0
        with self._lock:
            if len(rows) >= 50:
                base = term
                for f in _SPILL_REACTION:
                    if base.endswith(f):
                        continue  # never mint 'show s01 s01' chains
                    t2 = f"{base} {f}"
                    if t2 == term or t2 in self.searched_terms:
                        continue
                    if any(t == t2 for t, _ in self._slice_queue):
                        continue
                    if len(self._slice_queue) >= _SLICE_QUEUE_CAP:
                        break
                    self._slice_queue.append((t2, "slice-season"))
                    minted += 1
            head = term.rsplit(" ", 1)[0] if " " in term else term
            if _re.fullmatch(r"s\d{2}", term.rsplit(" ", 1)[-1] or ""):
                seen: set[int] = set()
                for r in rows:
                    seen.update(_seasons_in(r.get("title") or ""))
                for s in sorted(seen):
                    t2 = f"{head} s{s:02d}"
                    if t2 == term or t2 in self.searched_terms:
                        continue
                    if any(t == t2 for t, _ in self._slice_queue):
                        continue
                    if len(self._slice_queue) >= _SLICE_QUEUE_CAP:
                        break
                    self._slice_queue.append((t2, "slice-season"))
                    minted += 1
        if minted:
            self.stats["slice_minted"] = self.stats.get("slice_minted", 0) + minted

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
            allow_token = (self._pick % self.token_every == 0)
        # TOKEN/PHRASE SWEEP -- LAST RESORT, not first (measured 2026-10-04).
        # The sweep used to earn every pick at 7-10 new rows/search while the
        # pools were fresh. Both are now measured dry: 29,187 tokens searched,
        # 174 eligible left, and the 12k two-word phrase pool measures 0.00
        # new/search -- verified against the site directly (20 live queries,
        # every returned id already in the vault). That is structural, not
        # bad luck: the sweep queries are built FROM our own titles, and this
        # vault was itself built by sweeping those titles, so anything they
        # can generate we already hold. What still pays is a NEW query shape
        # over titles we hold -- other seasons (2.38), seed-spill (3.71),
        # seed-ep (2.75). So the sweep only takes a pick when the seed lanes
        # have nothing left.
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
        # SEASON-SLICE CHANNEL. Other seasons / episode numbers / season+facet
        # slices of vaulted series heads. Measured live 2026-10-04 at 3.11 new
        # rows/search over 18 unsearched probes (vs 0.00 for phrases, 0.04 for
        # leftover tokens): this is the only lane whose query SHAPE is new, so
        # it earns the pick whenever it has an unsearched slice.
        got_slice = self._claim_slice()
        if got_slice:
            term, kind = got_slice
            self.stats["picked_block"] = None
            self.stats["picked_era"] = kind
            return None, kind, [f"slc:{kind}:{term}"]
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
        # TOKEN/PHRASE SWEEP, last resort (see the note where it used to run):
        # only reached when every seed lane is exhausted for this pick.
        if allow_token:
            got = self._claim_token()
            if got:
                term, kind = got
                self.stats["picked_block"] = None
                self.stats["picked_era"] = kind
                return None, kind, [f"tok:{kind}:{term}"]
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
        forced_kind = None
        if raw.startswith(("tok:", "slc:")):
            # 'tok:<kind>:<term>' / 'slc:<kind>:<term>' -- the kind is carried
            # explicitly so a slice is not mislabelled 'seed-head' by
            # _term_kind().
            _, forced_kind, term = raw.split(":", 2)
        elif raw.startswith("seed:"):
            term = raw[5:]
        else:
            term = raw
        kind = forced_kind or ("seed-spill" if term.endswith(_SPILL_QUALS + _SPILL_EXTRAS)
                               else ("seed-ep" if term.endswith(_EP_SUFFIXES)
                                     else self._term_kind(term)))
        t0 = time.time()
        try:
            # client.search() is ALREADY http-first (client.py _fetch tries
            # plain HTTP before the browser), and it holds the _http_sem
            # permit. Do NOT call search_http() here: it bypasses that
            # semaphore and would let the miners stampede past the
            # MKV_HTTP_CONCURRENCY cap. Measured 2026-10-05: search_http and
            # search are indistinguishable (3.24s vs 3.15s over 3 terms).
            obj = self.client.search(term)
        except Exception as e:
            self.searched_terms[term] = time.time() - 18 * 3600  # retry sooner
            self.save()
            return {"status": "retry", "term": term, "agent": agent,
                    "err": f"{type(e).__name__}: {str(e)[:100]}"}
        rows = [r for r in (obj.get("results") or []) if isinstance(r, dict)]
        new, _upd = self.index.upsert(rows, source=f"idgap:{agent}:b{blk}")
        if kind.startswith(("token-", "phrase-")):
            # Rows the sweep just recovered carry tokens the vault has never
            # held -- free, self-generated search keys for the next pass.
            self._drain_token_hits(rows)
        if kind.startswith("slice-"):
            # Capped slice results mint their own facet sub-slices, so the
            # channel feeds itself below the 50-row window.
            self._drain_slice_hits(rows, term)
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
                for f in _SPILL_REACTION:
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
        self._start_phrase_builder(log)
        self._start_slice_builder(log)

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
                        # gap_s between SUCCESSFUL searches too. It used to be
                        # applied only on retry, which made MKV_IDGAP_GAP_S
                        # dead config: 8 agents ran flat out (84 searches/min,
                        # measured) and monopolised both HTTP slots, starving
                        # the discovery lane completely.
                        stop.wait(self.gap_s)
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
