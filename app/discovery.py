"""Discovery crawler — walk mkvbase's back catalog into the links index.

mkvbase exposes no bulk API and no pagination (verified against the live site):
the only enumeration primitive is signed search, which returns up to 50 matches
per term. Discovery exploits it with a term waterfall:

  1. SEEDS — years ("2019"), single letters and digits (cover most title
     starts), quality tags, languages, common title words.
  2. Every search returns up to 50 titles; each unknown word in each title is
     queued as the next term. Productive terms stay alive, dry ones retire.
  3. Rows are merged straight into the durable Mongo index (dedup by
     id/url/title), so overlap with the recent loop and earlier passes costs
     nothing. The upsert's new-row count is the saturation signal: a term
     whose results are all already-known rows is "exhausted".

A pass drains the queue (bounded); when a pass yields almost nothing new the
crawler cools down, re-queues previously exhausted terms for a re-check (new
uploads make some productive again), and repeats. New uploads also surface
continuously through the recent loop regardless.

Politeness: one search every MKV_DISCOVERY_GAP_S seconds (default 45) — the
same rate a curious human with a search box would produce.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time

from .client import MkvbaseClient, MkvbaseError

_WORD = re.compile(r"[a-z0-9]{3,}")
_STOP = {"the", "and", "for", "with", "from", "www", "com", "mkv", "dvd",
         "part", "1080", "720", "480", "2160", "1080p", "720p", "480p",
         "2160p", "x264", "x265", "h264", "h265", "aac", "dd5", "ddp5",
         "dts", "truehd", "atmos", "10bit", "8bit", "bluray", "web", "webdl",
         "webrip", "brrip", "dvdrip", "hdrip", "hdtv", "remux", "hdr10",
         "esub", "msub", "esubs", "org", "clean", "esc", "hevc", "avc",
         "upscale", "upscaled", "hdts", "camrip", "hdr", "dolby", "vision",
         "multi", "dual", "audio", "subtitle", "subtitles", "subbed",
         "dubbed", "hindi", "tamil", "telugu", "malayalam", "kannada",
         "bengali", "marathi", "punjabi", "english", "japanese", "korean",
         "mandarin", "spanish", "french", "german", "4k", "imax", "extended",
         "remastered", "unrated", "cut", "directors", "ultimate", "collector",
         "edition", "complete", "season", "episode", "ep", "eps", "vol",
         "chapter", "bangla", "chinese", "amzn", "netflix", "hdr10plus"}

# Fresh-first: years DESCEND (recent uploads are recent movies, so 2026->1950
# approximates walking created_at backwards; the API itself has no date query).
_SEEDS = [str(y) for y in range(2026, 1949, -1)] + [
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
    "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "love", "night", "life", "man", "house", "story", "movie", "film",
    "king", "war", "last", "first", "one", "two", "girl", "boy", "game",
    "world", "city", "home", "time", "day", "dark", "light", "dead",
    "live", "road", "sea", "star", "moon", "sun", "fire", "heart",
    "mission", "avengers", "spider", "batman", "godzilla", "kong",
]


class Discovery:
    """One crawler instance. Call step() for a single search+merge, or run()."""

    def __init__(self, client: MkvbaseClient, index, state_dir: str):
        self.client = client
        self.index = index  # MongoIndex (preferred) — needs upsert(rows, source)
        self._qlock = threading.Lock()  # queue is fed by pusher threads too
        self.state_path = os.path.join(state_dir, "discovery_state.json")
        self.gap_s = float(os.getenv("MKV_DISCOVERY_GAP_S", "45"))
        self.max_word_len = int(os.getenv("MKV_DISCOVERY_MAXWORD", "24"))
        self.max_pass_terms = int(os.getenv("MKV_DISCOVERY_PASS_TERMS", "2000"))
        self.min_new_per_pass = int(os.getenv("MKV_DISCOVERY_MINNEW", "3"))
        self.cool_down_s = float(os.getenv("MKV_DISCOVERY_COOLDOWN_S", "3600"))
        self.queued: list[str] = []          # FIFO frontier
        self.queued_set: set[str] = set()
        self.known_terms: set[str] = set()   # every term ever searched
        self.exhausted: set[str] = set()     # terms that yielded nothing new
        self.done_terms = 0
        self.found_rows = 0
        self._load()

    # ------------------------------------------------------------------ state
    def _load(self) -> None:
        try:
            with open(self.state_path, encoding="utf-8") as f:
                d = json.load(f)
            self.queued = list(d.get("queued") or [])
            self.queued_set = set(self.queued)
            self.known_terms = set(d.get("known_terms") or [])
            self.exhausted = set(d.get("exhausted") or [])
            self.done_terms = int(d.get("done_terms") or 0)
            self.found_rows = int(d.get("found_rows") or 0)
        except Exception:
            pass
        if not self.queued and not self.done_terms:
            for t in _SEEDS:
                self._queue(t)

    def save(self) -> None:
        try:
            os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
            with open(self.state_path, "w", encoding="utf-8") as f:
                json.dump({"queued": self.queued[:20000],
                           "known_terms": sorted(self.known_terms)[-30000:],
                           "exhausted": sorted(self.exhausted)[-30000:],
                           "done_terms": self.done_terms,
                           "found_rows": self.found_rows,
                           "ts": time.time()}, f)
        except Exception:
            pass

    def _queue(self, term: str, max_len: int | None = None, front: bool = False) -> bool:
        t = term.strip().lower()
        cap = max_len or self.max_word_len
        with self._qlock:
            if (t and 3 <= len(t) <= cap and t not in self.queued_set
                    and t not in self.known_terms and t not in self.exhausted):
                if front:
                    self.queued.insert(0, t)  # freshness-first: freshest words first
                else:
                    self.queued.append(t)
                self.queued_set.add(t)
                return True
        return False

    def seed_titles(self, titles: list[str], front: bool = False) -> int:
        """External seeds. front=True (words mined from the newest recent rows)
        jumps the queue so the freshest uploads' vocabulary is searched next —
        approximating a created_at walk backwards. front=False for /api/trending
        and other steady-state seeds."""
        added = 0
        for title in titles or []:
            title = (title or "").strip()
            if not title:
                continue
            if self._queue(title, max_len=80, front=front):
                added += 1
            for w in _WORD.findall(title.lower()):
                if w not in _STOP and self._queue(w, front=front):
                    added += 1
        if added:
            self.save()
        return added

    # ------------------------------------------------------------------ crawl
    def _mine(self, titles: list[str]) -> int:
        added = 0
        for title in titles:
            for w in _WORD.findall((title or "").lower()):
                if w not in _STOP and self._queue(w):
                    added += 1
        return added

    def stats_line(self) -> str:
        return (f"done={self.done_terms} queued={len(self.queued)} "
                f"exhausted={len(self.exhausted)} rows={self.found_rows}")

    def step(self, log=None) -> dict:
        """One signed search + merge into the index. Summary dict for logging."""
        with self._qlock:
            if not self.queued:
                return {"status": "queue-empty", "queued": 0}
            term = self.queued.pop(0)
            self.queued_set.discard(term)
        left = len(self.queued)
        if log:
            log(f"[discovery] crawling {term!r}  ({self.done_terms} done, {left} left in queue)",
                flush=True)
        t0 = time.time()
        try:
            obj = self.client.search(term)
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)[:120]}"
            transient = any(s in err.lower() for s in (
                "timeout", "needssession", "clearance", "target closed",
                "connection", "network", "browser cannot launch"))
            if transient:
                # put it back at the end — do not burn a seed on a flaky clear
                if term not in self.queued_set and term not in self.known_terms:
                    self.queued.append(term)
                    self.queued_set.add(term)
                self.save()
                return {"status": "retry", "term": term, "err": err,
                        "took_s": round(time.time() - t0, 1),
                        "queued": len(self.queued)}
            self.known_terms.add(term)  # permanent fail — retire
            self.save()
            return {"status": "fail", "term": term, "err": err,
                    "took_s": round(time.time() - t0, 1)}
        rows = [r for r in (obj.get("results") or []) if isinstance(r, dict)]
        mined = self._mine([r.get("title") or "" for r in rows])
        new = 0
        if rows:
            new, _upd = self.index.upsert(rows, source="discovery")
            self.found_rows += new
        self.known_terms.add(term)
        self.done_terms += 1
        if new == 0:
            self.exhausted.add(term)
        self.save()
        return {"status": "ok", "term": term, "rows": len(rows), "new": new,
                "mined": mined, "queued": len(self.queued),
                "took_s": round(time.time() - t0, 1)}

    def _fmt_step(self, info: dict) -> str:
        term = info.get("term", "?")
        took = info.get("took_s", "?")
        if info.get("status") == "ok":
            return (f"[discovery] ok {term!r}: {info.get('rows', 0)} rows, "
                    f"{info.get('new', 0)} new, mined {info.get('mined', 0)} words "
                    f"→ queue {info.get('queued', 0)}  ({took}s)  [{self.stats_line()}]")
        if info.get("status") == "retry":
            return (f"[discovery] RETRY {term!r} later: {info.get('err', '?')}  "
                    f"({took}s)  [{self.stats_line()}]")
        if info.get("status") == "fail":
            return (f"[discovery] FAIL {term!r}: {info.get('err', '?')}  ({took}s)  "
                    f"[{self.stats_line()}]")
        return f"[discovery] {info}"

    # ------------------------------------------------------------------ loop
    def run(self, log=print) -> None:
        """Polite forever-loop: bounded passes, cooldown when saturated,
        exhausted terms re-checked after each cooldown."""
        # wait for the pusher's upfront CF clear so we don't race Camoufox
        for i in range(36):  # up to ~3 min
            if self.client.session_ready():
                break
            if i == 0:
                log("[discovery] waiting for Cloudflare session before first crawl…",
                    flush=True)
            time.sleep(5)
        log(f"[discovery] start: {len(self.queued)} queued, {self.done_terms} done, "
            f"{len(self.exhausted)} exhausted, gap {self.gap_s:.0f}s between searches",
            flush=True)
        while True:
            pass_new, pass_terms = 0, 0
            while self.queued and pass_terms < self.max_pass_terms:
                try:
                    info = self.step(log=log)
                except Exception as e:
                    # one bad term must never kill the crawl thread
                    info = {"status": "fail", "term": "?", "err":
                            f"step-error: {type(e).__name__}: {str(e)[:90]}"}
                    time.sleep(5)
                pass_terms += 1
                if info.get("status") == "ok":
                    pass_new += info.get("new", 0)
                log(self._fmt_step(info), flush=True)
                # longer pause after a transient fail so CF/browser can recover
                gap = int(self.gap_s) * (2 if info.get("status") == "retry" else 1)
                left = gap
                while left > 0:
                    log(f"[discovery] next crawl in {left}s…  [{self.stats_line()}]",
                        flush=True)
                    chunk = min(15, left)
                    time.sleep(chunk)
                    left -= chunk
            log(f"[discovery] pass done: {pass_terms} terms, {pass_new} new rows, "
                f"{len(self.queued)} still queued  [{self.stats_line()}]", flush=True)
            if pass_new < self.min_new_per_pass:
                # re-check exhausted terms next pass: new uploads can revive them
                revived = 0
                for t in list(self.exhausted)[:5000]:
                    self.exhausted.discard(t)
                    if self._queue(t):
                        revived += 1
                cool = int(self.cool_down_s)
                log(f"[discovery] saturated — cooling {cool // 60} min, "
                    f"re-queued {revived} old terms "
                    "(recent loop keeps updating meanwhile)", flush=True)
                left = cool
                while left > 0:
                    log(f"[discovery] cooldown {left // 60}m {left % 60}s left…  "
                        f"[{self.stats_line()}]", flush=True)
                    chunk = min(60, left)
                    time.sleep(chunk)
                    left -= chunk
            elif not self.queued:
                log("[discovery] queue drained — reseeding for another sweep", flush=True)
                for t in _SEEDS:
                    self.known_terms.discard(t)  # allow re-search of seeds
                    self._queue(t)
                time.sleep(self.gap_s)
