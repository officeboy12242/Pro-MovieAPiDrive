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

_SEEDS = [str(y) for y in range(1950, 2027)] + [
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

    def _queue(self, term: str, max_len: int | None = None) -> bool:
        t = term.strip().lower()
        cap = max_len or self.max_word_len
        if (t and 3 <= len(t) <= cap and t not in self.queued_set
                and t not in self.known_terms and t not in self.exhausted):
            self.queued.append(t)
            self.queued_set.add(t)
            return True
        return False

    def seed_titles(self, titles: list[str]) -> int:
        """High-yield external seeds (e.g. /api/trending — what real users are
        searching right now). Queue the full title (its search returns all its
        links at once) plus its individual words."""
        added = 0
        for title in titles or []:
            title = (title or "").strip()
            if not title:
                continue
            if self._queue(title, max_len=80):
                added += 1
            for w in _WORD.findall(title.lower()):
                if w not in _STOP and self._queue(w):
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
        return (f"terms done={self.done_terms} queued={len(self.queued)} "
                f"exhausted={len(self.exhausted)} rows_found={self.found_rows}")

    def step(self) -> dict:
        """One signed search + merge into the index. Summary dict for logging."""
        if not self.queued:
            return {"status": "queue-empty", "queued": 0}
        term = self.queued.pop(0)
        self.queued_set.discard(term)
        t0 = time.time()
        try:
            obj = self.client.search(term)
        except MkvbaseError as e:
            self.known_terms.add(term)  # retire the term; do not retry forever
            self.save()
            return {"status": f"search-failed: {str(e)[:90]}", "term": term}
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

    # ------------------------------------------------------------------ loop
    def run(self, log=print) -> None:
        """Polite forever-loop: bounded passes, cooldown when saturated,
        exhausted terms re-checked after each cooldown."""
        log(f"[discovery] start: {len(self.queued)} queued, {self.done_terms} done, "
            f"{self.exhausted and len(self.exhausted)} exhausted, gap {self.gap_s:.0f}s",
            flush=True)
        while True:
            pass_new, pass_terms = 0, 0
            while self.queued and pass_terms < self.max_pass_terms:
                info = self.step()
                pass_terms += 1
                if info.get("status") == "ok":
                    pass_new += info.get("new", 0)
                log(f"[discovery] {info}", flush=True)
                time.sleep(self.gap_s)
            log(f"[discovery] pass done: {pass_terms} terms, {pass_new} new rows, "
                f"{len(self.queued)} still queued", flush=True)
            if pass_new < self.min_new_per_pass:
                # re-check exhausted terms next pass: new uploads can revive them
                revived = 0
                for t in list(self.exhausted)[:5000]:
                    self.exhausted.discard(t)
                    if self._queue(t):
                        revived += 1
                log(f"[discovery] saturated - cooling down "
                    f"{self.cool_down_s / 60:.0f} min, re-queued {revived} old terms "
                    "(new uploads keep flowing via the recent loop meanwhile)",
                    flush=True)
                time.sleep(self.cool_down_s)
            elif not self.queued:
                log("[discovery] queue drained without saturation - reseeding "
                    "seeds for another sweep", flush=True)
                for t in _SEEDS:
                    self.known_terms.discard(t)  # allow re-search of seeds
                    self._queue(t)
                time.sleep(self.gap_s)
