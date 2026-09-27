"""Discovery crawler — multi-agent back-catalog harvest into the links index.

mkvbase search is title-substring only and hard-caps at ~50 hits (newest uploads
first). Coverage = many strategies at once:

  Agents (MKV_DISCOVERY_AGENTS, default 10) run SIMULTANEOUSLY, each preferring:
    priority — movie/show title heads (Jana Nayagan) — always drained first
    day      — calendar-day seeds (also priority-pulled so names cannot slip)
    year     — years + year+facet/OTT slices
    alpha    — letter/prefix crawl
    words    — mined leftover words
    facet    — Quality/Codec/Format/Language/Season/OTT (zee5, amzn, nf, …)

  Day-walk + vault-pull extract every clean title head onto priority (+ OTT
  attach). Cap-spill fans out when a search returns 50 rows.

Politeness: each agent sleeps MKV_DISCOVERY_GAP_S (default 15) between searches.
"""
from __future__ import annotations

import json
import os
import re
import threading
import time
from datetime import date, timedelta

from .client import MkvbaseClient, MkvbaseError

_WORD = re.compile(r"[a-z0-9]{3,}")
_YEAR_IN_TITLE = re.compile(r"\b((?:19|20)\d{2})\b")
_YEAR_TERM = re.compile(r"^(?:19|20)\d{2}$")
_YEAR_SLICE_TERM = re.compile(r"^(?:19|20)\d{2}\s+\S+")
# Prison Break S01E10 / S01 / Season 1 / S4 E02
_SERIES_MARK = re.compile(
    r"\b(?:s(\d{1,2})\s*e\s*(\d{1,3})|s(\d{1,2})\b|season\s*(\d{1,2}))",
    re.I,
)
_SHOW_SEASON = re.compile(r"^(.+?)\s+s(\d{1,2})$", re.I)
_FACET_SEASON = re.compile(r"^(?:s\d{1,2}|season\s+\d{1,2})$", re.I)
_RESULT_CAP = 50
_ALPHA = "abcdefghijklmnopqrstuvwxyz"
# priority is drained first by every agent so title heads beat junk digraphs
_LANES = ("priority", "day", "year", "alpha", "words", "facet")
_LANE_SAVE_CAP = {
    "priority": 15000,
    "day": 4000,
    "year": 3000,
    "alpha": 2000,
    "words": 6000,
    "facet": 500,
}
_NOISE = re.compile(
    r"gdflix|hubcloud|mlwbd|http|www\.|skymovies|movies4u|moviesdrives|"
    r"xdmovies|4khdhub|telly\b|\.com\b|downloaded\s+from",
    re.I,
)

# Canonical facet catalog (search forms). Aliases normalized via _norm_term.
_FACETS: dict[str, tuple[str, ...]] = {
    "season": tuple(
        [f"s{n:02d}" for n in range(1, 13)]
        + [f"season {n}" for n in range(1, 13)]
    ),
    "format": ("zip", "mkv", "pack", "file", "complete", "repack"),
    "source": ("web dl", "webrip", "bluray", "hdtc"),
    "ott": ("zee5", "amzn", "nf", "hotstar", "sonyliv", "voot", "mx player",
            "aha", "sun nxt", "hoichoi", "altbalaji", "hulu", "hbo",
            "apple tv", "paramount", "crunchyroll", "stage", "prime"),
    "video": ("720p", "1080p", "480p", "2160p", "uhd", "sdr", "hdr",
              "10bit", "60fps"),
    "codec": ("265", "264", "hevc", "x264", "x265", "h264", "h265"),
    "audio": ("ddp", "aac", "dts", "atmos"),
    "subtitle": ("esub", "msub"),
    "language": ("hindi", "english", "multi", "tamil", "telugu", "malayalam",
                 "kannada", "dual"),
}
_FACET_SEEDS: tuple[str, ...] = tuple(
    dict.fromkeys(t for group in _FACETS.values() for t in group)
)
_OTT_SEEDS: tuple[str, ...] = _FACETS["ott"]
# Attached on 50-cap spill + day/vault title pull (keep short)
_SPILL_FACETS: tuple[str, ...] = (
    "zip", "mkv", "pack", "complete", "repack",
    "web dl", "webrip", "bluray", "hdtc",
    "zee5", "amzn", "nf", "hotstar", "sonyliv", "prime", "stage",
    "720p", "1080p", "480p", "uhd", "hdr", "10bit",
    "hevc", "265", "264", "x264", "x265",
    "ddp", "aac",
    "esub", "msub",
    "hindi", "english", "multi", "tamil", "telugu", "dual",
    "s01", "s02", "s03", "s04", "s05",
)
# Day/vault pull: only OTT+format so each head doesn't explode the queue
_PULL_ATTACH: tuple[str, ...] = (
    "zip", "mkv", "zee5", "amzn", "nf", "hotstar", "1080p", "720p", "hindi",
)
_SHOW_FORMATS = ("zip", "mkv", "pack", "complete")
_ALIASES = {
    "web-dl": "web dl", "webdl": "web dl", "web_dl": "web dl",
    "blu ray": "bluray", "blu-ray": "bluray", "blu_ray": "bluray",
    "acc": "aac",
    "h.265": "265", "h.264": "264", "h265": "265", "h264": "264",
    "netflix": "nf", "amazon": "amzn", "prime": "amzn", "prime video": "amzn",
    "zee 5": "zee5", "jiohotstar": "hotstar", "disney": "hotstar",
    "disney plus": "hotstar", "sony liv": "sonyliv", "mxplayer": "mx player",
    "sunnxt": "sun nxt", "alt balaji": "altbalaji",
    "appletv": "apple tv", "itunes": "apple tv", "hbo max": "hbo",
}
_STOP = {"the", "and", "for", "with", "from", "www", "com", "dvd",
         "part", "dd5", "ddp5", "truehd", "8bit",
         "webrip", "brrip", "dvdrip", "hdrip", "hdtv", "remux", "hdr10",
         "esubs", "org", "clean", "esc", "avc",
         "upscale", "upscaled", "hdts", "camrip", "dolby", "vision",
         "audio", "subtitle", "subtitles", "subbed", "dubbed",
         "bengali", "marathi", "punjabi", "japanese", "korean",
         "mandarin", "spanish", "french", "german", "4k", "imax", "extended",
         "remastered", "unrated", "cut", "directors", "ultimate", "collector",
         "edition", "season", "episode", "ep", "eps", "vol",
         "chapter", "bangla", "chinese", "hdr10plus",
         "rar", "foo", "movies4u", "moviesdrives", "xdmovies"}
# Facet tokens stay out of mining as lone words (seeded via facet lane instead)
_STOP |= set(_FACET_SEEDS)

_YEAR_SEEDS = [str(y) for y in range(date.today().year, 1949, -1)]
_FACET_SET = set(_FACET_SEEDS)


def _norm_term(term: str) -> str:
    t = re.sub(r"\s+", " ", (term or "").strip().lower())
    return _ALIASES.get(t, t)


def _reject_term(term: str) -> bool:
    """Drop host tags / giant release strings that drown real title searches."""
    t = _norm_term(term)
    if not t:
        return True
    if _NOISE.search(t):
        return True
    parts = t.split()
    if len(parts) > 5:
        return True
    if len(parts) >= 3 and parts[-1] in ("mkv", "mp4", "avi", "mka", "m4v"):
        return True
    return False


def _clean_title(title: str) -> str:
    """Strip leading site tags like '(Movies4u Foo)' / '@channel'."""
    t = (title or "").strip()
    t = re.sub(r"^\([^)]*\)\s*", "", t)
    t = re.sub(r"^@\S+\s+", "", t)
    return t


def _words_head(chunk: str, n: int = 5) -> str | None:
    words = [w for w in _WORD.findall((chunk or "").lower()) if w not in _STOP]
    if len(words) >= 2:
        return " ".join(words[:n])
    return words[0] if words else None


def _title_head(title: str) -> str | None:
    """Movie/show name: before Sxx/Season if present, else before year."""
    t = _clean_title(title)
    m = _SERIES_MARK.search(t)
    if m:
        return _words_head(t[:m.start()])
    ym = _YEAR_IN_TITLE.search(t)
    if ym:
        return _words_head(t[:ym.start()])
    return None


def _seasons_in(title: str) -> set[int]:
    found: set[int] = set()
    for m in _SERIES_MARK.finditer(title or ""):
        # groups: (sXeY season, sXeY ep, sX, season N) — never treat episode as season
        g = m.group(1) or m.group(3) or m.group(4)
        if g:
            found.add(int(g))
    return found


def _series_terms(show: str, seasons: set[int] | None = None) -> list[str]:
    """Expand a show into season + format searches (mkv episodes AND zip packs)."""
    show = (show or "").strip().lower()
    if not show or len(show) < 3:
        return []
    seen = set(seasons or ())
    hi = max(5, max(seen) if seen else 5)
    hi = min(hi + 1, 12)
    out = [show]
    for fmt in _SHOW_FORMATS:
        out.append(f"{show} {fmt}")
    for n in range(1, hi + 1):
        out.append(f"{show} s{n:02d}")
        out.append(f"{show} s{n}")
        out.append(f"{show} s{n:02d} zip")
    return out


def _episode_terms(show: str, season: int, hi_ep: int = 24) -> list[str]:
    show = (show or "").strip().lower()
    if not show:
        return []
    return [f"{show} s{season:02d}e{ep:02d}" for ep in range(1, hi_ep + 1)]


def _classify(term: str) -> str:
    t = _norm_term(term)
    if _YEAR_TERM.match(t) or _YEAR_SLICE_TERM.match(t):
        return "year"
    if t in _FACET_SET or _FACET_SEASON.match(t):
        return "facet"
    if t.isalpha() and 1 <= len(t) <= 3:
        return "alpha"
    # 2–4 word clean heads = movie/show names → priority (Jana Nayagan)
    parts = t.split()
    if 2 <= len(parts) <= 4 and not _reject_term(t):
        return "priority"
    return "words"


def _parse_day(s: str | None) -> date | None:
    if not s:
        return None
    try:
        return date.fromisoformat(str(s)[:10])
    except Exception:
        return None


class Discovery:
    """Shared vault-aware discovery with a multi-agent fleet. Call run() once."""

    def __init__(self, client: MkvbaseClient, index, state_dir: str):
        self.client = client
        self.index = index
        self._qlock = threading.RLock()
        self._savelock = threading.Lock()
        self.state_path = os.path.join(state_dir, "discovery_state.json")
        self.gap_s = float(os.getenv("MKV_DISCOVERY_GAP_S", "15"))
        self.agents_n = max(1, int(os.getenv("MKV_DISCOVERY_AGENTS", "10")))
        self.max_word_len = int(os.getenv("MKV_DISCOVERY_MAXWORD", "48"))
        self.day_walk_max = int(os.getenv("MKV_DISCOVERY_DAY_WALK", "730"))
        self.day_every_s = float(os.getenv("MKV_DISCOVERY_DAY_EVERY_S", "90"))
        self.vault_pull_max = int(os.getenv("MKV_DISCOVERY_VAULT_PULL", "5000"))
        self.lanes: dict[str, list[str]] = {k: [] for k in _LANES}
        self.queued_set: set[str] = set()
        self.known_terms: set[str] = set()
        self.exhausted: set[str] = set()
        self.done_terms = 0
        self.found_rows = 0
        self.day_cursor: date = date.today()
        self.agent_done: dict[str, int] = {}
        self._load()

    # --- compat for pusher heartbeat (total queued across lanes) ---
    @property
    def queued(self) -> list:
        with self._qlock:
            return [t for lane in _LANES for t in self.lanes[lane]]

    def _load(self) -> None:
        try:
            with open(self.state_path, encoding="utf-8") as f:
                d = json.load(f)
            self.known_terms = set(d.get("known_terms") or [])
            self.exhausted = set(d.get("exhausted") or [])
            self.done_terms = int(d.get("done_terms") or 0)
            self.found_rows = int(d.get("found_rows") or 0)
            dc = _parse_day(d.get("day_cursor"))
            if dc:
                self.day_cursor = dc
            raw_lanes = d.get("lanes") or {}
            if raw_lanes:
                for k in _LANES:
                    kept = []
                    for t in raw_lanes.get(k) or []:
                        t = _norm_term(t)
                        if _reject_term(t):
                            continue
                        kept.append(t)
                    self.lanes[k] = kept
            else:
                # migrate flat queue from older state
                for t in d.get("queued") or []:
                    t = _norm_term(t)
                    if _reject_term(t):
                        continue
                    self.lanes[_classify(t)].append(t)
            # move clean multi-word heads stuck in day/words into priority
            for src in ("day", "words"):
                stay, move = [], []
                for t in self.lanes[src]:
                    (move if _classify(t) == "priority" else stay).append(t)
                self.lanes[src] = stay
                self.lanes["priority"] = move + self.lanes["priority"]
            self.queued_set = set(self.queued)
        except Exception:
            pass
        if not self.queued_set and not self.done_terms:
            self._seed_backfill()

    def save(self) -> None:
        with self._savelock:
            try:
                os.makedirs(os.path.dirname(self.state_path) or ".", exist_ok=True)
                with self._qlock:
                    payload = {
                        "lanes": {k: self.lanes[k][:_LANE_SAVE_CAP.get(k, 4000)]
                                  for k in _LANES},
                        "queued": self.queued[:25000],  # compat
                        "known_terms": sorted(self.known_terms)[-30000:],
                        "exhausted": sorted(self.exhausted)[-30000:],
                        "done_terms": self.done_terms,
                        "found_rows": self.found_rows,
                        "day_cursor": self.day_cursor.isoformat(),
                        "agents": self.agents_n,
                        "ts": time.time(),
                    }
                with open(self.state_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f)
            except Exception:
                pass

    def _queue(self, term: str, max_len: int | None = None, front: bool = False,
               min_len: int = 3, lane: str | None = None) -> bool:
        t = _norm_term(term)
        if _reject_term(t):
            return False
        cap = max_len or self.max_word_len
        dest = lane or _classify(t)
        if dest not in self.lanes:
            dest = "words"
        # title heads always land in priority (unless caller forced year/facet/alpha)
        if dest in ("day", "words") and _classify(t) == "priority":
            dest = "priority"
        if dest == "facet":
            min_len = min(min_len, 2)
        with self._qlock:
            if (t and min_len <= len(t) <= cap and t not in self.queued_set
                    and t not in self.known_terms and t not in self.exhausted):
                if front:
                    self.lanes[dest].insert(0, t)
                else:
                    self.lanes[dest].append(t)
                self.queued_set.add(t)
                return True
        return False

    def _boost(self, term: str, lane: str = "priority") -> bool:
        """Move term to front of lane even if already queued (hot misses)."""
        t = _norm_term(term)
        if _reject_term(t) or not t:
            return False
        with self._qlock:
            self.known_terms.discard(t)
            self.exhausted.discard(t)
            if t in self.queued_set:
                for ln in self.lanes:
                    try:
                        self.lanes[ln].remove(t)
                    except ValueError:
                        pass
                self.lanes[lane].insert(0, t)
                return True
        return self._queue(t, max_len=80, front=True, lane=lane)

    def _pop(self, prefer: str | None = None) -> tuple[str, str] | None:
        """Priority lane always first, then prefer, then the rest."""
        order = ["priority"]
        if prefer and prefer != "priority":
            order.append(prefer)
        order += [k for k in _LANES if k not in order]
        with self._qlock:
            for lane in order:
                if not self.lanes.get(lane):
                    continue
                term = self.lanes[lane].pop(0)
                self.queued_set.discard(term)
                return term, lane
        return None

    def _seed_backfill(self) -> int:
        n = 0
        for t in _YEAR_SEEDS:
            if self._queue(t, lane="year"):
                n += 1
        for c in _ALPHA:
            if self._queue(c, min_len=1, lane="alpha"):
                n += 1
        for t in _FACET_SEEDS:
            if self._queue(t, min_len=2, lane="facet"):
                n += 1
        return n

    def _queue_show(self, show: str, seasons: set[int] | None = None,
                    front: bool = False, lane: str = "priority") -> int:
        added = 0
        for term in _series_terms(show, seasons):
            if self._queue(term, max_len=80, front=front, lane=lane):
                added += 1
        return added

    def seed_titles(self, titles: list[str], front: bool = False,
                    lane: str = "priority") -> int:
        added = 0
        for title in titles or []:
            title = (title or "").strip()
            if not title:
                continue
            head = _title_head(title)
            seasons = _seasons_in(title)
            if head:
                head_lane = "priority" if _classify(head) == "priority" else lane
                if seasons:
                    added += self._queue_show(head, seasons, front=front, lane=head_lane)
                elif self._queue(head, max_len=80, front=front, lane=head_lane):
                    added += 1
                    if " " in head:
                        for fmt in _SHOW_FORMATS:
                            if self._queue(f"{head} {fmt}", max_len=80,
                                           front=front, lane=head_lane):
                                added += 1
            for w in _WORD.findall(title.lower()):
                if w not in _STOP and self._queue(w, front=False, lane="words"):
                    added += 1
        if added:
            self.save()
        return added

    def _pull_heads(self, titles: list[str], *, front: bool = True,
                    attach: bool = True, log=None, tag: str = "pull") -> int:
        """Extract movie/show heads and put them on priority (+ OTT/format attach).
        Same 'Jana Nayagan' treatment so nothing slips on day-walk or vault scan."""
        added = 0
        seen: set[str] = set()
        for title in titles or []:
            head = _title_head(title or "")
            if not head or head in seen or _reject_term(head):
                continue
            seen.add(head)
            if front:
                if self._boost(head, lane="priority"):
                    added += 1
            elif self._queue(head, max_len=80, front=False, lane="priority"):
                added += 1
            if attach and " " in head:
                for sfx in _PULL_ATTACH:
                    if self._queue(f"{head} {sfx}", max_len=80, front=front,
                                   lane="priority"):
                        added += 1
            seasons = _seasons_in(title or "")
            if seasons:
                added += self._queue_show(head, seasons, front=front, lane="priority")
        if log and added:
            log(f"[discovery] {tag}: pulled {len(seen)} heads -> priority +{added}",
                flush=True)
        return added

    def seed_created_day(self, day: date | None = None, log=print) -> int:
        d = day or self.day_cursor
        fn = getattr(self.index, "titles_on_created_day", None)
        if not callable(fn):
            return 0
        titles = fn(d.isoformat())
        # Day titles go to PRIORITY first (name search) so they cannot slip like Jana
        n_pri = self._pull_heads(titles, front=True, attach=True, log=None, tag="day")
        # light day-lane word seeds for that calendar day
        n_day = self.seed_titles(titles, front=True, lane="day")
        if log:
            log(f"[discovery] created_at {d.isoformat()}: "
                f"{len(titles)} vault titles -> priority-pull +{n_pri}, "
                f"day/words +{n_day}", flush=True)
        return n_pri + n_day

    def pull_vault_heads(self, log=print) -> int:
        """Scan current vault and priority-queue every clean title head."""
        col = getattr(self.index, "_col", None)
        if col is None:
            return 0
        lim = max(100, self.vault_pull_max)
        try:
            # newest first, then a mid-slice so older catalog isn't ignored
            newest = list(col.find({}, {"title": 1, "_id": 0})
                          .sort("created_at", -1).limit(lim))
            mid = list(col.find({}, {"title": 1, "_id": 0})
                       .sort("_seq", 1).limit(min(2000, lim // 2)))
            titles = [r["title"] for r in newest + mid if r.get("title")]
            # front=False so we don't bury hot boosts; still all get queued
            n = self._pull_heads(titles, front=False, attach=True, log=log,
                                 tag=f"vault-pull({len(titles)})")
            return n
        except Exception as e:
            if log:
                log(f"[discovery] vault-pull failed: {type(e).__name__}: {e}",
                    flush=True)
            return 0

    def advance_day_cursor(self, log=print) -> None:
        self.seed_created_day(self.day_cursor, log=log)
        oldest = date.today() - timedelta(days=self.day_walk_max)
        if self.day_cursor > oldest:
            self.day_cursor -= timedelta(days=1)
        else:
            self.day_cursor = date.today()
            if log:
                log("[discovery] day-walk wrapped back to today", flush=True)
        self.save()

    def _mine(self, rows: list[dict]) -> int:
        ranked = sorted(
            (r for r in rows if isinstance(r, dict)),
            key=lambda r: str(r.get("created_at") or ""),
            reverse=True,
        )
        added = 0
        # collect seasons per show across the whole result page
        show_seasons: dict[str, set[int]] = {}
        for row in ranked:
            title = row.get("title") or ""
            head = _title_head(title)
            if not head:
                continue
            show_seasons.setdefault(head, set()).update(_seasons_in(title))
        for i, row in enumerate(ranked):
            front = i < 20
            title = row.get("title") or ""
            head = _title_head(title)
            if head:
                seas = show_seasons.get(head) or _seasons_in(title)
                if seas:
                    added += self._queue_show(head, seas, front=True, lane="priority")
                else:
                    if self._queue(head, max_len=80, front=True, lane="priority"):
                        added += 1
                    if " " in head:
                        for fmt in _SHOW_FORMATS:
                            if self._queue(f"{head} {fmt}", max_len=80,
                                           front=True, lane="priority"):
                                added += 1
            for w in _WORD.findall(title.lower()):
                if w not in _STOP and self._queue(w, front=False, lane="words"):
                    added += 1
        return added

    def _spill_facets_onto(self, base: str, lane: str = "words",
                           front: bool = True) -> int:
        """Attach spill facets onto a base term (year or show)."""
        added = 0
        base = _norm_term(base)
        if not base:
            return 0
        for sfx in _SPILL_FACETS:
            if self._queue(f"{base} {sfx}", max_len=80, front=front, lane=lane):
                added += 1
        return added

    def _spill_capped(self, term: str, n_rows: int, rows: list[dict] | None = None) -> int:
        """When API returns the 50-cap, fan out so buried titles/seasons/zips get reached."""
        if n_rows < _RESULT_CAP:
            return 0
        added = 0
        bare = _norm_term(term)
        rows = rows or []
        if _YEAR_TERM.match(bare):
            added += self._spill_facets_onto(bare, lane="year", front=True)
        if bare.isalpha() and 1 <= len(bare) <= 3 and bare not in _FACET_SET:
            for c in _ALPHA:
                if self._queue(bare + c, front=True, min_len=1, lane="alpha"):
                    added += 1
        # bare facet at cap -> try pairing with recent years (zip + 2024..)
        if bare in _FACET_SET or _FACET_SEASON.match(bare):
            for y in _YEAR_SEEDS[:8]:
                if self._queue(f"{y} {bare}", max_len=32, front=True, lane="year"):
                    added += 1
        # show name capped -> season + zip packs (Prison Break case)
        sm = _SHOW_SEASON.match(bare)
        if sm:
            show, sn = sm.group(1).strip(), int(sm.group(2))
            added += self._queue_show(show, {sn}, front=True, lane="words")
            for t in _episode_terms(show, sn):
                if self._queue(t, max_len=80, front=True, lane="words"):
                    added += 1
            added += self._spill_facets_onto(show, lane="words", front=True)
        elif " " in bare and not _YEAR_SLICE_TERM.match(bare):
            seas: set[int] = set()
            for r in rows:
                seas |= _seasons_in(r.get("title") or "")
            show = bare
            if not _SERIES_MARK.search(bare) and not _YEAR_IN_TITLE.search(bare):
                show = bare
            added += self._queue_show(show, seas or {1, 2, 3, 4, 5},
                                     front=True, lane="words")
            added += self._spill_facets_onto(show, lane="words", front=True)
        elif bare not in _FACET_SET and not _YEAR_TERM.match(bare):
            # single distinctive word at cap (e.g. show one-token) — attach facets
            if len(bare) >= 4:
                added += self._spill_facets_onto(bare, lane="words", front=True)
        return added

    def stats_line(self) -> str:
        with self._qlock:
            bits = " ".join(f"{k}={len(self.lanes[k])}" for k in _LANES)
            q = sum(len(self.lanes[k]) for k in _LANES)
        return (f"done={self.done_terms} queued={q} [{bits}] "
                f"exhausted={len(self.exhausted)} rows={self.found_rows} "
                f"day={self.day_cursor.isoformat()} agents={self.agents_n}")

    def step(self, log=None, agent: str = "a0", prefer: str | None = None) -> dict:
        got = self._pop(prefer)
        if not got:
            return {"status": "queue-empty", "queued": 0, "agent": agent}
        term, lane = got
        if log:
            log(f"[discovery:{agent}] crawl {term!r} lane={lane}  "
                f"[{self.stats_line()}]", flush=True)
        t0 = time.time()
        try:
            obj = self.client.search(term)
        except Exception as e:
            err = f"{type(e).__name__}: {str(e)[:120]}"
            transient = any(s in err.lower() for s in (
                "timeout", "needssession", "clearance", "target closed",
                "connection", "network", "browser cannot launch"))
            if transient:
                self._queue(term, min_len=1, front=True, lane=lane)
                self.save()
                return {"status": "retry", "term": term, "lane": lane, "agent": agent,
                        "err": err, "took_s": round(time.time() - t0, 1),
                        "queued": len(self.queued_set)}
            with self._qlock:
                self.known_terms.add(term)
            self.save()
            return {"status": "fail", "term": term, "lane": lane, "agent": agent,
                    "err": err, "took_s": round(time.time() - t0, 1)}
        rows = [r for r in (obj.get("results") or []) if isinstance(r, dict)]
        mined = self._mine(rows)
        spilled = self._spill_capped(term, len(rows), rows)
        new = 0
        if rows:
            new, _upd = self.index.upsert(rows, source=f"discovery:{lane}")
            with self._qlock:
                self.found_rows += new
        with self._qlock:
            self.known_terms.add(term)
            self.done_terms += 1
            self.agent_done[agent] = self.agent_done.get(agent, 0) + 1
            if new == 0:
                self.exhausted.add(term)
        self.save()
        return {"status": "ok", "term": term, "lane": lane, "agent": agent,
                "rows": len(rows), "new": new, "mined": mined, "spilled": spilled,
                "queued": len(self.queued_set),
                "took_s": round(time.time() - t0, 1)}

    def _fmt_step(self, info: dict) -> str:
        agent = info.get("agent", "?")
        term = info.get("term", "?")
        took = info.get("took_s", "?")
        lane = info.get("lane", "?")
        if info.get("status") == "ok":
            spill = info.get("spilled") or 0
            spill_bit = f", spill {spill}" if spill else ""
            return (f"[discovery:{agent}] ok {term!r}/{lane}: {info.get('rows', 0)} rows, "
                    f"{info.get('new', 0)} new, mined {info.get('mined', 0)}"
                    f"{spill_bit} ({took}s)  [{self.stats_line()}]")
        if info.get("status") == "retry":
            return (f"[discovery:{agent}] RETRY {term!r}: {info.get('err', '?')}  "
                    f"({took}s)")
        if info.get("status") == "fail":
            return (f"[discovery:{agent}] FAIL {term!r}: {info.get('err', '?')}  "
                    f"({took}s)")
        if info.get("status") == "queue-empty":
            return f"[discovery:{agent}] idle (lanes empty)"
        return f"[discovery:{agent}] {info}"

    def _agent_loop(self, idx: int, prefer: str, log) -> None:
        agent = f"a{idx}/{prefer}"
        log(f"[discovery] agent {agent} online (gap {self.gap_s:.0f}s)", flush=True)
        idle_rounds = 0
        while True:
            try:
                info = self.step(log=log, agent=agent, prefer=prefer)
            except Exception as e:
                info = {"status": "fail", "term": "?", "agent": agent,
                        "err": f"step-error: {type(e).__name__}: {str(e)[:90]}"}
                time.sleep(5)
            if info.get("status") != "queue-empty":
                log(self._fmt_step(info), flush=True)
                idle_rounds = 0
            else:
                idle_rounds += 1
                if idle_rounds == 1:
                    log(self._fmt_step(info), flush=True)
                if idle_rounds >= 3:
                    # revive a slice of exhausted + ensure backfill roots
                    revived = 0
                    with self._qlock:
                        old = list(self.exhausted)[:500]
                        for t in old:
                            self.exhausted.discard(t)
                    for t in old:
                        if self._queue(t, min_len=1):
                            revived += 1
                    if not self.queued_set:
                        self._seed_backfill()
                    if revived:
                        log(f"[discovery:{agent}] revived {revived} exhausted terms",
                            flush=True)
                    idle_rounds = 0
            gap = self.gap_s * (2 if info.get("status") == "retry" else 1)
            if info.get("status") == "queue-empty":
                gap = min(gap, 10)
            time.sleep(gap)

    def _day_loop(self, log) -> None:
        log(f"[discovery] day-walk agent online (every {self.day_every_s:.0f}s)",
            flush=True)
        self.day_cursor = date.today()
        self.advance_day_cursor(log=log)
        while True:
            time.sleep(self.day_every_s)
            try:
                self.advance_day_cursor(log=log)
            except Exception as e:
                log(f"[discovery] day-walk error: {type(e).__name__}: {e}", flush=True)

    def run(self, log=print) -> None:
        for i in range(36):
            if self.client.session_ready():
                break
            if i == 0:
                log("[discovery] waiting for Cloudflare session before fleet start...",
                    flush=True)
            time.sleep(5)
        # ensure each strategy has roots (re-allow years/letters even if crawled before)
        for t in _YEAR_SEEDS:
            with self._qlock:
                self.known_terms.discard(t)
                self.exhausted.discard(t)
            self._queue(t, lane="year")
        for c in _ALPHA:
            with self._qlock:
                self.known_terms.discard(c)
                self.exhausted.discard(c)
            self._queue(c, min_len=1, lane="alpha")
        for t in _FACET_SEEDS:
            with self._qlock:
                self.known_terms.discard(t)
                self.exhausted.discard(t)
            self._queue(t, min_len=2, lane="facet")
        log(f"[discovery] facet seeds: {len(_FACET_SEEDS)} tags across "
            f"{len(_FACETS)} categories (ott={len(_OTT_SEEDS)})", flush=True)
        # Pull EVERY clean title head already in the vault onto priority
        try:
            self.pull_vault_heads(log=log)
            col = getattr(self.index, "_col", None)
            if col is not None:
                import re as _re
                cur = col.find(
                    {"title": _re.compile(r"\bS\d{1,2}", _re.I)},
                    {"title": 1, "_id": 0},
                ).limit(400)
                n_series = self.seed_titles(
                    [r["title"] for r in cur if r.get("title")],
                    front=True, lane="priority")
                if n_series:
                    log(f"[discovery] vault series bootstrap: +{n_series} terms",
                        flush=True)
            for hot in ("prison break", "jana nayagan"):
                self._boost(hot, lane="priority")
                self._queue_show(hot, front=True, lane="priority")
                self._boost(hot, lane="priority")
            log("[discovery] boosted hot titles: jana nayagan, prison break",
                flush=True)
        except Exception as e:
            log(f"[discovery] vault bootstrap skipped: {type(e).__name__}: {e}",
                flush=True)
        n = self.agents_n
        log(f"[discovery] fleet start: {n} agents + day-walk (simultaneous), "
            f"{self.stats_line()}, gap {self.gap_s:.0f}s", flush=True)
        threading.Thread(target=self._day_loop, args=(log,), daemon=True,
                         name="disc-daywalk").start()
        # 10 agents all at once: priority x3, day, year, alpha, words, facet x2, words
        prefer_cycle = (
            "priority", "priority", "priority", "day", "year",
            "alpha", "words", "facet", "facet", "words",
        )
        for i in range(n):
            prefer = prefer_cycle[i % len(prefer_cycle)]
            threading.Thread(target=self._agent_loop, args=(i, prefer, log),
                             daemon=True, name=f"disc-a{i}-{prefer}").start()
            time.sleep(min(1.2, self.gap_s / max(n, 1)))
        while True:
            time.sleep(60)
            ad = " ".join(f"{k}={v}" for k, v in sorted(self.agent_done.items()))
            log(f"[discovery] fleet tick [{self.stats_line()}] per-agent {ad}",
                flush=True)
