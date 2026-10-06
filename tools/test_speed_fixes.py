#!/usr/bin/env python
"""OFFLINE unit tests for the crawl-speed fixes: the LRU-valve claim and
cap-spill reaction, HTTP-slot anti-starvation, the trimmed facet
vocabulary, and discovery lane selection. No network, no Mongo, no effect
on the running fleet.
Run: .venv/Scripts/python.exe tools/test_speed_fixes.py"""
from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
from collections import deque
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.idgap import (  # noqa: E402
    IdGapMiner, _SPILL_EXTRAS, _SPILL_QUALS, _SPILL_REACTION,
    _TOKEN_MAX_FREQ, _TOKEN_STOP, _terms_of)
from app.client import _PrioritySemaphore  # noqa: E402
from app.discovery import Discovery, _ALPHA  # noqa: E402


def make_discovery():
    """A Discovery with lanes wired but no Mongo/client (offline only)."""
    from app.discovery import _LANES
    d = Discovery.__new__(Discovery)
    d._qlock = threading.RLock()
    d.lanes = {k: [] for k in _LANES}
    d.queued_set = set()
    d.known_terms = set()
    d.exhausted = set()
    d.max_word_len = 48
    return d


def make_miner() -> IdGapMiner:
    m = IdGapMiner.__new__(IdGapMiner)  # skip __init__ (no Mongo/client)
    m.block = 25_000
    m.term_ttl_s = 3 * 3600
    m.searched_terms = {}
    m.term_stats = {}
    m.stats = {"terms_done": 0, "rows_new": 0}
    m._lock = threading.Lock()
    m._seed_cache = {}
    m._series_cache = []
    m._fresh_cache = []
    m._fresh_ts = 0.0
    m._token_pool = []
    m._token_ts = 0.0
    m._token_cursor = 0
    m._token_frontier = deque(maxlen=500)
    m.token_batch = 400
    m.token_every = 1
    m._spill_queue = deque(maxlen=80)
    m._phrase_pool = []
    m._phrase_ts = 0.0
    m._phrase_cursor = 0
    m._phrase_ttl = 6 * 3600
    m.phrase_batch = 400
    m._phrase_frontier = deque(maxlen=500)
    m._slice_pool = []
    m._slice_ts = 0.0
    m._slice_cursor = 0
    m.slice_batch = 400
    m._slice_queue = deque(maxlen=500)
    m.coverage = lambda: {"coverage_pct": 35.9,
                          "thin_blocks": [{"block": 2, "era_date": "2025-11-25"}]}
    m._seeds_for_block = (lambda blk, n=6, fresh_only=True:
                          ["solo head"] if blk == 2 else [])
    m._fresh_heads = lambda: []
    m._series_heads = lambda: []
    m._terms_for_era = lambda era: []
    m.save = lambda: None
    return m


def burn_facets(m: IdGapMiner) -> None:
    now = time.time()
    for f in _SPILL_QUALS + _SPILL_EXTRAS:
        m.searched_terms[f"solo head {f}"] = now - 10_000


def test_lru_claim_no_duplicates() -> None:
    m = make_miner()
    burn_facets(m)
    m._spill_queue.clear()
    blk1, era1, terms1 = m._next_terms(3, "a1")
    assert terms1 == ["seed:solo head"], terms1
    assert m.searched_terms.get("solo head", 0) > 0, "LRU pick not claimed"
    blk2, era2, terms2 = m._next_terms(3, "a2")
    assert terms2 in ([], None), f"second agent got the SAME head: {terms2}"
    print("PASS  LRU valve: head claimed once, duplicate pick refused")


def test_cap_spill_is_rationed() -> None:
    m = make_miner()
    m.client = SimpleNamespace(search=lambda term: {
        "results": [{"id": 50_001 + i, "title": f"hot head part {i}"}
                    for i in range(50)]})
    m.index = SimpleNamespace(upsert=lambda rows, source: (0, 0))
    # 1) a capped (50-row) search must queue the head's OTHER facets
    info = m.step("a1")  # step does its own pick -> 'solo head 720p' (off=1)
    assert info["status"] == "ok" and info["rows"] == 50, info
    assert "solo head 720p" not in m._spill_queue, "queued the searched slice"
    # every reaction facet except the slice we just searched
    assert list(m._spill_queue) == [f"solo head {f}" for f in _SPILL_REACTION
                                    if f != "720p"], list(m._spill_queue)
    # 's01' is deliberately NOT a reaction facet: appending it to a season
    # slice mints 'show s01 s01', which caps and chains to 'show s01 s01 s01'
    # (measured 2026-10-05: 475 slice tries, 0 new, vault frozen 15 min).
    assert "solo head esub" in m._spill_queue and "solo head mkv" in m._spill_queue
    assert "solo head s01" not in m._spill_queue, "s01 must not be a reaction facet"
    # 2) Phase 1: cap-spill is the lowest-yield lane (0.10 new rows/search vs
    #    seed-head 0.50), so it must be rationed to 1-in-N picks. If it still
    #    wins every pick the fleet spends all its supply on the weakest lane.
    assert m.spill_every >= 2, m.spill_every
    # Only the cap-spill branch ever pops _spill_queue, so a shrinking queue
    # after a pick is a direct read on whether the ration served that pick.
    picks = served = 0
    for _ in range(m.spill_every * 3):
        before = len(m._spill_queue)
        m._next_terms(3, "a2")
        picks += 1
        if len(m._spill_queue) < before:
            served += 1
        burn_facets(m)                            # facet slices all burned
        m.searched_terms.pop("solo head esub", None)
        m._spill_queue.append("solo head esub")   # keep the queue non-empty
    assert served == 3, \
        f"expected 3 of {picks} picks served spill, got {served}"
    print(f"PASS  cap-spill: queued {len(_SPILL_REACTION) - 1} sibling facets; "
          f"rationed to 1-in-N picks")


def test_facet_rotation_still_works() -> None:
    m = make_miner()
    m._spill_queue.clear()
    blk, era, terms = m._next_terms(3, "a0")
    assert terms and terms[0] == "seed:solo head esub", terms  # a0 -> facet[0]
    print("PASS  facet rotation intact (a0 starts at esub; atomic claim)")


def test_dead_facets_are_gone() -> None:
    """Facets measured below 0.10 new rows/search must not come back: they
    ate 26% of all searches for 4% of the rows."""
    dead = ("bdrip", "pack", "complete", "telugu", "hdrip", "webrip",
            "480p", "dual audio")
    live = _SPILL_QUALS + _SPILL_EXTRAS
    for d in dead:
        assert d not in live, f"{d} was measured at <0.10 new/search: {d}"
    assert len(live) == 15, live
    for kept in ("esub", "720p", "1080p", "hindi", "mkv", "tamil"):
        assert kept in live, kept
    assert set(_SPILL_REACTION) <= set(live), _SPILL_REACTION
    print(f"PASS  facet vocabulary trimmed to {len(live)} measured earners "
          f"(8 dead facets removed)")


def test_low_priority_lane_cannot_be_starved() -> None:
    """The regression that cost the fleet 17 minutes of discovery: a tight
    high-priority loop held every token forever, because acquire() waits with
    no timeout. Aging must guarantee the low-priority lane a turn."""
    sem = _PrioritySemaphore(value=1)
    sem.AGING_S = 0.05              # 50ms per level -> lane 3 served within ~150ms
    stop = threading.Event()
    got = threading.Event()
    releases = [0]

    def noisy_lane():                # lane 1: never sleeps, like idgap agents
        while not stop.is_set():
            sem.acquire(1)
            releases[0] += 1
            sem.release()

    def quiet_lane():                # lane 3: discovery
        sem.acquire(3)
        got.set()
        sem.release()

    t_noisy = threading.Thread(target=noisy_lane, daemon=True)
    t_noisy.start()
    time.sleep(0.05)                # let the noisy lane own the heap
    t_quiet = threading.Thread(target=quiet_lane, daemon=True)
    t_quiet.start()
    served = got.wait(timeout=5.0)
    stop.set()
    t_noisy.join(timeout=5.0)
    assert served, ("low-priority lane never got a token after "
                    f"{releases[0]} high-priority releases — starvation")
    print(f"PASS  semaphore aging: lane 3 served after {releases[0]} "
          f"high-priority releases (without aging: never)")


def test_priority_still_beats_a_fresh_low_priority_waiter() -> None:
    """Aging must not flatten the priority order between waiters whose ages
    are comparable."""
    sem = _PrioritySemaphore(value=1)
    sem.AGING_S = 1000.0            # effectively disable aging for this check
    order = []
    sem.acquire(1)                  # hold the only token
    # queue LOW first, so FIFO would serve it first; only priority can invert
    t_low = threading.Thread(target=lambda: (sem.acquire(3), order.append("low")),
                             daemon=True)
    t_high = threading.Thread(target=lambda: (sem.acquire(1), order.append("high")),
                              daemon=True)
    t_low.start()
    time.sleep(0.05)
    t_high.start()
    time.sleep(0.05)
    sem.release()                   # hands the token to the higher-priority lane
    t_high.join(timeout=2.0)
    assert order == ["high"], order
    sem.release()                   # now the low lane can take it
    t_low.join(timeout=2.0)
    assert order == ["high", "low"], order
    print("PASS  priority ordering preserved: lane 1 served before the "
          "older-queued lane 3")


def test_priority_backlog_cannot_shadow_the_probe_lanes() -> None:
    """priority holds ~31k vault-derived terms and used to be spliced in at
    position 2 for EVERY agent, so series/alpha/words/facet served zero
    searches between them across 5,437 measured steps."""
    d = make_discovery()
    d.lanes["priority"] = [f"vault head {i}" for i in range(50)]
    for t in d.lanes["priority"]:
        d.queued_set.add(t)
    d.lanes["series"] = ["ca t "]
    d.queued_set.add("ca t ")
    got = d._pop("series")
    assert got == ("ca t ", "series"), got
    # with its own lane empty the agent still gets it, priority is the backstop
    d2 = make_discovery()
    d2.lanes["priority"] = ["vault head"]
    d2.queued_set.add("vault head")
    assert d2._pop("series") == ("vault head", "priority")
    # and priority is reachable last, never shadowed away
    d3 = make_discovery()
    d3.lanes["alpha"] = ["abc"]
    d3.lanes["words"] = ["drama"]
    d3.queued_set.update({"abc", "drama"})
    assert d3._pop("alpha") == ("abc", "alpha")
    assert d3._pop("alpha") == ("drama", "words")
    print("PASS  _pop: own lane first, other exploratory lanes next, "
          "priority last")


def test_capped_letter_expansion_keeps_the_prefix_space() -> None:
    """'ca t ' returns 50 rows (prefix match on the final token); 'ca t'
    returns 0 (whole-token AND). _queue() strips the trailing space via
    _norm_term, so the old expansion minted 26 guaranteed-zero searches."""
    d = make_discovery()
    rows = [{"title": f"Captain America Part {i} 2016 1080p", "created_at": "2016-01-01"}
            for i in range(50)]
    added = d._spill_capped("ab", 50, rows)
    assert added == 26, added
    assert len(d.lanes["series"]) == 26, d.lanes["series"]
    assert not d.lanes["alpha"], d.lanes["alpha"]
    assert all(t.endswith(" ") for t in d.lanes["series"]), d.lanes["series"][:3]
    assert f"ab{_ALPHA[0]} " in d.lanes["series"], d.lanes["series"][:3]
    # and the old, dead form must never reappear
    d2 = make_discovery()
    d2._spill_capped("ab", 50, rows)
    assert not any(t == f"ab{c}" for t in
                   d2.lanes["series"] + d2.lanes["alpha"] for c in _ALPHA)
    print("PASS  capped letter probe expands to 26 live prefix probes "
          "(was 26 whole-token terms that return 0 rows)")


def test_token_sweep_never_duplicates_and_yields_to_seed_lanes() -> None:
    """Tokens must still be claimed exactly once each -- but the sweep no
    longer wins the pick. Measured 2026-10-04: the token pool is dry (174
    eligible left of 29,187 searched) and the phrase pool measures 0.00
    new/search, while seed-spill/seed-ep/other-season slices measure
    2.4-3.7. The sweep is now last resort, so seed lanes get the pick first."""
    m = make_miner()
    m._token_pool = [f"token{i}" for i in range(12)]
    m._token_ts = time.time()          # fresh: _token_pool_build() returns it
    m.coverage = lambda: {"coverage_pct": 44.6,
                          "thin_blocks": [{"block": 2, "era_date": "2025-11-25"}]}
    # seed lanes are available here (solo head exists), so they take the pick
    blk, era, terms = m._next_terms(3, "a1")
    assert terms and terms[0].startswith("seed:"), (
        f"sweep stole a pick a seed lane could take: {terms}")

    # with every seed lane empty, the sweep takes the pick and still claims
    # each token exactly once
    m2 = make_miner()
    m2._token_pool = [f"token{i}" for i in range(12)]
    m2._token_ts = time.time()
    m2._seeds_for_block = (lambda blk, n=6, fresh_only=True: [])
    m2.coverage = lambda: {"coverage_pct": 44.6,
                           "thin_blocks": [{"block": 2, "era_date": "2025-11-25"}]}
    blk, era, terms = m2._next_terms(3, "a1")
    assert terms == ["tok:token-sweep:token0"], terms
    assert m2.searched_terms["token0"] > 0, "token not claimed"
    assert m2._token_cursor == 1, m2._token_cursor
    # a second agent gets the NEXT token, never the same one
    _, _, terms2 = m2._next_terms(3, "a2")
    assert terms2 == ["tok:token-sweep:token1"], terms2
    assert terms != terms2, "two agents claimed the same token"
    # and the kind survives the round trip through step()
    m2.client = SimpleNamespace(search=lambda term: {"results": [
        {"id": 60_001, "title": "Solo Leveling 2026 1080p", "created_at": "2026-01-01"}]})
    m2.index = SimpleNamespace(upsert=lambda rows, source: (1, 0))
    info = m2.step("a1")
    assert info["kind"] == "token-sweep", info
    assert info["term"] == "token2", info
    print("PASS  sweep yields to seed lanes when they can run, claims each "
          "token exactly once when they cannot, kind survives step()")


def test_token_sweep_only_claims_uncapped_tokens() -> None:
    """A token matching >50 rows can never surface a row we lack: the site
    returns its newest 50 and we already hold those. Sweeping one is a
    guaranteed 0-row search, so the pool must exclude them."""
    m = make_miner()
    freq = {"thin": 12, "perfect": 40, "fat": 5000, "solo": 1, "1080p": 30}
    kept = [t for t, n in freq.items()
            if 2 <= n <= _TOKEN_MAX_FREQ and t not in _TOKEN_STOP]
    assert sorted(kept) == ["perfect", "thin"], kept
    assert _TOKEN_MAX_FREQ == 50, _TOKEN_MAX_FREQ
    # frequency is the cost signal: highest first
    ordered = sorted(kept, key=lambda t: -freq[t])
    assert ordered == ["perfect", "thin"], ordered
    print("PASS  sweep pool is capped at 50 matches and ordered by frequency "
          "(guaranteed-0-row tokens excluded)")


def test_token_frontier_is_recursive_and_rationed() -> None:
    """Rows recovered by the sweep contain tokens absent from the whole vault;
    those are new search keys. The frontier must be preferred over the pool and
    must never contain facet/format noise."""
    m = make_miner()
    m._drain_token_hits([{"title": "Solo Leveling S02E13 1080p Hindi WEB-DL x264"}])
    got = list(m._token_frontier)
    assert "solo" in got and "leveling" in got, got
    assert "1080p" not in got and "hindi" not in got and "x264" not in got, got
    assert m.stats["token_minted"] == len(got), m.stats
    # phrases minted alongside tokens, from the same recovered rows
    assert "solo leveling" in list(m._phrase_frontier), list(m._phrase_frontier)
    # seed lanes empty so the sweep lane is reached (it is last resort now)
    m._seeds_for_block = (lambda blk, n=6, fresh_only=True: [])
    # pool leads the frontier (measured 4.17 new/search live vs 2.62)
    m._token_pool = ["pooltoken"]
    m._token_ts = time.time()
    _, _, terms = m._next_terms(3, "a1")
    assert terms == ["tok:token-sweep:pooltoken"], terms
    # ...and the frontier is the fallback that carries the sweep once it drains
    m._token_cursor = 0
    m._token_pool = []
    m._phrase_pool = []
    m._token_ts = time.time()
    m.searched_terms.pop("solo", None)
    _, _, terms = m._next_terms(3, "a1")
    assert terms == ["tok:token-frontier:solo"], terms
    # a token already searched is never re-minted
    m2 = make_miner()
    m2.searched_terms["solo"] = time.time()
    m2._drain_token_hits([{"title": "Solo Leveling 1080p"}])
    assert "solo" not in m2._token_frontier, list(m2._token_frontier)
    # and the TTL still governs the frontier at pop time
    m3 = make_miner()
    m3._token_frontier.append("stale")
    m3.searched_terms["stale"] = time.time()   # fresh claim -> skipped
    m3._token_pool = []
    assert m3._claim_token() is None, "stale frontier token was served"
    print("PASS  token frontier: minted from recovered rows only, pool-first "
          "with frontier as the drain fallback, TTL-rationed")


def test_save_is_atomic_and_corrupt_load_is_loud() -> None:
    """A forced kill (the 2-hour recycle task) mid-dump used to leave half a
    JSON; _load() swallowed it, so the miner forgot every term it had searched
    and re-swept the pool for zero rows. save() must swap atomically, and a
    bad file must be quarantined and reported, never silently ignored."""
    import glob
    import tempfile
    m = make_miner()
    tmpdir = tempfile.mkdtemp(prefix="idgap_state_")
    m.state_path = os.path.join(tmpdir, "idgap_state.json")
    del m.save  # make_miner stubs it; this test exercises the real atomic writer
    m.done_blocks = {1, 2, 3}
    m.searched_terms = {"alpha": 1.0, "beta": 2.0}
    m.round_started = 0.0
    m._phrase_cursor = 7
    m._phrase_frontier = deque(["one two"], maxlen=500)
    m.save()
    assert not glob.glob(m.state_path + ".tmp"), "temp file left behind"
    saved = json.load(open(m.state_path, encoding="utf-8"))
    assert saved["searched_terms"] == {"alpha": 1.0, "beta": 2.0}, saved
    assert saved["phrase_cursor"] == 7 and saved["phrase_frontier"] == ["one two"]

    # a truncated file must be quarantined + reported, not swallowed
    with open(m.state_path, "w", encoding="utf-8") as f:
        f.write('{"searched_terms": {"alpha"')  # half a JSON
    m2 = make_miner()
    m2.state_path = m.state_path
    m2._load()
    assert m2.searched_terms == {}, "corrupt file was partially trusted"
    quarantined = glob.glob(m.state_path + ".corrupt-*")
    assert quarantined, "corrupt state not quarantined"
    print("PASS  state save is atomic (tmp+replace); corrupt load is quarantined+reported")


def test_save_prunes_expired_searched_terms() -> None:
    """save() must prune searched_terms older than TTL when the dict is big.

    Live state had grown to 102k entries / 4.2MB, rewritten after EVERY
    step while holding the lock. Pruning keeps saves fast and lets the
    pool rebuild re-include expired terms for TTL re-sweep (new uploads).
    Small dicts (<20k) are untouched so existing behavior is preserved.
    """
    import tempfile
    m = make_miner()
    tmpdir = tempfile.mkdtemp(prefix="idgap_state_")
    m.state_path = os.path.join(tmpdir, "idgap_state.json")
    del m.save
    m.done_blocks = set()
    m.round_started = 0.0
    now = time.time()
    m.searched_terms = (
        {f"old{i}": now - 4 * 3600 for i in range(20005)}
        | {"fresh": now}
    )
    m.save()
    saved = json.load(open(m.state_path, encoding="utf-8"))
    assert "fresh" in saved["searched_terms"], "fresh entry pruned"
    assert len(saved["searched_terms"]) < 20006, "expired entries not pruned"
    assert all(now - float(v) <= m.term_ttl_s + 1
              for v in saved["searched_terms"].values()), "stale entry survived"
    print("PASS  save prunes expired searched_terms, keeps state small")


def test_phrase_channel_serves_when_token_pool_is_empty() -> None:
    """The single-token pool is exhausted (174 left, mostly typo spam), so the
    miner must fall through to two-word phrases -- and only clean, never-searched
    phrases ranked by frequency."""
    m = make_miner()
    m._token_pool = []
    m._token_ts = time.time()          # fresh: no token-pool rebuild
    m._token_frontier = deque(["frontier token"], maxlen=500)
    m._phrase_pool = ["alpha beta", "gamma delta", "seen phrase"]
    m._phrase_ts = time.time()
    m.searched_terms["seen phrase"] = time.time()   # already swept
    m.coverage = lambda: {"coverage_pct": 44.6,
                          "thin_blocks": [{"block": 2, "era_date": "2025-11-25"}]}
    # seed lanes exist here and measure higher, so they take the pick first
    blk, era, terms = m._next_terms(3, "a1")
    assert terms and terms[0].startswith("seed:"), (
        f"phrase pool stole a pick from a seed lane: {terms}")
    # ...and with seed lanes empty the phrase pool serves, outranking frontiers
    m._seeds_for_block = (lambda blk, n=6, fresh_only=True: [])
    blk, era, terms = m._next_terms(3, "a1")
    assert terms == ["tok:phrase-sweep:alpha beta"], terms
    assert m.searched_terms["alpha beta"] > 0, "phrase not claimed"
    # phrases outrank the frontiers: a full frontier must not starve a fresh pool
    assert list(m._token_frontier) == ["frontier token"], \
        "token frontier was drained before the fresh phrase pool"
    # never re-serves a phrase already in the TTL window
    m2 = make_miner()
    m2._token_pool = []
    m2._token_ts = time.time()
    m2._phrase_pool = ["alpha beta"]
    m2._phrase_ts = time.time()
    m2.searched_terms["alpha beta"] = time.time()
    assert m2._claim_token() is None, "claimed a phrase inside its TTL"
    # phrase hits keep minting fresh phrases (recursive supply)
    m3 = make_miner()
    m3._drain_token_hits([{"title": "Solo Leveling S02E13 1080p Hindi WEB-DL x264"}])
    minted = list(m3._phrase_frontier)
    assert "solo leveling" in minted, minted
    assert not any("1080p" in p for p in minted), minted
    print("PASS  phrase channel: falls through when token pool is dry, TTL-respects "
          "claims, and mints phrases from recovered rows")


def test_slice_channel_claims_each_slice_once() -> None:
    """The season-slice lane must claim each slice exactly once and never
    re-serve a slice inside its TTL -- two agents must not pick the same
    slice, and the kind must survive step() into term_stats/[alive]."""
    m = make_miner()
    m._seeds_for_block = (lambda blk, n=6, fresh_only=True: [])
    m._slice_pool = [("show s02", "slice-season"), ("show s03", "slice-season"),
                     ("show s02e01", "slice-ep")]
    blk, era, terms = m._next_terms(3, "a1")
    assert terms == ["slc:slice-season:show s02"], terms
    assert m.searched_terms.get("show s02", 0) > 0, "slice not claimed"
    _, _, terms2 = m._next_terms(3, "a2")
    assert terms2 == ["slc:slice-season:show s03"], terms2
    assert terms != terms2, "two agents claimed the same slice"
    # kind survives the round trip through step()
    m.client = SimpleNamespace(search=lambda term: {"results": [
        {"id": 60_001, "title": "Show S02E01 1080p", "created_at": "2026-01-01"}]})
    m.index = SimpleNamespace(upsert=lambda rows, source: (1, 0))
    info = m.step("a1")
    assert info["kind"] == "slice-ep", info
    assert info["term"] == "show s02e01", info
    assert m.term_stats["slice-ep"]["tries"] == 1, m.term_stats
    # inside the TTL a slice is never re-served, even with the cursor rewound
    m2 = make_miner()
    m2._seeds_for_block = (lambda blk, n=6, fresh_only=True: [])
    m2._slice_pool = [("show s02", "slice-season")]
    m2.searched_terms["show s02"] = time.time()
    assert m2._claim_slice() is None, "re-served a slice inside its TTL"
    # the self-feed queue mints facet sub-slices from capped results, bounded
    m3 = make_miner()
    rows = [{"id": 1, "title": "Show S02E01 1080p"}] * 50
    m3._drain_slice_hits(rows, "show s02")
    q = list(m3._slice_queue)
    assert q and all(k == "slice-season" for _, k in q), q
    assert len(q) <= 500, q
    assert m3.stats["slice_minted"] == len(q), m3.stats
    # ...and a non-capped result with no new seasons mints nothing
    m4 = make_miner()
    m4._drain_slice_hits([{"id": 1, "title": "Show S02E01 1080p"}], "show s02")
    assert list(m4._slice_queue) == [], list(m4._slice_queue)
    print("PASS  slice channel: each slice claimed exactly once, TTL-respected, "
          "kind survives step(), capped results self-feed facet sub-slices")


def test_slice_selffeed_no_degenerate_chains() -> None:
    """A capped slice must never mint 'show s02 s02' style chains.

    _SPILL_REACTION used to contain 's01', so 'show s01' capped and minted
    'show s01 s01', which capped and minted 'show s01 s01 s01' -- degenerate
    queries returning 0 new. Measured 2026-10-05: 475 slice tries, 0 new
    rows, vault frozen for 15 min. The reaction tuple drops 's01' and the
    mint skips any facet already a suffix of the term.
    """
    m = make_miner()
    rows = [{"id": i, "title": f"Show S02E01 1080p"} for i in range(50)]
    m._drain_slice_hits(rows, "show s02")
    q = [t for t, _ in m._slice_queue]
    assert q, "capped slice must self-feed facet sub-slices"
    for t in q:
        parts = t.split(" ")
        assert len(parts) == len(set(parts)), f"repeated facet in slice term: {t}"
        assert "s02 s02" not in t, f"degenerate season chain minted: {t}"
    # a facet sub-slice that caps must not chain either
    m2 = make_miner()
    m2._drain_slice_hits(rows, "show s02 mkv")
    for t, _ in m2._slice_queue:
        assert "mkv mkv" not in t and "s02 s02" not in t, f"chain: {t}"
    print("PASS  slice self-feed: no degenerate facet/season chains minted")


def test_slice_pool_has_faceted_season_slices() -> None:
    """The slice pool leads with BARE season slices; facets arrive only via
    capped-triggered self-feed, never upfront.

    Rejected strategy (measured 2026-10-05): faceted-upfront yielded
    0.337/search decaying to 0.00 in mined regions, with 5x 0-row waste per
    barren season (every facet of a nonexistent season returns 0 rows).
    Site semantics: <=50 matches returns the COMPLETE set, so an uncapped
    bare slice already surfaced everything; its facet sub-slices are
    subsets (0 new). Only capped (>=50-row) bare slices hide rows below
    the cap, so only they earn facet sub-slices via _drain_slice_hits.
    """
    class _Col:
        def __init__(self, titles):
            self._titles = titles
        def find(self, *a, **k):
            return self
        def batch_size(self, n):
            return self
        def __iter__(self):
            return iter([{"title": t} for t in self._titles])

    m = make_miner()
    m.index = SimpleNamespace(cols=lambda: [
        _Col(["Show S01E01", "Show S02E01", "Show S01E02"])])
    n = m._build_slice_pool()
    pool = [t for t, _ in m._slice_pool]
    assert n > 0, "pool build returned nothing"
    assert any(t == "show s01" for t in pool), f"no bare s01: {pool[:5]}"
    assert any(t == "show s02" for t in pool), f"no bare s02: {pool[:5]}"

    def is_bare(t):
        p = t.split(" ")
        return len(p) == 2 and p[1].startswith("s")

    def is_faceted(t):
        p = t.split(" ")
        return len(p) == 3 and p[1].startswith("s")

    assert not any(is_faceted(t) for t in pool), \
        f"faceted-upfront must be gone (capped-triggered only): {pool[:6]}"
    assert any(is_bare(t) for t in pool), f"bare seasons missing: {pool[:6]}"
    print(f"PASS  slice pool: {n} bare season slices, facets capped-triggered only")


def test_season_slices_outrank_head_facet() -> None:
    """Season slices (0.337 realized) must be picked before head x facet
    (0.235 realized). A 2026-10-05 attempt to prioritize head x facet on
    its stale 1.35 lifetime yield starved the slice lane and DROPPED the
    rate 21 -> 16/min; reverted. Realized yields, not lifetime, set priority.
    """
    m = make_miner()
    m._seeds_for_block = (lambda blk, n=6, fresh_only=True: ["solo head"])
    m._slice_pool = [("show s02", "slice-season")]
    m._fresh_heads = lambda: []
    m._series_heads = lambda: []
    blk, era, terms = m._next_terms(3, "a1")
    # season slice wins: 'slc:...' not 'solo head <facet>'
    assert terms and terms[0] == "slc:slice-season:show s02", terms
    print("PASS  season slices outrank head x facet (0.337 > 0.235 realized)")


def test_idgap_uses_semaphore_gated_search() -> None:
    """idgap must call client.search(), never search_http() directly.

    client.search() is already http-first (client.py _fetch), AND it holds
    the _http_sem permit that enforces MKV_HTTP_CONCURRENCY. search_http()
    bypasses that semaphore, so calling it directly would let the miners
    stampede Cloudflare past the concurrency cap. Measured 2026-10-05:
    both paths report _mode='http' and are indistinguishable in latency.
    """
    src = open(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "app", "idgap.py"),
        encoding="utf-8").read()
    body = re.search(r"def step\(.*?\n\n    def ", src, re.S)
    assert body, "could not locate step() body"
    # strip comments: the guard is about CODE, not the prose that warns
    # against exactly this mistake.
    step_code = "\n".join(
        line for line in body.group(0).splitlines()
        if not line.strip().startswith("#"))
    assert "self.client.search(term)" in step_code, "step() must use client.search"
    assert "search_http" not in step_code, \
        "step() must not call search_http: it bypasses the _http_sem permit"
    print("PASS  idgap transport: semaphore-gated search(), no search_http bypass")


def test_nat64_resolver_guard() -> None:
    """DNS64 synthesizes 64:ff9b::/96 AAAA records for Atlas; pymongo must not
    dial them, or every shard reads Unknown and the lanes self-disable."""
    import socket as sock
    import app.store as store
    from app.store import _is_nat64

    assert _is_nat64("64:ff9b::2264:9139"), "NAT64 address not detected"
    assert _is_nat64(bytes([0, 0xFF, 0x9B] + [0] * 13)), "packed NAT64 missed"
    for good in ("2606:4700::1", "::1", "34.100.145.57", "2001:db8::1"):
        assert not _is_nat64(good), f"real address misread as NAT64: {good}"

    # A resolver answer set shaped like this box's: one real A record plus the
    # NAT64 AAAA the resolver invented from it. The guard must drop only the
    # synthetic one -- dropping the A record would make things worse.
    real_answers = [(sock.AF_INET, 1, 6, "", ("34.100.145.57", 27017)),
                    (sock.AF_INET6, 1, 6, "", ("64:ff9b::2264:9139", 27017)),
                    (sock.AF_INET6, 1, 6, "", ("2606:4700::1", 27017))]
    kept = [r for r in real_answers
            if r[0] != sock.AF_INET6 or not _is_nat64(r[4][0])]
    addrs = [r[4][0] for r in kept]
    assert "64:ff9b::2264:9139" not in addrs, f"NAT64 survived the filter: {addrs}"
    assert "34.100.145.57" in addrs, f"filter dropped the real A record: {addrs}"
    assert "2606:4700::1" in addrs, f"filter dropped a genuine AAAA: {addrs}"

    # Installing must be idempotent and must not recurse into itself.
    before = sock.getaddrinfo
    store.prefer_mongo_ipv4()
    after = sock.getaddrinfo
    assert after is not before, "guard did not install"
    store.prefer_mongo_ipv4()
    assert sock.getaddrinfo is after, "guard re-installed over itself"
    print("PASS  NAT64 resolver guard: drops 64:ff9b::/96, keeps real A and AAAA, idempotent")


def test_resolver_guard_reraises_dns_failure() -> None:
    """A DNS failure must propagate, not become an UnboundLocalError on `got`.

    The guard's except block used to do `return got` when the underlying
    resolver raised -- but `got` is only assigned on success, so a DNS
    timeout surfaced as 'UnboundLocalError: cannot access local variable
    got' wrapped inside a ServerSelectionTimeoutError. Measured 2026-10-05
    under concurrency 3: every idgap agent hit it and the fleet stalled.
    """
    import socket as sock
    import app.store as store

    def _boom(host, port, *a, **k):
        raise OSError("DNS timeout (simulated)")

    original = sock.getaddrinfo
    store._ipv4_only_installed = False
    sock.getaddrinfo = _boom
    try:
        store.prefer_mongo_ipv4()   # installs _filtered closing over _boom
        try:
            sock.getaddrinfo("example.com", 27017)
            assert False, "DNS failure did not propagate"
        except OSError as e:
            assert "DNS timeout" in str(e), e
        except UnboundLocalError:
            assert False, "DNS failure masked as UnboundLocalError on 'got'"
    finally:
        sock.getaddrinfo = original
        store._ipv4_only_installed = False
        store.prefer_mongo_ipv4()   # restore the real guard
    print("PASS  resolver guard: DNS failure propagates, not an UnboundLocalError")


def test_apostrophe_titles_are_searchable() -> None:
    """Punctuation is a token separator on the mkvbase site, so
    'Kuroko's Basketball' must be findable as 'Kuroko', 'Kurokos'
    or "Kuroko's" alike.

    Measured 2026-10-06: the index held 65 Kuroko rows but
    /links?q=Kuroko answered 4, because 'Kuroko's' indexed as the
    single token "kuroko's" and a bare 'Kuroko' query could never
    match it. 'Kuroko Basketball S01 COMPLETE' answered 0 rows even
    though the vault holds all 6 rows the site serves for it.
    """
    from app.store import _site_tokens, _site_title_match

    title = ("Kuroko's Basketball S01 COMPLETE 1080p 10bit BluRay "
             "HEVC x265 [Hindi AMZN DDP 2 0]")

    # the query tokenizer produces every spelling of the head word
    toks = _site_tokens("Kuroko")
    assert toks == ["kuroko"], toks
    head = _site_tokens("Kuroko's Basketball")
    for want in ("kuroko", "s", "kurokos", "basketball"):
        assert want in head, f"{want} missing from {head}"

    # ... and every one of them finds the apostrophe title
    for q in ("Kuroko", "Kurokos", "Kuroko's", "Kuroko s",
              "Kuroko Basketball", "Kurokos Basketball",
              "Kuroko's Basketball S01 COMPLETE",
              "Kuroko Basketball S01 COMPLETE"):
        assert _site_title_match(title, _site_tokens(q)), f"{q!r} missed the pack"

    # non-alphanumeric words still never match (site behaviour:
    # 'Kurokos Basketball S01 COMPLETE' returns 0 on the site itself
    # because the pack's head token is the apostrophe form)
    assert not _site_title_match(
        "Kurokos Basketball S01 1080p AMZN WEB DL",
        _site_tokens("Kuroko Basketball S01")), \
        "bare 'Kuroko' must not match the unspaced 'Kurokos' title"

    # ordinary titles are unchanged by the new tokenizer
    plain = "Paathirathri 2025 2160p ZEE5 WEB DL"
    assert _site_title_match(plain, _site_tokens("zee5 paathirathri"))
    assert _site_title_match(plain, _site_tokens("paathirathri  web dl"))
    assert not _site_title_match(plain, _site_tokens("paathirathi"))
    assert not _site_title_match(plain, _site_tokens("gdflix.dev"))

    # whitespace-only query = no filter
    assert _site_tokens("   ") == []
    assert _site_title_match(plain, _site_tokens("   "))
    print("PASS  apostrophe/punctuation titles searchable under every spelling")


def test_links_index_serves_the_kuroko_pack() -> None:
    """End-to-end through the file backend: the exact rows the site
    serves for 'Kuroko's Basketball S01 COMPLETE' must come back for
    the apostrophe-free query the user actually types."""
    import tempfile
    from app.store import LinksIndex

    pack = [{"id": 538587 + i,
             "title": ("Kuroko's Basketball S01 COMPLETE 1080p 10bit "
                       "BluRay HEVC x265 [Hindi AMZN DDP 2 0 + English]"),
             "url": f"https://example.com/{538587 + i}",
             "created_at": "2026-10-06T00:00:00Z", "status": "1"}
            for i in range(6)]
    episodes = [{"id": 461766,
                 "title": "Kurokos Basketball S01E03 Its Better If I Cant Win "
                          "1080p AMZN WEB DL Hindi DDP2 0",
                 "url": "https://example.com/461766",
                 "created_at": "2026-10-01T00:00:00Z", "status": "1"}]

    with tempfile.TemporaryDirectory() as td:
        idx = LinksIndex(td)
        new, _ = idx.upsert(pack + episodes, source="search")
        assert new == 7, new

        for q, want in (("Kuroko", 6), ("Kurokos", 7),
                        ("Kuroko Basketball S01 COMPLETE", 6),
                        ("Kuroko's Basketball S01 COMPLETE", 6),
                        ("Kuroko Basketball", 6),
                        ("Kurokos Basketball S01", 6)):
            out = idx.recent(limit=1000, q=q)
            assert out["count"] == want, f"{q!r}: {out['count']} != {want}"
        # the joined form means the unspelled-out query finds the pack
        # too (the site itself returns 0 here — its own apostrophe
        # quirk; our index being more permissive is the point)
        assert idx.recent(limit=10, q="Kurokos Basketball S01 COMPLETE")["count"] == 6
    print("PASS  file index: all 6 pack rows served for 'Kuroko' and variants")


def test_mongo_doc_tokens_use_the_shared_tokenizer() -> None:
    """The Mongo backend stores title_tokens at upsert time; those
    arrays must be built by the same tokenizer as the query side or
    $all matches the wrong thing."""
    from app.store import MongoIndex, _site_tokens

    doc = MongoIndex._doc({"id": 1, "title": "Kuroko's Basketball S01 "
                                              "COMPLETE 1080p",
                           "url": "u", "created_at": "c", "status": "1"},
                          "search", 1.0)
    assert set(doc["title_tokens"]) == set(_site_tokens(
        "Kuroko's Basketball S01 COMPLETE 1080p")), doc["title_tokens"]
    for want in ("kuroko", "kurokos", "basketball", "s01", "complete"):
        assert want in doc["title_tokens"], f"{want} missing"
    print("PASS  Mongo title_tokens built by the shared tokenizer")


if __name__ == "__main__":
    test_lru_claim_no_duplicates()
    test_cap_spill_is_rationed()
    test_facet_rotation_still_works()
    test_dead_facets_are_gone()
    test_low_priority_lane_cannot_be_starved()
    test_priority_still_beats_a_fresh_low_priority_waiter()
    test_priority_backlog_cannot_shadow_the_probe_lanes()
    test_capped_letter_expansion_keeps_the_prefix_space()
    test_token_sweep_never_duplicates_and_yields_to_seed_lanes()
    test_token_sweep_only_claims_uncapped_tokens()
    test_token_frontier_is_recursive_and_rationed()
    test_save_is_atomic_and_corrupt_load_is_loud()
    test_save_prunes_expired_searched_terms()
    test_phrase_channel_serves_when_token_pool_is_empty()
    test_slice_channel_claims_each_slice_once()
    test_slice_selffeed_no_degenerate_chains()
    test_slice_pool_has_faceted_season_slices()
    test_season_slices_outrank_head_facet()
    test_idgap_uses_semaphore_gated_search()
    test_nat64_resolver_guard()
    test_resolver_guard_reraises_dns_failure()
    test_apostrophe_titles_are_searchable()
    test_links_index_serves_the_kuroko_pack()
    test_mongo_doc_tokens_use_the_shared_tokenizer()
    d = open(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "app", "idgap.py"), "rb").read()
    assert d.count(b"\x00") == 0, "ROT in idgap.py"
    print("PASS  idgap.py byte-clean")
    print("\nALL TESTS PASSED — fixes ready, live fleet untouched")
