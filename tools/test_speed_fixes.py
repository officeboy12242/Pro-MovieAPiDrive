#!/usr/bin/env python
"""OFFLINE unit tests for the crawl-speed fixes: the LRU-valve claim and
cap-spill reaction, HTTP-slot anti-starvation, the trimmed facet
vocabulary, and discovery lane selection. No network, no Mongo, no effect
on the running fleet.
Run: .venv/Scripts/python.exe tools/test_speed_fixes.py"""
from __future__ import annotations

import os
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
    assert "solo head esub" in m._spill_queue and "solo head s01" in m._spill_queue
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


def test_token_sweep_is_served_and_never_duplicates() -> None:
    """The token sweep is the highest-yield lane ever measured on this site
    (7.1-10.0 new rows/search vs 0.02-0.27 for everything else), so it must win
    its pick -- and two agents must never claim the same token."""
    m = make_miner()
    m._token_pool = [f"token{i}" for i in range(12)]
    m._token_ts = time.time()          # fresh: _token_pool_build() returns it
    m.coverage = lambda: {"coverage_pct": 44.6,
                          "thin_blocks": [{"block": 2, "era_date": "2025-11-25"}]}
    blk, era, terms = m._next_terms(3, "a1")
    assert terms == ["tok:token-sweep:token0"], terms
    assert m.searched_terms["token0"] > 0, "token not claimed"
    assert m._token_cursor == 1, m._token_cursor
    # a second agent gets the NEXT token, never the same one
    _, _, terms2 = m._next_terms(3, "a2")
    assert terms2 == ["tok:token-sweep:token1"], terms2
    assert terms != terms2, "two agents claimed the same token"
    # and the kind survives the round trip through step()
    m.client = SimpleNamespace(search=lambda term: {"results": [
        {"id": 60_001, "title": "Solo Leveling 2026 1080p", "created_at": "2026-01-01"}]})
    m.index = SimpleNamespace(upsert=lambda rows, source: (1, 0))
    info = m.step("a1")
    assert info["kind"] == "token-sweep", info
    assert info["term"] == "token2", info
    print("PASS  token sweep served on its pick, tokens claimed exactly once, "
          "kind survives step()")


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
    # pool leads (measured 4.17 new/search live vs 2.62 for the frontier)
    m._token_pool = ["pooltoken"]
    m._token_ts = time.time()
    _, _, terms = m._next_terms(3, "a1")
    assert terms == ["tok:token-sweep:pooltoken"], terms
    # ...and the frontier is the fallback that carries the sweep once it drains
    m._token_cursor = 0
    m._token_pool = []
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


if __name__ == "__main__":
    test_lru_claim_no_duplicates()
    test_cap_spill_is_rationed()
    test_facet_rotation_still_works()
    test_dead_facets_are_gone()
    test_low_priority_lane_cannot_be_starved()
    test_priority_still_beats_a_fresh_low_priority_waiter()
    test_priority_backlog_cannot_shadow_the_probe_lanes()
    test_capped_letter_expansion_keeps_the_prefix_space()
    test_token_sweep_is_served_and_never_duplicates()
    test_token_sweep_only_claims_uncapped_tokens()
    test_token_frontier_is_recursive_and_rationed()
    test_nat64_resolver_guard()
    d = open(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "app", "idgap.py"), "rb").read()
    assert d.count(b"\x00") == 0, "ROT in idgap.py"
    print("PASS  idgap.py byte-clean")
    print("\nALL TESTS PASSED — fixes ready, live fleet untouched")
