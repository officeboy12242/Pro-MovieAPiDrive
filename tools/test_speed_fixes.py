#!/usr/bin/env python
"""OFFLINE unit tests for the two speed fixes (LRU-valve claim +
cap-spill reaction). No network, no Mongo, no effect on the running
fleet. Run: .venv/Scripts/python.exe tools/test_speed_fixes.py"""
from __future__ import annotations

import os
import sys
import threading
import time
from collections import deque
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.idgap import (  # noqa: E402
    IdGapMiner, _SPILL_EXTRAS, _SPILL_QUALS, _SPILL_REACTION)
from app.client import _PrioritySemaphore  # noqa: E402


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


if __name__ == "__main__":
    test_lru_claim_no_duplicates()
    test_cap_spill_is_rationed()
    test_facet_rotation_still_works()
    test_dead_facets_are_gone()
    test_low_priority_lane_cannot_be_starved()
    test_priority_still_beats_a_fresh_low_priority_waiter()
    d = open(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "app", "idgap.py"), "rb").read()
    assert d.count(b"\x00") == 0, "ROT in idgap.py"
    print("PASS  idgap.py byte-clean")
    print("\nALL TESTS PASSED — fixes ready, live fleet untouched")
