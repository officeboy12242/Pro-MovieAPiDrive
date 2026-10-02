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

from app.idgap import IdGapMiner, _SPILL_QUALS  # noqa: E402


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
    for f in _SPILL_QUALS + ("zip", "s01", "e01", "hindi", "mkv",
                             "dual audio", "tamil", "complete", "pack",
                             "telugu"):
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
    assert len(m._spill_queue) == 7, list(m._spill_queue)
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
    print("PASS  cap-spill: queued 7 sibling facets; rationed to 1-in-N picks")


def test_facet_rotation_still_works() -> None:
    m = make_miner()
    m._spill_queue.clear()
    blk, era, terms = m._next_terms(3, "a0")
    assert terms and terms[0] == "seed:solo head esub", terms  # a0 -> facet[0]
    print("PASS  facet rotation intact (a0 starts at esub; atomic claim)")


if __name__ == "__main__":
    test_lru_claim_no_duplicates()
    test_cap_spill_is_rationed()
    test_facet_rotation_still_works()
    d = open(os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "app", "idgap.py"), "rb").read()
    assert d.count(b"\x00") == 0, "ROT in idgap.py"
    print("PASS  idgap.py byte-clean")
    print("\nALL TESTS PASSED — fixes ready, live fleet untouched")
