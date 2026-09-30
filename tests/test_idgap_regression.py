"""Regression tests for the idgap miner's term-picking paths.

Fleet incident 2026-09-30: a running pusher (stale code in memory) logged
  [idgap:*] error ZeroDivisionError: integer modulo by zero
  [idgap:*] error NameError: name 'min_gap' is not defined
every 30s once all nearby seeds were searched/claimed. These tests drive the
SAME _next_terms()/step() code the fleet runs, against a fake index + client,
so every fallback path is exercised with no network and no Mongo:

  1. fresh seeds          -> direct pick, claim stamped
  2. all seeds searched   -> LRU stale-seed fallback (the min_gap path)
  3. everything inside    -> era-terms fallback for the thinnest block
     ttl + min_gap
  4. 12 concurrent agents -> 12 DISTINCT picks (no duplicate search)
  5. step() success       -> 90s claim upgraded to a real timestamp
  6. block size env       -> never zero (modulo guard)

Run:  .venv\\Scripts\\python tests\\test_idgap_regression.py
   or pytest tests\\test_idgap_regression.py
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.idgap import IdGapMiner  # noqa: E402

BLOCK = 25000


class FakeIndex:
    """Shard-API-compatible stand-in for MongoIndex (what coverage() uses)."""

    def __init__(self, rows):
        self._rows = list(rows)
        self._col = self  # idgap's coverage() reads index._col

    # --- store-like surface -------------------------------------------------
    def upsert(self, rows, source=""):
        known = {r["id"] for r in self._rows}
        new = sum(1 for r in rows if r["id"] not in known)
        have = {r["id"]: r for r in self._rows}
        for r in rows:
            have.setdefault(r["id"], r)
        self._rows = list(have.values())
        return new, 0

    def stats(self):
        return {"backend": "mongodb", "rows": len(self._rows)}

    # --- shard-aware surface used by coverage() ------------------------------
    def cols(self):
        return [self]

    def max_id(self):
        return max((r["id"] for r in self._rows), default=0)

    def aggregate(self, pipeline):
        out: dict[int, dict] = {}
        for r in self._rows:
            if r.get("id") is None:
                continue
            blk = r["id"] // BLOCK
            d = str(r.get("created_at") or "")[:10]
            cur = out.get(blk)
            if cur is None:
                out[blk] = {"_id": blk, "n": 1, "d": d}
            else:
                cur["n"] += 1
                if d and d > str(cur.get("d") or "")[:10]:
                    cur["d"] = d
        return iter(list(out.values()))

    def find(self, query, projection=None):
        gte = query["id"]["$gte"]
        lt = query["id"]["$lt"]
        rows = [r for r in self._rows if gte <= r["id"] < lt]
        return _FakeCursor(rows)


class _FakeCursor:
    """Mimics pymongo's chainable cursor: idgap calls .sort().limit() on it.
    (Returning a bare iterator broke under 12 concurrent cold-cache threads.)"""

    def __init__(self, rows):
        self._rows = list(rows)

    def sort(self, key, direction):
        self._rows.sort(key=lambda r: r[key], reverse=direction < 0)
        return self

    def limit(self, n):
        self._rows = self._rows[:n]
        return self

    def __iter__(self):
        return iter(self._rows)


class FakeClient:
    def __init__(self, results=None, fail=False):
        self.results = results if results is not None else []
        self.fail = fail
        self.searched: list[str] = []

    def search(self, term):
        self.searched.append(term)
        if self.fail:
            raise RuntimeError("boom")
        return {"results": self.results}


def _mk_rows():
    """Two thin blocks: ids 1..8 (era 2025-11-25) and 25001..25008
    (era 2026-02-22). Titles yield unique heads like 'paathirathri aa3'
    (the alpha keeps the head 2+ words after the len-1 digit is filtered;
    the two blocks use different title words so 16 unique seeds exist)."""
    rows = []
    for i in range(1, 9):
        rows.append({"id": i, "title": f"kaali khuhi aa{i} 2025 1080p zee5 web dl",
                     "created_at": "2025-11-25T09:00:00"})
        rows.append({"id": BLOCK + i,
                     "title": f"paathirathri aa{i} 2026 1080p zee5 web dl",
                     "created_at": "2026-02-22T09:00:00"})
    return rows


def _miner(rows=None, **env):
    old = {k: os.environ.get(k) for k in
           ("MKV_IDGAP_BLOCK", "MKV_IDGAP_SEED_MIN_GAP_S", "MKV_IDGAP_TERM_TTL_S",
            "MKV_IDGAP_CLAIM_S")}
    os.environ["MKV_IDGAP_BLOCK"] = str(BLOCK)
    for k, v in env.items():
        os.environ[k] = str(v)
    try:
        tmp = tempfile.mkdtemp(prefix="idgap_test_")
        m = IdGapMiner(FakeClient(), FakeIndex(rows if rows is not None else _mk_rows()),
                       tmp)
        # isolation from any real data/idgap_state.json on this machine
        m.searched_terms, m.done_blocks, m.term_stats = {}, set(), {}
        m.stats.update({"rounds": 0, "terms_done": 0, "rows_new": 0})
        return m
    finally:
        for k, v in old.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _heads(miner, blk):
    return list(miner._seeds_for_block(blk))


def test_fresh_seeds_pick_and_claim():
    m = _miner()
    now = time.time()
    blk, era, terms = m._next_terms(3, "a1")
    assert blk == 1, f"thinnest-first should pick block 1, got {blk}"
    # _era_date deliberately dates a block from its NEAREST NEIGHBOR block
    # (never itself), so block 1 inherits block 0's 2025-11-25 era.
    assert era == "2025-11-25", f"neighbor-era semantics changed? got {era}"
    assert terms and terms[0].startswith("seed:")
    head = terms[0][5:]
    stamp = m.searched_terms[head]
    assert stamp > now, "pick must be claim-stamped into the future"
    print(f"  ok fresh pick {head!r} blk={blk} era={era}")


def test_exhausted_seeds_use_lru_fallback_no_nameerror():
    """THE crash path: every seed searched/claimed -> stale-seed LRU valve.
    Old code died here with NameError: name 'min_gap' is not defined."""
    m = _miner(MKV_IDGAP_SEED_MIN_GAP_S=1200)
    now = time.time()
    for h in _heads(m, 0):
        m.searched_terms[h] = now - 4000  # stale enough (> min_gap)
    for h in _heads(m, 1):
        m.searched_terms[h] = now - 5000  # blk1 stalest -> LRU must pick it
    blk, era, terms = m._next_terms(3, "a1")
    assert blk is not None and terms, "LRU fallback must yield a pick"
    assert terms[0].startswith("seed:")
    head = terms[0][5:]
    assert m.searched_terms[head] > now, "fallback pick must be claimed too"
    assert blk == 1, "globally stalest seed lives in block 1 in this fixture"
    print(f"  ok LRU fallback {head!r} blk={blk} (no NameError)")


def test_all_within_min_gap_falls_back_to_era_terms():
    """Everything searched recently (< ttl AND < min_gap) -> era-term pool.
    Second leg of the same incident crash-loop."""
    m = _miner()
    now = time.time()
    for blk in (0, 1):
        for h in _heads(m, blk):
            m.searched_terms[h] = now - 500  # inside both gates
    blk, era, terms = m._next_terms(3, "a1")
    assert terms, "era-terms fallback must yield terms"
    assert not terms[0].startswith("seed:")
    assert blk == 1
    for t in terms:
        assert len(t) >= 4, f"suspicious era term {t!r}"
    print(f"  ok era fallback {terms} blk={blk} era={era}")


def test_12_agents_never_pick_the_same_term():
    """12 concurrent agents, 16 seeds: every pick must be distinct."""
    m = _miner()
    picks: list[str] = []
    lock = threading.Lock()
    n_agents = 12
    barrier = threading.Barrier(n_agents)
    errors: list[BaseException] = []

    def work(i):
        try:
            barrier.wait()
            _b, _e, terms = m._next_terms(3, f"a{i}")
            if not terms or not terms[0].startswith("seed:"):
                raise AssertionError(f"agent a{i} got no seed pick: {terms}")
            with lock:
                picks.append(terms[0][5:])
        except BaseException as e:  # surfaced below
            with lock:
                errors.append(e)

    ts = [threading.Thread(target=work, args=(i,)) for i in range(1, n_agents + 1)]
    for t in ts:
        t.start()
    for t in ts:
        t.join(30)
    assert not errors, errors[0]
    assert len(picks) == n_agents, f"only {len(picks)} agents got picks"
    assert len(set(picks)) == n_agents, f"duplicate picks: {sorted(picks)}"
    now = time.time()
    for p in picks:
        assert m.searched_terms[p] > now, f"{p!r} was not claim-stamped"
    print(f"  ok {n_agents} agents -> {n_agents} distinct claims")


def test_step_upgrades_claim_on_success():
    m = _miner()
    results = [{"id": BLOCK + 100 + i, "title": f"brandnew {i} 2026 1080p web dl",
                "created_at": "2026-02-22T11:00:00"} for i in range(5)]
    m.client = FakeClient(results=results)
    info = m.step("a1")
    assert info["status"] == "ok", info
    assert info["new"] == 5
    stamp = m.searched_terms[info["term"]]
    now = time.time()
    assert stamp <= now + 1, "claim (now+90s) must be upgraded to a real stamp"
    assert stamp > now - 120
    assert len(m.index._rows) == 21, "rows must be upserted"
    print(f"  ok step() {info['term']!r}: new={info['new']}, claim upgraded")


def test_step_failure_frees_term_quickly():
    m = _miner()
    m.client = FakeClient(fail=True)
    info = m.step("a1")
    assert info["status"] == "retry", info
    stamp = m.searched_terms[info["term"]]
    assert stamp < time.time() - 3600, "failed search must be retryable soon"
    print(f"  ok retry path frees {info['term']!r}")


def test_block_size_env_cannot_be_zero():
    m = _miner(MKV_IDGAP_BLOCK=0)
    assert m.block >= 5000, f"block clamped, got {m.block}"
    cov = m.coverage()  # mx // m.block must not raise ZeroDivisionError
    assert cov and cov["thin_blocks"], cov
    print(f"  ok block={m.block} coverage survives zero env")


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    print(f"running {len(tests)} idgap regression tests...")
    for t in tests:
        t()
    print(f"ALL {len(tests)} PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
