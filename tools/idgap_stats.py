#!/usr/bin/env python
"""READ-ONLY fleet performance analyzer. Never writes fleet state; parses
data/pusher.log [idgap:*] lines + [alive] heartbeats and data/idgap_state.json
(term_stats) to report what is actually hitting.

Usage (repo root):
    .venv/Scripts/python.exe tools/idgap_stats.py
    .venv/Scripts/python.exe tools/idgap_stats.py --window 6
    .venv/Scripts/python.exe tools/idgap_stats.py --tail 20000
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter, defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

RE_OK = re.compile(
    r"\[idgap:(a\d+)\] ok '(.*)'/(seed-spill|seed-ep|seed-head|era-\w+) "
    r"block=(\d+) era=([\d-]*): (\d+) rows, (\d+) new, (\d+) in-block \(([\d.]+)s\)")
RE_ALIVE = re.compile(
    r"\[alive\] up ([\dhms:]+) \|.*?\| idgap rounds=(\d+) terms=(\d+) "
    r"new_rows=(\d+) cov=([\d.]+)%")

FACETS = ("1080p", "720p", "480p", "2160p", "10bit", "hevc", "web-dl",
          "webrip", "hdrip", "bdrip", "bluray", "dvdscr", "aac", "esub",
          "hevc hd", "zip", "mkv", "pack", "complete", "s01", "e01",
          "hindi", "tamil", "telugu", "dual audio")


def facet_of(term: str) -> str | None:
    t = " " + term.strip().lower()
    best = None
    for f in FACETS:
        if t.endswith(" " + f) and (best is None or len(f) > len(best)):
            best = f
    return best


def pct(n: int, d: int) -> str:
    return f"{(100.0 * n / d):.1f}%" if d else "-"


def rate(n: float, hours: float) -> str:
    return f"{(n / hours):.1f}/h" if hours > 0.05 else "-"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.path.join(ROOT, "data"))
    ap.add_argument("--tail", type=int, default=0,
                    help="only read the last N lines of pusher.log")
    ap.add_argument("--window", type=float, default=0,
                    help="only stats from the last N hours (approx, uses wall clock)")
    args = ap.parse_args()

    log_path = os.path.join(args.data_dir, "pusher.log")
    if not os.path.exists(log_path):
        print(f"no log at {log_path}")
        return 1

    size = os.path.getsize(log_path)
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        if args.tail and size > args.tail * 160:
            f.seek(size - args.tail * 160)
            f.readline()  # drop partial line
        lines = f.readlines()
    n_all = len(lines)

    now = 0.0
    try:  # log lines have no timestamps; approximate window via heartbeat uptime
        import time
        now = time.time()
    except Exception:
        pass

    # ---- per-search lines -------------------------------------------------
    ok = []          # (agent, term, kind, block, rows, new, in_block, secs)
    retries = Counter()
    err_kinds = Counter()
    for ln in lines:
        m = RE_OK.search(ln)
        if m:
            agent, term, kind, blk, _era, rows, new, inb, secs = m.groups()
            ok.append((agent, term, kind, int(blk), int(rows), int(new),
                       int(inb), float(secs)))
            continue
        m = re.search(r"\[idgap:(a\d+)\] RETRY (.*?): (\w+):", ln)
        if m:
            retries[m.group(2)] += 1
            err_kinds[m.group(3)] += 1

    # ---- alive heartbeats --------------------------------------------------
    alive = []       # (uptime_str, terms, new_rows, cov)
    for ln in lines:
        m = RE_ALIVE.search(ln)
        if m:
            up, _rounds, terms, new_rows, cov = m.groups()
            alive.append((up, int(terms), int(new_rows), float(cov)))

    # ---- state file --------------------------------------------------------
    st = {}
    st_path = os.path.join(args.data_dir, "idgap_state.json")
    try:
        with open(st_path, "r", encoding="utf-8", errors="replace") as f:
            st = json.load(f)
    except Exception as e:
        print(f"(state file unreadable: {e})")
    term_stats = st.get("term_stats") or {}
    searched = st.get("searched_terms") or {}
    stats = st.get("stats") or {}

    W = 78
    print("=" * W)
    print("IDGAP FLEET PERFORMANCE REPORT (read-only)")
    print("=" * W)

    # ---- lifetime from state ----------------------------------------------
    print("\n-- LIFETIME (from idgap_state.json) --")
    print(f"  terms_done={stats.get('terms_done')}  new_rows={stats.get('rows_new')}"
          f"  coverage={stats.get('coverage_last')}%"
          f"  picked_block={stats.get('picked_block')} (era {stats.get('picked_era')})")
    if term_stats:
        tot_new = sum(v.get("new", 0) for v in term_stats.values())
        tot_tr = sum(v.get("tries", 0) for v in term_stats.values())
        print(f"  {'kind':<14}{'tries':>9}{'new':>9}{'new/try':>9}{'share_new':>11}")
        for k, v in sorted(term_stats.items(), key=lambda kv: -kv[1].get("new", 0)):
            print(f"  {k:<14}{v.get('tries', 0):>9}{v.get('new', 0):>9}"
                  f"{(v.get('new', 0) / v['tries']) if v.get('tries') else 0:>9.2f}"
                  f"{pct(v.get('new', 0), tot_new):>11}")
        print(f"  {'TOTAL':<14}{tot_tr:>9}{tot_new:>9}")

    # ---- window rates from heartbeats --------------------------------------
    print("\n-- HEARTBEAT DELTAS (rates between [alive] lines) --")
    if len(alive) >= 2:
        def up_s(u):
            mult = {"d": 86400, "h": 3600, "m": 60, "s": 1}
            return sum(int(n) * mult[c]
                       for n, c in re.findall(r"(\d+)([dhms])", u))
        d_new = d_t = d_min = 0
        best = None
        for (u0, t0, n0, _c0), (u1, t1, n1, c1) in zip(alive, alive[1:]):
            dt = up_s(u1) - up_s(u0)
            if dt <= 0:
                continue
            dn, dtk = n1 - n0, t1 - t0
            d_new += dn
            d_t += dtk
            d_min += dt / 60.0
            if best is None or dn / dt > best[0]:
                best = (dn / dt, u1, dn, dtk, dt, c1)
        if d_min:
            print(f"  span ~{d_min:.0f} min over {len(alive) - 1} deltas:"
                  f" {d_new} new rows ({rate(d_new, d_min / 60)}),"
                  f" {d_t} searches ({rate(d_t, d_min / 60)})")
            print(f"  avg yield {d_new / d_t if d_t else 0:.2f} new/search"
                  f" | latest cov {alive[-1][3]}%")
        if best:
            print(f"  hottest minute: up {best[1]} -> {best[2]} new"
                  f" from {best[3]} searches in {best[4]}s (cov {best[5]}%)")
    else:
        print("  (fewer than 2 heartbeats in the slice)")

    # ---- log-slice per-search aggregates ------------------------------------
    print(f"\n-- LOG SLICE: {len(ok)} ok searches, {sum(retries.values())} retries"
          f" (of {n_all} lines read) --")
    if ok:
        rows = sum(o[4] for o in ok)
        new = sum(o[5] for o in ok)
        inb = sum(o[6] for o in ok)
        print(f"  rows={rows}  new={new}  in-block={inb}  "
              f"yield={new / len(ok):.2f} new/search  zero-new={pct(len(ok) - sum(1 for o in ok if o[5]), len(ok))}"
              f"  capped50={pct(sum(1 for o in ok if o[4] == 50), len(ok))}")

        by_kind = defaultdict(lambda: [0, 0, 0, 0])
        for _a, _t, k, _b, r, n_, ib, _s in ok:
            v = by_kind[k]
            v[0] += 1
            v[1] += r
            v[2] += n_
            v[3] += ib
        print(f"\n  {'kind':<12}{'tries':>7}{'rows':>8}{'new':>8}{'in-blk':>8}"
              f"{'new/try':>9}{'zero%':>7}")
        for k, (t, r, n_, ib) in sorted(by_kind.items(), key=lambda kv: -kv[1][2]):
            print(f"  {k:<12}{t:>7}{r:>8}{n_:>8}{ib:>8}{n_ / t:>9.2f}"
                  f"{pct(t - sum(1 for o in ok if o[2] == k and o[5]), t):>7}")

        # facet leaderboard (seed-spill terms end with a facet)
        fac = defaultdict(lambda: [0, 0, 0])
        for _a, t_, k, _b, r, n_, _ib, _s in ok:
            if k != "seed-spill":
                continue
            f_ = facet_of(t_)
            if f_:
                v = fac[f_]
                v[0] += 1
                v[1] += r
                v[2] += n_
        if fac:
            print(f"\n  FACET LEADERBOARD (this slice, seed-spill only)")
            print(f"  {'facet':<12}{'tries':>7}{'rows':>8}{'new':>8}"
                  f"{'new/try':>9}{'rows/try':>9}")
            for f_, (t, r, n_) in sorted(fac.items(), key=lambda kv: -kv[1][2]):
                print(f"  {f_:<12}{t:>7}{r:>8}{n_:>8}{n_ / t:>9.2f}{r / t:>9.1f}")

        # per-agent fairness
        ag = Counter(o[0] for o in ok)
        ag_new = Counter()
        for o in ok:
            ag_new[o[0]] += o[5]
        print(f"\n  PER-AGENT: " + "  ".join(
            f"{a}:{ag[a]}t/{ag_new[a]}n" for a in sorted(ag, key=lambda x: int(x[1:]))))

        # slowest searches (retries + engine woe indicator)
        slow = sorted(ok, key=lambda o: -o[7])[:5]
        print("  SLOWEST: " + "  ".join(f"{o[1][:28]}({o[7]:.0f}s)" for o in slow))

        # top earners
        top = sorted(ok, key=lambda o: -o[5])[:8]
        print("  TOP NEW-ROW SEARCHES:")
        for o in top:
            print(f"    {o[5]:>3} new / {o[4]:>3} rows  '{o[1][:52]}' [{o[2]}] blk={o[3]}")

    if retries:
        print(f"\n  RETRIES by error: " + "  ".join(f"{k}:{v}" for k, v in err_kinds.items()))
        for t_, c in retries.most_common(5):
            print(f"    x{c}  '{t_[:52]}'")

    if searched:
        fresh = sum(1 for _t, ts in searched.items() if ts and ts > 0)
        print(f"\n-- TERM SPACE -- searched_terms in state: {len(searched)}"
              f" | distinct slices burned so far")
    print("=" * W)
    return 0


if __name__ == "__main__":
    sys.exit(main())
