#!/usr/bin/env python
"""OFFLINE BENCH — models tonight's HTTP-slot contention to compare lane
strategies. No network, no imports from app/, no effect on the running
fleet. Simulates _PrioritySemaphore behavior with 2 slots.

Strategies:
  current      sync=0, idgap=1, discovery-priority=2, rest=3, gap=0.4s
  idgap0       idgap promoted to 0 (beats everything incl. sync)
  idgap0-fast  idgap at 0 AND inter-request gap 0.4s -> 0.15s

Agent service times taken from tonight's real log (p50/p90): idgap ~10-45s,
discovery similar, sync bursts short.

Run:  .venv/Scripts/python.exe tools/bench_lanes.py [--secs 1800]
"""
from __future__ import annotations

import argparse
import heapq
import random
import threading
import time


class PrioSem:
    """Mirror of app.client._PrioritySemaphore (2 slots)."""

    SPEED = 20  # sim-seconds -> real-seconds divisor (20x fast-forward)
    def __init__(self, value: int = 2):
        self._cond = threading.Condition()
        self._tokens = value
        self._seq = 0
        self._waiters: list = []

    def acquire(self, priority: int = 3):
        with self._cond:
            if self._tokens > 0 and not self._waiters:
                self._tokens -= 1
                return
            ev = threading.Event()
            self._seq += 1
            heapq.heappush(self._waiters, (priority, self._seq, ev))
        ev.wait()
        with self._cond:
            pass  # woken holding a token handed over by release()

    def release(self):
        with self._cond:
            if self._waiters:
                _, _, ev = heapq.heappop(self._waiters)
                ev.set()
            else:
                self._tokens += 1


def run_strategy(name: str, lanes: dict, gap_s: float, secs: int,
                 service: dict, n_agents: int = 8, n_disc: int = 3):
    sem = PrioSem(2)
    pace_lock = threading.Lock()
    stats = {"idgap_done": 0, "disc_done": 0, "idgap_wait": 0.0,
             "disc_wait": 0.0, "idgap_n": 0, "disc_n": 0}
    lock = threading.Lock()
    t_end = time.monotonic() + secs / PrioSem.SPEED
    random.seed(42)

    def pace():
        with pace_lock:
            time.sleep(gap_s / PrioSem.SPEED)

    def agent(idx, kind):
        lane = lanes[kind]
        svc = service[kind]
        sp = PrioSem.SPEED
        while time.monotonic() < t_end:
            t0 = time.monotonic()
            sem.acquire(lane)
            waited = (time.monotonic() - t0) * sp
            time.sleep(random.uniform(*svc) / sp)  # the HTTP call
            sem.release()
            pace()
            with lock:
                if kind == "idgap":
                    stats["idgap_done"] += 1
                    stats["idgap_wait"] += waited
                    stats["idgap_n"] += 1
                else:
                    stats["disc_done"] += 1
                    stats["disc_wait"] += waited
                    stats["disc_n"] += 1

    # sync bursts: short calls every ~2 min, highest lane
    def sync_loop():
        sp = PrioSem.SPEED
        while time.monotonic() < t_end:
            sem.acquire(lanes["sync"])
            time.sleep(random.uniform(0.5, 3.0) / sp)
            sem.release()
            time.sleep(random.uniform(100, 140) / sp)

    ts = ([threading.Thread(target=agent, args=(i, "idgap"), daemon=True)
           for i in range(n_agents)]
          + [threading.Thread(target=agent, args=(i, "disc"), daemon=True)
             for i in range(n_disc)]
          + [threading.Thread(target=sync_loop, daemon=True)])
    for t in ts:
        t.start()
    while time.monotonic() < t_end:
        time.sleep(0.5)
    iw = stats["idgap_wait"] / max(stats["idgap_n"], 1)
    dw = stats["disc_wait"] / max(stats["disc_n"], 1)
    print(f"{name:<12} idgap: {stats['idgap_done']:>5} searches, "
          f"avg wait {iw:5.1f}s | disc: {stats['disc_done']:>4}, "
          f"avg wait {dw:5.1f}s | per-hr idgap "
          f"{stats['idgap_done'] * 3600 // secs}", flush=True)
    return stats


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--secs", type=int, default=600,
                    help="simulated wall seconds (default 600)")
    args = ap.parse_args()

    # service times (s) from tonight's log: idgap p50~10 p90~45 under load,
    # discovery similar; lighter when un-contended.
    service = {"idgap": (8, 40), "disc": (10, 45)}
    print(f"simulating {args.secs}s of fleet contention at {PrioSem.SPEED}x "
          f"fast-forward (2 HTTP slots, 8 idgap + 3 discovery + sync bursts)\n",
          flush=True)
    run_strategy("current-8ag", {"sync": 0, "idgap": 1, "disc": 2},
                 0.4, args.secs, service, n_agents=8)
    run_strategy("agents4", {"sync": 0, "idgap": 1, "disc": 2},
                 0.4, args.secs, service, n_agents=4)
    run_strategy("agents6", {"sync": 0, "idgap": 1, "disc": 2},
                 0.4, args.secs, service, n_agents=6)


if __name__ == "__main__":
    main()
