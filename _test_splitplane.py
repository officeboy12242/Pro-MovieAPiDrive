"""Smoke test for the split-plane changes: dedup index + /sync + serve-only routes.

Run: python _test_splitplane.py  (sets its own env before importing the app)
Exercises the exact flow a pusher + Render free instance would follow.
"""
from __future__ import annotations

import os
import tempfile

tmp = tempfile.mkdtemp()
os.environ["MKV_DATA_DIR"] = tmp
os.environ["MKV_SERVE_ONLY"] = "1"
os.environ["MKV_SYNC_KEY"] = "testkey123"

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402

c = TestClient(app)
fails: list[str] = []


def check(name: str, cond: bool, extra: str = "") -> None:
    print(f"{'PASS' if cond else 'FAIL'} {name} {extra}")
    if not cond:
        fails.append(name)


def push(term: str, rows: list[dict]) -> dict:
    r = c.post("/sync", json={"term": term, "count": len(rows), "results": rows},
               headers={"X-Sync-Key": "testkey123"})
    check(f"POST /sync ({term}) -> 200", r.status_code == 200, r.text[:120])
    return r.json()


ROWS_A = [
    {"id": 101, "title": "Alpha Movie 1080p", "url": "https://hub.example/a1",
     "created_at": "2026-09-26T10:00:00Z", "status": "active"},
    {"id": 102, "title": "Beta Movie 720p", "url": "https://hub.example/b1",
     "created_at": "2026-09-26T11:00:00Z", "status": "active"},
]
ROW_B_UPDATED = [{"id": 101, "title": "Alpha Movie 1080p", "url": "https://hub.example/a1",
                  "created_at": "2026-09-26T10:00:00Z", "status": "removed"}]
ROW_C = [{"id": 103, "title": "Gamma Movie 4k", "url": "https://hub.example/c1",
          "created_at": "2026-09-26T12:00:00Z"}]

# --- 1. push same rows twice: second push must report updated, not new (dedup)
r1 = push("recent", ROWS_A)
r2 = push("recent", ROWS_A)
check("first push  new=2", r1["links"]["new"] == 2, str(r1["links"]))
check("second push new=0 updated=0 (identical)", r2["links"]["new"] == 0 and r2["links"]["updated"] == 0,
      str(r2["links"]))
r3 = push("recent", ROW_B_UPDATED)
check("status change detected as update", r3["links"]["updated"] == 1, str(r3["links"]))
r = c.get("/links")
row101_now = next(row for row in r.json()["results"] if row["id"] == 101)
check("update merged in place (status=removed)", row101_now["status"] == "removed", str(row101_now))

# --- 2. overlapping data under a different term still dedups (same ids)
r4 = push("alpha", ROWS_A + ROW_C)
check("cross-term overlap adds only 1 new", r4["links"]["new"] == 1, str(r4["links"]))
check("index total == 3 unique rows", r4["links"]["total"] == 3, str(r4["links"]))
r = c.get("/links")
row101_final = next(row for row in r.json()["results"] if row["id"] == 101)
check("later push overwrites in place (last write wins)", row101_final["status"] == "active",
      str(row101_final))

# --- 3. sync key enforced
r = c.post("/sync", json={"term": "x", "results": []})
check("POST /sync without key -> 401", r.status_code == 401, r.text[:80])

# --- 4. GET /links: newest first, q filter, limit
r = c.get("/links")
body = r.json()
check("GET /links 200, count==3", r.status_code == 200 and body["count"] == 3, str(body)[:160])
ids_order = [row["id"] for row in body["results"]]
check("newest-first order", ids_order == [103, 102, 101], str(ids_order))
r = c.get("/links", params={"q": "gamma"})
check("q filter works", r.json()["count"] == 1 and r.json()["results"][0]["id"] == 103, r.text[:120])
r = c.get("/links", params={"limit": 1})
check("limit respected", r.json()["count"] == 3 and len(r.json()["results"]) == 1, r.text[:120])

# --- 5. serve-only /recent serves from the links index (no browser anywhere)
r = c.get("/recent")
check("serve-only /recent 200 from index", r.status_code == 200
      and r.json().get("source") == "links_index" and r.json()["count"] == 3, r.text[:200])

# --- 6. serve-only /search: synced term served from TTL cache
r = c.get("/search", params={"term": "alpha"})
body = r.json()
check("serve-only /search synced term", r.status_code == 200 and body.get("cached") is True
      and body.get("count") == 3 and len(body.get("results", [])) == 3, r.text[:200])

# --- 7. serve-only /search unknown term -> 404 (no live scrape attempted)
r = c.get("/search", params={"term": "never-synced-term"})
check("serve-only unknown term -> 404", r.status_code == 404, r.text[:120])

# --- 8. health exposes links stats
r = c.get("/health")
check("/health links stats", r.status_code == 200 and r.json()["links"]["rows"] == 3,
      r.text[:200])

# --- 9. persistence: a fresh LinksIndex over the same dir reloads all 3 rows
from app.store import LinksIndex  # noqa: E402
idx = LinksIndex(tmp)
check("index persists across restart", idx.stats()["rows"] == 3, str(idx.stats()))
out = idx.recent(limit=10)
check("reloaded index newest-first", [x["id"] for x in out["results"]][0] == 103, str(out)[:120])

# --- 10. cap enforcement (tiny cap -> oldest dropped)
idx2 = LinksIndex(os.path.join(tmp, "cap"))
idx2.MAX_ROWS = 2
idx2.upsert([{"id": i, "title": f"row{i}", "url": f"u{i}"} for i in (1, 2, 3, 4)])
check("cap enforced (kept newest 2)", idx2.stats()["rows"] == 2
      and all(x["id"] in (3, 4) for x in idx2.recent(10)["results"]), str(idx2.stats()))

print()
if fails:
    print("FAILURES:", ", ".join(fails))
    raise SystemExit(1)
print("ALL PASS")
