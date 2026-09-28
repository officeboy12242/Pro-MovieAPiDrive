"""Duplicate audit for the Mongo links vault (read-only, browser-free).

Answers: did anything get pushed twice? Three levels:
  1. key type    every _id must be unique by construction; report prefix mix
                 (id:/url:/title:) — url:/title: rows mean a scrape lacked ids
  2. id level    the same numeric site id under two keys = real double-push bug
  3. url level   same URL under different ids = the SITE re-posted the link
                 (mkvbase re-issues ids on re-uploads; identical title+url,
                 created_at identical or seconds apart). Expected and harmless —
                 each is a distinct site row, not a pusher duplicate.

Run:  .venv\\Scripts\\python _dedupe_audit.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from app.store import _mongo_uri  # noqa: E402


def main() -> None:
    from pymongo import MongoClient

    uri = _mongo_uri()
    if not uri:
        print("no Mongo URI configured (data/mongo_uri.txt)")
        return
    col = MongoClient(uri, serverSelectionTimeoutMS=8000)["mkvbase"].links
    total = col.estimated_document_count()
    print(f"total docs: {total:,}\n")

    print("== 1. key type mix (url:/title: keys would mean id-less rows) ==")
    for d in col.aggregate([
            {"$group": {"_id": {"$arrayElemAt": [{"$split": ["$_id", ":"]}, 0]},
                        "n": {"$sum": 1}}}, {"$sort": {"n": -1}}]):
        print(f"  {d['_id']}: {d['n']:,}")

    print("\n== 2. same numeric id under DIFFERENT keys (real double-push) ==")
    dup_ids = list(col.aggregate([
        {"$match": {"id": {"$ne": None}}},
        {"$group": {"_id": "$id", "n": {"$sum": 1}}},
        {"$match": {"n": {"$gt": 1}}}]))
    print(f"  {len(dup_ids)} found" + ("  <-- BUG" if dup_ids else "  (clean)"))

    print("\n== 3. same URL under different ids (site re-posts) ==")
    groups = list(col.aggregate([
        {"$group": {"_id": "$url", "n": {"$sum": 1}}},
        {"$match": {"n": {"$gt": 1}}}]))
    extra = sum(g["n"] for g in groups) - len(groups)
    print(f"  urls shared by 2+ docs : {len(groups)}")
    print(f"  extra docs beyond 1/url: {extra} "
          f"({100 * extra / max(1, total):.2f}% of vault)")
    print(f"  identical (title,url) twins: "
          f"{len(list(col.aggregate([
              {'$group': {'_id': {'u': '$url', 't': '$title'}, 'n': {'$sum': 1}}},
              {'$match': {'n': {'$gt': 1}}}])))} groups")
    for g in list(col.aggregate([
            {"$group": {"_id": "$url", "n": {"$sum": 1}, "keys": {"$push": "$_id"}}},
            {"$match": {"n": {"$gt": 1}}}, {"$sort": {"n": -1}}, {"$limit": 3}])):
        print(f"  {g['n']}x {g['_id']}")
        for k in g["keys"][:4]:
            r = col.find_one({"_id": k}) or {}
            print(f"     {k}: {str(r.get('title'))[:52]} | "
                  f"{str(r.get('created_at'))[:19]} | src={r.get('_src')}")


if __name__ == "__main__":
    main()
