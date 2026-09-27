"""Offline self-check for the series sweep agent (no network, fake index)."""
import sys
import tempfile

sys.path.insert(0, r"E:\Projects\mkvbase-cf-api")

from app.discovery import (  # noqa: E402
    Discovery, _idx_to_probe, _title_head,
)


class FakeIndex:
    def upsert(self, rows, source=""):
        return len(rows or []), 0

    def stats(self):
        return {"backend": "file"}


CA_ZIPS = [
    {"id": 1, "title": "Tribhuvan Mishra CA Topper S01 1080p NF WEB DL AAC5 1 AV1 PrimeFix zip",
     "url": "https://x/1", "created_at": "2026-09-01"},
    {"id": 2, "title": "Tribhuvan Mishra CA Topper S01 720p NF WEB DL AAC5 1 H 264 PrimeFix zip",
     "url": "https://x/2", "created_at": "2026-09-01"},
    {"id": 3, "title": "GDFlix | Tribhuvan Mishra CA Topper S01E08 Guns N Roses 1080p NF WEB DL DDP5 1 Atmos H264 cinemaluxe world mkv",
     "url": "https://x/3", "created_at": "2026-09-02"},
    {"id": 4, "title": "Captain America Brave New World 2025 720p BluRay DUAL x264 AAC5 1 ESub mkv",
     "url": "https://x/4", "created_at": "2026-09-02"},
    {"id": 5, "title": "NF Wednesday S01E04 1080p NF WEB DL DDP5 1 H 264 HMS mkv",
     "url": "https://x/5", "created_at": "2026-09-02"},
]
# 50 rows -> hits _RESULT_CAP -> last-token growth must kick in
BULK = [{"id": 100 + i, "url": f"https://x/b{i}",
         "title": f"Generic Movie {i} 2024 1080p WEB DL x264 mkv",
         "created_at": "2026-09-03"} for i in range(50)]


class FakeClient:
    def search(self, term):
        t = (term or "").lower()
        if t.startswith("zz"):
            return {"results": BULK}
        if t.startswith("ca t") or t.endswith(" "):
            return {"results": CA_ZIPS}
        return {"results": []}


def main():
    with tempfile.TemporaryDirectory() as td:
        assert _idx_to_probe(0) == "a ", _idx_to_probe(0)
        assert _idx_to_probe(25) == "z ", _idx_to_probe(25)
        assert _idx_to_probe(26) == "aa ", _idx_to_probe(26)
        assert _idx_to_probe(97) == "ct ", _idx_to_probe(97)
        assert _idx_to_probe(2073) == "cat ", _idx_to_probe(2073)
        # GDFlix tag stripped: head = show name, not 'gdflix'
        assert _title_head("GDFlix | Tribhuvan Mishra CA Topper S01E08 x") \
            == "tribhuvan mishra ca topper", \
            _title_head("GDFlix | Tribhuvan Mishra CA Topper S01E08 x")

        # sub-cap probe run: zip/season hits mined into show searches
        d = Discovery(FakeClient(), FakeIndex(), td)
        d._queue_probe("ca")
        d.step(log=lambda *a, **k: None, agent="aX", prefer="series")
        q = d.queued
        show_terms = [t for t in q if t.startswith("tribhuvan mishra ca topper")]
        assert show_terms, [t for t in q if "tribhuvan" in t]
        assert any("zip" in t for t in show_terms), show_terms
        assert "tribhuvan mishra ca topper s01" in show_terms, show_terms
        assert any(t.endswith("s01e01") for t in show_terms), show_terms
        # drain skips tag-prefixed rows ('NF Wednesday …' must yield nothing
        # from the drain itself); fresh state dir so nothing is pre-queued
        import tempfile as _tf
        with _tf.TemporaryDirectory() as td_b:
            d1b = Discovery(FakeClient(), FakeIndex(), td_b)
            drained = d1b._drain_series_hits(CA_ZIPS)
            assert drained > 0, "drain queued nothing"
            assert not any("wednesday" in t for t in d1b.queued), \
                [t for t in d1b.queued if "wedn" in t]
            assert any(t.startswith("tribhuvan mishra ca topper")
                       for t in d1b.queued), list(d1b.queued)[:10]
        print(f"show expansion queued {len(show_terms)} terms:", flush=True)
        for t in show_terms[:12]:
            print("  -", repr(t), flush=True)

        # capped probe run: last token grows letterwise
        d2 = Discovery(FakeClient(), FakeIndex(), td)
        d2._queue_probe("zz")
        d2.step(log=lambda *a, **k: None, agent="aX", prefer="series")
        q2 = d2.queued
        grown = [t for t in q2 if t.startswith("zz") and t.endswith(" ")]
        assert len(grown) == 26 and "zza " in grown and "zzz " in grown, grown
        print("capped probe grew:", len(grown), "children", flush=True)

        # state round-trip keeps the cursor
        d2.sweep_cursor = 41
        d2.save()
        d3 = Discovery(FakeClient(), FakeIndex(), td)
        assert d3.sweep_cursor == 41, d3.sweep_cursor
        print("OK: series sweep agent checks passed", flush=True)


if __name__ == "__main__":
    main()
