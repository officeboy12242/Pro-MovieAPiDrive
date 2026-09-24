# mkvbase-cf-api

Cloudflare-proof search API for **mkvbase.site** — the complete standalone version
of the bypass proven live in the Freebuff browser tab.

## How it beats Cloudflare

1. A real browser engine opens `https://mkvbase.site/api/links` **top-level**.
   CF's managed challenge auto-clears full-page navigations (validated live).
2. The engine harvests session cookies: `mkv_client_key`, `mkv_challenge`, `mkv_seq`.
3. The search URL is signed **locally** (no browser needed for the math):
   - `q = hex(utf8(term) XOR (Date.now() % 256))`
   - PoW: `sha256(salt:nonce) -> h1`, `sha256(h1:q)` must start with `"0"*difficulty` (2-3)
   - `sig = HMAC-SHA256(client_key, "q:t:seq:nonce:ent")`
4. The engine opens the signed URL top-level -> results JSON renders -> parsed, cached, returned.

## Engines

| Engine | Browser | Notes |
|---|---|---|
| `drissionpage` | your real Chrome via CDP | most human-like, best CF pass rate |
| `camoufox` | anti-detect Firefox | fully headless-friendly, ships own browser |

Auto-detect: `MKV_ENGINE=auto` (default) picks whichever is importable.

## Run

```bash
cd E:\Projects\mkvbase-cf-api
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python -m camoufox fetch          # one-time Camoufox browser download
set MKV_ENGINE=drissionpage                      # or camoufox
.venv\Scripts\uvicorn app.main:app --host 0.0.0.0 --port 8765
```

## Endpoints

| Route | Purpose |
|---|---|
| `GET /health` | liveness + engine + cache stats |
| `GET /search?term=predestination` | signed search, JSON rows (TTL-cached 5 min) |
| `GET /search?term=x&refresh=true` | force a live scrape, bypass cache |
| `GET /recent` | latest 50 links posted site-wide |
| `GET /saved` | list persisted result files |
| `GET /saved/search_predestination.json` | fetch a persisted result set |
| `POST /cache/clear` | drop all cached results |

## Performance (measured)

| Scenario | Latency |
|---|---|
| Cold start (engine + CF clearance, prewarmed at boot) | ~40-75s once |
| Live search, warm session (in-page fetch, no navigation) | **0.5-1s** |
| Cached term (within TTL) | **<5ms** |

The fast path fetches the signed URL via in-page `fetch()` with the
`X-Requested-With: XMLHttpRequest` header — the same call mkvbase's own client
makes — so no page navigation happens after the initial Cloudflare clearance.
Transient site flakes are absorbed by one automatic retry.

## Deploy notes (server / Render)

- Set `MKV_HEADLESS=true` (camoufox fine headless; drissionpage prefers visible for CF).
- Camoufox needs its browser fetched once at build: `python -m camoufox fetch`.
- First call after cold start takes ~15-40s (CF clearance); later calls ~2-5s.
