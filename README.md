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
| Cold start (background warmer clears CF once) | ~50-95s, requests do not wait for it |
| Request while warming | stale result or `503` + `Retry-After` within `MKV_REQUEST_WAIT` (25s) |
| Live search, warm session (plain HTTP via curl_cffi, no browser) | **~1-1.5s** |
| Cached term (within TTL) | **<5ms** |

After one browser clearance, every search is signed locally and fetched over
plain HTTPS with a Firefox TLS fingerprint (curl_cffi). mkvbase re-issues its
`mkv_*` cookies on every response; absorbing them keeps the session alive
without a browser, so the browser is closed right after clearance and only
relaunched when Cloudflare's `cf_clearance` itself expires.

## Deploy notes (server / Render)

- `render.yaml` is the source of truth. Key settings: `MKV_ENGINE=camoufox`,
  `MKV_HEADLESS=true`, `MKV_RELEASE_BROWSER=true`, `MKV_KEEPALIVE=true`.
- Camoufox needs its browser fetched once at build: `python -m camoufox fetch` (Dockerfile does it).
- Requests never block on the browser. Check `GET /health` -> `warmer`:
  `state` (`warming` / `ready` / `failed`), `last_error`, `retry_after_s`, `plain_http_ok`,
  plus `mem_mb` and `browser_open`.
- If `warmer.state` stays `failed` with "Cloudflare did not clear", the host IP is
  being challenged harder than a home IP. Set `MKV_PROXY=http://user:pass@host:port`
  (residential) in the Render dashboard; the browser and curl_cffi both use it,
  since `cf_clearance` is bound to the IP that solved it.
- Keepalive pings the public `RENDER_EXTERNAL_URL`, so the free instance does not
  sleep (a sleep wipes `/tmp` and forces a new clearance).
- `MKV_SERVE_ONLY=true` turns off live scraping: `/search` and `/recent` only serve
  results pushed via `POST /sync` or saved earlier.
