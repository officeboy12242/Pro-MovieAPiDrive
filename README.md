# mkvbase-cf-api

Cloudflare-proof search API for **mkvbase.site** — the complete standalone version
of the bypass proven live in the Freebuff browser tab.

## Split-plane deployment (RECOMMENDED for Render free) ⭐

Render free = 512MB / 0.1 CPU. Measured live: Firefox wedges at the `new_page`
phase and is OOM-killed on every warm attempt (~370MB held, then `killed 4
browser processes`), so Cloudflare can never clear there. The fix is to split
the work across two hosts — neither needs to be paid:

```
┌─────────────────────────────┐        POST /sync         ┌──────────────────────────┐
│ PUSHER — needs RAM only     │  ───────────────────────▶ │ RENDER — 512MB is plenty │
│ home PC, or Android phone   │   JSON, deduplicated by   │ MKV_SERVE_ONLY=true      │
│ (Termux+proot), 2GB+        │   id/url/title            │ serves cache+links index │
│ clears CF once, then plain  │                           │ ~50MB RAM, no browser    │
│ HTTP ~1s per scrape         │                           │ GET /search /recent      │
└─────────────────────────────┘                           │ GET /links?q=&limit=     │
                                                          └──────────────────────────┘
```

### 1. Render side (serve-only)

- Deploy from `render.yaml` (already set to `MKV_SERVE_ONLY=true`).
- Render dashboard → Environment → `MKV_SYNC_KEY=<secret>` (generate:
  `python -c "import secrets; print(secrets.token_urlsafe(32))"`).
- Serve-only uses ~50MB RAM; the memory launch-guard (`check_launch_ram`)
  fails fast with an explanation if a browser is ever attempted.

### 2. Pusher side (anything with 2GB+ RAM)

```bash
# on the pusher host (same repo)
pip install -r requirements.txt && python -m camoufox fetch
set MKV_RENDER_URL=https://pro-movieapidrive.onrender.com
set MKV_SYNC_KEY=<same secret as Render>
set MKV_PUSHER_TERMS=godzilla,interstellar,predestination   # optional watch-list
python -m app.pusher
```

Loops forever: polls recent every 2 min and POSTs **only when the id-set
changes** (an idle site costs nothing), refreshes each watched term hourly,
replays the backlog of previously-seen terms on cold start, and retries
failed pushes. Every response reports `links.new / updated / total`.

### 3. Android phone as the pusher (works, free, residential IP helps)

Phones have 6–12GB RAM and Camoufox ships **arm64 Linux builds**, and a
residential mobile IP clears Cloudflare more easily than any datacenter IP:

1. Install [Termux](https://f-droid.org/en/packages/com.termux/) (F-Droid build).
2. `pkg update && pkg install python git proot-distro`
3. `proot-distro install ubuntu && proot-distro login ubuntu`
4. Inside Ubuntu: `apt update && apt install python3-venv git && git clone <your-repo> && cd mkvbase-cf-api`
5. `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt && .venv/bin/python -m camoufox fetch`
6. `MKV_RENDER_URL=... MKV_SYNC_KEY=... .venv/bin/python -m app.pusher`
7. Keep it alive: `termux-wake-lock` before step 6; disable battery optimization
   for Termux. The pusher is restart-safe: `data/pusher_state.json` remembers
   watch terms and the last recent id-set, so it resumes without re-pushing.

Dedup guarantee: the receiving side keys every row by id (fallback url/title)
and merges updates in place — overlapping pushes from multiple pushers can
never create duplicate rows. `GET /links` returns the merged, newest-first
index; `GET /health` → `links: {rows, ...}` shows the total.

### Alternatives (if you don't want to run a pusher)

- **Owner allowlist, no browser at all** — if you control mkvbase.site's
  Cloudflare zone: WAF skip-rule for a secret header (see below), set
  `MKV_ORIGIN_KEY` on Render, unset `MKV_SERVE_ONLY`. Live scraping on Render
  itself, ~200MB RAM.
- **Oracle Always Free ARM VM (up to 24GB, $0)** — `deploy/oracle-setup.sh` +
  `deploy/oracle-push.ps1` are ready; runs the whole thing including browser.
- **Bigger Render plan** — Standard 2GB ($25/mo) runs the browser comfortably,
  though a datacenter IP may still be challenged harder by Cloudflare.

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

## Owner allowlist: no browser at all (recommended for Render free)

Solving the Cloudflare challenge in a browser needs ~1.9GB RAM, which a 512MB
host cannot provide. As the site owner, let your own server through instead:

1. Generate a secret locally: `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
   Keep it out of git and chat.
2. Cloudflare dashboard -> Security -> WAF -> Custom rules -> Create rule, placed first:
   - Expression: `(starts_with(http.request.uri.path, "/api/links") and any(http.request.headers["x-mkv-key"][*] eq "<secret>"))`
   - Action: **Skip**: all remaining custom rules, and under *More components to skip*:
     Security Level, Browser Integrity Check (plus Super Bot Fight Mode rules on Pro+).
3. Check Security -> Events for which service issued the challenge. If it is
   **Bot Fight Mode** (Free plan), it cannot be skipped per request: turn it off and
   use a custom rule to challenge everything except requests with the header.
4. Render dashboard -> Environment: set `MKV_ORIGIN_KEY=<secret>`
   (header name via `MKV_ORIGIN_HEADER`, default `X-Mkv-Key`).

With the key set, the warmer bootstraps the session from bare `/api/links` over
plain HTTP; `/health` shows `origin_key: true` and `plain_http_ok: true`, and no
browser is launched.

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
