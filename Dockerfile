# mkvbase-cf-api — Cloudflare-proof mkvbase scraper API
# Engine: Camoufox (headless anti-detect Firefox, proven to clear Turnstile)
FROM python:3.11-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MKV_ENGINE=camoufox \
    MKV_HEADLESS=true

# Firefox-runtime libraries Camoufox needs (it is a patched Firefox)
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgtk-3-0 libdbus-glib-1-2 libxt6 libx11-xcb1 libxcomposite1 \
        libxdamage1 libxrandr2 libasound2 libxkbcommon0 libpango-1.0-0 \
        libcairo2 libgdk-pixbuf-2.0-0 fonts-liberation curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python deps first (layer-cached), then the Camoufox browser (~150MB) at build time
COPY requirements.txt .
RUN pip install -r requirements.txt && python -m camoufox fetch

COPY app ./app

# Render injects $PORT; default 8765 for local docker runs
EXPOSE 8765
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8765}"]
