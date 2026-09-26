"""Engine contract: every engine must be able to (1) get a CF-cleared session on
mkvbase.site, (2) open an arbitrary URL top-level, (3) return cookies + body text.

Fast paths (optional overrides):
  inpage_fetch(url) -> body text via in-page XHR/fetch from the already-cleared
  page — no navigation at all. This mirrors what mkvbase's own client does
  after clearance, and it survives because the fetch originates from the page.
"""
from __future__ import annotations

import os
import time
from abc import ABC, abstractmethod


def check_launch_ram() -> None:
    """Refuse to launch a browser when the container's memory ceiling cannot fit one
    (Render free = 512MB; Firefox wants ~1.5-2GB). On such hosts the launch does not
    crash cleanly — it wedges at new_page for minutes, holds ~370MB, and gets
    OOM-killed, every attempt. Fail in milliseconds instead, with the fix in the
    message. No cgroup limit (desktop, phone, most VMs) -> no-op.
    """
    if os.getenv("MKV_ALLOW_BROWSER_ANYWAY", "").lower() in ("1", "true", "yes"):
        return
    try:
        floor = float(os.getenv("MKV_MIN_LAUNCH_MEM_MB", "700") or 0)
    except ValueError:
        floor = 700.0
    if floor <= 0:
        return
    limit_mb: float | None = None
    for p in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            with open(p) as f:
                v = f.read().strip()
            if v and v != "max":
                limit_mb = int(v) / 2 ** 20
            break
        except OSError:
            continue
    if limit_mb is None or limit_mb >= floor:
        return
    raise RuntimeError(
        f"browser cannot launch: container memory limit {limit_mb:.0f}MB < ~{floor:.0f}MB Firefox needs; "
        "launches wedge at new_page and are OOM-killed. On Render free use a browser-free mode instead: "
        "MKV_ORIGIN_KEY (owner Cloudflare skip-rule header -> live scraping, no browser) or "
        "MKV_SERVE_ONLY=true (serve results pushed via POST /sync from another host). "
        "Or upgrade the plan / use a 1GB+ host. Override with MKV_ALLOW_BROWSER_ANYWAY=true.")


class Session:
    """Cookies + UA captured after a successful Cloudflare clearance."""

    def __init__(self, cookies: dict[str, str], user_agent: str = ""):
        self.cookies = cookies
        self.user_agent = user_agent
        self.cleared_at = time.time()

    def mkv(self) -> dict[str, str]:
        return {k: v for k, v in self.cookies.items() if k.startswith("mkv_")}

    def has_mkv_session(self) -> bool:
        return all(k in self.cookies for k in ("mkv_client_key", "mkv_challenge", "mkv_seq"))


def looks_like_challenge(text: str) -> bool:
    t = (text or "")[:1200].lower()
    return ("just a moment" in t or "attention required" in t
            or "performing security verification" in t or "challenge-platform" in t)


class BaseEngine(ABC):
    name = "base"

    @abstractmethod
    def get_session(self, url: str, timeout_s: int = 60) -> Session:
        """Top-level navigate, wait out any Cloudflare interstitial, return Session."""

    @abstractmethod
    def fetch(self, url: str, timeout_s: int = 60) -> tuple[str, Session]:
        """Top-level navigate (keeps session warm), return (body_text, session)."""

    def inpage_fetch(self, url: str, timeout_s: int = 30) -> str | None:
        """In-page XHR fetch without navigation. Returns None if unsupported/failed."""
        return None

    def is_open(self) -> bool:
        """Whether a browser process is currently running (both engines keep _page)."""
        return getattr(self, "_page", None) is not None

    @abstractmethod
    def close(self) -> None:
        ...
