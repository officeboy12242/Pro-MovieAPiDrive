"""Engine contract: every engine must be able to (1) get a CF-cleared session on
mkvbase.site, (2) open an arbitrary URL top-level, (3) return cookies + body text."""
from __future__ import annotations

import time
from abc import ABC, abstractmethod


class Session:
    """Cookies + UA captured after a successful Cloudflare clearance."""

    def __init__(self, cookies: dict[str, str], user_agent: str = ""):
        self.cookies = cookies
        self.user_agent = user_agent
        self.cleared_at = time.time()

    def mkv(self) -> dict[str, str]:
        """Only the mkv_* protocol cookies."""
        return {k: v for k, v in self.cookies.items() if k.startswith("mkv_")}

    def has_mkv_session(self) -> bool:
        return all(k in self.cookies for k in ("mkv_client_key", "mkv_challenge", "mkv_seq"))


class BaseEngine(ABC):
    name = "base"

    @abstractmethod
    def get_session(self, url: str, timeout_s: int = 60) -> Session:
        """Navigate top-level to url, wait out any Cloudflare interstitial, return Session."""

    @abstractmethod
    def fetch(self, url: str, timeout_s: int = 60) -> tuple[str, Session]:
        """Top-level navigate to url (session stays warm in the browser),
        return (body_text, session)."""

    @abstractmethod
    def close(self) -> None:
        ...
