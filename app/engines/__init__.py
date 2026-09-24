"""Engine factory — pick by name or auto-detect what's installed."""
from __future__ import annotations

import os

from .base import BaseEngine
from .drission_engine import DrissionPageEngine
from .camoufox_engine import CamoufoxEngine

AVAILABLE = ("drissionpage", "camoufox")


def _headless_default() -> bool:
    # visible browser clears CF more reliably; servers should set MKV_HEADLESS=true
    return os.getenv("MKV_HEADLESS", "false").lower() in ("1", "true", "yes")


def make_engine(name: str | None = None) -> BaseEngine:
    name = (name or os.getenv("MKV_ENGINE", "auto")).lower()
    if name == "drissionpage":
        return DrissionPageEngine(headless=_headless_default(),
                                  user_data_dir=os.getenv("MKV_CHROME_PROFILE"))
    if name == "camoufox":
        return CamoufoxEngine(headless=_headless_default())
    # auto: first library that imports wins
    for candidate, cls in (("drissionpage", DrissionPageEngine), ("camoufox", CamoufoxEngine)):
        try:
            __import__(cls.__module__.split(".")[0] if False else {
                "drissionpage": "DrissionPage",
                "camoufox": "camoufox",
            }[candidate])
            return cls(headless=_headless_default())
        except ImportError:
            continue
    raise RuntimeError(
        f"No CF engine available. Install one of: pip install DrissionPage  /  pip install camoufox[geoip]")
