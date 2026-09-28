"""Engine factory — pick by name or auto-detect what's installed.

Default is a HEADLESS browser so Cloudflare clearance never pops a window
over the user's work. Set MKV_HEADLESS=false only when debugging a clear.
"""
from __future__ import annotations

import os

from .base import BaseEngine
from .drission_engine import DrissionPageEngine
from .camoufox_engine import CamoufoxEngine

AVAILABLE = ("camoufox", "drissionpage")


def _headless_default() -> bool:
    # Headless by default: invisible clearance (Camoufox is built for it).
    # Opt OUT with MKV_HEADLESS=false / 0 / no when debugging.
    return os.getenv("MKV_HEADLESS", "true").lower() not in ("0", "false", "no")


def make_engine(name: str | None = None) -> BaseEngine:
    name = (name or os.getenv("MKV_ENGINE", "auto")).lower()
    if name == "drissionpage":
        return DrissionPageEngine(headless=_headless_default(),
                                  user_data_dir=os.getenv("MKV_CHROME_PROFILE"))
    if name == "camoufox":
        return CamoufoxEngine(headless=_headless_default(), proxy=os.getenv("MKV_PROXY") or None)
    # auto: camoufox first (anti-detect + headless-friendly), then drissionpage
    for candidate, cls in (("camoufox", CamoufoxEngine), ("drissionpage", DrissionPageEngine)):
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
