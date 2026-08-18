"""Centralized configuration for the Meltdown demo."""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

GOOGLE_API_KEY: str | None = os.environ.get("GOOGLE_API_KEY") or None
GOOGLE_MAPS_API_KEY: str | None = os.environ.get("GOOGLE_MAPS_API_KEY") or None
DEFAULT_MODEL: str = os.environ.get("DEFAULT_MODEL", "gemini-2.5-flash")
TEMPORAL_ADDRESS: str = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
FLEET_DB_PATH: str = os.environ.get(
    "FLEET_DB_PATH", str(Path(__file__).parent.parent / "fleet_state.db")
)
