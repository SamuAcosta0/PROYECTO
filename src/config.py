"""Configuration helpers for the NEXO sweep utility."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Tuple


@dataclass
class SweepConfig:
    google_api_key: str
    database_url: str
    lat_min: float = -34.95
    lat_max: float = -34.80
    lon_min: float = -56.30
    lon_max: float = -56.05
    step_km: float = 2.0
    radius_m: int = 1500
    sleep_seconds: float = 0.1
    max_results: int = 20

    @property
    def bounding_box(self) -> Tuple[float, float, float, float]:
        return self.lat_min, self.lat_max, self.lon_min, self.lon_max


def load_config() -> SweepConfig:
    google_api_key = os.environ.get("GOOGLE_PLACES_API_KEY")
    database_url = os.environ.get("DATABASE_URL")
    if not google_api_key:
        raise RuntimeError("GOOGLE_PLACES_API_KEY must be set")
    if not database_url:
        raise RuntimeError("DATABASE_URL must be set")

    return SweepConfig(
        google_api_key=google_api_key,
        database_url=database_url,
    )
