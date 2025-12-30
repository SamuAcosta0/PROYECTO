"""Lightweight client for Google Places API (New)."""
from __future__ import annotations

import json
import logging
import random
import time
from typing import Any, Dict, Iterable, List, Optional

import requests

logger = logging.getLogger(__name__)

PLACES_SEARCH_URL = "https://places.googleapis.com/v1/places:searchNearby"
FIELD_MASK = ",".join(
    [
        "places.id",
        "places.displayName",
        "places.formattedAddress",
        "places.location",
        "places.primaryType",
        "places.types",
    ]
)


class GooglePlacesClient:
    def __init__(
        self,
        api_key: str,
        session: Optional[requests.Session] = None,
        max_retries: int = 5,
        base_backoff: float = 1.0,
    ) -> None:
        self.api_key = api_key
        self.session = session or requests.Session()
        self.max_retries = max_retries
        self.base_backoff = base_backoff

    def search_nearby(
        self,
        latitude: float,
        longitude: float,
        radius_m: int,
        included_types: Iterable[str],
        max_results: int = 20,
    ) -> List[Dict[str, Any]]:
        payload = {
            "includedTypes": list(included_types),
            "maxResultCount": max_results,
            "locationRestriction": {
                "circle": {
                    "center": {
                        "latitude": latitude,
                        "longitude": longitude,
                    },
                    "radius": radius_m,
                }
            },
        }
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": FIELD_MASK,
        }

        attempt = 0
        while True:
            response = self.session.post(PLACES_SEARCH_URL, headers=headers, json=payload, timeout=30)
            if response.status_code == 200:
                data = response.json()
                return data.get("places", [])

            attempt += 1
            if response.status_code == 429 or 500 <= response.status_code < 600:
                if attempt > self.max_retries:
                    response.raise_for_status()
                sleep_for = self._compute_backoff(attempt)
                logger.warning(
                    "Transient error %s on search_nearby. attempt=%s sleep=%.2fs",
                    response.status_code,
                    attempt,
                    sleep_for,
                )
                time.sleep(sleep_for)
                continue
            response.raise_for_status()

    def _compute_backoff(self, attempt: int) -> float:
        jitter = random.uniform(0, 1)
        return self.base_backoff * (2 ** (attempt - 1)) + jitter
