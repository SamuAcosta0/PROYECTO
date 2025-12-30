"""Database helpers for Google Places sweep."""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg

logger = logging.getLogger(__name__)

SOURCE_GOOGLE_PLACES = "google_places"


class Database:
    def __init__(self, database_url: str):
        self.database_url = database_url

    def connect(self) -> psycopg.Connection:
        return psycopg.connect(self.database_url, autocommit=False)


def _extract_location(place: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    location = place.get("location", {}) or {}
    lat = location.get("latitude")
    lon = location.get("longitude")
    return lat, lon


def upsert_snapshot(conn: psycopg.Connection, place: Dict[str, Any]) -> None:
    external_id = place.get("id")
    if not external_id:
        raise ValueError("Place missing id")
    display_name = (place.get("displayName") or {}).get("text", "")
    formatted_address = place.get("formattedAddress", "")
    primary_type = place.get("primaryType")
    types = place.get("types") or []
    lat, lon = _extract_location(place)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO store_snapshots_google (
                external_id, display_name, formatted_address, primary_type, types, google_location, fetched_at, raw_json
            ) VALUES (
                %(external_id)s, %(display_name)s, %(formatted_address)s, %(primary_type)s, %(types)s,
                CASE WHEN %(lon)s IS NOT NULL AND %(lat)s IS NOT NULL THEN ST_SetSRID(ST_Point(%(lon)s, %(lat)s), 4326) ELSE NULL END,
                NOW(),
                %(raw_json)s
            )
            ON CONFLICT (external_id) DO UPDATE SET
                display_name = EXCLUDED.display_name,
                formatted_address = EXCLUDED.formatted_address,
                primary_type = EXCLUDED.primary_type,
                types = EXCLUDED.types,
                google_location = EXCLUDED.google_location,
                fetched_at = EXCLUDED.fetched_at,
                raw_json = EXCLUDED.raw_json
            """,
            {
                "external_id": external_id,
                "display_name": display_name,
                "formatted_address": formatted_address,
                "primary_type": primary_type,
                "types": types,
                "lon": lon,
                "lat": lat,
                "raw_json": json.dumps(place),
            },
        )


def ensure_store(conn: psycopg.Connection, place: Dict[str, Any]) -> None:
    external_id = place.get("id")
    display_name = (place.get("displayName") or {}).get("text", "")
    formatted_address = place.get("formattedAddress", None)
    if not external_id:
        raise ValueError("Place missing id")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT store_id FROM store_external_ids WHERE source = %s AND external_id = %s",
            (SOURCE_GOOGLE_PLACES, external_id),
        )
        row = cur.fetchone()
        if row:
            return

        cur.execute(
            """
            INSERT INTO stores (canonical_name, address)
            VALUES (%s, %s)
            RETURNING store_id
            """,
            (display_name, formatted_address),
        )
        store_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO store_external_ids (store_id, source, external_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (source, external_id) DO NOTHING
            """,
            (store_id, SOURCE_GOOGLE_PLACES, external_id),
        )


def get_expired_snapshots(conn: psycopg.Connection, ttl_days: int) -> List[Dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=ttl_days)
    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT external_id, ST_Y(google_location) AS latitude, ST_X(google_location) AS longitude
            FROM store_snapshots_google
            WHERE fetched_at < %s
            """,
            (cutoff,),
        )
        return list(cur.fetchall())


def update_snapshot(conn: psycopg.Connection, external_id: str, place: Dict[str, Any]) -> None:
    lat, lon = _extract_location(place)
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE store_snapshots_google SET
                display_name = %(display_name)s,
                formatted_address = %(formatted_address)s,
                primary_type = %(primary_type)s,
                types = %(types)s,
                google_location = CASE WHEN %(lon)s IS NOT NULL AND %(lat)s IS NOT NULL THEN ST_SetSRID(ST_Point(%(lon)s, %(lat)s), 4326) ELSE NULL END,
                fetched_at = NOW(),
                raw_json = %(raw_json)s
            WHERE external_id = %(external_id)s
            """,
            {
                "display_name": (place.get("displayName") or {}).get("text", ""),
                "formatted_address": place.get("formattedAddress", ""),
                "primary_type": place.get("primaryType"),
                "types": place.get("types") or [],
                "lon": lon,
                "lat": lat,
                "raw_json": json.dumps(place),
                "external_id": external_id,
            },
        )
