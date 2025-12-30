"""CLI orchestrator for Google Places sweep over Montevideo."""
from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Iterable

from .client_google_places import GooglePlacesClient
from .config import SweepConfig, load_config
from .db import Database, ensure_store, get_expired_snapshots, update_snapshot, upsert_snapshot
from .grid import generate_grid

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

INCLUDED_TYPES = ["supermarket", "grocery_store"]


def sweep(config: SweepConfig) -> None:
    """Run the grid sweep, persisting snapshots and store identifiers."""
    client = GooglePlacesClient(config.google_api_key)
    db = Database(config.database_url)

    with db.connect() as conn:
        for lat, lon in generate_grid(*config.bounding_box, config.step_km):
            logger.info("Scanning center (%s, %s)", lat, lon)
            places = client.search_nearby(
                latitude=lat,
                longitude=lon,
                radius_m=config.radius_m,
                included_types=INCLUDED_TYPES,
                max_results=config.max_results,
            )
            for place in places:
                upsert_snapshot(conn, place)
                ensure_store(conn, place)
            conn.commit()
            time.sleep(config.sleep_seconds)


def refresh_expired(config: SweepConfig, ttl_days: int) -> None:
    """Refresh snapshots that are older than the configured TTL."""
    client = GooglePlacesClient(config.google_api_key)
    db = Database(config.database_url)
    refreshed = 0
    skipped = 0

    with db.connect() as conn:
        expired = get_expired_snapshots(conn, ttl_days)
        logger.info("Found %s expired snapshots", len(expired))
        for item in expired:
            lat = item.get("latitude")
            lon = item.get("longitude")
            if lat is None or lon is None:
                logger.warning("Skipping %s due to missing coordinates", item["external_id"])
                skipped += 1
                continue
            places = client.search_nearby(
                latitude=lat,
                longitude=lon,
                radius_m=config.radius_m,
                included_types=INCLUDED_TYPES,
                max_results=config.max_results,
            )
            found = [p for p in places if p.get("id") == item["external_id"]]
            if not found:
                logger.warning("Place %s not returned on refresh", item["external_id"])
                continue
            update_snapshot(conn, item["external_id"], found[0])
            refreshed += 1
        conn.commit()
    logger.info("Refresh completed. refreshed=%s skipped=%s", refreshed, skipped)


def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Google Places Market Sweep for Montevideo")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_cmd = subparsers.add_parser("run", help="Execute sweep over grid")
    run_cmd.add_argument("--step-km", type=float, default=2.0, help="Grid step in kilometers")
    run_cmd.add_argument("--radius-m", type=int, default=1500, help="Search radius in meters")
    run_cmd.add_argument("--sleep", type=float, default=0.1, help="Sleep between API calls")

    refresh_cmd = subparsers.add_parser("refresh", help="Refresh expired snapshots")
    refresh_cmd.add_argument("--ttl-days", type=int, default=30, help="TTL in days for cached results")

    return parser.parse_args(argv)


def main(argv: Iterable[str]) -> None:
    args = parse_args(argv)
    config = load_config()
    config.step_km = getattr(args, "step_km", config.step_km)
    config.radius_m = getattr(args, "radius_m", config.radius_m)
    config.sleep_seconds = getattr(args, "sleep", config.sleep_seconds)

    if args.command == "run":
        sweep(config)
    elif args.command == "refresh":
        refresh_expired(config, ttl_days=args.ttl_days)
    else:
        raise ValueError(f"Unsupported command {args.command}")


if __name__ == "__main__":
    main(sys.argv[1:])
