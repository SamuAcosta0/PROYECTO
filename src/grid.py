"""Grid utilities to cover a bounding box over Montevideo."""
from __future__ import annotations

import math
from typing import Generator, Iterable, Tuple


def km_to_latitude_degrees(km: float) -> float:
    return km / 111.32


def km_to_longitude_degrees(km: float, latitude: float) -> float:
    return km / (111.32 * math.cos(math.radians(latitude)))


def generate_grid(
    lat_min: float,
    lat_max: float,
    lon_min: float,
    lon_max: float,
    step_km: float,
) -> Iterable[Tuple[float, float]]:
    """Yield grid points spaced by step_km across the bounding box.

    The grid is inclusive of the bounding box edges.
    """
    lat = lat_min
    while lat <= lat_max:
        lon = lon_min
        lon_step = km_to_longitude_degrees(step_km, lat)
        while lon <= lon_max:
            yield (round(lat, 6), round(lon, 6))
            lon += lon_step
        lat += km_to_latitude_degrees(step_km)
