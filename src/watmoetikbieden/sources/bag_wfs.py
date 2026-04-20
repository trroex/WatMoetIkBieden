"""
BAG building characteristics via the PDOK BAG WFS v2.0 service.

Docs: https://service.pdok.nl/lv/bag/wfs/v2_0?SERVICE=WFS&REQUEST=GetCapabilities
No API key required.

CQL_FILTER IS BROKEN on this GeoServer instance — it silently returns the
same first record in the dataset regardless of the filter value. Confirmed by
fetching with three different filters (identificatie, postcode+huisnummer,
postcode+huisnummer URL-encoded) and always receiving verblijfsobject
0003010000125985 (Appingedam).

Workaround: use the WFS BBOX parameter with RD New coordinates from the
locatieserver (centroide_rd). After fetching we verify the returned
identificatie matches the expected adresseerbaarobject_id.
"""

from __future__ import annotations
import re

import httpx

from watmoetikbieden.models import BagPand, BagVerblijfsobject

_WFS_BASE = "https://service.pdok.nl/lv/bag/wfs/v2_0"
_RD_CRS = "urn:ogc:def:crs:EPSG::28992"
_BBOX_BUFFER_M = 12.0  # metres; small enough to avoid neighbour, large enough for big parcels


def _parse_rd_bbox(centroide_rd: str) -> str:
    """
    Turn 'POINT(120964 487732)' into the BBOX string expected by WFS.
    Buffer is intentionally small – we match by identificatie afterwards.
    """
    m = re.match(r"POINT\(([0-9.]+)\s+([0-9.]+)\)", centroide_rd)
    if not m:
        raise ValueError(f"Cannot parse centroide_rd: {centroide_rd!r}")
    x, y = float(m.group(1)), float(m.group(2))
    b = _BBOX_BUFFER_M
    return f"{x-b},{y-b},{x+b},{y+b},{_RD_CRS}"


def _base_params(type_name: str, bbox: str, count: int = 10) -> dict:
    return {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeName": type_name,
        "outputFormat": "application/json",
        "count": str(count),
        "BBOX": bbox,
    }


async def fetch_verblijfsobject(
    centroide_rd: str,
    expected_id: str,
    client: httpx.AsyncClient,
) -> tuple[BagVerblijfsobject | None, dict]:
    """
    Fetch unit-level data (floor area, usage) using a spatial BBOX query.

    centroide_rd  – WKT point string from locatieserver, e.g. 'POINT(120964 487732)'
    expected_id   – adresseerbaarobject_id from locatieserver; used to pick the
                    correct feature when multiple verblijfsobjecten share a bbox.
    """
    bbox = _parse_rd_bbox(centroide_rd)
    params = _base_params("bag:verblijfsobject", bbox)

    resp = await client.get(_WFS_BASE, params=params)
    resp.raise_for_status()
    raw = resp.json()

    features = raw.get("features", [])
    # Match by identificatie so we're not fooled by a neighbouring unit
    matching = [f for f in features if f.get("properties", {}).get("identificatie") == expected_id]
    if not matching:
        return None, raw

    props = matching[0]["properties"]

    raw_pand = props.get("pandidentificatie", [])
    if isinstance(raw_pand, str):
        raw_pand = [raw_pand]

    raw_gebruik = props.get("gebruiksdoel", [])
    if isinstance(raw_gebruik, str):
        raw_gebruik = [raw_gebruik]

    vbo = BagVerblijfsobject(
        identificatie=props.get("identificatie"),
        oppervlakte=props.get("oppervlakte"),
        gebruiksdoel=raw_gebruik,
        status=props.get("status"),
        pandidentificatie=raw_pand,
        bouwjaar=props.get("bouwjaar"),
    )
    return vbo, raw


async def fetch_pand(
    centroide_rd: str,
    expected_pand_id: str,
    client: httpx.AsyncClient,
) -> tuple[BagPand | None, dict]:
    """
    Fetch building-level data (build year, unit count) using a spatial BBOX query.

    expected_pand_id – pandidentificatie from the verblijfsobject response.
    """
    bbox = _parse_rd_bbox(centroide_rd)
    params = _base_params("bag:pand", bbox)

    resp = await client.get(_WFS_BASE, params=params)
    resp.raise_for_status()
    raw = resp.json()

    features = raw.get("features", [])
    matching = [f for f in features if f.get("properties", {}).get("identificatie") == expected_pand_id]
    if not matching:
        return None, raw

    props = matching[0]["properties"]
    pand = BagPand(
        identificatie=props.get("identificatie"),
        bouwjaar=props.get("bouwjaar"),
        status=props.get("status"),
        aantal_verblijfsobjecten=props.get("aantal_verblijfsobjecten"),
    )
    return pand, raw
