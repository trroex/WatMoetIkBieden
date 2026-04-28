"""
BRK (Basisregistratie Kadaster) parcel area via the PDOK kadastralekaart WFS v5.0.

No API key required.

The gekoppeld_perceel field from the BAG locatieserver contains strings like
'MCG00-G-1427' or 'ASD15-S-12345' encoding:
  {AKRKadastraleGemeenteCode}-{sectie}-{perceelnummer}

CQL_FILTER is broken on this service (same bug as BAG WFS), so we use a BBOX
spatial query and match the returned features by gemeente/sectie/perceelnummer.

Rate limiting: no stated limit; using the shared httpx.AsyncClient is fine.
"""

from __future__ import annotations

import re
import sys

import httpx

from watmoetikbieden.models import BrkPerceel

_WFS_BASE = "https://service.pdok.nl/kadaster/kadastralekaart/wfs/v5_0"
_RD_CRS = "urn:ogc:def:crs:EPSG::28992"
# Use a larger buffer than BAG WFS: parcels are area features whose centroid
# may not coincide with the building's centroid.
_BBOX_BUFFER_M = 80.0


def _diag(msg: str) -> None:
    print(f"[BRK] {msg}", file=sys.stderr, flush=True)


def _parse_perceel_ref(ref: str) -> tuple[str, str, int] | None:
    """
    Parse 'MCG00-G-1427' → ('MCG00', 'G', 1427).
    Returns None if the format is unrecognised.
    """
    m = re.match(r"^([A-Z0-9]+)-([A-Z]+)-(\d+)$", ref.strip())
    if not m:
        return None
    return m.group(1), m.group(2), int(m.group(3))


def _build_bbox(centroide_rd: str, buffer_m: float = _BBOX_BUFFER_M) -> str:
    """
    Turn 'POINT(120964 487732)' into the WFS BBOX query string.
    """
    m = re.match(r"POINT\(([0-9.]+)\s+([0-9.]+)\)", centroide_rd)
    if not m:
        raise ValueError(f"Cannot parse centroide_rd: {centroide_rd!r}")
    x, y = float(m.group(1)), float(m.group(2))
    return f"{x-buffer_m},{y-buffer_m},{x+buffer_m},{y+buffer_m},{_RD_CRS}"


async def fetch_perceel(
    centroide_rd: str,
    gekoppeld_perceel: list[str],
    client: httpx.AsyncClient,
) -> tuple[BrkPerceel | None, dict]:
    """
    Fetch the plot area for a property using the BRK kadastralekaart WFS.

    centroide_rd       – WKT point from BAG locatieserver (EPSG:28992)
    gekoppeld_perceel  – list of perceel references from BAG locatieserver,
                         e.g. ['ASD15-S-12345']

    Returns (BrkPerceel, raw_geojson) or (None, {}) on failure / no match.
    Only the matched feature is stored in raw to keep output small.
    """
    if not gekoppeld_perceel:
        _diag("no gekoppeld_perceel – skipping BRK fetch")
        return None, {}

    # Try each reference until one resolves
    for ref in gekoppeld_perceel:
        parsed = _parse_perceel_ref(ref)
        if parsed is None:
            _diag(f"unrecognised perceel reference format: {ref!r}")
            continue

        gemeente_code, sectie, perceelnummer = parsed
        _diag(
            f"looking up perceel {ref!r} "
            f"→ gemeente={gemeente_code!r} sectie={sectie!r} nr={perceelnummer}"
        )

        try:
            bbox = _build_bbox(centroide_rd)
        except ValueError as exc:
            _diag(str(exc))
            return None, {}

        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": "kadastralekaart:Perceel",
            "outputFormat": "application/json",
            "count": "20",
            "BBOX": bbox,
        }

        try:
            resp = await client.get(_WFS_BASE, params=params)
        except Exception as exc:
            _diag(f"request error: {type(exc).__name__}: {exc}")
            return None, {}

        _diag(f"WFS → HTTP {resp.status_code}")

        if resp.status_code != 200:
            _diag(f"unexpected status; body: {resp.text[:200]!r}")
            return None, {}

        try:
            raw = resp.json()
        except Exception:
            _diag(f"non-JSON response; body: {resp.text[:200]!r}")
            return None, {}

        features = raw.get("features", [])
        _diag(f"features in BBOX: {len(features)}")

        for f in features:
            props = f.get("properties", {})
            # perceelnummer may come back as int or string; normalise to int
            raw_nr = props.get("perceelnummer")
            try:
                feat_nr = int(raw_nr) if raw_nr is not None else None
            except (ValueError, TypeError):
                feat_nr = None

            if (
                props.get("AKRKadastraleGemeenteCodeWaarde") == gemeente_code
                and props.get("sectie") == sectie
                and feat_nr == perceelnummer
            ):
                grootte = props.get("kadastraleGrootteWaarde")
                _diag(f"matched perceel {ref!r}: grootte={grootte} m²")
                perceel = BrkPerceel(
                    kadastraleGemeenteCode=props.get("AKRKadastraleGemeenteCodeWaarde"),
                    sectie=props.get("sectie"),
                    perceelnummer=feat_nr,
                    kadastraleGrootteWaarde=grootte,
                )
                # Store only the matched feature to keep raw output small
                matched_raw = {"type": "Feature", "properties": props}
                return perceel, matched_raw

        _diag(
            f"no match in BBOX for {gemeente_code}/{sectie}/{perceelnummer}; "
            f"seen: {[(f['properties'].get('AKRKadastraleGemeenteCodeWaarde'), f['properties'].get('sectie'), f['properties'].get('perceelnummer')) for f in features]}"
        )

    return None, {}
