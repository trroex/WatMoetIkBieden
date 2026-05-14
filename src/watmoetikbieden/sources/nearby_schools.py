"""
Nearby educational facilities via the OpenStreetMap Overpass API.

Queries a configurable radius around a WGS84 coordinate pair for all
education-related amenities (schools, kindergartens, colleges, universities)
and returns them sorted by walking distance.

No API key required.  Overpass is a free public service — keep queries
reasonable (radius ≤ 5 km, cache results).

Categories returned
───────────────────
  "Basisschool"                 amenity=school, ISCED 1 / name heuristics
  "Middelbare school"           amenity=school, ISCED 2-3 / name heuristics
  "School (onbekend type)"      amenity=school, type undetermined
  "Kinderopvang / peuterspeelzaal"  amenity=kindergarten
  "MBO / Vakschool"             amenity=college
  "Universiteit / Hogeschool"   amenity=university

Caching
───────
Results are cached in .cache/overpass/ as JSON keyed by (lat, lon, radius).
TTL: 7 days.  Stale entries are silently refreshed.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import FrozenSet

_OVERPASS_URL = "https://overpass-api.de/api/interpreter"
_CACHE_DIR    = Path(".cache/overpass")
_CACHE_TTL    = 7 * 24 * 3600   # 7 days

DEFAULT_RADIUS_M = 1500   # metres; covers a comfortable walking catchment area


# ── data model ─────────────────────────────────────────────────────────────────

@dataclass
class NearbySchool:
    name:       str
    category:   str
    distance_m: int
    walk_min:   int        # estimated walking time at 5 km/h
    lat:        float
    lon:        float
    website:    str | None = None
    phone:      str | None = None
    operator:   str | None = None
    # DUO enrichment (filled by enrich_with_duo_ratings)
    brin:         str | None = None
    denominatie:  str | None = None
    rating:       str | None = None
    rating_label: str | None = None


# ── coordinate helpers ──────────────────────────────────────────────────────────

def parse_bag_centroide(centroide_ll: str | None) -> tuple[float, float] | None:
    """
    Parse a BAG WKT centroid string to (lat, lon).

    The BAG Locatieserver returns coordinates as 'POINT(lon lat)' in WGS84,
    i.e. longitude first.  We return (lat, lon) for consistency with haversine.
    """
    if not centroide_ll:
        return None
    m = re.match(r"POINT\(([+-]?\d+\.?\d*)\s+([+-]?\d+\.?\d*)\)", centroide_ll.strip())
    if m:
        return float(m.group(2)), float(m.group(1))   # lat, lon
    return None


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in metres between two WGS84 points."""
    R = 6_371_000.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi   = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


# ── category heuristics ────────────────────────────────────────────────────────

# Name fragments that strongly suggest primary school / IKC
_PO_TOKENS = {
    "basisschool", "basisonder", "obs ", "cbs ", "prot. chr", "protestants",
    "openbare school", "montessori", "dalton", "jenaplan", "vrije school",
    "rudolf steiner", "pestalozzi", "de borg", "de rank", "de beuk",
}

# Name/IKC fragments that suggest a kindcentrum (peuterspeelzaal + basisschool)
_IKC_TOKENS = {
    "kindcentrum", "kindercentrum", "ikc", "integraal kind", "brede school",
    "kindplein", "kindcampus",
}

# Name fragments that strongly suggest secondary school
_VO_TOKENS = {
    "gymnasium", "lyceum", "atheneum", "havo", "vmbo", "vwo", "mavo",
    "middelbaar", "scholengemeen", "college", "scholengemeenschap",
    "unilocatie", "vmbo-t", "vmbo-k", "vmbo-b",
}

# Operators whose schools are (almost) always speciaal onderwijs
_SO_OPERATORS = {
    "koninklijke kentalis", "kentalis",
    "royal dutch visio", "visio",
    "bartimeus",
    "la salle",
    "mytylschool",
    "heliomare",
}

# Name fragments that strongly suggest speciaal onderwijs
_SO_TOKENS = {
    "speciaal onderwijs", "speciale school",
    "sbo ",          # speciaal basisonderwijs (often "sbo de ...")
    "cluster ",      # cluster 1-4 SO schools
    "kentalis",
    "mytyl",
    "vso ",          # voortgezet speciaal onderwijs
    "so-school",
    "behandelschool",
    "orthopedagogie",
}


def _parse_isced_levels(isced: str) -> frozenset[int]:
    """
    Parse compound OSM isced:level values to a set of ISCED level ints.

    Handles:
      "1"     → {1}
      "0;1"   → {0, 1}
      "2;3"   → {2, 3}
      "0-2"   → {0, 1, 2}  (range notation)
      ""      → {}
    """
    if not isced:
        return frozenset()
    # Range notation "a-b"
    m = re.match(r"^(\d+)-(\d+)$", isced.strip())
    if m:
        return frozenset(range(int(m.group(1)), int(m.group(2)) + 1))
    # Semicolon-separated "a;b;c"
    levels: set[int] = set()
    for part in isced.split(";"):
        try:
            levels.add(int(part.strip()))
        except ValueError:
            pass
    return frozenset(levels)


def _categorize(tags: dict) -> str:
    amenity    = tags.get("amenity", "")
    name       = (tags.get("name") or "").lower()
    isced      = tags.get("isced:level", "")
    school_tag = tags.get("school:nl:type", "").lower()
    operator   = (tags.get("operator") or "").lower()

    if amenity == "kindergarten":
        return "Kinderopvang / peuterspeelzaal"
    if amenity == "university":
        return "Universiteit / Hogeschool"
    if amenity == "college":
        return "MBO / Vakschool"

    if amenity == "school":
        # ── Speciaal onderwijs: check operator + name first (high precision) ──
        if any(op in operator for op in _SO_OPERATORS):
            return "Speciaal onderwijs"
        if any(tok in name for tok in _SO_TOKENS):
            return "Speciaal onderwijs"
        if "speciaal" in school_tag or "sbo" in school_tag:
            return "Speciaal onderwijs"

        # ── ISCED level — parse compound values ───────────────────────────────
        levels = _parse_isced_levels(isced)

        if levels == {0}:
            return "Kinderopvang / peuterspeelzaal"
        if levels == {1}:
            return "Basisschool"
        if levels == {0, 1}:
            # Many basisscholen host a peuterspeelzaal and get isced=0;1 in OSM.
            # Default to Basisschool; only use Kindcentrum / IKC when the name
            # explicitly says so (e.g. "IKC De Wegwijzer", "Kindcentrum Meander").
            if any(tok in name for tok in _IKC_TOKENS):
                return "Kindcentrum / IKC"
            return "Basisschool"
        if levels and levels <= {2, 3}:
            return "Middelbare school"
        # Ranges like {0,1,2} or {1,2} typically indicate SO spanning levels
        if levels and max(levels) <= 2 and 1 in levels and 3 not in levels:
            # e.g. isced=0-2 without VO signals → likely SO
            return "Speciaal onderwijs"

        # ── school:nl:type tag ────────────────────────────────────────────────
        if "basis" in school_tag:
            return "Basisschool"
        if any(w in school_tag for w in ("voortgezet", "middelbaar", "vwo", "havo", "vmbo", "gymnasium", "lyceum")):
            return "Middelbare school"

        # ── Name heuristics (lowest confidence) ──────────────────────────────
        if any(tok in name for tok in _IKC_TOKENS):
            return "Kindcentrum / IKC"
        if any(tok in name for tok in _PO_TOKENS):
            return "Basisschool"
        if any(tok in name for tok in _VO_TOKENS):
            return "Middelbare school"

        return "School (onbekend type)"

    return "Onderwijsinstelling"


# ── caching ────────────────────────────────────────────────────────────────────

def _cache_key(lat: float, lon: float, radius_m: int) -> str:
    raw = f"{lat:.5f},{lon:.5f},{radius_m}"
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_path(key: str) -> Path:
    return _CACHE_DIR / f"{key}.json"


def _load_cache(key: str) -> list[dict] | None:
    p = _cache_path(key)
    if not p.exists():
        return None
    if time.time() - p.stat().st_mtime > _CACHE_TTL:
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def _save_cache(key: str, data: list[dict]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(key).write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


# ── Overpass fetch ─────────────────────────────────────────────────────────────

def _fetch_overpass(lat: float, lon: float, radius_m: int) -> list[dict]:
    import json
    import urllib.parse
    import urllib.request

    query = (
        f"[out:json][timeout:30];\n"
        f"(\n"
        f'  node["amenity"~"^(school|kindergarten|college|university)$"]'
        f"(around:{radius_m},{lat},{lon});\n"
        f'  way["amenity"~"^(school|kindergarten|college|university)$"]'
        f"(around:{radius_m},{lat},{lon});\n"
        f");\n"
        f"out center tags;\n"
    )
    url = _OVERPASS_URL + "?" + urllib.parse.urlencode({"data": query})
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "WatMoetIkBieden/1.0"},
    )
    with urllib.request.urlopen(req, timeout=35) as resp:
        return json.loads(resp.read()).get("elements", [])


# ── public API ─────────────────────────────────────────────────────────────────

def fetch_nearby_schools(
    lat: float,
    lon: float,
    radius_m: int = DEFAULT_RADIUS_M,
) -> list[NearbySchool]:
    """
    Return all educational facilities within *radius_m* metres of (lat, lon),
    sorted by walking distance.

    Results are cached for 7 days.  Duplicates (OSM node + way for the same
    building) are de-duplicated by name.

    Parameters
    ----------
    lat, lon    WGS84 coordinates of the reference address.
    radius_m    Search radius in metres (default 1 500 m).
    """
    key = _cache_key(lat, lon, radius_m)
    cached = _load_cache(key)

    if cached is not None:
        elements = cached
    else:
        elements = _fetch_overpass(lat, lon, radius_m)
        _save_cache(key, elements)

    schools: list[NearbySchool] = []
    seen_names: set[str] = set()

    for el in elements:
        tags = el.get("tags", {})
        name = (tags.get("name") or tags.get("brand") or "").strip()
        if not name:
            continue

        # Extract coordinates (node → lat/lon directly; way → center)
        if el["type"] == "node":
            el_lat, el_lon = el.get("lat"), el.get("lon")
        else:
            center = el.get("center", {})
            el_lat, el_lon = center.get("lat"), center.get("lon")

        if el_lat is None or el_lon is None:
            continue

        dist = _haversine_m(lat, lon, el_lat, el_lon)
        category = _categorize(tags)
        walk_min = max(1, round(dist / (5000 / 60)))   # 5 km/h

        # De-duplicate: keep closest entry per name
        if name in seen_names:
            continue
        seen_names.add(name)

        schools.append(NearbySchool(
            name=name,
            category=category,
            distance_m=round(dist),
            walk_min=walk_min,
            lat=el_lat,
            lon=el_lon,
            website=tags.get("website") or tags.get("contact:website"),
            phone=tags.get("phone") or tags.get("contact:phone"),
            operator=tags.get("operator"),
        ))

    return sorted(schools, key=lambda s: s.distance_m)


# ── DUO rating enrichment ──────────────────────────────────────────────────────

def enrich_with_duo_ratings(
    schools: list[NearbySchool],
    gemeente: str,
) -> list[NearbySchool]:
    """
    Look up each school in the DUO vestigingen + inspection ratings index and
    attach brin, denominatie, rating, and rating_label to matching records.

    Only Basisschool and Middelbare school categories are attempted — DUO's
    dataset covers PO and VO; kindergartens/colleges/universities are skipped.

    Parameters
    ----------
    schools     List returned by fetch_nearby_schools().
    gemeente    Municipality name from BAG (a.gemeentenaam).  Used to restrict
                the fuzzy search to the right gemeente.
    """
    from watmoetikbieden.sources.duo_schools import lookup_school

    eligible = {"Basisschool", "Middelbare school", "School (onbekend type)"}

    for s in schools:
        if s.category not in eligible:
            continue
        info = lookup_school(s.name, gemeente)
        if info:
            s.brin         = info.brin
            s.denominatie  = info.denominatie
            s.rating       = info.rating
            s.rating_label = info.rating_label

    return schools
