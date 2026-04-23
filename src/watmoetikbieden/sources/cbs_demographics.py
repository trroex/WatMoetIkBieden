"""
CBS Kerncijfers Wijken en Buurten 2024 (85984NED).

Fetches neighbourhood demographics via the CBS OData v1 API.
Falls back from buurt → wijk → gemeente if a level has no data.

Caching: .cache/cbs/{code}.json with a 7-day TTL.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx

from watmoetikbieden.models import CbsDemographicsResult

_BASE = "https://datasets.cbs.nl/odata/v1/CBS/85984NED"
_CACHE_DIR = Path(".cache/cbs")
_CACHE_TTL_DAYS = 7

# CBS measure code → our field name
_MEASURES: dict[str, str] = {
    "T001036":   "inwoners",             # total population (count)
    "M000100":   "bevolkingsdichtheid",  # pop density per km²
    "10680":     "n_0_15",               # count 0–14 yr
    "53050":     "n_15_25",              # count 15–24 yr
    "53715":     "n_45_65",              # count 45–64 yr
    "80200":     "n_65plus",             # count 65+
    "1050010_2": "huishoudens_totaal",   # total households
    "1050015":   "eenpersoonshuishoudens",
    "M000114":   "gem_huishoudensgrootte",
    "1014800":   "koopwoningen_pct",     # already a % in source
    "1014850_2": "huurwoningen_pct",     # already a %
    "M000224":   "gem_inkomen_inwoner",  # in €1 000
    "ST0001":    "stedelijkheid",        # 1 (very urban) – 5 (rural)
    "ST0003":    "oad",                  # addresses per km²
    "M000297":   "woningvoorraad",
    "M001642":   "gem_woz_waarde",       # in €1 000
}


def _diag(msg: str) -> None:
    print(f"[CBS] {msg}", file=sys.stderr, flush=True)


def _cache_path(code: str) -> Path:
    return _CACHE_DIR / f"{code}.json"


def _load_cache(code: str) -> dict | None:
    p = _cache_path(code)
    if not p.exists():
        return None
    if (time.time() - p.stat().st_mtime) / 86400 > _CACHE_TTL_DAYS:
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def _save_cache(code: str, data: dict) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(code).write_text(
        json.dumps(data, ensure_ascii=False), encoding="utf-8"
    )


async def _fetch_raw(code: str, client: httpx.AsyncClient) -> dict[str, float | str | None]:
    cached = _load_cache(code)
    if cached is not None:
        _diag(f"cache hit: {code}")
        return cached

    _diag(f"fetching {code} from CBS OData…")
    params = {"$filter": f"WijkenEnBuurten eq '{code}'"}
    try:
        resp = await client.get(f"{_BASE}/Observations", params=params)
        resp.raise_for_status()
    except Exception as exc:
        _diag(f"fetch error for {code}: {exc}")
        return {}

    rows = resp.json().get("value", [])
    raw: dict[str, float | str | None] = {}
    for row in rows:
        val = row.get("Value") if row.get("Value") is not None else row.get("StringValue")
        raw[row["Measure"]] = val

    _save_cache(code, raw)
    return raw


def _extract(raw: dict, buurtcode: str, level: str) -> CbsDemographicsResult:
    """Map raw CBS measure codes to named fields; compute derived percentages."""
    def get(code: str) -> float | None:
        v = raw.get(code)
        return float(v) if v is not None else None

    inwoners = get("T001036")

    def pct_of_pop(count: float | None) -> float | None:
        if count is None or not inwoners:
            return None
        return round(count / inwoners * 100, 1)

    huish = get("1050010_2")

    def pct_of_huish(count: float | None) -> float | None:
        if count is None or not huish:
            return None
        return round(count / huish * 100, 1)

    return CbsDemographicsResult(
        code=buurtcode,
        data_level=level,
        inwoners=int(inwoners) if inwoners else None,
        bevolkingsdichtheid=get("M000100"),
        pct_0_15=pct_of_pop(get("10680")),
        pct_15_25=pct_of_pop(get("53050")),
        pct_45_65=pct_of_pop(get("53715")),
        pct_65plus=pct_of_pop(get("80200")),
        huishoudens_totaal=int(huish) if huish else None,
        eenpersoonshuishoudens_pct=pct_of_huish(get("1050015")),
        gem_huishoudensgrootte=get("M000114"),
        koopwoningen_pct=get("1014800"),
        huurwoningen_pct=get("1014850_2"),
        gem_inkomen_inwoner=get("M000224"),   # €1 000 units
        stedelijkheid=int(get("ST0001")) if get("ST0001") else None,
        oad=get("ST0003"),
        woningvoorraad=int(get("M000297")) if get("M000297") else None,
        gem_woz_waarde=get("M001642"),        # €1 000 units
        raw_all=raw,
    )


async def fetch_demographics(
    buurtcode: str,
    wijkcode: str | None,
    gemeentecode: str | None,
    client: httpx.AsyncClient,
) -> CbsDemographicsResult | None:
    """
    Try buurt, then wijk, then gemeente.
    Returns None if all levels fail.
    """
    levels = [
        (buurtcode, "buurt"),
        *(([(wijkcode, "wijk")] if wijkcode else [])),
        *(([(gemeentecode, "gemeente")] if gemeentecode else [])),
    ]

    for code, level in levels:
        raw = await _fetch_raw(code, client)
        if not raw:
            continue
        # Check we actually got population data (the key indicator)
        if raw.get("T001036") is None:
            _diag(f"{code} ({level}): no population data — trying next level")
            continue
        _diag(f"using {level} data: {code}")
        return _extract(raw, buurtcode, level)

    _diag(f"no usable CBS data for {buurtcode}")
    return None
