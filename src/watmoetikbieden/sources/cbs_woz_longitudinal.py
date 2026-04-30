"""
Fetch gem_woz_waarde (M001642) for a buurt across CBS yearly datasets.

Synchronous httpx — called once per CLI run, after the async wave is done.
Results are cached permanently in .cache/cbs_longitudinal/ (same cache as
the standalone scripts/fetch_cbs_longitudinal.py).
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx

_API_BASE   = "https://datasets.cbs.nl/odata/v1/CBS"
_CACHE_DIR  = Path(".cache/cbs_longitudinal")
_MEASURE    = "M001642"   # gem_woz_waarde (stored in CBS as x1 000 EUR)

# Datasets that contain M001642 (2016 onward for most buurten)
_DATASETS: list[tuple[int, str]] = [
    (2013, "82339NED"),
    (2014, "82931NED"),
    (2015, "83220NED"),
    (2016, "83487NED"),
    (2017, "83765NED"),
    (2018, "84286NED"),
    (2019, "84583NED"),
    (2020, "84799NED"),
    (2021, "85039NED"),
    (2022, "85318NED"),
    (2023, "85618NED"),
    (2024, "85984NED"),
    (2025, "86165NED"),
]

# Older datasets use a different filter field name
_FILTER_FIELD: dict[str, str] = {
    "82339NED": "RegioS",
}


def _diag(msg: str) -> None:
    print(f"[CBS-WOZ-LNG] {msg}", file=sys.stderr, flush=True)


def _cache_path(dataset_id: str, suffix: str) -> Path:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return _CACHE_DIR / f"{dataset_id}_{suffix}.json"


def _load(p: Path):
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def _save(p: Path, data) -> None:
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def _fetch_year(
    client: httpx.Client,
    year: int,
    dataset_id: str,
    buurtcode: str,
    wijkcode: str | None,
    gemeentecode: str | None,
) -> float | None:
    """Return gem_woz_waarde in EUR (already multiplied by 1000), or None."""
    ff = _FILTER_FIELD.get(dataset_id, "WijkenEnBuurten")

    levels: list[tuple[str, str]] = [(buurtcode, "buurt")]
    if wijkcode:
        levels.append((wijkcode, "wijk"))
    if gemeentecode:
        levels.append((gemeentecode, "gemeente"))

    seen: set[str] = set()
    for code, label in levels:
        if label in seen:
            continue

        cp = _cache_path(dataset_id, f"woz_{code.strip()}_{label}")
        cached = _load(cp)
        if cached is not None:
            if cached:
                v = cached.get(_MEASURE)
                if v is not None:
                    return float(v) * 1000
            seen.add(label)
            continue

        # Try with trailing space too (some old datasets need it)
        for attempt_code in ([code, code + " "] if label == "buurt" else [code]):
            try:
                r = client.get(
                    f"{_API_BASE}/{dataset_id}/Observations",
                    params={
                        "$filter": f"{ff} eq '{attempt_code}'",
                        "$select": "Measure,Value",
                    },
                    timeout=20,
                )
                r.raise_for_status()
                rows = r.json().get("value", [])
            except Exception as exc:
                _diag(f"[{year}] ERROR {label}: {exc}")
                time.sleep(0.5)
                continue

            if rows:
                result_map = {row["Measure"]: row.get("Value") for row in rows}
                _save(cp, result_map)
                time.sleep(0.3)
                v = result_map.get(_MEASURE)
                if v is not None:
                    return float(v) * 1000
                break   # data exists but measure absent at this level
            else:
                time.sleep(0.3)

        seen.add(label)

    return None


def fetch_gem_woz_series(
    buurtcode: str,
    wijkcode: str | None = None,
    gemeentecode: str | None = None,
) -> dict[int, float]:
    """
    Return {year: gem_woz_waarde_EUR} for the given buurt, fetching only what
    isn't already in the cache.  Levels tried: buurt → wijk → gemeente.
    """
    result: dict[int, float] = {}
    missing_years = []

    for year, dataset_id in _DATASETS:
        # Check all cache files for this dataset to see if we already have a value
        found = False
        for label in ("buurt", "wijk", "gemeente"):
            code = buurtcode if label == "buurt" else (wijkcode if label == "wijk" else gemeentecode)
            if not code:
                continue
            cp = _cache_path(dataset_id, f"woz_{code.strip()}_{label}")
            cached = _load(cp)
            if cached:
                v = cached.get(_MEASURE)
                if v is not None:
                    result[year] = float(v) * 1000
                    found = True
                    break
        if not found:
            missing_years.append((year, dataset_id))

    if missing_years:
        _diag(f"fetching {len(missing_years)} missing year(s) from CBS…")
        with httpx.Client(timeout=25) as client:
            for year, dataset_id in missing_years:
                v = _fetch_year(client, year, dataset_id, buurtcode, wijkcode, gemeentecode)
                if v is not None:
                    result[year] = v

    return result
