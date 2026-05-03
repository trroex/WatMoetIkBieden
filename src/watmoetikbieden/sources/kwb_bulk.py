"""
CBS 86165NED bulk loader – all buurten, 7 selected measures.

Fetches the measures needed for regional OLS in one API call (long format),
pivots to wide format, enriches each buurt with its province name via a
CBS Gebiedsindelingen mapping, and caches the result as a CSV.

Cache: .cache/kwb_bulk/buurten_2024.csv   (TTL 365 days)

API calls
─────────
1. Observations (long format, ~24 000 rows – well within $top=100 000):
     https://datasets.cbs.nl/odata/v1/CBS/86165NED/Observations
       ?$filter=startswith(WijkenEnBuurten,'BU')
         and Measure in ('M001642','ZW25805','ZW25806','ZW10300','ZW10320','ZW10340','M000297')
       &$select=WijkenEnBuurten,Measure,Value

2. Gemeente → provincie (CBS Gebiedsindelingen 2024, dataset 85215NED):
     https://opendata.cbs.nl/ODataApi/odata/85215NED/UntypedDataSet
       ?$select=GemeentecodeGM,ProvincienaamPV

No new package dependencies: uses httpx (already in core deps).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

# ── cache locations ────────────────────────────────────────────────────────────
_CACHE_DIR = Path(".cache/kwb_bulk")
_CSV_CACHE = _CACHE_DIR / "buurten_2025.csv"    # 86165NED (KWB 2025)
_META_FILE = _CACHE_DIR / "buurten_2025.meta.json"
_TTL_SECS  = 365 * 24 * 3600   # 1 year; data is published annually

# ── CBS measure code → wide-format column name ────────────────────────────────
MEASURE_COLS: dict[str, str] = {
    "M001642": "gem_woz_waarde",           # average WOZ in €1 000
    "ZW25805": "pct_tussenwoning",
    "ZW25806": "pct_hoekwoning",
    "ZW10300": "pct_twee_onder_een_kap",
    "ZW10320": "pct_vrijstaand",
    "ZW10340": "pct_meergezinswoning",
    "M000297": "woningvoorraad",           # housing stock (used as OLS weight)
}

_WIDE_COLS = ["buurtcode", "provincie"] + list(MEASURE_COLS.values())


def _diag(msg: str) -> None:
    import sys
    print(f"[KWB] {msg}", file=sys.stderr, flush=True)


# ── cache helpers ─────────────────────────────────────────────────────────────

def _is_cache_fresh() -> bool:
    if not _CSV_CACHE.exists() or not _META_FILE.exists():
        return False
    meta = json.loads(_META_FILE.read_text(encoding="utf-8"))
    return (time.time() - meta.get("fetched_at", 0)) < _TTL_SECS


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def _fetch_odata_all(url: str, label: str) -> list[dict]:
    """
    Fetch all pages of an OData endpoint, following @odata.nextLink.

    The full URL (including query string) must be passed as-is — do NOT use
    httpx's params= dict, because httpx encodes '$' in param keys as '%24',
    which breaks OData query options ($filter, $select, $top).
    """
    import httpx

    rows: list[dict] = []
    current: str | None = url
    page = 1
    while current:
        _diag(f"  {label} – page {page} …")
        resp = httpx.get(current, timeout=120, follow_redirects=True)
        resp.raise_for_status()
        data = resp.json()
        rows.extend(data.get("value", []))
        current = data.get("@odata.nextLink")
        page += 1
    _diag(f"  {label}: {len(rows):,} rows total")
    return rows


# ── build and cache ───────────────────────────────────────────────────────────

def _fetch_gemeente_to_provincie() -> dict[str, str]:
    """
    Return {GM-code → province name}, e.g. {'GM0845': 'Noord-Brabant'}.

    Uses CBS 84992NED (Gebiedsindelingen) which maps woonplaatsen to gemeente
    (Code_3) and province (Naam_4).  Multiple woonplaatsen per gemeente are
    fine — they all map to the same province so duplicates are harmless.
    """
    url = (
        "https://opendata.cbs.nl/ODataApi/odata/84992NED/UntypedDataSet"
        "?$select=Code_3,Naam_4&$top=5000"
    )
    rows = _fetch_odata_all(url, label="gemeente→provincie (84992NED)")
    mapping: dict[str, str] = {}
    for row in rows:
        gm = (row.get("Code_3") or "").strip()   # e.g. "GM0845"
        pv = (row.get("Naam_4") or "").strip()   # e.g. "Noord-Brabant"
        if gm.startswith("GM") and pv:
            mapping[gm] = pv
    _diag(f"  gemeente→provincie: {len(mapping)} gemeenten")
    return mapping


def _fetch_and_cache() -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Gemeente → provincie
    gm_to_pv = _fetch_gemeente_to_provincie()

    # 2. Bulk observations (long format)
    # OData $filter and $select must be part of the URL string — see _fetch_odata_all.
    obs_url = (
        "https://datasets.cbs.nl/odata/v1/CBS/86165NED/Observations"
        "?$filter=startswith(WijkenEnBuurten,'BU')"
        " and Measure in ('M001642','ZW25805','ZW25806',"
        "'ZW10300','ZW10320','ZW10340','M000297')"
        "&$select=WijkenEnBuurten,Measure,Value"
        "&$top=100000"
    )
    obs = _fetch_odata_all(obs_url, label="86165NED observations")

    # 3. Pivot long → wide
    records: dict[str, dict[str, Any]] = {}
    for row in obs:
        bu      = (row.get("WijkenEnBuurten") or "").strip()
        measure = (row.get("Measure") or "").strip()
        value   = row.get("Value")
        if not bu or measure not in MEASURE_COLS:
            continue
        if bu not in records:
            records[bu] = {"buurtcode": bu}
        records[bu][MEASURE_COLS[measure]] = value

    # 4. Enrich with provincie: buurtcode[2:6] → 'GM{code}' → province
    #    e.g. 'BU08450002' → 'GM0845' → 'Noord-Brabant'
    for bu, rec in records.items():
        gm_code = "GM" + bu[2:6]
        rec["provincie"] = gm_to_pv.get(gm_code, "")

    # 5. Write CSV
    import csv
    with _CSV_CACHE.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_WIDE_COLS, extrasaction="ignore")
        writer.writeheader()
        for rec in records.values():
            writer.writerow({col: rec.get(col, "") for col in _WIDE_COLS})

    # 6. Write metadata
    _META_FILE.write_text(
        json.dumps({"fetched_at": time.time(), "n_rows": len(records)}),
        encoding="utf-8",
    )
    _diag(f"cached {len(records):,} buurten → {_CSV_CACHE}")


# ── public API ────────────────────────────────────────────────────────────────

def load_buurten_df():
    """
    Return a wide-format DataFrame with one row per buurt.

    Columns
    -------
    buurtcode          str   e.g. 'BU08450002'
    provincie          str   e.g. 'Noord-Brabant'
    gem_woz_waarde     float average WOZ in €1 000 (NaN when CBS reports missing)
    woningvoorraad     float housing stock (used as OLS weight)
    pct_tussenwoning   float
    pct_hoekwoning     float
    pct_twee_onder_een_kap float
    pct_vrijstaand     float
    pct_meergezinswoning   float

    Data is downloaded from CBS on first call; cached for 365 days.
    """
    import pandas as pd

    if not _is_cache_fresh():
        _fetch_and_cache()

    df = pd.read_csv(_CSV_CACHE, dtype={"buurtcode": str, "provincie": str})
    numeric = list(MEASURE_COLS.values())
    df[numeric] = df[numeric].apply(pd.to_numeric, errors="coerce")
    return df
