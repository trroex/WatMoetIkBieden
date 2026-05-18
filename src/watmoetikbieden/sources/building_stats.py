"""
CBS housing stock mutations per COROP region — tables 86054NED and 86084NED.

86054NED  Tracks how many homes are added (new builds, transformations) and
          removed (demolitions) from the housing stock each year.
          Geographic coverage: national, 4 landsdelen, 12 provinces,
          40 COROP regions, ~360 gemeenten.  Annual data, 2020–2024.

86084NED  Nieuwbouw and transformations by woningtype at COROP level.
          Woningtypes: tussenwoning, hoekwoning, 2-onder-1-kap,
          vrijstaand, meergezins.  Annual data, 2018–2024.

Supply pressure ratio (derived from 85819NED transactions passed in via pbk):
  supply_pressure = completions / annual_transactions × 100
  Low ratio  (<  8 %) → krappe markt  (little new supply vs demand)
  Mid ratio  (8–15 %) → gemiddeld
  High ratio (> 15 %) → ruime markt  (supply keeping up with demand)

Cache:
  .cache/pbk/bouw_GM{CODE}.json   TTL 7 days  (86054NED gemeente)
  .cache/pbk/bouwtype_{CR}.json   TTL 7 days  (86084NED COROP × woningtype)

License: CBS / NLOD — commercial use allowed with attribution.
"""

from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

from watmoetikbieden.sources.pbk_corop import (
    PbkCoropResult,
    get_corop_code,
    _load_corop_names,   # shared helper
    _CACHE_DIR,
    _DATA_TTL,
    _ODATA,
)

_BOUW_TABLE      = "86054NED"
_BOUW_TYPE_TABLE = "86084NED"

# Woningkenmerken codes in 86084NED that we care about
_TYPE_CODES: dict[str, str] = {
    "ZW25805": "Tussenwoning",
    "ZW25806": "Hoekwoning",
    "ZW10300": "2-onder-1-kap",
    "ZW10320": "Vrijstaand",
    "ZW10340": "Meergezins",
}
# Used to filter for totals across BewoningWoonruimte and Huishoudenskenmerken
_BEWONING_TOTAAL     = "T001697"
_HUISHOUDENS_TOTAAL  = "10000"


# ── data model ────────────────────────────────────────────────────────────────

@dataclass
class BuildingYear:
    year:             int
    nieuwbouw:        int | None    # new completions (gemeente, 86054NED)
    sloop:            int | None    # demolitions
    netto:            int | None    # net stock change (SaldoVoorraad)
    eindstand:        int | None    # total housing stock at year-end
    transactions:     int | None    # annual sales (from 85819NED, derived)
    supply_pressure:  float | None  # nieuwbouw / transactions × 100


@dataclass
class BuildingTypeYear:
    """COROP-level nieuwbouw by woningtype (86084NED)."""
    year:        int
    tussenwoning: int | None
    hoekwoning:   int | None
    twee_kap:     int | None    # 2-onder-1-kapwoning
    vrijstaand:   int | None
    meergezins:   int | None

    @property
    def totaal_eengezins(self) -> int | None:
        vals = [self.tussenwoning, self.hoekwoning, self.twee_kap, self.vrijstaand]
        known = [v for v in vals if v is not None]
        return sum(known) if known else None

    @property
    def grand_total(self) -> int | None:
        parts = [self.totaal_eengezins, self.meergezins]
        known = [v for v in parts if v is not None]
        return sum(known) if known else None


@dataclass
class BuildingStatsResult:
    gemeente_code: str              # zero-padded, e.g. "0796"
    corop_code:    str              # COROP used for transaction denominator
    corop_name:    str
    years:         list[BuildingYear]       # gemeente-level, ascending by year
    type_years:    list[BuildingTypeYear]   # COROP-level by woningtype, ascending

    @property
    def latest(self) -> BuildingYear | None:
        return self.years[-1] if self.years else None

    @property
    def supply_label(self) -> str:
        sp = (self.latest or BuildingYear(0,None,None,None,None,None,None)).supply_pressure
        if sp is None:
            return "Onbekend"
        if sp < 8:
            return "Krap"
        if sp < 15:
            return "Gemiddeld"
        return "Ruim"

    @property
    def supply_emoji(self) -> str:
        return {"Krap": "🔴", "Gemiddeld": "🟡", "Ruim": "🟢"}.get(
            self.supply_label, "⚪"
        )


# ── cache helpers (reuse pbk_corop pattern) ───────────────────────────────────

def _cache_read(name: str) -> list | None:
    p = _CACHE_DIR / f"{name}.json"
    if p.exists() and time.time() - p.stat().st_mtime < _DATA_TTL:
        return json.loads(p.read_text(encoding="utf-8"))
    return None


def _cache_write(name: str, data: list) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (_CACHE_DIR / f"{name}.json").write_text(
        json.dumps(data, ensure_ascii=False), encoding="utf-8"
    )


def _get(url: str) -> dict:
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "WatMoetIkBieden/1.0"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


# ── fetch 86054NED (gemeente-level totals) ────────────────────────────────────

def _fetch_bouw_gemeente(gm_code: str) -> list[dict]:
    """Fetch annual building rows for one gemeente from 86054NED.

    Parameters
    ----------
    gm_code   CBS RegioS prefix, e.g. ``'GM0796'`` (GM + zero-padded 4-digit code).
    """
    cache_key = f"bouw_{gm_code}"
    cached = _cache_read(cache_key)
    if cached is not None:
        return cached

    filter_expr = f"startswith(RegioS,'{gm_code}')"
    url = (
        f"{_ODATA}/{_BOUW_TABLE}/TypedDataSet"
        f"?$format=json&$filter={urllib.parse.quote(filter_expr)}"
    )
    rows = _get(url).get("value", [])
    _cache_write(cache_key, rows)
    return rows


# ── fetch 86084NED (COROP-level by woningtype) ────────────────────────────────

def _fetch_bouw_type_rows(corop_code: str) -> list[dict]:
    """Fetch annual nieuwbouw by woningtype for one COROP from 86084NED.

    Filters for totals across BewoningWoonruimte and Huishoudenskenmerken.
    """
    cache_key = f"bouwtype_{corop_code}"
    cached = _cache_read(cache_key)
    if cached is not None:
        return cached

    filter_expr = (
        f"startswith(RegioS,'{corop_code}')"
        f" and startswith(BewoningWoonruimte,'{_BEWONING_TOTAAL}')"
        f" and startswith(Huishoudenskenmerken,'{_HUISHOUDENS_TOTAAL}')"
    )
    url = (
        f"{_ODATA}/{_BOUW_TYPE_TABLE}/TypedDataSet"
        f"?$format=json&$filter={urllib.parse.quote(filter_expr)}"
    )
    rows = _get(url).get("value", [])
    _cache_write(cache_key, rows)
    return rows


def _parse_type_years(rows: list[dict]) -> list[BuildingTypeYear]:
    """
    Pivot 86084NED rows (one row per year × woningtype) into BuildingTypeYear objects.
    Only includes the five woningtype codes defined in _TYPE_CODES.
    """
    # {year: {type_label: count}}
    by_year: dict[int, dict[str, int | None]] = {}

    for row in rows:
        yr = _parse_year(str(row.get("Perioden", "")))
        if yr is None:
            continue
        wk = str(row.get("Woningkenmerken", "")).strip()
        label = _TYPE_CODES.get(wk)
        if label is None:
            continue
        val = row.get("Nieuwbouw_1")
        by_year.setdefault(yr, {})[label] = val

    result: list[BuildingTypeYear] = []
    for yr, vals in sorted(by_year.items()):
        result.append(BuildingTypeYear(
            year         = yr,
            tussenwoning = vals.get("Tussenwoning"),
            hoekwoning   = vals.get("Hoekwoning"),
            twee_kap     = vals.get("2-onder-1-kap"),
            vrijstaand   = vals.get("Vrijstaand"),
            meergezins   = vals.get("Meergezins"),
        ))
    return result


# ── period helpers ────────────────────────────────────────────────────────────

def _parse_year(period: str) -> int | None:
    """'2023JJ00' → 2023"""
    m = re.match(r"(\d{4})JJ", period.strip())
    return int(m.group(1)) if m else None


# ── derive annual transactions from PBK quarterly data ───────────────────────

def _annual_transactions(pbk: PbkCoropResult) -> dict[int, int]:
    """
    Sum quarterly sales counts from 85819NED by calendar year.
    Returns {year: total_transactions}.
    """
    totals: dict[int, int] = {}
    for pt in pbk.corop_data:
        m = re.match(r"(\d{4})KW", pt.period)
        if not m or pt.sales_count is None:
            continue
        yr = int(m.group(1))
        totals[yr] = totals.get(yr, 0) + pt.sales_count
    # Only include years with all 4 quarters present
    # (partial years give misleading annual totals)
    quarter_counts: dict[int, int] = {}
    for pt in pbk.corop_data:
        m = re.match(r"(\d{4})KW", pt.period)
        if m:
            yr = int(m.group(1))
            quarter_counts[yr] = quarter_counts.get(yr, 0) + 1
    return {yr: v for yr, v in totals.items() if quarter_counts.get(yr, 0) == 4}


# ── public API ────────────────────────────────────────────────────────────────

def fetch_building_stats(
    gemeentecode: str | int,
    pbk: PbkCoropResult | None = None,
) -> BuildingStatsResult | None:
    """
    Fetch annual building activity for the gemeente and its COROP region.

    Parameters
    ----------
    gemeentecode   BAG gemeente code (e.g. '0796' or 796).
    pbk            Optional PbkCoropResult from fetch_pbk_corop() — used to
                   derive annual transaction counts for the supply pressure ratio.
                   When None the ratio is omitted.

    Returns None if no COROP mapping is found for the gemeente.

    Data sources
    ------------
    years       — gemeente-level totals from 86054NED (nieuwbouw, sloop, stock)
    type_years  — COROP-level breakdown by woningtype from 86084NED
    """
    corop_code = get_corop_code(gemeentecode)
    if not corop_code:
        return None

    gm_padded  = str(int(gemeentecode)).zfill(4)
    gm_code    = f"GM{gm_padded}"

    names      = _load_corop_names()
    corop_name = names.get(corop_code, corop_code)

    # Building completions at gemeente level; transactions at COROP level
    # (85819NED only publishes quarterly sales at COROP granularity).
    rows       = _fetch_bouw_gemeente(gm_code)
    annual_tx  = _annual_transactions(pbk) if pbk else {}

    years: list[BuildingYear] = []
    for row in rows:
        yr = _parse_year(str(row.get("Perioden", "")))
        if yr is None:
            continue
        nieuwbouw = row.get("Nieuwbouw_2")
        tx        = annual_tx.get(yr)
        sp        = round(nieuwbouw / tx * 100, 1) if (nieuwbouw and tx) else None

        years.append(BuildingYear(
            year            = yr,
            nieuwbouw       = nieuwbouw,
            sloop           = row.get("Sloop_15"),
            netto           = row.get("SaldoVoorraad_26"),
            eindstand       = row.get("EindstandVoorraad_27"),
            transactions    = tx,
            supply_pressure = sp,
        ))
    years.sort(key=lambda y: y.year)

    # Woningtype breakdown at COROP level (86084NED)
    try:
        type_rows  = _fetch_bouw_type_rows(corop_code)
        type_years = _parse_type_years(type_rows)
    except Exception as _exc:
        import warnings
        warnings.warn(f"[building_stats] 86084NED fetch failed: {_exc}")
        type_years = []

    return BuildingStatsResult(
        gemeente_code = gm_padded,
        corop_code    = corop_code,
        corop_name    = corop_name,
        years         = years,
        type_years    = type_years,
    )
