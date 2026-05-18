"""
CBS housing stock mutations per COROP region — table 86054NED.

Tracks how many homes are added (new builds, transformations) and removed
(demolitions) from the housing stock each year, at COROP and gemeente level.

Annual data, 2020–2024.  Geographic coverage: national, 4 large districts,
12 provinces, 40 COROP regions, ~360 gemeenten.

Combined with quarterly transaction volumes from 85819NED (passed in from
pbk_corop) to derive a supply pressure ratio:
  supply_pressure = completions / annual_transactions × 100
  Low ratio  (<  8 %) → krappe markt  (little new supply vs demand)
  Mid ratio  (8–15 %) → gemiddeld
  High ratio (> 15 %) → ruime markt  (supply keeping up with demand)

Cache: .cache/pbk/bouw_{CR_CODE}.json   TTL 7 days
License: CBS / NLOD — commercial use allowed with attribution.
"""

from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

from watmoetikbieden.sources.pbk_corop import (
    PbkCoropResult,
    get_corop_code,
    _load_corop_names,   # shared helper
    _CACHE_DIR,
    _DATA_TTL,
    _ODATA,
)

_BOUW_TABLE = "86054NED"


# ── data model ────────────────────────────────────────────────────────────────

@dataclass
class BuildingYear:
    year:             int
    nieuwbouw:        int | None    # new completions
    sloop:            int | None    # demolitions
    netto:            int | None    # net stock change (SaldoVoorraad)
    eindstand:        int | None    # total housing stock at year-end
    transactions:     int | None    # annual sales (from 85819NED, derived)
    supply_pressure:  float | None  # nieuwbouw / transactions × 100


@dataclass
class BuildingStatsResult:
    gemeente_code: str              # zero-padded, e.g. "0796"
    corop_code:    str              # COROP used for transaction denominator
    corop_name:    str
    years:         list[BuildingYear]  # ascending by year

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


# ── fetch 86054NED ────────────────────────────────────────────────────────────

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
    Fetch annual building activity for the COROP region containing the gemeente.

    Parameters
    ----------
    gemeentecode   BAG gemeente code (e.g. '0796' or 796).
    pbk            Optional PbkCoropResult from fetch_pbk_corop() — used to
                   derive annual transaction counts for the supply pressure ratio.
                   When None the ratio is omitted.

    Returns None if no COROP mapping is found for the gemeente.
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

    return BuildingStatsResult(
        gemeente_code = gm_padded,
        corop_code    = corop_code,
        corop_name    = corop_name,
        years         = years,
    )
