"""
CBS monthly inter-municipal migration flows per gemeente — table 37230ned.

"Bevolkingsontwikkeling per gemeente per maand"

Tracks how many people move into (vestiging) and out of (vertrek) each gemeente
from/to other Dutch municipalities each month.  Net internal migration is a
demand-side heat indicator: sustained positive net inflow compresses supply.

Measures used:
  VestigingVanuitEenAndereGemeente_5  – arrivals from other NL municipalities
  VertrekNaarAndereGemeente_8         – departures to other NL municipalities
  Net = vestiging − vertrek

International immigration/emigration is excluded; we only track domestic
relocation pressure.

Data coverage: from 2002 through current year (typ. 2–3 month lag).
Geographic coverage: all ~342 active gemeenten.

Cache: .cache/pbk/migration_GM{CODE}.json   TTL 7 days
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
    _CACHE_DIR,
    _DATA_TTL,
    _ODATA,
)

_MIGRATION_TABLE = "37230ned"
_N_MONTHS        = 48   # ~4 years of monthly history


# ── data model ────────────────────────────────────────────────────────────────

@dataclass
class MigrationMonth:
    period:      str        # "2025MM05"
    period_label: str       # "mei 2025"
    vestiging:   int | None # arrivals from other municipalities
    vertrek:     int | None # departures to other municipalities

    @property
    def net(self) -> int | None:
        if self.vestiging is None or self.vertrek is None:
            return None
        return self.vestiging - self.vertrek


@dataclass
class MigrationResult:
    gemeente_code: str                  # zero-padded, e.g. "0796"
    months:        list[MigrationMonth] # ascending chronological order

    @property
    def latest(self) -> MigrationMonth | None:
        return self.months[-1] if self.months else None

    def trailing_12m_net(self, as_of_idx: int | None = None) -> int | None:
        """Sum of net monthly migration over the 12 months ending at *as_of_idx*."""
        end = len(self.months) if as_of_idx is None else as_of_idx + 1
        window = [m.net for m in self.months[max(0, end - 12):end]]
        known  = [v for v in window if v is not None]
        return sum(known) if len(known) == 12 else None

    def yoy_trailing_12m(self) -> int | None:
        """Change in trailing-12m net migration vs one year prior."""
        n = len(self.months)
        if n < 24:
            return None
        curr = self.trailing_12m_net()
        prev = self.trailing_12m_net(as_of_idx=n - 13)  # 12 months ago
        if curr is None or prev is None:
            return None
        return curr - prev


# ── period helpers ─────────────────────────────────────────────────────────────

_NL_MONTHS = [
    "", "jan", "feb", "mrt", "apr", "mei", "jun",
    "jul", "aug", "sep", "okt", "nov", "dec",
]


def _month_label(period: str) -> str:
    """'2025MM05' → 'mei 2025'"""
    m = re.match(r"(\d{4})MM(\d{2})", period.strip())
    if not m:
        return period.strip()
    year, month = int(m.group(1)), int(m.group(2))
    mn = _NL_MONTHS[month] if 1 <= month <= 12 else f"M{month:02d}"
    return f"{mn} {year}"


# ── cache helpers ──────────────────────────────────────────────────────────────

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


# ── fetch ──────────────────────────────────────────────────────────────────────

def _fetch_migration_rows(gm_code: str) -> list[dict]:
    """
    Fetch monthly inter-municipal migration rows for one gemeente from 37230ned.

    Only fetches data from 2019 onward to cap response size (~72+ months).
    """
    cache_key = f"migration_{gm_code}"
    cached = _cache_read(cache_key)
    if cached is not None:
        return cached

    # Filter by gemeente AND restrict to 2019+ to limit payload size.
    # CBS OData supports string comparison on Perioden (YYYYMM## is lex-sortable).
    filter_expr = (
        f"startswith(RegioS,'{gm_code}')"
        f" and Perioden ge '2019MM01'"
    )
    url = (
        f"{_ODATA}/{_MIGRATION_TABLE}/TypedDataSet"
        f"?$format=json&$filter={urllib.parse.quote(filter_expr)}"
        f"&$select=RegioS,Perioden,VestigingVanuitEenAndereGemeente_5,VertrekNaarAndereGemeente_8"
        f"&$top=100"
    )
    rows = _get(url).get("value", [])
    _cache_write(cache_key, rows)
    return rows


# ── public API ─────────────────────────────────────────────────────────────────

def fetch_migration(
    gemeentecode: str | int,
    n_months: int = _N_MONTHS,
) -> MigrationResult | None:
    """
    Fetch monthly inter-municipal migration for a gemeente.

    Parameters
    ----------
    gemeentecode   BAG gemeente code (e.g. '0796' or 796).
    n_months       Most-recent months to return (default 48 = 4 years).

    Returns None if no data is found for the gemeente.
    """
    gm_padded = str(int(gemeentecode)).zfill(4)
    gm_code   = f"GM{gm_padded}"

    rows = _fetch_migration_rows(gm_code)
    if not rows:
        return None

    # Keep only monthly periods (YYYYMM##), sort ascending, slice to n_months
    monthly = sorted(
        [r for r in rows if re.match(r"\d{4}MM\d{2}", str(r.get("Perioden", "")))],
        key=lambda r: r["Perioden"],
    )[-n_months:]

    if not monthly:
        return None

    result = [
        MigrationMonth(
            period       = r["Perioden"].strip(),
            period_label = _month_label(r["Perioden"]),
            vestiging    = r.get("VestigingVanuitEenAndereGemeente_5"),
            vertrek      = r.get("VertrekNaarAndereGemeente_8"),
        )
        for r in monthly
    ]

    return MigrationResult(
        gemeente_code = gm_padded,
        months        = result,
    )
