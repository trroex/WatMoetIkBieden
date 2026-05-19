"""
CBS quarterly building permits per gemeente — table 83671NED.

"Bouwvergunningen woonruimten; type, opdrachtgever, eigendom, gemeente"

Reports the number of residential dwellings for which a building permit was
granted each quarter, at gemeente level.  Building permits are a leading
indicator of future housing supply: completions typically follow 12–18 months
after permit issuance.

Filters applied:
  Opdrachtgever = T001209  (totaal — all commissioners)
  Eigendom      = T001258  (totaal — koop + huur combined)

Data coverage: Q1 2012 – Q4 2025 (Q4 2025 preliminary).
Geographic coverage: ~380 gemeenten.

Cache: .cache/pbk/permits_GM{CODE}.json   TTL 7 days
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
    _period_label,
)

_PERMITS_TABLE       = "83671NED"
_OPDRACHTGEVER_TOT   = "T001209"   # Totaal opdrachtgevers
_EIGENDOM_TOT        = "T001258"   # Totaal (koop + huur)
_N_QUARTERS          = 20          # ~5 years of quarterly history


# ── data model ────────────────────────────────────────────────────────────────

@dataclass
class PermitQuarter:
    period:       str           # "2025KW03"
    period_label: str           # "Q3 2025"
    permits:      int | None    # granted dwelling units


@dataclass
class BuildingPermitsResult:
    gemeente_code: str                  # zero-padded, e.g. "0796"
    quarters:      list[PermitQuarter]  # ascending chronological order

    @property
    def latest(self) -> PermitQuarter | None:
        return self.quarters[-1] if self.quarters else None

    def trailing_12m(self, as_of_idx: int | None = None) -> int | None:
        """Sum of permits over the 4 quarters ending at *as_of_idx* (default: latest)."""
        end = len(self.quarters) if as_of_idx is None else as_of_idx + 1
        window = [q.permits for q in self.quarters[max(0, end - 4):end]]
        known = [v for v in window if v is not None]
        return sum(known) if len(known) == 4 else None

    def yoy_trailing_12m(self) -> int | None:
        """Change in trailing-12m permits vs one year prior."""
        n = len(self.quarters)
        if n < 8:
            return None
        curr = self.trailing_12m()
        prev = self.trailing_12m(as_of_idx=n - 5)  # 4 quarters ago
        if curr is None or prev is None or prev == 0:
            return None
        return curr - prev


# ── cache helpers ─────────────────────────────────────────────────────────────

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


# ── fetch ─────────────────────────────────────────────────────────────────────

def _fetch_permit_rows(gm_code: str) -> list[dict]:
    """Fetch all quarterly permit rows for one gemeente from 83671NED."""
    cache_key = f"permits_{gm_code}"
    cached = _cache_read(cache_key)
    if cached is not None:
        return cached

    filter_expr = (
        f"startswith(RegioS,'{gm_code}')"
        f" and startswith(Opdrachtgever,'{_OPDRACHTGEVER_TOT}')"
        f" and startswith(Eigendom,'{_EIGENDOM_TOT}')"
    )
    url = (
        f"{_ODATA}/{_PERMITS_TABLE}/TypedDataSet"
        f"?$format=json&$filter={urllib.parse.quote(filter_expr)}"
    )
    rows = _get(url).get("value", [])
    _cache_write(cache_key, rows)
    return rows


# ── public API ────────────────────────────────────────────────────────────────

def fetch_building_permits(
    gemeentecode: str | int,
    n_quarters: int = _N_QUARTERS,
) -> BuildingPermitsResult | None:
    """
    Fetch quarterly building permit counts for a gemeente.

    Parameters
    ----------
    gemeentecode   BAG gemeente code (e.g. '0796' or 796).
    n_quarters     Most-recent quarters to return (default 20 = 5 years).

    Returns None if no data is found for the gemeente.
    """
    gm_padded = str(int(gemeentecode)).zfill(4)
    gm_code   = f"GM{gm_padded}"

    rows = _fetch_permit_rows(gm_code)
    if not rows:
        return None

    # Keep only quarterly periods, sort ascending, slice to n_quarters
    quarters = sorted(
        [r for r in rows if re.match(r"\d{4}KW", str(r.get("Perioden", "")))],
        key=lambda r: r["Perioden"],
    )[-n_quarters:]

    result = [
        PermitQuarter(
            period       = r["Perioden"].strip(),
            period_label = _period_label(r["Perioden"]),
            permits      = r.get("Woningen_2"),
        )
        for r in quarters
    ]

    return BuildingPermitsResult(
        gemeente_code = gm_padded,
        quarters      = result,
    )
