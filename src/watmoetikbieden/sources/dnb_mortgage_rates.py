"""
Dutch mortgage interest rates via ECB SDMX API.

Data source: ECB MFI Interest Rate Statistics (MIR), reported by
De Nederlandsche Bank (DNB).  The ECB SDMX REST API is publicly
accessible without authentication.

Series used:
  MIR / M.NL.B.A2C.AM.R.A.2250.EUR.N
    M    – monthly
    NL   – Netherlands
    B    – MFIs excluding ESCB (banks)
    A2C  – loans for house purchase (new business)
    AM   – all maturities (weighted average across fixation periods)
    R    – rate
    A    – annualised agreed rate (AAR)
    2250 – households
    EUR  – euro
    N    – not seasonally adjusted

Coverage: January 2003 – current month (typ. 1-month lag).
Update cadence: monthly.

Cache: .cache/dnb/mortgage_rates.json   TTL 7 days
License: ECB / DNB open data — no restrictions on commercial use.
"""

from __future__ import annotations

import json
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

_CACHE_DIR  = Path(".cache/dnb")
_CACHE_TTL  = 7 * 86400   # 7 days
_CACHE_FILE = _CACHE_DIR / "mortgage_rates.json"

# ECB SDMX REST API — no auth required
_ECB_URL = (
    "https://data-api.ecb.europa.eu/service/data"
    "/MIR/M.NL.B.A2C.AM.R.A.2250.EUR.N"
    "?format=jsondata&startPeriod=2003-01"
)


# ── data model ────────────────────────────────────────────────────────────────

@dataclass
class MortgageRatePoint:
    period:      str        # "2025-03"  (YYYY-MM)
    rate_pct:    float      # annualised agreed rate, % per annum


@dataclass
class MortgageRateSeries:
    points: list[MortgageRatePoint]  # ascending chronological order

    @property
    def latest(self) -> MortgageRatePoint | None:
        return self.points[-1] if self.points else None

    def as_dict(self) -> dict[str, float]:
        """Return {period: rate_pct} for easy chart lookup."""
        return {p.period: p.rate_pct for p in self.points}


# ── cache ──────────────────────────────────────────────────────────────────────

def _load_cache() -> list[dict] | None:
    if not _CACHE_FILE.exists():
        return None
    if time.time() - _CACHE_FILE.stat().st_mtime > _CACHE_TTL:
        return None
    return json.loads(_CACHE_FILE.read_text(encoding="utf-8"))


def _save_cache(rows: list[dict]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _CACHE_FILE.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")


# ── fetch & parse ECB SDMX JSON ───────────────────────────────────────────────

def _fetch_raw() -> list[dict]:
    req = urllib.request.Request(
        _ECB_URL,
        headers={
            "Accept": "application/json",
            "User-Agent": "WatMoetIkBieden/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        payload = json.loads(r.read())

    # ECB SDMX JSON structure:
    #   structure.dimensions.observation  → list of dimension objects
    #   dataSets[0].series["0:0:..."].observations  → {"0": [val, ...], ...}
    structure  = payload["structure"]
    obs_dims   = structure["dimensions"]["observation"]

    # Find index of TIME_PERIOD dimension
    time_idx = next(
        (i for i, d in enumerate(obs_dims) if d["id"] == "TIME_PERIOD"), None
    )
    if time_idx is None:
        raise ValueError("TIME_PERIOD dimension not found in ECB SDMX response")

    periods = [v["id"] for v in obs_dims[time_idx]["values"]]

    # First (and only) series in the dataset
    series_map = payload["dataSets"][0]["series"]
    series_key = next(iter(series_map))
    observations = series_map[series_key]["observations"]

    rows = []
    for idx_str, obs in observations.items():
        idx  = int(idx_str)
        rate = obs[0]  # first element is the rate value
        if rate is not None and idx < len(periods):
            rows.append({"period": periods[idx], "rate_pct": float(rate)})

    rows.sort(key=lambda r: r["period"])
    return rows


# ── public API ─────────────────────────────────────────────────────────────────

def fetch_mortgage_rates() -> MortgageRateSeries | None:
    """
    Fetch monthly Dutch mortgage interest rates (2003–present).

    Returns None on fetch failure (network unavailable, API down).
    """
    cached = _load_cache()
    if cached is not None:
        rows = cached
    else:
        try:
            rows = _fetch_raw()
            _save_cache(rows)
        except Exception:
            return None

    points = [
        MortgageRatePoint(period=r["period"], rate_pct=r["rate_pct"])
        for r in rows
    ]
    return MortgageRateSeries(points=points) if points else None
