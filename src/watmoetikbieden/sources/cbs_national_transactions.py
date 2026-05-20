"""
CBS Prijsindex Bestaande Koopwoningen – Nationale maandelijkse transactiedata.

Fetches monthly national housing transaction data from CBS 85773NED (the same
table already used for national PBK comparison in pbk_corop.py, but here we
fetch monthly rows instead of quarterly ones).

OData endpoint: https://opendata.cbs.nl/ODataFeed/odata/85773NED/TypedDataSet
Filter: recent years (2023–2026), monthly rows only (Perioden contains "MM").
Selected fields:
  Perioden                        – period code e.g. "2026MM03"
  VerkochteWoningen_4             – number of transactions
  OntwikkelingTOVEenJaarEerder_6  – transactions YoY %
  OntwikkelingTOVEenJaarEerder_3  – price YoY %
  GemiddeldeVerkoopprijs_7        – average sale price (€)

Cache: .cache/cbs/national_transactions.json  TTL 7 days.

License: CBS data is published under NLOD — commercial use allowed with
attribution ("Bron: CBS").
"""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

_CACHE_DIR  = Path(".cache/cbs")
_CACHE_FILE = _CACHE_DIR / "national_transactions.json"
_TTL        = 7 * 24 * 3600   # 7 days

_ODATA_URL  = "https://opendata.cbs.nl/ODataFeed/odata/85773NED/TypedDataSet"

_MONTH_ABBR = {
    1: "jan", 2: "feb", 3: "mrt", 4: "apr",
    5: "mei", 6: "jun", 7: "jul", 8: "aug",
    9: "sep", 10: "okt", 11: "nov", 12: "dec",
}


# ── data model ────────────────────────────────────────────────────────────────

@dataclass
class NationalTxMonth:
    period: str               # "2026MM03"
    period_label: str         # "mrt 2026"
    transactions: int | None
    tx_yoy_pct: float | None    # OntwikkelingTOVEenJaarEerder_6
    price_yoy_pct: float | None # OntwikkelingTOVEenJaarEerder_3
    avg_price: int | None


@dataclass
class NationalTxSeries:
    months: list[NationalTxMonth]   # ascending chronological

    @property
    def latest(self) -> NationalTxMonth | None:
        return self.months[-1] if self.months else None

    def trailing_avg(self, field: str, n: int = 3) -> float | None:
        """Average of the last *n* non-None values for *field*.

        *field* must be one of 'tx_yoy_pct' or 'price_yoy_pct'.
        Returns None when fewer than *n* non-None values exist.
        """
        values: list[float] = []
        for m in reversed(self.months):
            v = getattr(m, field, None)
            if v is not None:
                values.append(v)
            if len(values) == n:
                break
        if len(values) < n:
            return None
        return sum(values) / len(values)


# ── helpers ───────────────────────────────────────────────────────────────────

def _period_label(period: str) -> str:
    """Convert "2026MM03" → "mrt 2026"."""
    try:
        year  = int(period[:4])
        month = int(period[6:8])
        return f"{_MONTH_ABBR.get(month, period[5:])} {year}"
    except (ValueError, IndexError):
        return period


def _safe_int(v) -> int | None:
    try:
        if v is None:
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_float(v) -> float | None:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


# ── fetching ──────────────────────────────────────────────────────────────────

def _build_url() -> str:
    filter_str = (
        "startswith(Perioden,'2023') or "
        "startswith(Perioden,'2024') or "
        "startswith(Perioden,'2025') or "
        "startswith(Perioden,'2026')"
    )
    select_str = (
        "Perioden,"
        "VerkochteWoningen_4,"
        "OntwikkelingTOVEenJaarEerder_6,"
        "OntwikkelingTOVEenJaarEerder_3,"
        "GemiddeldeVerkoopprijs_7"
    )
    params = urllib.parse.urlencode({
        "$filter":  filter_str,
        "$select":  select_str,
        "$top":     "200",
        "$format":  "json",
    })
    return f"{_ODATA_URL}?{params}"


def _fetch_raw() -> list[dict]:
    url = _build_url()
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    return data.get("value", [])


def _load_cache() -> list[dict] | None:
    if not _CACHE_FILE.exists():
        return None
    try:
        payload = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
        if time.time() - payload.get("ts", 0) > _TTL:
            return None
        return payload["rows"]
    except Exception:
        return None


def _save_cache(rows: list[dict]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _CACHE_FILE.write_text(
        json.dumps({"ts": time.time(), "rows": rows}, ensure_ascii=False),
        encoding="utf-8",
    )


def _parse_rows(rows: list[dict]) -> NationalTxSeries:
    months: list[NationalTxMonth] = []
    for row in rows:
        period = (row.get("Perioden") or "").strip()
        # Keep monthly rows only
        if "MM" not in period:
            continue
        months.append(NationalTxMonth(
            period       = period,
            period_label = _period_label(period),
            transactions = _safe_int(row.get("VerkochteWoningen_4")),
            tx_yoy_pct   = _safe_float(row.get("OntwikkelingTOVEenJaarEerder_6")),
            price_yoy_pct= _safe_float(row.get("OntwikkelingTOVEenJaarEerder_3")),
            avg_price    = _safe_int(row.get("GemiddeldeVerkoopprijs_7")),
        ))
    months.sort(key=lambda m: m.period)
    return NationalTxSeries(months=months)


# ── public API ────────────────────────────────────────────────────────────────

def fetch_national_transactions() -> NationalTxSeries | None:
    """Fetch monthly national transaction data from CBS 85773NED.

    Returns None on any failure.
    """
    try:
        rows = _load_cache()
        if rows is None:
            rows = _fetch_raw()
            _save_cache(rows)
        return _parse_rows(rows)
    except Exception:
        return None
