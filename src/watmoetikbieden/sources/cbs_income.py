"""
CBS inkomen van personen — median personal gross income per year.

Data source: CBS OData table 83931NED "Inkomen van personen"
  https://opendata.cbs.nl/#/CBS/nl/dataset/83931NED/table

Series used:
  Inkomensbegrippen = A043962  → persoonlijk bruto inkomen
  Inkomensklassen   = T001226  → Totaal inkomensklassen
  Geslacht          = T001038  → Totaal geslacht
  KenmerkenVanPersonen = T009002 → Totaal personen

Measure returned:
  MediaanInkomen_3  → median income in €1,000 per person per year.

Coverage: 2011 – present (annual, JJ00 periods).

⚠ NOTE ON "MODAAL LOON":
The CPB-defined "modaal inkomen" (~€44,000 in 2024) refers to the full-year,
full-time employee at the modal wage.  This CBS series covers ALL persons with
personal income (including part-time, multi-job, small self-employment income),
so the median is lower (~€38,700 in 2024).  We use the CBS median as a
publicly verifiable, programmatic anchor.  For mortgage-capacity calculations
this slightly underestimates the reference borrower; adjust WOONQUOTE or use a
different column/filter in market_indicators.py if needed.

Cache: .cache/cbs/income_83931NED.json   TTL 365 days (annual data; rarely
       updated mid-year).

License: CBS open data under NLOD — attribution "Bron: CBS".
"""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from pathlib import Path

_ODATA     = "https://opendata.cbs.nl/ODataFeed/odata"
_TABLE     = "83931NED"

_CACHE_DIR  = Path(".cache/cbs")
_CACHE_FILE = _CACHE_DIR / "income_83931NED.json"
_CACHE_TTL  = 365 * 86400   # 1 year — annual data, rarely updated mid-year


# ── internal helpers ──────────────────────────────────────────────────────────

def _load_cache() -> dict[int, float] | None:
    if not _CACHE_FILE.exists():
        return None
    if time.time() - _CACHE_FILE.stat().st_mtime > _CACHE_TTL:
        return None
    raw = json.loads(_CACHE_FILE.read_text(encoding="utf-8"))
    return {int(k): v for k, v in raw.items()}


def _save_cache(data: dict[int, float]) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _CACHE_FILE.write_text(
        json.dumps({str(k): v for k, v in sorted(data.items())}, ensure_ascii=False),
        encoding="utf-8",
    )


def _fetch_raw() -> dict[int, float]:
    """
    Return {year: median_income_eur} for all available annual periods.

    Median income is MediaanInkomen_3 (€1 000 units → converted to EUR).
    """
    filter_expr = (
        "startswith(Inkomensbegrippen,'A043962')"
        " and startswith(Inkomensklassen,'T001226')"
        " and startswith(Geslacht,'T001038')"
        " and startswith(KenmerkenVanPersonen,'T009002')"
    )
    url = (
        f"{_ODATA}/{_TABLE}/TypedDataSet"
        f"?$format=json"
        f"&$filter={urllib.parse.quote(filter_expr)}"
        f"&$select=Perioden,MediaanInkomen_3"
        f"&$top=50"
    )

    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "WatMoetIkBieden/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=20) as r:
        payload = json.loads(r.read())

    result: dict[int, float] = {}
    for row in payload.get("value", []):
        period = (row.get("Perioden") or "").strip()   # e.g. "2024JJ00"
        value  = row.get("MediaanInkomen_3")
        if not period or value is None:
            continue
        try:
            year = int(period[:4])
            result[year] = round(float(value) * 1000)  # €1k → EUR
        except (ValueError, TypeError):
            continue

    return result


# ── public API ─────────────────────────────────────────────────────────────────

def fetch_median_income() -> dict[int, float]:
    """
    Return {year: median_personal_gross_income_eur} from CBS 83931NED.

    Returns an empty dict on failure (network unavailable, CBS API down).
    Values are in EUR (MediaanInkomen_3 × 1 000).
    Coverage typically 2011 – most recent completed year.
    """
    cached = _load_cache()
    if cached is not None:
        return cached

    try:
        data = _fetch_raw()
        if data:
            _save_cache(data)
        return data
    except Exception:
        return {}
