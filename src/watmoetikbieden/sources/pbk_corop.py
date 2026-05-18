"""
CBS Prijsindex Bestaande Koopwoningen (PBK) per COROP-regio.

Fetches quarterly price index, sales volume, and average price data from
CBS table 85819NED for the COROP region that contains a given gemeente,
then compares against the national series (85773NED).

Geographic mapping
──────────────────
BAG gives gemeentecode (4-digit integer, e.g. 796 for Sint-Michielsgestel).
We map gemeente → COROP using CBS Gebiedsindelingen 84721NED:
  RegioS (GM-code)  →  Code_8 (CR-code, e.g. 'CR35')

Data
────
85819NED  Quarterly PBK per COROP, 2020=100, Q1 1995 – present
           Includes 95% confidence intervals (useful for thin markets)
85773NED  Quarterly + monthly PBK national, 2020=100, Q1 1995 – present

Caching
───────
.cache/pbk/corop_mapping.json    TTL 365 days  (gemeente → COROP)
.cache/pbk/corop_names.json      TTL 365 days  (CR code → display name)
.cache/pbk/{CR_CODE}.json        TTL 7 days    (COROP quarterly data)
.cache/pbk/national.json         TTL 7 days    (national quarterly data)

License: CBS data is published under NLOD — commercial use allowed with
attribution ("Bron: CBS").
"""

from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path

_CACHE_DIR   = Path(".cache/pbk")
_DATA_TTL    = 7 * 24 * 3600    # 7 days  – quarterly data, ~6-week release lag
_MAP_TTL     = 365 * 24 * 3600  # 365 days – COROP boundaries rarely change

_ODATA       = "https://opendata.cbs.nl/ODataFeed/odata"
_COROP_TABLE = "85819NED"
_NAT_TABLE   = "85773NED"
_GEO_TABLE   = "84721NED"   # Gebiedsindelingen: gemeente → COROP (Code_8)

N_QUARTERS   = 12


# ── data model ────────────────────────────────────────────────────────────────

@dataclass
class PbkPoint:
    period:        str            # "2025KW4"
    period_label:  str            # "Q4 2025"
    price_index:   float | None   # 2020 = 100
    yoy_pct:       float | None   # year-over-year price change %
    qoq_pct:       float | None   # quarter-over-quarter price change %
    sales_count:   int | None     # Kadaster transactions
    sales_yoy_pct: float | None   # year-over-year sales count change %
    avg_price:     int | None     # average sale price (€)
    ci_lower:      float | None   # 95 % CI lower bound (COROP only)
    ci_upper:      float | None   # 95 % CI upper bound (COROP only)


@dataclass
class PbkCoropResult:
    corop_code:    str
    corop_name:    str
    corop_data:    list[PbkPoint]   # last N_QUARTERS, this COROP
    national_data: list[PbkPoint]   # last N_QUARTERS, national

    @property
    def latest(self) -> PbkPoint | None:
        return self.corop_data[-1] if self.corop_data else None

    @property
    def national_latest(self) -> PbkPoint | None:
        return self.national_data[-1] if self.national_data else None

    @property
    def heat_delta_pct(self) -> float | None:
        """COROP YoY% minus national YoY% — positive means hotter than national."""
        l, n = self.latest, self.national_latest
        if l and n and l.yoy_pct is not None and n.yoy_pct is not None:
            return round(l.yoy_pct - n.yoy_pct, 1)
        return None

    @property
    def heat_label(self) -> str:
        """Human-readable heat label based on absolute YoY% and delta vs national."""
        latest = self.latest
        if not latest or latest.yoy_pct is None:
            return "Onbekend"
        yoy = latest.yoy_pct
        delta = self.heat_delta_pct or 0.0
        # Primary signal: absolute YoY price growth
        if yoy > 10 or (yoy > 7 and delta > 2):
            return "Oververhit"
        if yoy > 5 or delta > 1.5:
            return "Warm"
        if yoy >= 0:
            return "Neutraal"
        if yoy >= -5:
            return "Afkoelend"
        return "Koud"

    @property
    def heat_emoji(self) -> str:
        return {
            "Oververhit": "🔴",
            "Warm":       "🟠",
            "Neutraal":   "🟡",
            "Afkoelend":  "🔵",
            "Koud":       "⚪",
        }.get(self.heat_label, "⚪")


# ── HTTP + cache helpers ──────────────────────────────────────────────────────

def _get(url: str) -> dict:
    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json", "User-Agent": "WatMoetIkBieden/1.0"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def _cache_read(name: str, ttl: int) -> list | dict | None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _CACHE_DIR / f"{name}.json"
    if p.exists() and time.time() - p.stat().st_mtime < ttl:
        return json.loads(p.read_text(encoding="utf-8"))
    return None


def _cache_write(name: str, data) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    (_CACHE_DIR / f"{name}.json").write_text(
        json.dumps(data, ensure_ascii=False), encoding="utf-8"
    )


# ── period helpers ────────────────────────────────────────────────────────────

def _is_quarter(p: str) -> bool:
    return bool(re.match(r"\d{4}KW\d", p.strip()))


def _period_label(p: str) -> str:
    """'2025KW04' or '2025KW4' → 'Q4 2025'"""
    m = re.match(r"(\d{4})KW0?(\d)", p.strip())
    return f"Q{m.group(2)} {m.group(1)}" if m else p.strip()


# ── gemeente → COROP mapping ──────────────────────────────────────────────────

def _load_corop_mapping() -> dict[str, str]:
    """
    Return {gemeentecode_4digit: corop_code}, e.g. {"0796": "CR35"}.

    Source: CBS 84721NED Gebiedsindelingen.
    RegioS = "GM0796    " (padded GM-codes for gemeenten)
    Code_8 = "CR35      " (padded COROP code)
    """
    cached = _cache_read("corop_mapping", _MAP_TTL)
    if cached is not None:
        return cached

    # Fetch only gemeente rows (RegioS starts with 'GM') and the two relevant fields
    url = (
        f"{_ODATA}/{_GEO_TABLE}/TypedDataSet"
        f"?$format=json"
        f"&$filter=startswith(RegioS,'GM')"
        f"&$select=RegioS,Code_8"
        f"&$top=500"
    )
    rows = _get(url).get("value", [])

    mapping: dict[str, str] = {}
    for row in rows:
        gm_raw = str(row.get("RegioS") or "").strip()   # "GM0796"
        cr_raw = str(row.get("Code_8") or "").strip()   # "CR35"
        if not gm_raw.startswith("GM") or not cr_raw.startswith("CR"):
            continue
        gm_code = gm_raw[2:].zfill(4)                  # "0796"
        mapping[gm_code] = cr_raw                       # "CR35"

    _cache_write("corop_mapping", mapping)
    return mapping


def _load_corop_names() -> dict[str, str]:
    """Return {corop_code: display_name}, e.g. {"CR35": "Noordoost-Noord-Brabant (CR)"}."""
    cached = _cache_read("corop_names", _MAP_TTL)
    if cached is not None:
        return cached

    url = f"{_ODATA}/{_COROP_TABLE}/RegioS?$format=json"
    rows = _get(url).get("value", [])
    names = {r["Key"].strip(): r["Title"].strip() for r in rows if r.get("Key")}
    _cache_write("corop_names", names)
    return names


def get_corop_code(gemeentecode: str | int) -> str | None:
    """
    Map a BAG gemeentecode (e.g. '0796', 796) to a COROP code (e.g. 'CR35').
    Returns None if the gemeente is not found in the mapping.
    """
    padded = str(int(gemeentecode)).zfill(4)
    try:
        return _load_corop_mapping().get(padded)
    except Exception:
        return None


# ── CBS data fetchers ─────────────────────────────────────────────────────────

def _fetch_corop_rows(corop_code: str) -> list[dict]:
    """Fetch all quarterly rows for one COROP from 85819NED, with 7-day cache."""
    cached = _cache_read(corop_code, _DATA_TTL)
    if cached is not None:
        return cached

    # CBS pads dimension codes with trailing spaces; use startswith filter
    filter_expr = f"startswith(RegioS,'{corop_code}')"
    url = (
        f"{_ODATA}/{_COROP_TABLE}/TypedDataSet"
        f"?$format=json&$filter={urllib.parse.quote(filter_expr)}"
    )
    rows = _get(url).get("value", [])
    _cache_write(corop_code, rows)
    return rows


def _fetch_national_rows() -> list[dict]:
    """Fetch all rows from 85773NED (national, quarterly + monthly), 7-day cache."""
    cached = _cache_read("national", _DATA_TTL)
    if cached is not None:
        return cached

    url = f"{_ODATA}/{_NAT_TABLE}/TypedDataSet?$format=json"
    rows = _get(url).get("value", [])
    _cache_write("national", rows)
    return rows


# ── row parsers ───────────────────────────────────────────────────────────────

def _parse_corop(rows: list[dict], n: int) -> list[PbkPoint]:
    quarters = sorted(
        [r for r in rows if _is_quarter(str(r.get("Perioden", "")))],
        key=lambda r: r["Perioden"],
    )[-n:]
    return [
        PbkPoint(
            period       = r["Perioden"].strip(),
            period_label = _period_label(r["Perioden"]),
            price_index  = r.get("PrijsindexVerkoopprijzen_1"),
            yoy_pct      = r.get("OntwikkelingTOVEenJaarEerder_5"),
            qoq_pct      = r.get("OntwikkelingTOVVorigePeriode_4"),
            sales_count  = r.get("VerkochteWoningen_6"),
            sales_yoy_pct= r.get("OntwikkelingTOVEenJaarEerder_8"),
            avg_price    = r.get("GemiddeldeVerkoopprijs_9"),
            ci_lower     = r.get("k_95BetrouwbaarheidsmargeOndergrens_2"),
            ci_upper     = r.get("k_95BetrouwbaarheidsmargeBovengrens_3"),
        )
        for r in quarters
    ]


def _parse_national(rows: list[dict], n: int) -> list[PbkPoint]:
    quarters = sorted(
        [r for r in rows if _is_quarter(str(r.get("Perioden", "")))],
        key=lambda r: r["Perioden"],
    )[-n:]
    return [
        PbkPoint(
            period       = r["Perioden"].strip(),
            period_label = _period_label(r["Perioden"]),
            price_index  = r.get("PrijsindexVerkoopprijzen_1"),
            yoy_pct      = r.get("OntwikkelingTOVEenJaarEerder_3"),
            qoq_pct      = r.get("OntwikkelingTOVVoorgaandePeriode_2"),
            sales_count  = r.get("VerkochteWoningen_4"),
            sales_yoy_pct= r.get("OntwikkelingTOVEenJaarEerder_6"),
            avg_price    = r.get("GemiddeldeVerkoopprijs_7"),
            ci_lower     = None,
            ci_upper     = None,
        )
        for r in quarters
    ]


# ── public API ────────────────────────────────────────────────────────────────

def fetch_pbk_corop(
    gemeentecode: str | int,
    n_quarters: int = N_QUARTERS,
) -> PbkCoropResult | None:
    """
    Fetch PBK price index data for the COROP region containing the gemeente.

    Parameters
    ----------
    gemeentecode   BAG gemeente code (e.g. '0796' or 796).
    n_quarters     Most-recent quarters to return (default 12 = 3 years).

    Returns None when no COROP mapping can be found for the gemeente.
    """
    corop_code = get_corop_code(gemeentecode)
    if not corop_code:
        return None

    names      = _load_corop_names()
    corop_name = names.get(corop_code, corop_code)

    corop_pts    = _parse_corop(_fetch_corop_rows(corop_code), n_quarters)
    national_pts = _parse_national(_fetch_national_rows(), n_quarters)

    return PbkCoropResult(
        corop_code    = corop_code,
        corop_name    = corop_name,
        corop_data    = corop_pts,
        national_data = national_pts,
    )
