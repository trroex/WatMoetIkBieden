"""
Plot gem_woz_waarde for buurtcode BU08450002 across available CBS years,
with a secondary reference line showing the individual Ambachtsweg 72 WOZ values.

Data sources:
  - CBS Kerncijfers longitudinal CSV  (data/cbs_longitudinal/BU08450002_timeseries.csv)
    OR direct CBS OData fetch if CSV is absent
  - .cache/woz/0845200000000061.json  (individual property WOZ)

Usage:
    python scripts/plot_woz_longitudinal.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx
import pandas as pd

# ── stdout must handle Unicode box-drawing used by plotext ──────────────────
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import plotext as plt

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BUURTCODE   = "BU08450002"
WOZ_CACHE   = Path(".cache/woz/0845200000000061.json")
CSV_PATH    = Path("data/cbs_longitudinal") / f"{BUURTCODE}_timeseries.csv"
CACHE_DIR   = Path(".cache/cbs_longitudinal")

WOZ_MEASURE = "M001642"   # gem_woz_waarde (x1000 EUR)

# Datasets covering 2013–2025 that contain M001642
MODERN_DATASETS: list[tuple[int, str]] = [
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

API_BASE = "https://datasets.cbs.nl/odata/v1/CBS"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def cache_path(dataset_id: str, suffix: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{dataset_id}_{suffix}.json"


def load_json(p: Path):
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return None


def save_json(p: Path, data) -> None:
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# CBS fetch (single measure, single buurt, with buurt→wijk→gemeente fallback)
# ---------------------------------------------------------------------------

WIJKCODE      = "WK084500"
GEMEENTECODE  = "GM0845"

FILTER_FIELDS_BY_ERA = {
    # dataset_id → filter field name
    "82339NED": "RegioS",   # 2013 uses RegioS
}


def _get_filter_field(dataset_id: str) -> str:
    if dataset_id in FILTER_FIELDS_BY_ERA:
        return FILTER_FIELDS_BY_ERA[dataset_id]
    return "WijkenEnBuurten"


def fetch_woz_value_for_year(client: httpx.Client, year: int, dataset_id: str) -> float | None:
    """Fetch gem_woz_waarde (M001642) for BUURTCODE, with fallback."""
    ff = _get_filter_field(dataset_id)
    codes_to_try = [
        (BUURTCODE,      "buurt"),
        (BUURTCODE + " ", "buurt_space"),
        (WIJKCODE,       "wijk"),
        (GEMEENTECODE,   "gemeente"),
    ]
    seen: set[str] = set()

    for code, label in codes_to_try:
        base_label = label.split("_")[0]
        if base_label in seen:
            continue

        cp = cache_path(dataset_id, f"woz_{code.strip()}_{base_label}")
        cached = load_json(cp)
        if cached is not None:
            if cached:
                val = cached.get(WOZ_MEASURE)
                if val is not None:
                    print(f"  [{year}] cache ({base_label}): {val}", flush=True)
                    return float(val)
                seen.add(base_label)
                continue
            else:
                seen.add(base_label)
                continue

        try:
            params = {
                "$filter": f"{ff} eq '{code}'",
                "$select": "Measure,Value",
            }
            r = client.get(f"{API_BASE}/{dataset_id}/Observations", params=params, timeout=20)
            r.raise_for_status()
            rows = r.json().get("value", [])
        except Exception as exc:
            print(f"  [{year}] ERROR ({base_label}): {exc}", flush=True)
            save_json(cp, {})
            seen.add(base_label)
            time.sleep(0.5)
            continue

        if rows:
            result_map = {row["Measure"]: row.get("Value") for row in rows}
            save_json(cp, result_map)
            time.sleep(0.4)
            val = result_map.get(WOZ_MEASURE)
            if val is not None:
                print(f"  [{year}] fetched ({base_label}): {val}", flush=True)
                return float(val)
            # Data present but M001642 not in this dataset at this level
            seen.add(base_label)
        else:
            save_json(cp, {})
            seen.add(base_label)
            time.sleep(0.4)

    print(f"  [{year}] not found at any level", flush=True)
    return None


# ---------------------------------------------------------------------------
# Load buurt WOZ series
# ---------------------------------------------------------------------------

def load_buurt_series() -> dict[int, float]:
    """Return {year: gem_woz_waarde_EUR} for the buurt."""
    # ── Option A: load from pre-built CSV ────────────────────────────────────
    if CSV_PATH.exists():
        print(f"Loading buurt WOZ from {CSV_PATH}", flush=True)
        df = pd.read_csv(CSV_PATH, index_col="year")
        if "gem_woz_waarde" in df.columns:
            series = df["gem_woz_waarde"].dropna()
            return {int(y): float(v) * 1000 for y, v in series.items()}
        print("Column 'gem_woz_waarde' not found in CSV — falling back to direct fetch.")

    # ── Option B: fetch directly from CBS OData ───────────────────────────────
    print("Fetching gem_woz_waarde directly from CBS OData (no CSV found)…", flush=True)
    result: dict[int, float] = {}
    with httpx.Client(timeout=25) as client:
        for year, dataset_id in MODERN_DATASETS:
            val = fetch_woz_value_for_year(client, year, dataset_id)
            if val is not None:
                result[year] = val * 1000   # CBS stores in €1 000
    return result


# ---------------------------------------------------------------------------
# Load individual property WOZ series (Ambachtsweg 72)
# ---------------------------------------------------------------------------

def load_property_series() -> dict[int, int]:
    """Return {year: vastgesteldeWaarde_EUR} from cached WOZ JSON."""
    if not WOZ_CACHE.exists():
        print(f"WOZ cache not found: {WOZ_CACHE}", flush=True)
        return {}
    data = json.loads(WOZ_CACHE.read_text(encoding="utf-8"))
    waarden = data.get("wozWaarden", [])
    return {
        int(w["peildatum"][:4]): w["vastgesteldeWaarde"]
        for w in waarden
        if w.get("vastgesteldeWaarde") is not None
    }


# ---------------------------------------------------------------------------
# Chart + table
# ---------------------------------------------------------------------------

def plot_and_table(
    buurt: dict[int, float],
    prop: dict[int, int],
) -> None:
    if not buurt:
        print("No buurt WOZ data to plot.", flush=True)
        return

    # Align on common years for the table; chart uses all available years
    all_years_buurt = sorted(buurt)
    all_years_prop  = sorted(prop)

    # ── Chart ─────────────────────────────────────────────────────────────────
    plt.clf()
    plt.plotsize(90, 22)
    plt.title(f"WOZ-waarde ontwikkeling — {BUURTCODE}")
    plt.xlabel("Jaar")
    plt.ylabel("EUR")

    # Buurt series
    plt.plot(
        all_years_buurt,
        [buurt[y] for y in all_years_buurt],
        label="Buurtgemiddelde",
        marker="dot",
        color="cyan",
    )

    # Property series (if available)
    if prop:
        common_prop_years = [y for y in all_years_prop if y in range(min(all_years_buurt), max(all_years_buurt) + 1)]
        if common_prop_years:
            plt.plot(
                common_prop_years,
                [prop[y] for y in common_prop_years],
                label="Ambachtsweg 72",
                marker="dot",
                color="orange",
            )

    # Use integer year labels on x-axis
    plt.xticks(all_years_buurt, [str(y) for y in all_years_buurt])
    plt.show()

    # ── Comparison table ──────────────────────────────────────────────────────
    # Combined years where at least the buurt value exists
    table_years = sorted(y for y in buurt if y in prop or y in buurt)

    col_w = [6, 18, 18, 14]
    sep   = "  "

    def fmt_eur(val: float | None) -> str:
        if val is None:
            return "–"
        return f"EUR {int(round(val)):,}".replace(",", ".")

    def fmt_diff(b: float | None, p: int | None) -> str:
        if b is None or p is None:
            return "–"
        diff = p - b
        sign = "+" if diff >= 0 else ""
        return f"{sign}EUR {int(round(diff)):,}".replace(",", ".")

    header = f"{'Jaar':<{col_w[0]}}{sep}{'Buurt gem WOZ':>{col_w[1]}}{sep}{'Ambachtsweg 72':>{col_w[2]}}{sep}{'Verschil':>{col_w[3]}}"
    line   = "─" * (sum(col_w) + len(sep) * 3)

    print()
    print(line)
    print(header)
    print(line)
    for y in table_years:
        b_val = buurt.get(y)
        p_val = prop.get(y)
        print(
            f"{y:<{col_w[0]}}"
            f"{sep}{fmt_eur(b_val):>{col_w[1]}}"
            f"{sep}{fmt_eur(p_val):>{col_w[2]}}"
            f"{sep}{fmt_diff(b_val, p_val):>{col_w[3]}}"
        )
    print(line)
    print()

    # ── Quick summary ─────────────────────────────────────────────────────────
    if prop and buurt:
        overlap = set(buurt) & set(prop)
        if overlap:
            latest = max(overlap)
            b = buurt[latest]
            p = prop[latest]
            ratio = p / b
            print(f"Meest recent ({latest}): Ambachtsweg 72 is {ratio:.2f}x het buurtgemiddelde "
                  f"(EUR {int(p):,} vs EUR {int(b):,})".replace(",", "."))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"\n=== WOZ Longitudinal: {BUURTCODE} ===\n", flush=True)

    buurt  = load_buurt_series()
    prop   = load_property_series()

    print(f"\nBuurt series: {len(buurt)} years  ({sorted(buurt)})")
    print(f"Property series: {len(prop)} years  ({sorted(prop)})\n")

    plot_and_table(buurt, prop)


if __name__ == "__main__":
    main()
