"""
Longitudinal CBS Kerncijfers Wijken en Buurten for buurtcode BU08450002.

Fetches all available years (1995–2025), checks measure availability,
builds a time-series DataFrame, and saves to data/cbs_longitudinal/.

Usage:
    python scripts/fetch_cbs_longitudinal.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import httpx
import pandas as pd

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BUURTCODE  = "BU08450002"
WIJKCODE   = "WK084500"
GEMEENTECODE = "GM0845"

API_BASE   = "https://datasets.cbs.nl/odata/v1/CBS"
CACHE_DIR  = Path(".cache/cbs_longitudinal")
OUT_DIR    = Path("data/cbs_longitudinal")

# Dataset inventory: year label → dataset ID
# Year labels for multi-year bundles use the first year as key.
DATASETS: list[tuple[int, str]] = [
    (1995, "37108"),
    (1997, "37515"),
    (1999, "37681"),
    (2001, "70139NED"),
    (2003, "80868ned"),
    (2004, "81903NED"),   # covers 2004–2008
    (2009, "70904NED"),   # covers 2009–2012
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

# Target measures: code → friendly name, grouped by category
TARGET_MEASURES: dict[str, str] = {
    # Property / valuation
    "M001642":   "gem_woz_waarde",
    "ZW10290":   "pct_eengezinswoning",
    "ZW25805":   "pct_tussenwoning",
    "ZW25806":   "pct_hoekwoning",
    "ZW10300":   "pct_twee_onder_een_kap",
    "ZW10320":   "pct_vrijstaand",
    "ZW10340":   "pct_meergezins",
    "M000297":   "woningvoorraad",
    "1014800":   "pct_koopwoningen",
    "1014850_2": "pct_huurwoningen",
    # Demographics
    "T001036":   "inwoners",
    "M000100":   "bevolkingsdichtheid",
    "M000114":   "gem_huishoudensgrootte",
    "1050015":   "eenpersoonshuishoudens",
    "10680":     "pct_0_15",
    "53310":     "pct_25_45",
    "53715":     "pct_45_65",
    "80200":     "pct_65plus",
    # Socioeconomic
    "M000224":   "gem_inkomen_per_inwoner",
    "M000939":   "mediaan_vermogen",
    "M001796_2": "nettoarbeidsparticipatie",
    "2018790":   "pct_hbo_wo",
    # Sustainability
    "M008297":   "pct_zonnestroom",
    "M008295":   "pct_aardgasvrij",
    "M008296":   "pct_aardgas",
    # Facilities
    "D000025":   "afstand_supermarkt_km",
    "D000028":   "afstand_huisarts_km",
    "D000045":   "afstand_school_km",
    # Urbanity
    "ST0001":    "stedelijkheid",
    "ST0003":    "OAD",
}

CATEGORIES = {
    "Property/Valuation": ["M001642","ZW10290","ZW25805","ZW25806","ZW10300","ZW10320","ZW10340","M000297","1014800","1014850_2"],
    "Demographics":       ["T001036","M000100","M000114","1050015","10680","53310","53715","80200"],
    "Socioeconomic":      ["M000224","M000939","M001796_2","2018790"],
    "Sustainability":     ["M008297","M008295","M008296"],
    "Facilities":         ["D000025","D000028","D000045"],
    "Urbanity":           ["ST0001","ST0003"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    print(msg, flush=True)


def cache_path(dataset_id: str, suffix: str) -> Path:
    return CACHE_DIR / f"{dataset_id}_{suffix}.json"


def load_cache(p: Path) -> dict | list | None:
    if p.exists():
        return json.loads(p.read_text(encoding="utf-8"))
    return None


def save_cache(p: Path, data) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")


def filter_field_from_entity_sets(entity_sets: list[str]) -> str:
    """Derive the Observations filter field name from entity set names."""
    for name in entity_sets:
        if "WijkenEnBuurten" in name and "Groups" in name:
            # e.g. WijkenEnBuurtenGroups → WijkenEnBuurten
            #      WijkenEnBuurten1995Groups → WijkenEnBuurten1995
            return name.replace("Groups", "")
        if name == "RegioSGroups":
            return "RegioS"
        if name == "RegioGroups":
            return "Regio"
    return "WijkenEnBuurten"  # safe default


# ---------------------------------------------------------------------------
# Step 1 & 2: fetch measure availability per dataset
# ---------------------------------------------------------------------------

def fetch_measure_codes(client: httpx.Client, dataset_id: str) -> tuple[set[str], list[str]]:
    """Return (set of all measure codes, entity set names) for a dataset."""
    cp = cache_path(dataset_id, "properties")
    cached = load_cache(cp)
    if cached:
        return set(cached["codes"]), cached["entity_sets"]

    # Fetch entity set list
    try:
        r = client.get(f"{API_BASE}/{dataset_id}", timeout=10)
        r.raise_for_status()
        entity_sets = [v.get("name", "") for v in r.json().get("value", [])]
    except Exception as e:
        log(f"  ERROR fetching root for {dataset_id}: {e}")
        return set(), []

    # Fetch measure codes
    try:
        r = client.get(f"{API_BASE}/{dataset_id}/MeasureCodes", params={"$top": 500}, timeout=15)
        r.raise_for_status()
        codes = {m["Identifier"] for m in r.json().get("value", [])}
    except Exception as e:
        log(f"  ERROR fetching MeasureCodes for {dataset_id}: {e}")
        codes = set()

    save_cache(cp, {"codes": list(codes), "entity_sets": entity_sets})
    time.sleep(0.3)
    return codes, entity_sets


# ---------------------------------------------------------------------------
# Step 3: fetch observations for one dataset
# ---------------------------------------------------------------------------

def fetch_observations(
    client: httpx.Client,
    dataset_id: str,
    filter_field: str,
) -> tuple[dict[str, object] | None, str]:
    """
    Try buurt → wijk → gemeente. Returns (measure_dict, data_level) or (None, "").
    measure_dict maps CBS code → value.
    """
    codes_to_try = [
        (BUURTCODE, "buurt"),
        (BUURTCODE + " ", "buurt"),   # trailing-space variant
        (WIJKCODE,   "wijk"),
        (GEMEENTECODE, "gemeente"),
    ]
    seen_levels: set[str] = set()

    for code, level in codes_to_try:
        if level in seen_levels:
            continue

        cp = cache_path(dataset_id, f"obs_{code.strip()}_{level}")
        cached = load_cache(cp)
        if cached is not None:
            if cached:  # non-empty → hit
                log(f"    cache hit: {level} ({code.strip()})")
                return cached, level
            else:
                seen_levels.add(level)
                continue

        try:
            params = {"$filter": f"{filter_field} eq '{code}'"}
            r = client.get(
                f"{API_BASE}/{dataset_id}/Observations",
                params=params, timeout=20,
            )
            r.raise_for_status()
            rows = r.json().get("value", [])
        except Exception as e:
            log(f"    ERROR fetching {level} ({code.strip()}) from {dataset_id}: {e}")
            save_cache(cp, {})
            seen_levels.add(level)
            time.sleep(0.5)
            continue

        if rows:
            result = {}
            for row in rows:
                val = row.get("Value") if row.get("Value") is not None else row.get("StringValue")
                result[row["Measure"]] = val
            save_cache(cp, result)
            time.sleep(0.5)
            log(f"    fetched {level} ({code.strip()}): {len(rows)} measures")
            return result, level
        else:
            save_cache(cp, {})
            seen_levels.add(level)
            time.sleep(0.5)

    return None, ""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    availability: dict[str, dict] = {}   # dataset_id → {year, available, missing}
    observations: list[dict] = []        # one dict per year

    with httpx.Client(timeout=20) as client:

        # ── Step 2: check measure availability ────────────────────────────────
        log("\n=== STEP 2: MEASURE AVAILABILITY PER DATASET ===\n")

        entity_sets_by_dataset: dict[str, list[str]] = {}

        for year, dataset_id in DATASETS:
            codes, entity_sets = fetch_measure_codes(client, dataset_id)
            entity_sets_by_dataset[dataset_id] = entity_sets

            if not codes:
                log(f"  [{year}] {dataset_id} → SKIP (could not fetch measures)")
                availability[dataset_id] = {
                    "year": year, "available": set(), "missing": set(TARGET_MEASURES),
                }
                continue

            available = set(TARGET_MEASURES) & codes
            missing   = set(TARGET_MEASURES) - codes
            availability[dataset_id] = {
                "year": year, "available": available, "missing": missing,
            }
            log(f"  [{year}] {dataset_id} → {len(available)}/{len(TARGET_MEASURES)} measures "
                f"({'NEW' if not load_cache(cache_path(dataset_id,'properties')) else 'cached'})")
            if missing:
                log(f"    missing: {', '.join(sorted(missing))}")

        # ── Step 3: fetch observations ─────────────────────────────────────────
        log("\n=== STEP 3: FETCHING OBSERVATIONS FOR BU08450002 ===\n")

        for year, dataset_id in DATASETS:
            avail = availability[dataset_id]
            if not avail["available"]:
                log(f"[{year}] {dataset_id} → no target measures — skip")
                observations.append({"year": year, "dataset_id": dataset_id, "data_level": "none"})
                continue

            entity_sets = entity_sets_by_dataset.get(dataset_id, [])
            ff = filter_field_from_entity_sets(entity_sets)
            log(f"[{year}] {dataset_id} (filter field: {ff})")

            raw, level = fetch_observations(client, dataset_id, ff)

            row: dict = {"year": year, "dataset_id": dataset_id, "data_level": level or "not_found"}
            if raw:
                for code, name in TARGET_MEASURES.items():
                    row[name] = raw.get(code)
            else:
                log(f"  NOT FOUND at any level")
                for name in TARGET_MEASURES.values():
                    row[name] = None

            observations.append(row)

    # ── Step 4: build DataFrame ────────────────────────────────────────────────
    log("\n=== STEP 4: LONGITUDINAL DATAFRAME ===\n")

    df = pd.DataFrame(observations).sort_values("year").set_index("year")
    # Coerce measure columns to numeric
    measure_cols = list(TARGET_MEASURES.values())
    df[measure_cols] = df[measure_cols].apply(pd.to_numeric, errors="coerce")

    display = df[["dataset_id", "data_level"] + measure_cols].copy()
    with pd.option_context("display.max_columns", None, "display.width", 200,
                           "display.float_format", "{:.1f}".format):
        print(display.to_string(na_rep="–"))

    # ── Step 5: availability matrix ────────────────────────────────────────────
    log("\n=== STEP 5: MEASURE AVAILABILITY MATRIX ===\n")

    years = df.index.tolist()
    matrix_rows = []
    for category, codes in CATEGORIES.items():
        matrix_rows.append({"measure": f"── {category} ──", **{y: "" for y in years}})
        for code in codes:
            name = TARGET_MEASURES[code]
            if name not in df.columns:
                row_d = {"measure": name, **{y: "–" for y in years}}
            else:
                row_d = {"measure": name}
                for y in years:
                    val = df.loc[y, name] if y in df.index else None
                    row_d[y] = "✓" if (val is not None and not pd.isna(val)) else "–"
            matrix_rows.append(row_d)

    mat = pd.DataFrame(matrix_rows).set_index("measure")
    print(mat.to_string())

    # ── Step 6: summary ────────────────────────────────────────────────────────
    log("\n=== STEP 6: SUMMARY ===\n")

    found = df[df["data_level"].isin(["buurt","wijk","gemeente"])]
    log(f"Years fetched successfully:  {len(found)} / {len(df)}")

    level_summary = df.groupby("data_level").apply(lambda g: sorted(g.index.tolist()))
    for level, yrs in level_summary.items():
        log(f"  {level}: {yrs}")

    not_found = df[df["data_level"] == "not_found"].index.tolist()
    if not_found:
        log(f"Not found at any level:  {not_found}")

    log("")
    full_coverage, partial_coverage, new_only = [], [], []
    for code, name in TARGET_MEASURES.items():
        if name not in df.columns:
            new_only.append(name)
            continue
        series = df[name].dropna()
        if len(series) == len(df):
            full_coverage.append(name)
        elif len(series) > 0:
            first_year = series.index.min()
            partial_coverage.append((name, first_year))
        else:
            new_only.append(name)

    log(f"Full coverage ({len(full_coverage)} measures):")
    for m in full_coverage:
        log(f"  {m}")

    log(f"\nPartial coverage ({len(partial_coverage)} measures):")
    for m, fy in sorted(partial_coverage, key=lambda x: x[1]):
        log(f"  {m:35s}  from {fy}")

    log(f"\nNo data found ({len(new_only)} measures):")
    for m in new_only:
        log(f"  {m}")

    # ── Step 7: save ────────────────────────────────────────────────────────────
    ts_path  = OUT_DIR / f"{BUURTCODE}_timeseries.csv"
    mat_path = OUT_DIR / "availability_matrix.csv"

    df.to_csv(ts_path)
    mat.to_csv(mat_path)

    log(f"\nSaved:\n  {ts_path}\n  {mat_path}")


if __name__ == "__main__":
    main()
