"""
Log-corrected WOZ estimator — standalone validation script.

Model:
  woz_estimate = avg_woz_buurt × (target_m2 / avg_m2_buurt) ^ elasticity
               = avg_woz × (target/mean)^0.7

Equivalently:
  size_factor  = (target / mean)^(0.7 - 1) = (target/mean)^(-0.3)
  woz_estimate = (avg_woz / mean) × target × size_factor

Validated against 5 test addresses across different Dutch regions.
No LVWOZ API calls — CBS buurt avg WOZ used as reference.
"""
from __future__ import annotations

import json
import math
import statistics
import sys
import time
from pathlib import Path

import httpx
import geopandas as gpd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GPKG        = Path("data/leefbaarometer/geometrie/geometrie-lbm3-2024/buurt 2024.gpkg")
BAG_LOC     = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"
BAG_WFS     = "https://service.pdok.nl/lv/bag/wfs/v2_0"
CBS         = "https://datasets.cbs.nl/odata/v1/CBS"

CACHE_CBS   = Path(".cache/cbs_validation")
CACHE_FLOOR = Path(".cache/buurt_floor")
CACHE_TXN   = Path(".cache/cbs_transactions")
OUT_DIR     = Path("data/validation")

ELASTICITY  = 0.7
EXP_RATIO_75  = math.pow(0.75, ELASTICITY)   # ≈ 0.8176
EXP_RATIO_125 = math.pow(1.25, ELASTICITY)   # ≈ 1.1691

_STEDELK = {1: "zeer sterk stedelijk", 2: "sterk stedelijk",
            3: "matig stedelijk",      4: "weinig stedelijk",
            5: "niet stedelijk"}

TEST_ADDRESSES = [
    "Ambachtsweg 72, 5272 RW Sint-Michielsgestel",
    "Olympiaplein 85, 1077 CK Amsterdam",
    "Julianastraat 12, 7311 MX Apeldoorn",
    "Veenkampen 8, 8446 CT Heerenveen",
    "Laan van Meerdervoort 600, 2563 AX Den Haag",
]
SHORT_LABELS = [
    "Ambachtsweg 72 (NB)",
    "Olympiaplein 85 (AMS)",
    "Julianastraat 12 (APE)",
    "Veenkampen 8 (HEE)",
    "Laan v Meerdervoort (DH)",
]

for d in (CACHE_CBS, CACHE_FLOOR, CACHE_TXN, OUT_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _load(p: Path, ttl_days: float) -> dict | None:
    if not p.exists():
        return None
    if (time.time() - p.stat().st_mtime) / 86400 > ttl_days:
        return None
    return json.loads(p.read_text("utf-8"))


def _save(p: Path, d: dict) -> None:
    p.write_text(json.dumps(d, ensure_ascii=False), "utf-8")


def _eur(v: float | None, suffix: str = "") -> str:
    if v is None:
        return "n/a"
    return "EUR {:>10,.0f}".format(v).replace(",", ".") + suffix


def _eur_short(v: float | None) -> str:
    if v is None:
        return "n/a"
    return "EUR {:,.0f}".format(v).replace(",", ".")

# ---------------------------------------------------------------------------
# Lazy gpkg loader
# ---------------------------------------------------------------------------

_gdf_cache: gpd.GeoDataFrame | None = None


def _buurt_gdf() -> gpd.GeoDataFrame:
    global _gdf_cache
    if _gdf_cache is None:
        _gdf_cache = gpd.read_file(GPKG).to_crs(28992)
    return _gdf_cache

# ---------------------------------------------------------------------------
# BAG locatieserver
# ---------------------------------------------------------------------------

def bag_locate(address: str, client: httpx.Client) -> dict | None:
    r = client.get(BAG_LOC, params={
        "q":   address,
        "fq":  "type:adres",
        "rows": 1,
        "fl":  ("id,weergavenaam,straatnaam,huisnummer,postcode,woonplaatsnaam,"
                "nummeraanduiding_id,adresseerbaarobject_id,gemeentecode,"
                "buurtcode,buurtnaam,wijkcode,centroide_rd"),
    }, timeout=15)
    r.raise_for_status()
    docs = r.json().get("response", {}).get("docs", [])
    if not docs:
        return None
    d = docs[0]
    gm = d.get("gemeentecode", "")
    if gm and not str(gm).startswith("GM"):
        d["gemeentecode"] = f"GM{gm}"
    return d

# ---------------------------------------------------------------------------
# BAG WFS – single verblijfsobject (via centroide_rd BBOX + vbo_id match)
# CQL_FILTER on identificatie is unreliable on PDOK WFS 2.0; use a tight
# spatial BBOX instead and match the returned features by vbo_id.
# ---------------------------------------------------------------------------

def bag_vbo_oppervlakte(
    vbo_id: str,
    centroide_rd: str,
    client: httpx.Client,
) -> int | None:
    # Parse "POINT(x y)" from locatieserver centroide_rd
    try:
        parts = centroide_rd.strip()[6:-1].split()
        cx, cy = float(parts[0]), float(parts[1])
    except Exception:
        return None

    # ±15 m box — enough to catch the unit, small enough not to pull neighbours
    r = client.get(BAG_WFS, params={
        "SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature",
        "TYPENAMES": "bag:verblijfsobject",
        "BBOX": f"{cx-15},{cy-15},{cx+15},{cy+15},EPSG:28992",
        "outputFormat": "application/json",
        "count": 50,
    }, timeout=20)
    r.raise_for_status()
    feats = r.json().get("features", [])

    # 1. Exact identificatie match
    for f in feats:
        p = f["properties"]
        if str(p.get("identificatie", "")).lstrip("0") == str(vbo_id).lstrip("0"):
            return p.get("oppervlakte")

    # 2. Fallback: first woonfunctie unit in gebruik at this location
    for f in feats:
        p = f["properties"]
        if ("woonfunctie" in (p.get("gebruiksdoel") or "")
                and p.get("status") == "Verblijfsobject in gebruik"):
            return p.get("oppervlakte")

    return feats[0]["properties"].get("oppervlakte") if feats else None

# ---------------------------------------------------------------------------
# BAG WFS – buurt floor stats (cached 90 days)
# ---------------------------------------------------------------------------

def buurt_floor_stats(buurtcode: str, client: httpx.Client) -> dict | None:
    cp = CACHE_FLOOR / f"{buurtcode}.json"
    cached = _load(cp, 90)
    if cached:
        return cached

    gdf = _buurt_gdf()
    row = gdf[gdf["bu_code"] == buurtcode]
    if row.empty:
        print(f"    [WARN] {buurtcode} not in gpkg", flush=True)
        return None

    minx, miny, maxx, maxy = [
        int(v + d * 50)
        for v, d in zip(row.geometry.iloc[0].bounds, (-1, -1, 1, 1))
    ]

    base = dict(SERVICE="WFS", VERSION="2.0.0", REQUEST="GetFeature",
                TYPENAMES="bag:verblijfsobject",
                outputFormat="application/json",
                BBOX=f"{minx},{miny},{maxx},{maxy},EPSG:28992",
                count=2000)
    areas: list[int] = []
    offset = 0
    while True:
        time.sleep(0.5)
        r = client.get(BAG_WFS, params={**base, "startIndex": offset}, timeout=30)
        r.raise_for_status()
        feats = r.json().get("features", [])
        for f in feats:
            p = f["properties"]
            if ("woonfunctie" in (p.get("gebruiksdoel") or "")
                    and p.get("status") == "Verblijfsobject in gebruik"
                    and (a := p.get("oppervlakte"))):
                areas.append(int(a))
        if len(feats) < 2000:
            break
        offset += 2000

    if not areas:
        return None

    result = {
        "n":         len(areas),
        "mean_m2":   statistics.mean(areas),
        "median_m2": statistics.median(areas),
        "std_m2":    statistics.stdev(areas) if len(areas) > 1 else 0.0,
    }
    _save(cp, result)
    return result

# ---------------------------------------------------------------------------
# CBS 85984NED – WOZ + stedelijkheid (buurt → wijk → gemeente fallback)
# ---------------------------------------------------------------------------

def cbs_woz_data(
    buurtcode: str,
    wijkcode: str | None,
    gemeentecode: str | None,
    client: httpx.Client,
) -> tuple[float | None, int | None, str]:
    """Returns (avg_woz_eur, stedelijkheid, data_level)."""
    levels: list[tuple[str, str]] = [(buurtcode, "buurt")]
    if wijkcode:
        levels.append((wijkcode, "wijk"))
    if gemeentecode:
        levels.append((gemeentecode, "gemeente"))

    for code, level in levels:
        cp = CACHE_CBS / f"{code}.json"
        cached = _load(cp, 7)
        if cached is not None:
            v = cached.get("M001642")
            if v is not None:
                return float(v) * 1000, cached.get("ST0001"), level
            continue  # cached empty or M001642 absent → try next level

        time.sleep(0.3)
        try:
            r = client.get(
                f"{CBS}/85984NED/Observations",
                params={"$filter": f"WijkenEnBuurten eq '{code}'",
                        "$select": "Measure,Value"},
                timeout=20,
            )
            r.raise_for_status()
            rows = r.json().get("value", [])
        except Exception as e:
            print(f"    [CBS ERR] {code}: {e}", flush=True)
            continue

        if not rows:
            _save(cp, {})
            continue

        raw = {row["Measure"]: row.get("Value") for row in rows}
        _save(cp, raw)
        v = raw.get("M001642")
        if v is not None:
            return float(v) * 1000, raw.get("ST0001"), level

    return None, None, "not_found"

# ---------------------------------------------------------------------------
# CBS 83625NED – NOTE: this dataset contains price INDICES, not EUR prices.
# We probe it and report the raw index value; absolute transaction prices
# are not reliably extractable from this dataset without a base-year anchor.
# ---------------------------------------------------------------------------

def cbs_txn_index(gemeentecode: str, client: httpx.Client) -> float | None:
    """
    Returns the most recent CBS 83625NED index value for the gemeente,
    or None if unavailable.  The index is relative to a CBS base year
    (typically 2015=100) — NOT an absolute EUR price.
    """
    if not gemeentecode:
        return None
    cp = CACHE_TXN / f"{gemeentecode}.json"
    cached = _load(cp, 30)
    if cached is not None:
        return cached.get("index")

    index: float | None = None
    try:
        time.sleep(0.3)
        r = client.get(f"{CBS}/83625NED", timeout=10)
        entity_sets: list[str] = []
        if r.status_code == 200:
            entity_sets = [v.get("name", "") for v in r.json().get("value", [])]

        ff = "RegioS"
        for name in entity_sets:
            if "Regio" in name and "Groups" in name:
                ff = name.replace("Groups", "")
                break

        time.sleep(0.3)
        r = client.get(
            f"{CBS}/83625NED/Observations",
            params={"$filter": f"{ff} eq '{gemeentecode}'", "$top": 50},
            timeout=20,
        )
        r.raise_for_status()
        rows = r.json().get("value", [])
        # Take the first non-null value (it's an index, e.g. ~100–250)
        for row in rows:
            v = row.get("Value")
            if v is not None:
                index = float(v)
                break
    except Exception as e:
        print(f"    [TXN WARN] {gemeentecode}: {e}", flush=True)

    _save(cp, {"index": index})
    return index

# ---------------------------------------------------------------------------
# Estimator (pure function)
# ---------------------------------------------------------------------------

def log_estimate(target_m2: float, avg_woz: float, avg_m2: float,
                 e: float = ELASTICITY) -> float:
    """woz_estimate = avg_woz × (target_m2 / avg_m2)^e"""
    return avg_woz * (target_m2 / avg_m2) ** e

# ---------------------------------------------------------------------------
# Validate one address
# ---------------------------------------------------------------------------

def validate(address: str, client: httpx.Client) -> dict:
    R: dict = {"address": address, "ok": False}

    # 1. BAG locatieserver
    print(f"  a) locatieserver…", end="", flush=True)
    try:
        bag = bag_locate(address, client)
    except Exception as e:
        R["error"] = f"BAG loc: {e}"; print(f" ERR: {e}"); return R
    if not bag:
        R["error"] = "BAG: no match"; print(" no match"); return R

    buurtcode    = bag.get("buurtcode")
    wijkcode     = bag.get("wijkcode")
    gemeentecode = bag.get("gemeentecode")
    vbo_id       = bag.get("adresseerbaarobject_id")
    centroide_rd = bag.get("centroide_rd", "")

    R.update({"buurtcode": buurtcode, "wijkcode": wijkcode,
              "gemeentecode": gemeentecode,
              "city":        bag.get("woonplaatsnaam", "?"),
              "weergavenaam": bag.get("weergavenaam", address),
              "straatnaam":  bag.get("straatnaam"),
              "huisnummer":  bag.get("huisnummer")})
    print(f" OK  [{buurtcode}]", flush=True)

    if not buurtcode:
        R["error"] = "no buurtcode returned"; return R

    # 2. BAG WFS – oppervlakte for this specific unit (BBOX + vbo_id match)
    print(f"  b) verblijfsobject oppervlakte…", end="", flush=True)
    time.sleep(0.5)
    try:
        opp = bag_vbo_oppervlakte(vbo_id, centroide_rd, client) if vbo_id else None
    except Exception as e:
        print(f" WARN: {e}", flush=True); opp = None

    if opp is None:
        R["error"] = "oppervlakte not found"; print(" MISSING"); return R
    R["target_m2"] = int(opp)
    print(f" OK  [{opp} m²]", flush=True)

    # 3. CBS 85984NED – avg WOZ + stedelijkheid
    print(f"  c) CBS 85984NED (M001642)…", end="", flush=True)
    avg_woz, stedelk, cbs_level = cbs_woz_data(
        buurtcode, wijkcode, gemeentecode, client
    )
    R.update({"avg_woz_buurt": avg_woz, "cbs_data_level": cbs_level,
              "stedelijkheid": int(stedelk) if stedelk else None})
    if avg_woz is None:
        R["error"] = "CBS M001642 not found at any level"
        print(" MISSING"); return R
    print(f" OK  [{_eur_short(avg_woz)} at {cbs_level}]", flush=True)

    # 4. BAG WFS – buurt floor stats (bbox)
    print(f"  d) buurt floor stats (WFS bbox)…", end="", flush=True)
    stats = buurt_floor_stats(buurtcode, client)
    if not stats:
        R["error"] = "0 units in buurt bbox"; print(" MISSING"); return R
    R["floor_stats"] = stats
    avg_m2 = stats["mean_m2"]
    print(f" OK  [n={stats['n']}, mean={avg_m2:.1f} m²]", flush=True)

    # 5. Compute estimate
    target_m2   = R["target_m2"]
    est         = log_estimate(target_m2, avg_woz, avg_m2)
    base_per_m2 = avg_woz / avg_m2
    size_factor = (target_m2 / avg_m2) ** (ELASTICITY - 1)

    R.update({
        "woz_estimate":     est,
        "base_woz_per_m2":  base_per_m2,
        "size_factor":      size_factor,
        "size_vs_mean_pct": (target_m2 / avg_m2 - 1) * 100,
        "ratio_est_buurt":  est / avg_woz,
        "est_100m2":        log_estimate(100, avg_woz, avg_m2),
    })

    # Sensitivity at ±25%
    e75  = log_estimate(target_m2 * 0.75, avg_woz, avg_m2)
    e125 = log_estimate(target_m2 * 1.25, avg_woz, avg_m2)
    R["sensitivity"] = {
        "at_75pct":  e75,   "ratio_75":  e75  / est,
        "at_125pct": e125,  "ratio_125": e125 / est,
    }

    # 6. CBS 83625NED – price index (not absolute EUR)
    print(f"  e) CBS 83625NED (price index)…", end="", flush=True)
    txn_idx = cbs_txn_index(gemeentecode, client) if gemeentecode else None
    R["cbs_price_index"]  = txn_idx
    # Cannot compute absolute transaction estimate without a base-year anchor
    R["avg_transaction"]  = None
    R["woz_txn_ratio"]    = None
    R["transaction_est"]  = None
    if txn_idx is not None:
        print(f" OK  [index={txn_idx:.1f}]", flush=True)
    else:
        print(" n/a", flush=True)

    R["ok"] = True
    return R

# ---------------------------------------------------------------------------
# Print one address result
# ---------------------------------------------------------------------------

SEP60 = "=" * 60
SEP58 = "-" * 58


def print_result(R: dict, idx: int) -> None:
    print(f"\n{SEP60}")
    print(f"[{idx}] {R['address']}")
    print(SEP60)

    if not R.get("ok"):
        print(f"  ERROR: {R.get('error', 'unknown')}")
        return

    target_m2 = R["target_m2"]
    avg_m2    = R["floor_stats"]["mean_m2"]
    n         = R["floor_stats"]["n"]
    avg_woz   = R["avg_woz_buurt"]
    est       = R["woz_estimate"]
    stedelk   = R.get("stedelijkheid")
    stlabel   = _STEDELK.get(stedelk, "?") if stedelk else "?"

    direction = "below" if target_m2 < avg_m2 else "above"
    sf_note   = "premium for small unit" if R["size_factor"] > 1 else "discount for large unit"

    print(f"  Property:")
    print(f"    Floor area (target):       {target_m2} m²")
    print(f"    Buurt mean floor area:     {avg_m2:.1f} m²  (n={n} units)")
    print(f"    Size vs mean:              {R['size_vs_mean_pct']:+.1f}%  ({direction} average)")
    print(f"    Buurt avg WOZ (CBS):       {_eur_short(avg_woz)}")
    print(f"    CBS data level:            {R['cbs_data_level']}")
    print(f"    Stedelijkheid:             {stedelk} ({stlabel})")

    print(f"\n  Model inputs:")
    print(f"    base_woz_per_m2:           {_eur_short(R['base_woz_per_m2'])}/m²")
    print(f"    size_factor:               {R['size_factor']:.4f}  ({sf_note})")
    print(f"    elasticity:                {ELASTICITY}")

    print(f"\n  Estimates:")
    print(f"    WOZ estimate:              {_eur_short(est)}")
    print(f"    CBS buurt avg WOZ:         {_eur_short(avg_woz)}  (reference)")
    print(f"    Ratio est/buurt_avg:       {R['ratio_est_buurt']:.4f}")

    idx = R.get("cbs_price_index")
    if idx is not None:
        print(f"\n    CBS 83625NED price index:  {idx:.1f}  (base 2015=100; not EUR)")
        print(f"    Note: absolute transaction prices not available from this dataset.")
    else:
        print(f"\n    CBS 83625NED price index:  n/a")

    s    = R["sensitivity"]
    m75  = int(round(target_m2 * 0.75))
    m125 = int(round(target_m2 * 1.25))
    print(f"\n  Sensitivity:")
    print(f"    At {m75:>4d} m² (-25%):  {_eur_short(s['at_75pct'])}  (ratio: {s['ratio_75']:.4f})")
    print(f"    At {m125:>4d} m² (+25%):  {_eur_short(s['at_125pct'])}  (ratio: {s['ratio_125']:.4f})")
    print(SEP58)

# ---------------------------------------------------------------------------
# Print validation summary
# ---------------------------------------------------------------------------

def print_summary(results: list[dict]) -> None:
    ok = [r for r in results if r.get("ok")]

    print(f"\n\n{'VALIDATION SUMMARY':^68}")
    print("=" * 68)

    # Table
    print(f"\n  {'Address':<32}  {'Est WOZ':>12}  {'Buurt avg':>12}  {'Ratio':>6}")
    print(f"  {'-'*66}")
    for r, lbl in zip(results, SHORT_LABELS):
        if r.get("ok"):
            print(f"  {lbl:<32}  {_eur_short(r['woz_estimate']):>12}  "
                  f"{_eur_short(r['avg_woz_buurt']):>12}  {r['ratio_est_buurt']:>6.3f}")
        else:
            print(f"  {lbl:<32}  {'ERROR':>12}  {r.get('error','?')}")
    print(f"  {'-'*66}")

    if ok:
        ratios  = [r["ratio_est_buurt"] for r in ok]
        mean_r  = statistics.mean(ratios)
        std_r   = statistics.stdev(ratios) if len(ratios) > 1 else 0.0
        within  = sum(1 for r in ratios if abs(r - 1.0) <= 0.15)
        print(f"  {'Mean ratio:':<32}  {mean_r:.3f}")
        print(f"  {'Std dev of ratios:':<32}  {std_r:.3f}")
        print(f"  {'Within ±15% of buurt avg:':<32}  {within}/{len(ok)} addresses")
    print(f"  {'-'*66}")

    # Regional ranking
    ranked = sorted(
        [r for r in ok if r.get("est_100m2")],
        key=lambda r: r["est_100m2"], reverse=True,
    )
    print(f"\n  REGIONAL PRICE RANKING (hypothetical 100 m² unit):")
    for i, r in enumerate(ranked, 1):
        city = r.get("city") or r["address"].split(",")[-1].strip()
        print(f"  {i}.  {city:<30}  {_eur_short(r['est_100m2'])}")
    print(f"  {'-'*66}")

    # Elasticity validation
    all_r75  = [r["sensitivity"]["ratio_75"]  for r in ok]
    all_r125 = [r["sensitivity"]["ratio_125"] for r in ok]
    obs_75   = statistics.mean(all_r75)  if all_r75  else None
    obs_125  = statistics.mean(all_r125) if all_r125 else None

    print(f"\n  ELASTICITY VALIDATION (mathematical identity — must always PASS):")
    print(f"  Expected ratio at -25% size:  {EXP_RATIO_75:.4f}  (= 0.75^{ELASTICITY})")
    print(f"  Observed (mean across {len(ok)}):    {obs_75:.6f}" if obs_75 else "  Observed: n/a")
    print(f"  Expected ratio at +25% size:  {EXP_RATIO_125:.4f}  (= 1.25^{ELASTICITY})")
    print(f"  Observed (mean across {len(ok)}):    {obs_125:.6f}" if obs_125 else "  Observed: n/a")

    ok_flag = (obs_75 is not None and abs(obs_75 - EXP_RATIO_75) < 1e-9
               and obs_125 is not None and abs(obs_125 - EXP_RATIO_125) < 1e-9)
    conclusion = ("PASS — ratios match theoretical values exactly"
                  if ok_flag else "FAIL — deviation from expected (implementation bug?)")
    print(f"\n  CONCLUSION: {conclusion}")
    print("=" * 68)

# ---------------------------------------------------------------------------
# Validation A: internal consistency check
# ---------------------------------------------------------------------------

def run_validation_a() -> None:
    print("\nVALIDATION A — Internal consistency: estimate(mean, avg_woz, mean) == avg_woz")
    for mean in (80, 120, 200):
        for avg_woz in (200_000, 385_000, 600_000):
            got = log_estimate(mean, avg_woz, mean)
            assert abs(got - avg_woz) < 0.01, f"FAIL at mean={mean} avg_woz={avg_woz}: got {got}"
    print("  All checks PASS — estimate at mean m2 always equals avg_woz exactly.")

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 68)
    print("LOG-CORRECTED WOZ ESTIMATOR — VALIDATION RUN")
    print(f"Elasticity = {ELASTICITY}")
    print("=" * 68)

    run_validation_a()

    results: list[dict] = []
    with httpx.Client(timeout=30) as client:
        for i, addr in enumerate(TEST_ADDRESSES, 1):
            print(f"\n[{i}/5] {addr}", flush=True)
            R = validate(addr, client)
            results.append(R)
            print_result(R, i)

    print_summary(results)

    # Save JSON
    clean = []
    for r in results:
        clean.append({k: v for k, v in r.items()})
    out = OUT_DIR / "estimator_validation.json"
    out.write_text(
        json.dumps(clean, indent=2, ensure_ascii=False, default=str), "utf-8"
    )
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
