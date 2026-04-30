"""
Two-variable log-linear WOZ estimator — standalone validation.

Model (eengezinswoning):
  woz = avg_woz × (floor_m2 / mean_floor)^0.55 × (plot_m2 / mean_plot)^0.15

Model (meergezins/appartement, no private plot):
  woz = avg_woz × (floor_m2 / mean_floor)^0.65

Validated against real LVWOZ individual WOZ values.
# VALIDATION ONLY — LVWOZ not used in production pipeline
"""
from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path
from shapely.geometry import shape as to_shape, LineString, MultiLineString

import httpx
import geopandas as gpd

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GPKG       = Path("data/leefbaarometer/geometrie/geometrie-lbm3-2024/buurt 2024.gpkg")
BAG_LOC    = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"
BAG_WFS    = "https://service.pdok.nl/lv/bag/wfs/v2_0"
CBS        = "https://datasets.cbs.nl/odata/v1/CBS"
LVWOZ_BASE = "https://api.kadaster.nl/lvwoz/wozwaardeloket-api/v1"  # validation only

CACHE_CBS   = Path(".cache/cbs_validation")
CACHE_FLOOR = Path(".cache/buurt_floor")
CACHE_PLOT  = Path(".cache/buurt_plot")
CACHE_WOZ   = Path(".cache/validation_woz")
OUT_DIR     = Path("data/validation")

for _d in (CACHE_CBS, CACHE_FLOOR, CACHE_PLOT, CACHE_WOZ, OUT_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# Elasticities
E_FLOOR_EEN  = 0.55   # eengezinswoning — floor
E_PLOT_EEN   = 0.15   # eengezinswoning — plot
E_FLOOR_MEER = 0.65   # meergezins/appartement — floor only

# Pand footprint → estimated plot area multipliers
PLOT_FACTOR: dict[str, float] = {
    "tussenwoning":       1.4,
    "hoekwoning":         1.8,
    "twee_onder_een_kap": 2.2,
    "vrijstaand":         3.5,
    "meergezins":         0.0,
}

# CBS 85984NED measure codes for housing type percentages
CBS_TYPE: dict[str, str] = {
    "ZW25805": "tussenwoning",
    "ZW25806": "hoekwoning",
    "ZW10300": "twee_onder_een_kap",
    "ZW10320": "vrijstaand",
    "ZW10340": "meergezins",
}

# National-average type mix fallback (fractions, sum=1)
NL_MIX: dict[str, float] = {
    "tussenwoning": 0.35, "hoekwoning": 0.15,
    "twee_onder_een_kap": 0.15, "vrijstaand": 0.15, "meergezins": 0.20,
}

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
    "Sint-Michielsgestel",
    "Amsterdam",
    "Apeldoorn",
    "Heerenveen",
    "Den Haag",
]

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


def _eur(v: float | None) -> str:
    return f"EUR {int(round(v)):,}".replace(",", ".") if v is not None else "n/a"

# ---------------------------------------------------------------------------
# Lazy gpkg loader
# ---------------------------------------------------------------------------

_gdf_cache: gpd.GeoDataFrame | None = None


def _buurt_gdf() -> gpd.GeoDataFrame:
    global _gdf_cache
    if _gdf_cache is None:
        _gdf_cache = gpd.read_file(GPKG).to_crs(28992)
    return _gdf_cache


def _buurt_bbox(buurtcode: str) -> tuple[int, int, int, int] | None:
    gdf = _buurt_gdf()
    row = gdf[gdf["bu_code"] == buurtcode]
    if row.empty:
        return None
    return tuple(
        int(v + d * 50)
        for v, d in zip(row.geometry.iloc[0].bounds, (-1, -1, 1, 1))
    )  # type: ignore

# ---------------------------------------------------------------------------
# Parse WKT POINT (centroide_rd)
# ---------------------------------------------------------------------------

def _rd(centroide_rd: str) -> tuple[float, float] | None:
    try:
        parts = centroide_rd.strip()[6:-1].split()
        return float(parts[0]), float(parts[1])
    except Exception:
        return None

# ---------------------------------------------------------------------------
# BAG locatieserver
# ---------------------------------------------------------------------------

def bag_locate(address: str, client: httpx.Client) -> dict | None:
    r = client.get(BAG_LOC, params={
        "q": address, "fq": "type:adres", "rows": 1,
        "fl": ("id,weergavenaam,straatnaam,huisnummer,postcode,woonplaatsnaam,"
               "nummeraanduiding_id,adresseerbaarobject_id,gemeentecode,"
               "buurtcode,buurtnaam,wijkcode,centroide_rd"),
    }, timeout=15)
    r.raise_for_status()
    docs = r.json().get("response", {}).get("docs", [])
    if not docs:
        return None
    d = docs[0]
    gm = str(d.get("gemeentecode", "") or "")
    if gm and not gm.startswith("GM"):
        d["gemeentecode"] = f"GM{gm}"
    return d

# ---------------------------------------------------------------------------
# BAG WFS – verblijfsobject (floor area + pand link)
# ---------------------------------------------------------------------------

def bag_vbo(vbo_id: str, centroide_rd: str, client: httpx.Client) -> dict | None:
    xy = _rd(centroide_rd)
    if not xy:
        return None
    cx, cy = xy
    r = client.get(BAG_WFS, params={
        "SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature",
        "TYPENAMES": "bag:verblijfsobject",
        "BBOX": f"{cx-15},{cy-15},{cx+15},{cy+15},EPSG:28992",
        "outputFormat": "application/json", "count": 50,
    }, timeout=20)
    r.raise_for_status()
    feats = r.json().get("features", [])
    # Exact match on identificatie
    for f in feats:
        p = f["properties"]
        if str(p.get("identificatie", "")).lstrip("0") == str(vbo_id).lstrip("0"):
            return p
    # Fallback: first woonfunctie in gebruik
    for f in feats:
        p = f["properties"]
        if ("woonfunctie" in (p.get("gebruiksdoel") or "")
                and p.get("status") == "Verblijfsobject in gebruik"):
            return p
    return feats[0]["properties"] if feats else None

# ---------------------------------------------------------------------------
# BAG WFS – pand polygon(s) around a centroide
# ---------------------------------------------------------------------------

def bag_pand_nearby(centroide_rd: str, client: httpx.Client) -> list[dict]:
    """Returns all pand GeoJSON features within ±100 m of the centroide."""
    xy = _rd(centroide_rd)
    if not xy:
        return []
    cx, cy = xy
    r = client.get(BAG_WFS, params={
        "SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature",
        "TYPENAMES": "bag:pand",
        "BBOX": f"{cx-100},{cy-100},{cx+100},{cy+100},EPSG:28992",
        "SRSNAME": "EPSG:28992",
        "outputFormat": "application/json", "count": 200,
    }, timeout=25)
    r.raise_for_status()
    return r.json().get("features", [])

# ---------------------------------------------------------------------------
# Shapely area (m²) from GeoJSON feature — SRSNAME must be EPSG:28992
# ---------------------------------------------------------------------------

def footprint_m2(feature: dict) -> float | None:
    try:
        a = to_shape(feature["geometry"]).buffer(0).area
        return a if a > 1.0 else None
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Property type inference from shared pand boundaries
# ---------------------------------------------------------------------------

def infer_type(
    target: dict,
    neighbors: list[dict],
    n_units: int,
) -> tuple[str, str]:
    """Returns (prop_type, confidence: 'hoog'|'laag')."""
    if n_units >= 4:
        return "meergezins", "hoog"
    if n_units == 2:
        return "twee_onder_een_kap", "hoog"

    try:
        tg = to_shape(target["geometry"]).buffer(0)
        tg_exp = tg.buffer(0.3)   # slight expansion to catch touching walls
    except Exception:
        return "tussenwoning", "laag"

    shared_walls = 0
    for f in neighbors:
        if f is target:
            continue
        if f["properties"].get("status") != "Pand in gebruik":
            continue
        try:
            nb = to_shape(f["geometry"]).buffer(0)
            if not tg_exp.intersects(nb):
                continue
            ix = tg_exp.intersection(nb)
            # Shared wall: narrow intersection (area < 1 m²) with significant length (> 2 m)
            if ix.area < 1.0 and ix.length > 2.0:
                shared_walls += 1
        except Exception:
            continue

    if shared_walls >= 2:
        return "tussenwoning", "hoog"
    if shared_walls == 1:
        return "hoekwoning", "hoog"
    return "vrijstaand", "hoog"

# ---------------------------------------------------------------------------
# CBS 85984NED – WOZ + housing type mix
# ---------------------------------------------------------------------------

def _parse_cbs_raw(raw: dict, level: str) -> dict:
    woz = raw.get("M001642")
    st  = raw.get("ST0001")
    mix: dict[str, float] = {}
    for code, name in CBS_TYPE.items():
        v = raw.get(code)
        if v is not None:
            mix[name] = float(v)
    return {
        "avg_woz_eur":  float(woz) * 1000 if woz is not None else None,
        "stedelijkheid": int(st) if st else None,
        "type_mix":      mix or None,
        "data_level":    level,
    }


def fetch_cbs(
    buurtcode: str,
    wijkcode: str | None,
    gemeentecode: str | None,
    client: httpx.Client,
) -> dict:
    levels = [(buurtcode, "buurt")]
    if wijkcode:
        levels.append((wijkcode, "wijk"))
    if gemeentecode:
        levels.append((gemeentecode, "gemeente"))

    for code, level in levels:
        cp = CACHE_CBS / f"{code}.json"
        cached = _load(cp, 7)
        if cached is not None:
            if cached.get("M001642") is not None:
                return _parse_cbs_raw(cached, level)
            continue

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
        if raw.get("M001642") is not None:
            return _parse_cbs_raw(raw, level)

    return {"avg_woz_eur": None, "stedelijkheid": None, "type_mix": None,
            "data_level": "not_found"}

# ---------------------------------------------------------------------------
# Buurt correction factor (footprint → plot area multiplier)
# ---------------------------------------------------------------------------

def correction_factor(type_mix: dict[str, float] | None) -> float:
    mix = type_mix or NL_MIX
    total = sum(mix.values()) or 1.0
    return sum((mix.get(t, 0.0) / total) * f for t, f in PLOT_FACTOR.items())

# ---------------------------------------------------------------------------
# Buurt floor stats (cached 90 days)
# ---------------------------------------------------------------------------

def buurt_floor(buurtcode: str, client: httpx.Client) -> dict | None:
    cp = CACHE_FLOOR / f"{buurtcode}.json"
    if (c := _load(cp, 90)):
        return c

    bbox = _buurt_bbox(buurtcode)
    if not bbox:
        return None
    minx, miny, maxx, maxy = bbox

    areas: list[int] = []
    offset = 0
    while True:
        time.sleep(0.5)
        r = client.get(BAG_WFS, params={
            "SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature",
            "TYPENAMES": "bag:verblijfsobject", "outputFormat": "application/json",
            "BBOX": f"{minx},{miny},{maxx},{maxy},EPSG:28992",
            "count": 2000, "startIndex": offset,
        }, timeout=30)
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
    result = {"n": len(areas), "mean_m2": statistics.mean(areas),
              "median_m2": statistics.median(areas),
              "std_m2": statistics.stdev(areas) if len(areas) > 1 else 0.0}
    _save(cp, result)
    return result

# ---------------------------------------------------------------------------
# Buurt plot stats (pand footprints × correction, cached 90 days)
# ---------------------------------------------------------------------------

def buurt_plot(buurtcode: str, type_mix: dict | None, client: httpx.Client) -> dict | None:
    cp = CACHE_PLOT / f"{buurtcode}.json"
    if (c := _load(cp, 90)):
        return c

    bbox = _buurt_bbox(buurtcode)
    if not bbox:
        return None
    minx, miny, maxx, maxy = bbox

    footprints: list[float] = []
    offset = 0
    while True:
        time.sleep(0.5)
        r = client.get(BAG_WFS, params={
            "SERVICE": "WFS", "VERSION": "2.0.0", "REQUEST": "GetFeature",
            "TYPENAMES": "bag:pand", "outputFormat": "application/json",
            "SRSNAME": "EPSG:28992",
            "BBOX": f"{minx},{miny},{maxx},{maxy},EPSG:28992",
            "count": 2000, "startIndex": offset,
        }, timeout=30)
        r.raise_for_status()
        feats = r.json().get("features", [])
        for f in feats:
            p = f["properties"]
            if (p.get("status") == "Pand in gebruik"
                    and (p.get("aantal_verblijfsobjecten") or 0) >= 1):
                a = footprint_m2(f)
                if a:
                    footprints.append(a)
        if len(feats) < 2000:
            break
        offset += 2000

    if not footprints:
        return None

    corr       = correction_factor(type_mix)
    mean_fp    = statistics.mean(footprints)
    result = {
        "n":               len(footprints),
        "mean_footprint":  round(mean_fp, 1),
        "correction":      round(corr, 4),
        "mean_plot_m2":    round(mean_fp * corr, 1),
    }
    _save(cp, result)
    return result

# ---------------------------------------------------------------------------
# LVWOZ (validation only — not used in production pipeline)
# ---------------------------------------------------------------------------

def lvwoz_fetch(numid: str, client: httpx.Client) -> dict | None:
    # VALIDATION ONLY — LVWOZ not used in production pipeline
    cp = CACHE_WOZ / f"{numid}.json"
    if (c := _load(cp, 365)):
        return c

    time.sleep(0.5)
    try:
        r = client.get(
            f"{LVWOZ_BASE}/wozwaarde/nummeraanduiding/{numid}",
            headers={"User-Agent": "Mozilla/5.0 (compatible; validation-script)",
                     "Accept": "application/json",
                     "Referer": "https://www.wozwaardeloket.nl/"},
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"    [LVWOZ ERR] {numid}: {e}", flush=True)
        return None

    waarden = data.get("wozWaarden", [])
    if not waarden:
        return None
    most_recent = max(waarden, key=lambda w: w.get("peildatum", ""))
    result = {
        "actual_woz": most_recent.get("vastgesteldeWaarde"),
        "peildatum":  most_recent.get("peildatum"),
        "history":    sorted(
            [{"year": int(w["peildatum"][:4]), "woz": w.get("vastgesteldeWaarde")}
             for w in waarden],
            key=lambda x: x["year"],
        ),
    }
    _save(cp, result)
    return result

# ---------------------------------------------------------------------------
# Estimators
# ---------------------------------------------------------------------------

def est_two_var(
    floor: float, plot: float,
    avg_woz: float, mean_floor: float, mean_plot: float,
    prop_type: str,
) -> float:
    if prop_type == "meergezins":
        return avg_woz * (floor / mean_floor) ** E_FLOOR_MEER
    return avg_woz * (floor / mean_floor) ** E_FLOOR_EEN * (plot / mean_plot) ** E_PLOT_EEN


def est_one_var(floor: float, avg_woz: float, mean_floor: float) -> float:
    return avg_woz * (floor / mean_floor) ** 0.70

# ---------------------------------------------------------------------------
# Validate one address
# ---------------------------------------------------------------------------

def validate(address: str, client: httpx.Client) -> dict:
    R: dict = {"address": address, "ok": False}

    # 1. BAG locatieserver
    print("  a) locatieserver…", end="", flush=True)
    try:
        bag = bag_locate(address, client)
    except Exception as e:
        R["error"] = f"BAG: {e}"; print(f" ERR"); return R
    if not bag:
        R["error"] = "no match"; print(" no match"); return R

    buurtcode    = bag.get("buurtcode")
    wijkcode     = bag.get("wijkcode")
    gemeentecode = bag.get("gemeentecode")
    vbo_id       = bag.get("adresseerbaarobject_id")
    centroide_rd = bag.get("centroide_rd", "")
    numid        = bag.get("nummeraanduiding_id")
    for k in ("buurtcode", "wijkcode", "gemeentecode", "woonplaatsnaam",
              "weergavenaam", "straatnaam", "huisnummer", "nummeraanduiding_id"):
        R[k] = bag.get(k)
    print(f" OK  [{buurtcode}]", flush=True)
    if not buurtcode:
        R["error"] = "no buurtcode"; return R

    # 2. BAG WFS – verblijfsobject
    print("  b) verblijfsobject…", end="", flush=True)
    time.sleep(0.5)
    try:
        vbo = bag_vbo(vbo_id, centroide_rd, client)
    except Exception as e:
        vbo = None; print(f" WARN: {e}", flush=True)
    if not vbo or not vbo.get("oppervlakte"):
        R["error"] = "oppervlakte missing"; print(" MISSING"); return R

    floor_m2     = int(vbo["oppervlakte"])
    gebruiksdoel = vbo.get("gebruiksdoel") or ""
    pand_ids_raw = vbo.get("pandidentificatie", "")
    pand_ids     = pand_ids_raw if isinstance(pand_ids_raw, list) else [pand_ids_raw] if pand_ids_raw else []
    pand_id      = pand_ids[0] if pand_ids else None
    R.update({"floor_m2": floor_m2, "gebruiksdoel": gebruiksdoel, "pand_id": pand_id})
    print(f" OK  [{floor_m2} m², pand={pand_id}]", flush=True)

    # 3. BAG WFS – pand polygon + type inference
    print("  c) pand polygon + type…", end="", flush=True)
    time.sleep(0.5)
    target_feat = None
    nearby_panden: list[dict] = []
    if centroide_rd:
        try:
            nearby_panden = bag_pand_nearby(centroide_rd, client)
        except Exception as e:
            print(f" WFS WARN: {e}", flush=True)

    # Find target pand by id
    if pand_id and nearby_panden:
        for f in nearby_panden:
            if (str(f["properties"].get("identificatie", "")).lstrip("0")
                    == str(pand_id).lstrip("0")):
                target_feat = f
                break

    fp_m2: float | None = None
    prop_type = "tussenwoning"
    type_conf  = "laag"
    n_units    = 1

    if target_feat:
        fp_m2   = footprint_m2(target_feat)
        n_units = int(target_feat["properties"].get("aantal_verblijfsobjecten") or 1)
        prop_type, type_conf = infer_type(target_feat, nearby_panden, n_units)

    if fp_m2 is None or fp_m2 <= 1.0:
        print(" MISSING polygon — floor-only fallback", flush=True)
        R["plot_missing"] = True
        target_plot_m2 = 0.0
    else:
        tc = PLOT_FACTOR.get(prop_type, 1.4)
        target_plot_m2 = fp_m2 * tc if prop_type != "meergezins" else 0.0
        R.update({
            "pand_footprint_m2": round(fp_m2, 1),
            "prop_type":         prop_type,
            "type_confidence":   type_conf,
            "type_correction":   tc,
            "plot_m2":           round(target_plot_m2, 1),
        })
        print(f" OK  [{fp_m2:.0f} m² → {prop_type} × {tc:.1f} = {target_plot_m2:.0f} m² plot]", flush=True)

    R.setdefault("prop_type", prop_type)
    R.setdefault("type_confidence", type_conf)
    R.setdefault("plot_missing", False)
    R.setdefault("plot_m2", target_plot_m2)

    # 4. CBS 85984NED
    print("  d) CBS 85984NED…", end="", flush=True)
    cbs = fetch_cbs(buurtcode, wijkcode, gemeentecode, client)
    avg_woz = cbs["avg_woz_eur"]
    R.update({"avg_woz_buurt": avg_woz, "cbs_data_level": cbs["data_level"],
              "stedelijkheid": cbs.get("stedelijkheid"),
              "cbs_type_mix":  cbs.get("type_mix")})
    if avg_woz is None:
        R["error"] = "CBS M001642 missing"; print(" MISSING"); return R
    print(f" OK  [{_eur(avg_woz)} at {cbs['data_level']}]", flush=True)

    # 5. Buurt floor stats
    print("  e) buurt floor stats…", end="", flush=True)
    fs = buurt_floor(buurtcode, client)
    if not fs:
        R["error"] = "buurt floor: 0 units"; print(" MISSING"); return R
    R["floor_stats"] = fs
    mean_floor = fs["mean_m2"]
    print(f" OK  [n={fs['n']}, mean={mean_floor:.1f} m²]", flush=True)

    # 6. Buurt plot stats
    print("  f) buurt plot stats…", end="", flush=True)
    ps = buurt_plot(buurtcode, cbs.get("type_mix"), client)
    R["plot_stats"] = ps
    mean_plot = ps["mean_plot_m2"] if ps else None
    if ps:
        print(f" OK  [n={ps['n']}, corr={ps['correction']:.3f}, mean={mean_plot:.1f} m²]", flush=True)
    else:
        print(" n/a", flush=True)

    # 7. LVWOZ (validation only)
    print("  g) LVWOZ…", end="", flush=True)
    actual = None
    if numid:
        try:
            actual = lvwoz_fetch(str(numid), client)
        except Exception as e:
            print(f" ERR: {e}", flush=True)
    R["actual_woz"] = actual
    if actual:
        print(f" OK  [{_eur(actual['actual_woz'])} @ {actual['peildatum']}]", flush=True)
    else:
        print(" n/a", flush=True)

    # 8. Estimates
    fl = float(floor_m2)
    est_old = est_one_var(fl, avg_woz, mean_floor)

    plot_ok = (not R["plot_missing"] and mean_plot and mean_plot > 0
               and target_plot_m2 > 0 and prop_type != "meergezins")
    if plot_ok:
        est_new = est_two_var(fl, target_plot_m2, avg_woz, mean_floor, mean_plot, prop_type)
        R["used_floor_only"] = False
    else:
        # Meergezins or missing plot: use meergezins elasticity if applicable
        if prop_type == "meergezins":
            est_new = avg_woz * (fl / mean_floor) ** E_FLOOR_MEER
        else:
            est_new = est_old
        R["used_floor_only"] = True

    R.update({"est_old": est_old, "est_new": est_new})

    # 9. Compare vs actual WOZ
    if actual and actual.get("actual_woz"):
        aw = float(actual["actual_woz"])
        R.update({
            "ratio_old":     est_old / aw,
            "ratio_new":     est_new / aw,
            "err_pct_old":   abs(est_old / aw - 1) * 100,
            "err_pct_new":   abs(est_new / aw - 1) * 100,
        })

    R["ok"] = True
    return R

# ---------------------------------------------------------------------------
# Print one address result
# ---------------------------------------------------------------------------

_SEP  = "=" * 64
_sep  = "-" * 62


def print_result(R: dict, idx: int) -> None:
    print(f"\n{_SEP}")
    print(f"[{idx}] {R['address']}")
    print(_SEP)

    if not R.get("ok"):
        print(f"  ERROR: {R.get('error', '?')}")
        return

    fl      = R["floor_m2"]
    avg_woz = R["avg_woz_buurt"]
    mf      = R["floor_stats"]["mean_m2"]
    n_fl    = R["floor_stats"]["n"]
    ps      = R.get("plot_stats")
    mean_pl = ps["mean_plot_m2"] if ps else None

    print("  Property inputs:")
    print(f"    Floor area (target):        {fl} m²")
    if not R.get("plot_missing"):
        fp = R.get("pand_footprint_m2", 0)
        tc = R.get("type_correction", 0)
        pl = R.get("plot_m2", 0)
        print(f"    Plot area (target):         {pl:.0f} m²  "
              f"[{fp:.0f} m² footprint × {tc:.1f}]")
    else:
        print(f"    Plot area (target):         MISSING — floor-only fallback")
    print(f"    Property type inferred:     {R.get('prop_type','?')}  "
          f"[confidence: {R.get('type_confidence','?')}]")
    print(f"    Gebruiksdoel:               {R.get('gebruiksdoel','?')}")

    stedelk = R.get("stedelijkheid")
    print(f"\n  Buurt averages ({R.get('buurtcode','?')}):")
    print(f"    Mean floor area:            {mf:.1f} m²  (n={n_fl} units)")
    if ps:
        print(f"    Mean plot area:             {mean_pl:.1f} m²  "
              f"(n={ps['n']} panden, corr={ps['correction']:.3f})")
    else:
        print(f"    Mean plot area:             n/a")
    print(f"    Avg WOZ (CBS 2023):         {_eur(avg_woz)}")
    if stedelk:
        print(f"    Stedelijkheid:              {stedelk} ({_STEDELK.get(stedelk,'?')})")

    prop_type = R.get("prop_type", "tussenwoning")
    ef = E_FLOOR_MEER if prop_type == "meergezins" else E_FLOOR_EEN
    ep = 0.0      if prop_type == "meergezins" else E_PLOT_EEN
    ff = (fl / mf) ** ef

    print(f"\n  Model calculation:")
    print(f"    floor_factor (^{ef}):      ({fl}/{mf:.1f})^{ef} = {ff:.4f}")
    if not R.get("used_floor_only") and mean_pl and mean_pl > 0:
        pl  = R.get("plot_m2", 0.0)
        pf  = (pl / mean_pl) ** ep
        comb = ff * pf
        print(f"    plot_factor  (^{ep}):      ({pl:.0f}/{mean_pl:.1f})^{ep} = {pf:.4f}")
        print(f"    combined multiplier:        {comb:.4f}")
    else:
        if prop_type == "meergezins":
            print(f"    plot_factor:                0.00  (meergezins — no private plot)")
        else:
            print(f"    plot_factor:                n/a  (pand polygon missing)")
        print(f"    combined multiplier:        {ff:.4f}")

    diff    = R["est_new"] - R["est_old"]
    diff_pct = (diff / R["est_old"]) * 100 if R["est_old"] else 0

    print(f"\n  Estimates:")
    print(f"    Previous model (floor only): {_eur(R['est_old'])}")
    print(f"    Updated model (floor+plot):  {_eur(R['est_new'])}")
    print(f"    Difference:                  "
          f"{'+'if diff>=0 else ''}{_eur(diff)} ({diff_pct:+.1f}%)")

    actual = R.get("actual_woz")
    if actual and actual.get("actual_woz"):
        aw = actual["actual_woz"]
        imp = R["err_pct_old"] - R["err_pct_new"]
        print(f"\n  Validation:")
        print(f"    Actual WOZ (LVWOZ):         {_eur(aw)}  [{actual['peildatum']}]")
        print(f"    CBS year used:              2023 (M001642)")
        if actual["peildatum"] and "2024" not in actual["peildatum"]:
            print(f"    Year alignment note:        CBS=2023, LVWOZ={actual['peildatum'][:4]}")
        print(f"    Ratio old model / actual:   {R['ratio_old']:.3f}")
        print(f"    Ratio new model / actual:   {R['ratio_new']:.3f}")
        print(f"    Improvement:                {imp:+.1f} pp  "
              f"({'better' if imp > 0 else 'worse or unchanged'})")
    else:
        print(f"\n  Validation:                   n/a (LVWOZ not available)")
    print(_sep)

# ---------------------------------------------------------------------------
# Validation summary
# ---------------------------------------------------------------------------

def print_summary(results: list[dict]) -> None:
    ok_all  = [r for r in results if r.get("ok")]
    ok_woz  = [r for r in ok_all if r.get("actual_woz") and r.get("actual_woz", {}).get("actual_woz")]

    print(f"\n\n{'VALIDATION SUMMARY — TWO-VARIABLE MODEL':^68}")
    print("=" * 68)

    print(f"\n  {'Address':<22} {'Floor':>6} {'Plot':>7} {'Estimate':>12} {'Actual WOZ':>12} {'Ratio':>6}")
    print(f"  {'-'*67}")
    for r, lbl in zip(results, SHORT_LABELS):
        if r.get("ok"):
            pl_str  = f"{r.get('plot_m2',0):.0f}m²" if not r.get("plot_missing") else "n/a"
            aw      = (r.get("actual_woz") or {}).get("actual_woz")
            act_str = _eur(aw) if aw else "n/a"
            rat_str = f"{r['ratio_new']:.3f}" if "ratio_new" in r else "n/a"
            print(f"  {lbl:<22} {str(r['floor_m2'])+'m²':>6} {pl_str:>7} "
                  f"{_eur(r['est_new']):>12} {act_str:>12} {rat_str:>6}")
        else:
            print(f"  {lbl:<22} ERROR: {r.get('error','?')}")
    print(f"  {'-'*67}")

    if ok_woz:
        mae_old = statistics.mean(r["err_pct_old"] for r in ok_woz)
        mae_new = statistics.mean(r["err_pct_new"] for r in ok_woz)
        w15_old = sum(1 for r in ok_woz if abs(r["ratio_old"] - 1) <= 0.15)
        w15_new = sum(1 for r in ok_woz if abs(r["ratio_new"] - 1) <= 0.15)
        print(f"  {'Mean absolute error (old):':<37} {mae_old:.1f}%")
        print(f"  {'Mean absolute error (new):':<37} {mae_new:.1f}%")
        print(f"  {'Improvement from plot variable:':<37} {mae_old - mae_new:+.1f} pp")
        print(f"  {'Within ±15% (old model):':<37} {w15_old}/{len(ok_woz)}")
        print(f"  {'Within ±15% (new model):':<37} {w15_new}/{len(ok_woz)}")
    else:
        print("  No LVWOZ data available — accuracy metrics not computed.")
    print(f"  {'-'*67}")

    print(f"\n  ELASTICITY USED:")
    print(f"    Eengezinswoning:  floor^{E_FLOOR_EEN} × plot^{E_PLOT_EEN}"
          f"  (total={E_FLOOR_EEN + E_PLOT_EEN})")
    print(f"    Meergezins:       floor^{E_FLOOR_MEER}  (no plot term)")
    print(f"\n  NOTE: Elasticities are literature-based priors.")
    print(f"  Calibration against a larger transaction dataset will")
    print(f"  improve accuracy significantly.")
    print("=" * 68)

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print("=" * 68)
    print("TWO-VARIABLE WOZ ESTIMATOR — VALIDATION RUN")
    print(f"floor^{E_FLOOR_EEN} × plot^{E_PLOT_EEN}  |  meergezins: floor^{E_FLOOR_MEER}")
    print("=" * 68)

    results: list[dict] = []
    with httpx.Client(timeout=30) as client:
        for i, addr in enumerate(TEST_ADDRESSES, 1):
            print(f"\n[{i}/5] {addr}", flush=True)
            R = validate(addr, client)
            results.append(R)
            print_result(R, i)

    print_summary(results)

    out = OUT_DIR / "two_variable_estimator.json"
    clean = [{k: v for k, v in r.items()} for r in results]
    out.write_text(json.dumps(clean, indent=2, ensure_ascii=False, default=str), "utf-8")
    print(f"\nSaved: {out}")


if __name__ == "__main__":
    main()
