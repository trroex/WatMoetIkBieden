"""Avg floor area for woonfunctie verblijfsobjecten in BU08450002. One WFS call."""
import statistics, httpx, geopandas as gpd

GPKG  = "data/leefbaarometer/geometrie/geometrie-lbm3-2024/buurt 2024.gpkg"
BUURT = "BU08450002"
WFS   = "https://service.pdok.nl/lv/bag/wfs/v2_0"

# Step 1 — bbox from gpkg
gdf  = gpd.read_file(GPKG).to_crs(28992)
geom = gdf[gdf["bu_code"] == BUURT].geometry.iloc[0]
minx, miny, maxx, maxy = [round(v + d * 50) for v, d in zip(geom.bounds, (-1,-1,1,1))]

# Step 2 — WFS fetch (paginate)
params = dict(SERVICE="WFS", VERSION="2.0.0", REQUEST="GetFeature",
              TYPENAMES="bag:verblijfsobject", outputFormat="application/json",
              BBOX=f"{minx},{miny},{maxx},{maxy},EPSG:28992", count=2000)
areas = []
offset = 0
while True:
    r = httpx.get(WFS, params={**params, "startIndex": offset}, timeout=30).json()
    feats = r.get("features", [])
    for f in feats:
        p = f["properties"]
        if "woonfunctie" in (p.get("gebruiksdoel") or "") and p.get("status") == "Verblijfsobject in gebruik":
            if (a := p.get("oppervlakte")):
                areas.append(int(a))
    if len(feats) < 2000:
        break
    offset += 2000

# Step 3 — stats + WOZ estimate
n, mean, med, std = len(areas), statistics.mean(areas), statistics.median(areas), statistics.stdev(areas)
avg_woz_buurt   = 385_000
woz_per_m2      = avg_woz_buurt / mean
target_estimate = woz_per_m2 * 105
actual_woz_2024 = 364_000
ratio           = target_estimate / actual_woz_2024

print(f"\nBuurt {BUURT} — woonfunctie verblijfsobjecten in gebruik")
print(f"  n={n}  mean={mean:.1f}m²  median={med:.1f}m²  std={std:.1f}m²")
print(f"\nWOZ/m² estimate")
print(f"  avg_woz_buurt    = EUR {avg_woz_buurt:,}  (CBS 2023, M001642×1000)")
print(f"  woz_per_m2       = EUR {woz_per_m2:,.0f}/m²")
print(f"  target_estimate  = EUR {target_estimate:,.0f}  (×105m²)")
print(f"  actual_woz_2024  = EUR {actual_woz_2024:,}")
print(f"  ratio            = {ratio:.3f}x")
