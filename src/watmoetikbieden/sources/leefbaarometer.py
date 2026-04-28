"""
Leefbaarometer 2024 – neighbourhood liveability scores.

Local file lookup: no API call, no rate limit.
Data must be present at:
  data/leefbaarometer/scores/Leefbaarometer 3.0 - Meting 2024 - open data/
      Leefbaarometer-scores buurten 2002-2024.csv
  data/leefbaarometer/geometrie/geometrie-lbm3-2024/buurt 2024.gpkg

The GeoPackage uses RD New (EPSG:28992). Coordinate lookups reproject the
query point to RD New before the point-in-polygon test so the GDF is only
loaded once and never reprojected.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np

from watmoetikbieden.models import LeefbarometerResult

_SCORES_CSV = Path(
    "data/leefbaarometer/scores"
    "/Leefbaarometer 3.0 - Meting 2024 - open data"
    "/Leefbaarometer-scores buurten 2002-2024.csv"
)
_GEOM_GPKG = Path(
    "data/leefbaarometer/geometrie/geometrie-lbm3-2024/buurt 2024.gpkg"
)

_SCORE_COLS = ["lbm", "fys", "onv", "soc", "vrz", "won"]


def _diag(msg: str) -> None:
    print(f"[LBM] {msg}", file=sys.stderr, flush=True)


def _safe_float(val) -> float | None:
    try:
        f = float(val)
        return None if np.isnan(f) else round(f, 6)
    except (TypeError, ValueError):
        return None


class LeefbarometerLookup:
    """
    Singleton-friendly loader. Instantiate once at application start;
    the CSV and GeoPackage are loaded lazily on first use and cached.
    """

    def __init__(self) -> None:
        self._df = None        # full multi-year scores DataFrame
        self._df24 = None      # 2024 slice
        self._gdf = None       # buurt polygons in RD New (EPSG:28992)
        self._national_mean: float | None = None
        self._transformer = None  # WGS84 → RD New

    # ── loading ──────────────────────────────────────────────────────────────

    def _ensure_loaded(self) -> None:
        if self._df24 is not None:
            return
        import pandas as pd
        import geopandas as gpd

        if not _SCORES_CSV.exists():
            raise FileNotFoundError(f"Leefbaarometer scores not found: {_SCORES_CSV}")
        if not _GEOM_GPKG.exists():
            raise FileNotFoundError(f"Leefbaarometer geometry not found: {_GEOM_GPKG}")

        _diag("loading scores CSV…")
        self._df = pd.read_csv(_SCORES_CSV, low_memory=False)
        self._df24 = self._df[self._df["jaar"] == 2024].copy()
        self._national_mean = round(float(self._df24["lbm"].mean()), 6)

        _diag(f"loading buurt GeoPackage… ({len(self._df24)} buurten in 2024)")
        self._gdf = gpd.read_file(_GEOM_GPKG)  # stays in EPSG:28992

        _diag(f"ready — {len(self._gdf)} buurt polygons, national mean lbm={self._national_mean:.3f}")

    def _wgs84_to_rd(self, lat: float, lon: float):
        """Return a Shapely Point in EPSG:28992 (RD New)."""
        from pyproj import Transformer
        from shapely.geometry import Point
        if self._transformer is None:
            self._transformer = Transformer.from_crs(
                "EPSG:4326", "EPSG:28992", always_xy=True
            )
        x, y = self._transformer.transform(lon, lat)
        return Point(x, y)

    # ── score extraction ─────────────────────────────────────────────────────

    def _build_result(self, bu_code: str, bu_naam: str, lookup_method: str) -> LeefbarometerResult | None:
        row24 = self._df24[self._df24["bu_code"] == bu_code]
        if row24.empty:
            _diag(f"no 2024 score for bu_code={bu_code}")
            return None

        s = row24.iloc[0]
        scores_2024 = {col: _safe_float(s[col]) for col in _SCORE_COLS}

        history_rows = self._df[self._df["bu_code"] == bu_code].sort_values("jaar")
        lbm_history = [
            {"jaar": int(r["jaar"]), "lbm": _safe_float(r["lbm"])}
            for _, r in history_rows.iterrows()
        ]

        lbm_val = scores_2024["lbm"]
        vs_national = round(lbm_val - self._national_mean, 6) if lbm_val is not None else 0.0

        return LeefbarometerResult(
            bu_code=bu_code,
            bu_naam=bu_naam,
            scores_2024=scores_2024,
            lbm_history=lbm_history,
            national_mean_2024=self._national_mean,
            score_vs_national=vs_national,
            lookup_method=lookup_method,
        )

    # ── public API ───────────────────────────────────────────────────────────

    def lookup_by_buurtcode(self, bu_code: str) -> LeefbarometerResult | None:
        self._ensure_loaded()
        geo_row = self._gdf[self._gdf["bu_code"] == bu_code]
        bu_naam = geo_row.iloc[0]["bu_naam"] if not geo_row.empty else bu_code
        _diag(f"buurtcode lookup: {bu_code} → {bu_naam}")
        return self._build_result(bu_code, bu_naam, "buurtcode")

    def lookup_by_coordinates(self, lat: float, lon: float) -> LeefbarometerResult | None:
        self._ensure_loaded()
        pt = self._wgs84_to_rd(lat, lon)
        hit = self._gdf[self._gdf.geometry.contains(pt)]
        if hit.empty:
            _diag(f"coordinates ({lat}, {lon}) did not match any buurt polygon")
            return None
        row = hit.iloc[0]
        bu_code = row["bu_code"]
        bu_naam = row["bu_naam"]
        _diag(f"coordinates lookup: ({lat}, {lon}) → {bu_naam} ({bu_code})")
        return self._build_result(bu_code, bu_naam, "coordinates")
