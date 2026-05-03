"""
Leefbaarometer 2024 – neighbourhood liveability scores.

Local file lookup: no API call, no rate limit.
Data is downloaded automatically on first use from leefbaarometer.nl (CC0 licence).

Expected paths after download + extraction:
  data/leefbaarometer/scores/
      Leefbaarometer 3.0 - Meting 2024 - open data/
          Leefbaarometer-scores buurten 2002-2024.csv   (or .xlsx)
  data/leefbaarometer/geometrie/
      geometrie-lbm3-2024/
          buurt 2024.gpkg

The GeoPackage uses RD New (EPSG:28992). Coordinate lookups reproject the
query point to RD New before the point-in-polygon test so the GDF is only
loaded once and never reprojected.
"""

from __future__ import annotations

import io
import sys
import zipfile
from pathlib import Path

import numpy as np

from watmoetikbieden.models import LeefbarometerResult

# ── download URLs (CC0 open data) ────────────────────────────────────────────
_SCORES_URL = (
    "https://www.leefbaarometer.nl/resources/open-data-leefbaarometer-meting-2024.zip"
)
_GEOM_URL = (
    "https://www.leefbaarometer.nl/resources/geometrie-lbm3-2024.zip"
)

# ── local paths ───────────────────────────────────────────────────────────────
_SCORES_DIR = Path("data/leefbaarometer/scores")
_GEOM_DIR   = Path("data/leefbaarometer/geometrie")

# Preferred paths (what the zip extracts to, if structure matches exactly)
_SCORES_CSV = _SCORES_DIR / (
    "Leefbaarometer 3.0 - Meting 2024 - open data"
    "/Leefbaarometer-scores buurten 2002-2024.csv"
)
_GEOM_GPKG = _GEOM_DIR / "geometrie-lbm3-2024/buurt 2024.gpkg"

_SCORE_COLS = ["lbm", "fys", "onv", "soc", "vrz", "won"]


def _diag(msg: str) -> None:
    print(f"[LBM] {msg}", file=sys.stderr, flush=True)


def _safe_float(val) -> float | None:
    try:
        f = float(val)
        return None if np.isnan(f) else round(f, 6)
    except (TypeError, ValueError):
        return None


# ── download helpers ──────────────────────────────────────────────────────────

def _download_and_extract(url: str, dest_dir: Path, label: str) -> None:
    """Download a zip from *url* and extract it into *dest_dir*."""
    import httpx

    dest_dir.mkdir(parents=True, exist_ok=True)
    _diag(f"downloading {label} from leefbaarometer.nl … (may take a minute)")
    try:
        resp = httpx.get(url, follow_redirects=True, timeout=180)
        resp.raise_for_status()
    except Exception as exc:
        raise RuntimeError(
            f"Failed to download Leefbaarometer {label}:\n  {exc}\n"
            f"  URL: {url}"
        ) from exc

    size_kb = len(resp.content) // 1024
    _diag(f"extracting {label} ({size_kb:,} KB) → {dest_dir}")
    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        z.extractall(dest_dir)
    _diag(f"{label} ready in {dest_dir}")


def _find_file(base: Path, glob: str) -> Path | None:
    """Return the first match for *glob* under *base*, or None."""
    hits = sorted(base.rglob(glob))
    return hits[0] if hits else None


def _resolve_scores_path() -> Path:
    """
    Return the path to the scores file, downloading if necessary.
    Handles CSV and Excel (xlsx) formats transparently.
    """
    # Try exact expected path first
    if _SCORES_CSV.exists():
        return _SCORES_CSV

    # Download if the whole scores dir is missing or the file isn't there
    if not _SCORES_DIR.exists() or not any(_SCORES_DIR.iterdir()):
        _download_and_extract(_SCORES_URL, _SCORES_DIR, "scores")

    # Search for CSV or Excel after extraction
    for pattern in (
        "Leefbaarometer-scores buurten*.csv",
        "Leefbaarometer-scores buurten*.xlsx",
        "*.csv",
        "*.xlsx",
    ):
        hit = _find_file(_SCORES_DIR, pattern)
        if hit is not None:
            _diag(f"scores file found at: {hit}")
            return hit

    raise FileNotFoundError(
        f"Leefbaarometer scores file not found after download.\n"
        f"  Searched under: {_SCORES_DIR}\n"
        f"  Try downloading manually from:\n    {_SCORES_URL}"
    )


def _resolve_geom_path() -> Path:
    """Return the path to the GeoPackage, downloading if necessary."""
    if _GEOM_GPKG.exists():
        return _GEOM_GPKG

    if not _GEOM_DIR.exists() or not any(_GEOM_DIR.iterdir()):
        _download_and_extract(_GEOM_URL, _GEOM_DIR, "geometry")

    for pattern in ("buurt 2024.gpkg", "*.gpkg"):
        hit = _find_file(_GEOM_DIR, pattern)
        if hit is not None:
            _diag(f"geometry file found at: {hit}")
            return hit

    raise FileNotFoundError(
        f"Leefbaarometer GeoPackage not found after download.\n"
        f"  Searched under: {_GEOM_DIR}\n"
        f"  Try downloading manually from:\n    {_GEOM_URL}"
    )


def _read_scores(path: Path):
    """Read scores as a DataFrame, handling both CSV and Excel formats."""
    import pandas as pd

    suffix = path.suffix.lower()
    if suffix in (".xlsx", ".xls"):
        _diag(f"reading Excel scores from {path.name} …")
        return pd.read_excel(path, engine="openpyxl")
    _diag(f"reading CSV scores from {path.name} …")
    return pd.read_csv(path, low_memory=False)


# ── main class ────────────────────────────────────────────────────────────────

class LeefbarometerLookup:
    """
    Singleton-friendly loader. Instantiate once at application start;
    the scores file and GeoPackage are loaded lazily on first use and cached.
    Data is downloaded automatically from leefbaarometer.nl if not present.
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
        import geopandas as gpd

        scores_path = _resolve_scores_path()
        geom_path   = _resolve_geom_path()

        self._df = _read_scores(scores_path)
        self._df24 = self._df[self._df["jaar"] == 2024].copy()
        self._national_mean = round(float(self._df24["lbm"].mean()), 6)

        _diag(f"loading buurt GeoPackage … ({len(self._df24)} buurten in 2024)")
        self._gdf = gpd.read_file(geom_path)  # stays in EPSG:28992

        _diag(
            f"ready — {len(self._gdf)} buurt polygons, "
            f"national mean lbm={self._national_mean:.3f}"
        )

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

    def _build_result(
        self, bu_code: str, bu_naam: str, lookup_method: str
    ) -> LeefbarometerResult | None:
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
        vs_national = (
            round(lbm_val - self._national_mean, 6)
            if lbm_val is not None
            else 0.0
        )

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

    def lookup_by_coordinates(
        self, lat: float, lon: float
    ) -> LeefbarometerResult | None:
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
