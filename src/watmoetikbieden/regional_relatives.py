"""
Province-level WOZ price relatives derived from CBS 85984NED via weighted OLS.

═══════════════════════════════════════════════════════════════════════════════
REGRESSION MODEL (per province, no intercept, weighted by woningvoorraad)
═══════════════════════════════════════════════════════════════════════════════

  gem_woz_buurt_i = β_tuss × pct_tussenwoning_i / 100
                  + β_hoek × pct_hoekwoning_i    / 100
                  + β_twee × pct_twee_i           / 100
                  + β_vrij × pct_vrijstaand_i     / 100
                  + β_meer × pct_meergezins_i     / 100
                  + ε_i

  weights = woningvoorraad_i   (larger buurten get more influence)
  no intercept  →  β_t can be interpreted as the province-level average
                   WOZ value (in €1 000) for type t

The β coefficients are stored per province using the same CBS type codes as
woz_estimator.py (ZW25805 / ZW25806 / ZW10300 / ZW10320 / ZW25810).

The formula in woz_estimator.py is:
  W_type = gem_woz_buurt × β_target / Σ_t(pct_t/100 × β_t)

Because this is a ratio, the β units (€1 000) cancel and the formula gives
the same result as using dimensionless relatives.  No normalisation needed.

═══════════════════════════════════════════════════════════════════════════════
CACHE
═══════════════════════════════════════════════════════════════════════════════

Result is cached at .cache/kwb_bulk/regional_relatives.json.
The cache is invalidated whenever buurten_2024.csv is refreshed (i.e. when
kwb_bulk._META_FILE is newer than the JSON).

No new package dependencies: numpy (already in core deps).
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

# ── cache ──────────────────────────────────────────────────────────────────────
_CACHE_DIR  = Path(".cache/kwb_bulk")
_JSON_CACHE = _CACHE_DIR / "regional_relatives.json"
_META_FILE  = _CACHE_DIR / "buurten_2025.meta.json"   # source freshness reference (86165NED)

# Minimum number of buurten with complete data required to use regional OLS
_MIN_BUURTEN = 30

# CBS type codes → pct column name (matches woz_estimator._PCT_FIELD_TO_TYPE)
# ── OLS groups ────────────────────────────────────────────────────────────────
#
# Tussenwoning, hoekwoning, and twee-onder-één-kap are highly correlated at
# the buurt level (they always appear together in the same urban environments).
# Fitting them as separate OLS columns routinely produces negative betas due
# to multicollinearity.
#
# Solution: combine all three into a single "grondgebonden" group for the OLS
# (3 predictors instead of 5).  After fitting, the grondgebonden β is split
# back into individual type betas using the *national price ratios* as fixed
# weights.  This captures provincial variation in the overall grondgebonden
# price level while maintaining the CBS-sourced within-group type ordering.
#
# OLS columns (3):
#   grondgebonden  = pct_tuss + pct_hoek + pct_twee  (all terrace / semi-det.)
#   vrijstaand     = pct_vrijstaand
#   meergezins     = pct_meergezinswoning
#
# National prices used to split β_grond into individual type betas (in €):
_NAT_PRICES: dict[str, int] = {
    "ZW25805": 435_470,   # Tussenwoning
    "ZW25806": 446_521,   # Hoekwoning
    "ZW10300": 483_654,   # Twee-onder-één-kap
    "ZW10320": 650_139,   # Vrijstaande woning
    "ZW25810": 380_110,   # Appartement / meergezins
    "T001100":  450_985,  # National all-types average (split denominator)
}

# β_tuss = β_grond × (NAT_PRICES["ZW25805"] / NAT_PRICES["T001100"]), etc.
# The denominator T001100 cancels in the step-1 ratio formula, so the choice
# only affects interpretation — any positive constant would do.


def _diag(msg: str) -> None:
    import sys
    print(f"[REG] {msg}", file=sys.stderr, flush=True)


def _cache_is_fresh() -> bool:
    """JSON cache is fresh when it exists and is at least as new as the source CSV."""
    if not _JSON_CACHE.exists() or not _META_FILE.exists():
        return False
    return _JSON_CACHE.stat().st_mtime >= _META_FILE.stat().st_mtime


def _run_ols(df_prov) -> dict[str, float] | None:
    """
    Fit weighted OLS (no intercept) with 3 grouped predictors.

    X columns:
      0  grondgebonden fraction  = (pct_tuss + pct_hoek + pct_twee) / 100
      1  vrijstaand fraction     = pct_vrijstaand / 100
      2  meergezins fraction     = pct_meergezinswoning / 100

    Returns {type_code: beta_in_k_eur} with all 5 CBS type codes, or None when
    the fit cannot be trusted (too few buurten, singular, non-positive betas).

    The 3 grondgebonden betas are expanded to 5 individual type betas via
    national price ratios — see module docstring.
    """
    needed = [
        "gem_woz_waarde", "woningvoorraad",
        "pct_tussenwoning", "pct_hoekwoning", "pct_twee_onder_een_kap",
        "pct_vrijstaand", "pct_meergezinswoning",
    ]
    sub = df_prov.dropna(subset=needed)
    sub = sub[sub["woningvoorraad"] > 0]

    if len(sub) < _MIN_BUURTEN:
        return None

    # Build 3-column design matrix
    grond = (
        sub["pct_tussenwoning"].values
        + sub["pct_hoekwoning"].values
        + sub["pct_twee_onder_een_kap"].values
    ) / 100.0
    vrij  = sub["pct_vrijstaand"].values / 100.0
    meer  = sub["pct_meergezinswoning"].values / 100.0

    X = np.column_stack([grond, vrij, meer])   # shape (n, 3)
    y = sub["gem_woz_waarde"].values            # €1 000, shape (n,)
    w = sub["woningvoorraad"].values.astype(float)

    # Weighted OLS: multiply rows by √w
    sqrt_w = np.sqrt(w)
    Xw = X * sqrt_w[:, np.newaxis]
    yw = y * sqrt_w

    betas, _residuals, _rank, _sv = np.linalg.lstsq(Xw, yw, rcond=None)
    b_grond, b_vrij, b_meer = betas

    # Sanity: all three betas must be positive
    if b_grond <= 0 or b_vrij <= 0 or b_meer <= 0:
        _diag(f"    rejected: non-positive beta(s): grond={b_grond:.1f} vrij={b_vrij:.1f} meer={b_meer:.1f}")
        return None

    # ── expand grondgebonden β into individual type betas ────────────────────
    # Scale each type by its national price relative to T001100 (all-types avg).
    # The common denominator cancels in the step-1 ratio, so absolute scaling
    # doesn't matter — only the within-group ordering (hoek > tuss, twee > hoek).
    nat = _NAT_PRICES
    denom = nat["T001100"]
    b_tuss = b_grond * nat["ZW25805"] / denom
    b_hoek = b_grond * nat["ZW25806"] / denom
    b_twee = b_grond * nat["ZW10300"] / denom

    return {
        "ZW25805": round(b_tuss, 4),
        "ZW25806": round(b_hoek, 4),
        "ZW10300": round(b_twee, 4),
        "ZW10320": round(b_vrij, 4),
        "ZW25810": round(b_meer, 4),
    }


def _build_relatives() -> dict[str, dict[str, float]]:
    """Run OLS for every province; return only provinces with enough data."""
    from watmoetikbieden.sources.kwb_bulk import load_buurten_df

    df = load_buurten_df()
    result: dict[str, dict[str, float]] = {}

    provinces = sorted(p for p in df["provincie"].dropna().unique() if p)
    _diag(f"fitting OLS for {len(provinces)} provinces …")

    for prov in provinces:
        df_prov = df[df["provincie"] == prov]
        betas = _run_ols(df_prov)
        if betas is None:
            _diag(f"  {prov}: skipped (insufficient / unreliable data)")
        else:
            result[prov] = betas
            _diag(f"  {prov}: {betas}")

    _diag(f"regional relatives ready for {len(result)}/{len(provinces)} provinces")
    return result


# ── public API ─────────────────────────────────────────────────────────────────

def load_regional_relatives() -> dict[str, dict[str, float]]:
    """
    Return province-level price relatives.

    Structure
    ---------
    {
      "Noord-Brabant": {
        "ZW25805": 420.1,   # Tussenwoning     – estimated avg WOZ in €1 000
        "ZW25806": 455.3,   # Hoekwoning
        "ZW10300": 520.8,   # Twee-onder-één-kap
        "ZW10320": 720.4,   # Vrijstaand
        "ZW25810": 290.5,   # Appartement / meergezins
      },
      …
    }

    Provinces with fewer than 30 buurten with complete data are omitted;
    the estimator falls back to national relatives for those.

    Data is fetched from CBS on first call; results are cached as JSON.
    The JSON is automatically regenerated when the source CSV changes.
    """
    if not _cache_is_fresh():
        _diag("(re)computing regional relatives …")
        rels = _build_relatives()
        _CACHE_DIR.mkdir(parents=True, exist_ok=True)
        _JSON_CACHE.write_text(
            json.dumps(rels, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        _diag(f"written → {_JSON_CACHE}")
    else:
        rels = json.loads(_JSON_CACHE.read_text(encoding="utf-8"))
        _diag(f"loaded {len(rels)} provinces from cache")

    return rels
