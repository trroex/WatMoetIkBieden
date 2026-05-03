"""
WOZ value estimator for a specific woningtype at a given address.

═══════════════════════════════════════════════════════════════════════════════
HOW THE MODEL WORKS  (read this before tuning anything)
═══════════════════════════════════════════════════════════════════════════════

The estimate is built in two sequential steps.  Each step is independent and
can be tuned separately.

──────────────────────────────────────────────────────────────────────────────
STEP 1 – Type-adjusted buurt estimate
──────────────────────────────────────────────────────────────────────────────

CBS 85984NED gives us, for every buurt:
  • gem_woz_waarde  – overall average WOZ of ALL woningen in the buurt (€1 000)
  • pct_tussenwoning / pct_hoekwoning / …  – woningtype composition (0–100 %)

The buurt average is a weighted mix of type-specific values:

    gem_woz_buurt = α_buurt × Σ_t (pct_t/100 × r_t)         … (1)

where r_t  is the *national price relative* for type t:
    r_t = national_avg_price_t / national_avg_price_all_types

Solving (1) for α_buurt (the buurt's overall price level) and multiplying by
r_target gives the type-specific estimate for this buurt:

    W_type = gem_woz_buurt × r_target / Σ_t (pct_t/100 × r_t)  … (2)

Intuition: if a buurt is 80 % vrijstaand, its gem_woz is already elevated.
Formula (2) corrects for that before applying the type premium.

National price relatives are sourced from CBS 83910NED
"Bestaande koopwoningen; verkoopprijzen; woningtype" (period 2023JJ00).
See NATIONAL_PRICES_2023 below.

──────────────────────────────────────────────────────────────────────────────
STEP 2 – Perceel-size adjustment
──────────────────────────────────────────────────────────────────────────────

W_type from step 1 assumes the house has a *typical* plot for its type.
We correct for the actual parcel size using a log-linear (power-law) model,
which is the standard approach in hedonic pricing literature:

    perceel_factor = (actual_m2 / reference_m2_for_type) ^ γ_type  … (3)

    W_final = W_type × perceel_factor                               … (4)

Key properties:
  • actual_m2 == reference_m2  →  factor = 1.0  (no adjustment)
  • actual_m2  > reference_m2  →  factor > 1.0  (premium for larger plot)
  • actual_m2  < reference_m2  →  factor < 1.0  (discount for smaller plot)
  • γ = 0  →  perceel size has no effect  (used for appartementen)

The factor is clamped to [MIN_PERCEEL_FACTOR, MAX_PERCEEL_FACTOR] to prevent
extreme outliers from producing nonsensical estimates.

reference_m2 and γ are hardcoded per type in PERCEEL_PARAMS below.
These come from CBS BAG/WOZ publications and Dutch hedonic pricing research.

══════════════════════════════════════════════════════════════════════════════
HOW TO TUNE THE MODEL
══════════════════════════════════════════════════════════════════════════════

All tuneable parameters are in clearly marked sections below.  You never need
to touch the formula logic to adjust the model's behaviour.

  1. NATIONAL_PRICES_2023   – update when a newer CBS 83910NED year is available
  2. PERCEEL_PARAMS          – tune reference_m2 and γ per woningtype
  3. MIN/MAX_PERCEEL_FACTOR  – widen or narrow the clamping range

To validate changes: compare estimated_value against real Kadaster WOZ values
using the validation delta shown in the app's WOZ tab.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from watmoetikbieden.models import CbsDemographicsResult

# ══════════════════════════════════════════════════════════════════════════════
# TUNEABLE PARAMETER BLOCK 1 – National price relatives  (Step 1)
# ══════════════════════════════════════════════════════════════════════════════
#
# Source: CBS 85791NED "Bestaande koopwoningen; verkoopprijzen; woningtype;
#         prijsindex 2020=100" – measure GemiddeldeVerkoopprijs_7, period 2024JJ00,
#         fetched 2026-05 via opendata.cbs.nl.
#
# Year alignment: matches CBS 85984NED (KWB 2024) used for gem_woz_waarde
# and pct_* fields.  Both datasets reflect the 2024 reference year.
#
# To update to a newer year: run the query below, replace the values, and
# rename the constant to reflect the new year.  Relative factors recompute
# automatically from the denominator (T001100 = national all-types average).
#
#   https://opendata.cbs.nl/ODataApi/odata/85791NED/UntypedDataSet
#     ?$filter=Perioden eq '20XXJJ00' and TypeWoning ne 'ZW25809'
#     &$select=TypeWoning,GemiddeldeVerkoopprijs_7
#
NATIONAL_PRICES_2024: dict[str, int] = {
    "T001100": 450_985,   # Totaal woningen  ← denominator for all relatives
    "ZW25805": 435_470,   # Tussenwoning          → relative 0.9657
    "ZW25806": 446_521,   # Hoekwoning            → relative 0.9902
    "ZW10300": 483_654,   # 2-onder-1-kapwoning   → relative 1.0725
    "ZW10320": 650_139,   # Vrijstaande woning    → relative 1.4415
    "ZW25810": 380_110,   # Appartement           → relative 0.8429
}

# Derived: r_t = price_t / price_all.  Not meant to be edited directly.
_NATIONAL_AVG = NATIONAL_PRICES_2024["T001100"]
RELATIVES: dict[str, float] = {
    k: v / _NATIONAL_AVG for k, v in NATIONAL_PRICES_2024.items()
}

# ══════════════════════════════════════════════════════════════════════════════
# TUNEABLE PARAMETER BLOCK 2 – Perceel adjustment  (Step 2)
# ══════════════════════════════════════════════════════════════════════════════
#
# Each woningtype gets two parameters:
#
#   reference_m2  – national median/average parcel size for this type.
#                   A house with exactly this plot size gets factor = 1.0.
#                   Source: CBS BAG/WOZ publications and Kadaster research.
#                   These are approximate; refine with empirical data if available.
#
#   gamma         – price elasticity w.r.t. plot size (log-log coefficient).
#                   Interpretation: a 1 % larger plot → γ % higher value.
#                   Typical range in Dutch hedonic literature: 0.05 – 0.30.
#                   Set to 0.0 for types where the plot is not privately owned
#                   (appartementen, bovenwoning, etc.).
#
# Format:  CBS_type_code: {"reference_m2": int, "gamma": float}
#
PERCEEL_PARAMS: dict[str, dict[str, float]] = {
    "ZW25805": {                        # Tussenwoning
        "reference_m2": 130,            # typical mid-terrace garden ~130 m²
        "gamma":        0.08,           # small elasticity – plot varies little
    },
    "ZW25806": {                        # Hoekwoning
        "reference_m2": 175,            # corner plots are wider ~175 m²
        "gamma":        0.10,
    },
    "ZW10300": {                        # Twee-onder-één-kapwoning
        "reference_m2": 260,            # semi-detached, larger side garden
        "gamma":        0.15,
    },
    "ZW10320": {                        # Vrijstaande woning
        "reference_m2": 550,            # wide spread; ~550 m² is a reasonable median
        "gamma":        0.25,           # larger elasticity – plot is a key value driver
    },
    "ZW25810": {                        # Appartement / meergezinswoning
        "reference_m2": None,           # no private plot → adjustment skipped
        "gamma":        0.0,
    },
}

# ══════════════════════════════════════════════════════════════════════════════
# TUNEABLE PARAMETER BLOCK 3 – Safety clamping
# ══════════════════════════════════════════════════════════════════════════════
#
# The perceel factor is clamped to this range to prevent extreme plot sizes
# (e.g. a 10 000 m² outlier) from producing nonsensical estimates.
# Widen if you trust the data; narrow for more conservative behaviour.
#
MIN_PERCEEL_FACTOR = 0.60   # a very small plot can at most reduce value by 40 %
MAX_PERCEEL_FACTOR = 2.00   # a very large plot can at most double the value

# ══════════════════════════════════════════════════════════════════════════════
# FIXED MAPPINGS  (edit only if the UI dropdown options change)
# ══════════════════════════════════════════════════════════════════════════════

# CBS 85984NED pct field → 83910NED type code  (for composition weighting)
_PCT_FIELD_TO_TYPE: dict[str, str] = {
    "pct_tussenwoning":       "ZW25805",
    "pct_hoekwoning":         "ZW25806",
    "pct_twee_onder_een_kap": "ZW10300",
    "pct_vrijstaand":         "ZW10320",
    "pct_meergezinswoning":   "ZW25810",
}

# UI label → CBS type code  (None = unknown type, falls back to buurt average)
HOUSE_TYPE_TO_CBS: dict[str, str | None] = {
    "Tussenwoning":          "ZW25805",
    "Hoekwoning":            "ZW25806",
    "Twee-onder-één-kap":    "ZW10300",
    "Vrijstaande woning":    "ZW10320",
    "Appartement / flat":    "ZW25810",
    "Bovenwoning":           "ZW25810",
    "Benedenwoning":         "ZW25810",
    "Penthouse":             "ZW25810",
    "Woonboerderij":         "ZW10320",
    "Anders / onbekend":     None,
}


# ══════════════════════════════════════════════════════════════════════════════
# RESULT DATACLASS
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class WozEstimate:
    """
    Full output of estimate().  All monetary values in EUR.

    Fields prefixed with perceel_ describe the Step 2 adjustment.
    If perceel_m2 was not provided (or type has gamma=0), perceel_factor == 1.0
    and perceel_applied is False.
    """
    # ── core result ───────────────────────────────────────────────────────────
    estimated_value: int | None       # final estimated WOZ (EUR)

    # ── Step 1 internals ─────────────────────────────────────────────────────
    source_gem_woz_eur: int | None    # buurt avg WOZ input (EUR)
    type_relative: float | None       # r_target (e.g. 1.454 for vrijstaand)
    composition_weight: float | None  # Σ(pct_t/100 × r_t) – denominator in (2)
    coverage_pct: float               # % of buurt composition known from CBS
    step1_value: int | None           # W_type before perceel adjustment

    # ── Step 2 internals ─────────────────────────────────────────────────────
    perceel_m2: int | None            # actual plot size used (m²)
    perceel_reference_m2: int | None  # reference size for this type (m²)
    perceel_gamma: float              # elasticity used
    perceel_factor: float             # (actual/reference)^gamma, clamped
    perceel_applied: bool             # False when skipped (no data or gamma=0)

    # ── metadata ──────────────────────────────────────────────────────────────
    confidence: str                   # "high" | "medium" | "low" | "unavailable"
    method: str                       # human-readable description of what was done
    house_type_code: str | None       # CBS type code used
    relatives_source: str = "national"  # "national" | "regional (Noord-Brabant)" etc.

    @property
    def label(self) -> str:
        """Dutch name for the house type code."""
        return {
            "ZW25805": "Tussenwoning",
            "ZW25806": "Hoekwoning",
            "ZW10300": "Twee-onder-één-kap",
            "ZW10320": "Vrijstaande woning",
            "ZW25810": "Appartement",
        }.get(self.house_type_code or "", "Onbekend type")

    def validation_delta_pct(self, real_woz: int) -> float | None:
        """Signed % difference vs. real Kadaster WOZ: (estimated − real) / real × 100."""
        if self.estimated_value is None or real_woz <= 0:
            return None
        return (self.estimated_value - real_woz) / real_woz * 100


# ══════════════════════════════════════════════════════════════════════════════
# REGIONAL RELATIVES LOADER  (lazy, module-level cache)
# ══════════════════════════════════════════════════════════════════════════════

_regional_cache: dict | None = None
_regional_loaded: bool = False


def _get_regional_relatives(province: str) -> dict[str, float] | None:
    """
    Return province-level β relatives for *province*, or None if unavailable.

    The full dict is loaded once per process and cached in _regional_cache.
    A failed load (e.g. CBS unreachable) is cached as an empty dict so
    subsequent calls don't retry on every request.
    """
    global _regional_cache, _regional_loaded
    if not _regional_loaded:
        try:
            from watmoetikbieden.regional_relatives import load_regional_relatives
            _regional_cache = load_regional_relatives()
        except Exception as exc:
            import sys
            print(f"[WOZ] regional relatives unavailable: {exc}", file=sys.stderr)
            _regional_cache = {}
        _regional_loaded = True
    return (_regional_cache or {}).get(province)


# ══════════════════════════════════════════════════════════════════════════════
# INTERNAL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def _perceel_factor(type_code: str, actual_m2: int | None) -> tuple[float, bool]:
    """
    Compute the perceel adjustment factor for *type_code* given *actual_m2*.

    Returns (factor, applied):
      factor   – multiplicative adjustment to apply to W_type
      applied  – True if a real adjustment was made (useful for display)

    The factor is 1.0 (no adjustment) when:
      • actual_m2 is None  (parcel data not available)
      • gamma is 0.0       (type has no private plot, e.g. appartement)
      • reference_m2 is None (type not configured for adjustment)
    """
    params = PERCEEL_PARAMS.get(type_code)
    if params is None or actual_m2 is None:
        return 1.0, False

    gamma = params["gamma"]
    ref_m2 = params["reference_m2"]

    if gamma == 0.0 or ref_m2 is None or ref_m2 <= 0 or actual_m2 <= 0:
        return 1.0, False

    # Core formula: (actual / reference) ^ gamma
    raw_factor = (actual_m2 / ref_m2) ** gamma

    # Clamp to prevent extreme outliers
    clamped = max(MIN_PERCEEL_FACTOR, min(MAX_PERCEEL_FACTOR, raw_factor))

    return round(clamped, 4), True


# ══════════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════════

def estimate(
    cbs: "CbsDemographicsResult",
    house_type_label: str,
    perceel_m2: int | None = None,
    province: str | None = None,
) -> WozEstimate:
    """
    Estimate the WOZ value for a specific house type at a given address.

    Parameters
    ----------
    cbs               CBS buurt demographics (must contain gem_woz_waarde and pct_* fields).
    house_type_label  UI label string, e.g. "Tussenwoning" or "Vrijstaande woning".
    perceel_m2        Actual cadastral plot area in m² (from BRK or WOZ grondoppervlakte).
                      Pass None to skip the perceel adjustment.
    province          Province name as returned by BAG Locatieserver (e.g. "Noord-Brabant").
                      When provided and regional relatives are available, they replace the
                      national relatives in Step 1.  Falls back to national if the province
                      is not covered (< 30 buurten) or CBS data is unavailable.

    Returns
    -------
    WozEstimate with estimated_value in EUR and full intermediate values for
    transparency and debugging.
    """
    type_code = HOUSE_TYPE_TO_CBS.get(house_type_label)

    # ── read base value early (needed for both guard and regional check) ──────
    gem_woz_k = cbs.gem_woz_waarde

    # ── resolve relatives (regional if available and beneficial, else national) ──
    #
    # Regional OLS gives province-specific β values (in €1 000) derived from
    # weighted OLS across all buurten in the province.  The formula:
    #   W_type = gem_woz × β_type / Σ(pct_t × β_t)
    # is a ratio so units cancel.  However, the OLS β_vrijstaand is inflated by
    # rural buurten with 60-70% vrijstaand and very high WOZ values.  For a
    # *mixed* buurt (e.g. 20 % vrijstaand), this inflates the denominator and
    # produces a lower estimate than national — the opposite of the intended fix.
    #
    # "Take-max" rule: use regional only when its Step-1 estimate is strictly
    # higher than the national estimate.  Regional can only *help* (correct
    # underestimation in affluent buurten); when it gives a lower value it
    # means the vrijstaand inflation is dominant and national is safer.
    prov_rels: dict[str, float] | None = None
    relatives_source = "national"

    if province and type_code and gem_woz_k and gem_woz_k > 0:
        _prov_rels_candidate = _get_regional_relatives(province)
        if _prov_rels_candidate and type_code in _prov_rels_candidate:
            _gem_eur = round(gem_woz_k * 1000)
            # Compute step1 under both relative sets and prefer the higher one.
            def _quick_step1(rels_dict: dict) -> int | None:
                r_t = rels_dict.get(type_code)
                if r_t is None:
                    return None
                w = sum(
                    (getattr(cbs, f, None) or 0) / 100.0 * (rels_dict.get(tc) or 0)
                    for f, tc in _PCT_FIELD_TO_TYPE.items()
                )
                return round(_gem_eur * r_t / w) if w > 0.01 else None

            _s1_nat = _quick_step1(RELATIVES)
            _s1_reg = _quick_step1(_prov_rels_candidate)
            if _s1_reg and _s1_nat and _s1_reg > _s1_nat:
                prov_rels = _prov_rels_candidate
                relatives_source = f"regional ({province})"
            # else: regional ≤ national → vrijstaand inflation dominant → keep national

    # Active relatives dict: β values in €1 000 (regional) or dimensionless
    # factors (national).  The Step-1 formula is a ratio, so units cancel.
    rels = prov_rels if prov_rels else RELATIVES

    # ── guard: need buurt average WOZ as base ────────────────────────────────
    if not gem_woz_k or gem_woz_k <= 0:
        return WozEstimate(
            estimated_value=None,
            source_gem_woz_eur=None,
            type_relative=None,
            composition_weight=None,
            coverage_pct=0.0,
            step1_value=None,
            perceel_m2=perceel_m2,
            perceel_reference_m2=None,
            perceel_gamma=0.0,
            perceel_factor=1.0,
            perceel_applied=False,
            confidence="unavailable",
            method="unavailable – geen CBS gem_woz_waarde voor deze buurt",
            house_type_code=type_code,
            relatives_source=relatives_source,
        )

    gem_woz_eur = round(gem_woz_k * 1000)

    # ── guard: unknown type → return buurt average unchanged ─────────────────
    if type_code is None:
        return WozEstimate(
            estimated_value=gem_woz_eur,
            source_gem_woz_eur=gem_woz_eur,
            type_relative=1.0,
            composition_weight=None,
            coverage_pct=0.0,
            step1_value=gem_woz_eur,
            perceel_m2=perceel_m2,
            perceel_reference_m2=None,
            perceel_gamma=0.0,
            perceel_factor=1.0,
            perceel_applied=False,
            confidence="low",
            method="buurt_average_only – woningtype onbekend, geen type-aanpassing",
            house_type_code=None,
            relatives_source=relatives_source,
        )

    r_target = rels[type_code]

    # ── Step 1: composition-adjusted type estimate ────────────────────────────
    # Compute Σ_t (pct_t/100 × r_t) using the buurt's woningtype composition.
    weighted_r = 0.0
    coverage = 0.0
    for field, tcode in _PCT_FIELD_TO_TYPE.items():
        pct = getattr(cbs, field, None)
        r_t = rels.get(tcode)
        if pct is not None and r_t is not None:
            weighted_r += (pct / 100.0) * r_t
            coverage += pct

    confidence = "high" if coverage >= 90 else "medium" if coverage >= 70 else "low"

    if weighted_r > 0.01:
        step1 = round(gem_woz_eur * r_target / weighted_r)
        step1_method = f"composition_adjusted ({relatives_source})"
    else:
        # Fallback: no composition data, apply relative directly.
        step1 = round(gem_woz_eur * r_target / (r_target or 1.0))   # simplifies to gem_woz_eur
        step1_method = f"national_relative_fallback"
        confidence = "low"

    # ── Step 2: perceel-size adjustment ──────────────────────────────────────
    params = PERCEEL_PARAMS.get(type_code, {})
    ref_m2 = params.get("reference_m2")
    gamma  = params.get("gamma", 0.0)

    factor, applied = _perceel_factor(type_code, perceel_m2)
    final_value = round(step1 * factor)

    # Build method description for display / debugging
    if applied:
        method = (
            f"{step1_method} + perceel_adjustment "
            f"({perceel_m2} m² vs ref {ref_m2} m², γ={gamma}, factor={factor:.3f})"
        )
    else:
        reason = (
            "geen perceeldata" if perceel_m2 is None
            else "γ=0 (geen privétuin)"
        )
        method = f"{step1_method} (perceel overgeslagen: {reason})"

    return WozEstimate(
        estimated_value=final_value,
        source_gem_woz_eur=gem_woz_eur,
        type_relative=round(r_target, 4),
        composition_weight=round(weighted_r, 4) if weighted_r > 0.01 else None,
        coverage_pct=round(coverage, 1),
        step1_value=step1,
        perceel_m2=perceel_m2,
        perceel_reference_m2=ref_m2,
        perceel_gamma=gamma,
        perceel_factor=factor,
        perceel_applied=applied,
        confidence=confidence,
        method=method,
        house_type_code=type_code,
        relatives_source=relatives_source,
    )
