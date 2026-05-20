"""
Bid range estimation — pure calculation module.

No fetching, no Streamlit.  All inputs are optional (None = not available).
Degrades gracefully: missing inputs are noted in BidRangeResult.missing_inputs
and the corresponding factor is skipped or assigned a neutral value.

Assembly
────────
1. F_lag (multiplicative, point value): adjusts WOZ estimate to current market.
2. Factor adjustments (additive percentages applied to adjusted_base):
     Market heat · Supply pressure · Demand (migration) ·
     Energy label · Condition · Days on market
3. Renovation cost deduction (range, subtracted from pre-renovation range).

bid_low  = round((adjusted_base × (1 + Σlow)  - reno_high) / 1000) × 1000
bid_high = round((adjusted_base × (1 + Σhigh) - reno_low)  / 1000) × 1000
"""

from __future__ import annotations

from dataclasses import dataclass, field

# ── renovation cost lookup ────────────────────────────────────────────────────

RENOVATION_COSTS: dict[str, tuple[int, int]] = {
    "Badkamer vernieuwen":          (8_000,  18_000),
    "Keuken vernieuwen":           (10_000,  22_000),
    "Dak vernieuwen / isoleren":   (12_000,  30_000),
    "Vloerisolatie":                (4_000,  10_000),
    "Gevelisolatie":               (10_000,  22_000),
    "Kozijnen / ramen vervangen":   (8_000,  20_000),
    "Elektra vernieuwen":           (8_000,  18_000),
    "Loodgieterswerk vernieuwen":   (5_000,  12_000),
    "Vloeren vervangen":            (4_000,  12_000),
    "Schilderwerk buiten":          (3_000,   7_000),
    "Schilderwerk binnen":          (2_000,   6_000),
    "Asbest sanering":             (10_000,  35_000),
    "Fundering herstel":           (20_000,  80_000),
    "CV-installatie vervangen":     (5_000,  10_000),
    "Warmtepomp installeren":       (8_000,  18_000),
    "Zonnepanelen":                 (5_000,  12_000),
}

# ── energy label adjustments ──────────────────────────────────────────────────

_LABEL_ADJ: dict[str, tuple[float, float]] = {
    "A++++": (0.04, 0.07),
    "A+++":  (0.04, 0.07),
    "A++":   (0.03, 0.05),
    "A+":    (0.03, 0.05),
    "A":     (0.02, 0.04),
    "B":     (0.01, 0.02),
    "C":     (0.0,  0.0),   # reference
    "D":     (-0.02, -0.01),
    "E":     (-0.05, -0.03),
    "F":     (-0.08, -0.05),
    "G":     (-0.12, -0.07),
}

# ── condition adjustments ─────────────────────────────────────────────────────

_COND_ADJ: dict[str, tuple[float, float]] = {
    "Instapklaar – niks aan te doen":                    (0.0,   0.0),
    "Goed onderhouden – kleine opknapbeurten":           (-0.02, -0.01),
    "Bewoonbaar maar verouderd – modernisering gewenst": (-0.07, -0.04),
    "Beperkt bewoonbaar – grote renovatie nodig":        (-0.15, -0.10),
    "Onbewoonbaar – volledig renovatieproject":          (-0.25, -0.18),
}

# ── heat label base adjustments ───────────────────────────────────────────────

_HEAT_ADJ: dict[str, tuple[float, float]] = {
    "Heet":     (0.08, 0.20),
    "Warm":     (0.03, 0.10),
    "Neutraal": (-0.02, 0.05),
    "Koel":     (-0.08, 0.01),
}


# ── data classes ──────────────────────────────────────────────────────────────

@dataclass
class BidFactor:
    label: str           # e.g. "Markt (Heet)"
    description: str     # explanation of value used
    low_pct: float       # pessimistic % adjustment as decimal (e.g. -0.05)
    high_pct: float      # optimistic % adjustment as decimal (e.g. +0.10)
    source: str          # data source string
    available: bool = True


@dataclass
class BidRangeResult:
    woz_estimate: int
    f_lag: float                  # multiplicative lag factor
    adjusted_base: int            # woz_estimate * f_lag rounded to 1000
    factors: list[BidFactor]      # each factor's contribution
    renovation_cost_low: int
    renovation_cost_high: int
    bid_low: int                  # rounded to nearest 1000
    bid_high: int
    missing_inputs: list[str]

    @property
    def bid_midpoint(self) -> int:
        return round((self.bid_low + self.bid_high) / 2 / 1000) * 1000

    @property
    def confidence(self) -> str:
        n = len(self.missing_inputs)
        if n == 0:
            return "hoog"
        if n <= 2:
            return "gemiddeld"
        return "laag"

    @property
    def confidence_icon(self) -> str:
        return {"hoog": "🟢", "gemiddeld": "🟡", "laag": "🔴"}.get(self.confidence, "⚪")


# ── estimation function ───────────────────────────────────────────────────────

def estimate_bid_range(
    woz_estimate: int,
    # Lag / recency
    national_price_yoy_pct: float | None = None,
    corop_price_yoy_pct: float | None = None,
    # Market heat
    heat_label: str | None = None,
    national_tx_yoy_pct: float | None = None,
    corop_sales_yoy_pct: float | None = None,
    # Supply
    supply_pressure: float | None = None,
    permits_yoy_pct: float | None = None,
    # Demand
    migration_net_12m: int | None = None,
    inwoners: int | None = None,
    # Property
    energy_label: str | None = None,
    condition: str | None = None,
    days_for_sale: int | None = None,
    renovation_items: list[str] | None = None,
) -> BidRangeResult:
    """Compute a bid range estimate from available market and property data."""

    missing: list[str] = []
    factors: list[BidFactor] = []

    # ── Factor 0: F_lag (multiplicative point correction) ─────────────────────
    if national_price_yoy_pct is not None:
        chosen_yoy  = national_price_yoy_pct
        lag_source  = "CBS 85773NED (nationaal maandgemiddelde)"
        lag_desc    = f"Nationale maandelijkse prijsindex YoY: {chosen_yoy:+.1f}%"
    elif corop_price_yoy_pct is not None:
        chosen_yoy  = corop_price_yoy_pct
        lag_source  = "CBS 85819NED (COROP kwartaal)"
        lag_desc    = f"COROP prijsindex YoY: {chosen_yoy:+.1f}%"
    else:
        chosen_yoy  = 0.0
        lag_source  = "–"
        lag_desc    = "Geen prijsdata beschikbaar; geen tijdcorrectie"
        missing.append("Tijdcorrectie")

    f_lag = 1.0 + chosen_yoy / 100.0
    adjusted_base = round(woz_estimate * f_lag / 1000) * 1000

    # ── Factor 1: Market heat ─────────────────────────────────────────────────
    if heat_label and heat_label in _HEAT_ADJ:
        mkt_low, mkt_high = _HEAT_ADJ[heat_label]
        mkt_desc = f"Marktklassificatie: {heat_label}"
        mkt_avail = True
    else:
        mkt_low, mkt_high = 0.0, 0.03
        mkt_desc = "Marktklassificatie onbekend; neutrale schatting"
        mkt_avail = False
        missing.append("Markt (hitte-indicator)")

    # Recency modifier from national tx YoY
    if national_tx_yoy_pct is not None:
        if national_tx_yoy_pct > 15:
            mkt_low += 0.01; mkt_high += 0.02
        elif national_tx_yoy_pct > 5:
            mkt_low += 0.005; mkt_high += 0.01
        elif national_tx_yoy_pct < -15:
            mkt_low -= 0.02; mkt_high -= 0.01
        elif national_tx_yoy_pct < -5:
            mkt_low -= 0.01; mkt_high -= 0.005

    # Volume modifier from COROP sales YoY
    if corop_sales_yoy_pct is not None:
        if corop_sales_yoy_pct > 10:
            mkt_high += 0.02
        elif corop_sales_yoy_pct < -10:
            mkt_low -= 0.01

    if mkt_avail or not mkt_avail:
        factors.append(BidFactor(
            label       = f"Markt ({heat_label or 'onbekend'})",
            description = mkt_desc,
            low_pct     = mkt_low,
            high_pct    = mkt_high,
            source      = "CBS 85819NED · 85773NED",
            available   = mkt_avail,
        ))

    # ── Factor 2: Supply pressure ─────────────────────────────────────────────
    if supply_pressure is not None:
        if supply_pressure < 5:
            sp_low, sp_high = 0.015, 0.035
            sp_desc = f"Lage aanboddruk ({supply_pressure:.1f}% van transacties)"
        elif supply_pressure <= 15:
            sp_low, sp_high = 0.0, 0.0
            sp_desc = f"Neutraal aanbod ({supply_pressure:.1f}% van transacties)"
        else:
            sp_low, sp_high = -0.02, -0.005
            sp_desc = f"Hoog nieuwboudaanbod ({supply_pressure:.1f}% van transacties)"

        # Permit trend modifier
        if permits_yoy_pct is not None:
            if permits_yoy_pct > 20:
                sp_low -= 0.01; sp_high -= 0.01
            elif permits_yoy_pct < -20:
                sp_low += 0.01; sp_high += 0.01

        factors.append(BidFactor(
            label       = "Aanboddruk",
            description = sp_desc,
            low_pct     = sp_low,
            high_pct    = sp_high,
            source      = "CBS 86054NED · 83671NED",
            available   = True,
        ))
    else:
        missing.append("Aanboddruk (nieuwbouw/transacties)")

    # ── Factor 3: Demand (migration) ──────────────────────────────────────────
    if migration_net_12m is not None and inwoners and inwoners > 0:
        permille = migration_net_12m / inwoners * 1000
        if permille > 5:
            dm_low, dm_high = 0.01, 0.025
            dm_desc = f"Sterke instroom ({permille:+.1f}‰)"
        elif permille >= 1:
            dm_low, dm_high = 0.005, 0.015
            dm_desc = f"Lichte instroom ({permille:+.1f}‰)"
        elif permille >= -1:
            dm_low, dm_high = 0.0, 0.0
            dm_desc = f"Stabiele bevolking ({permille:+.1f}‰)"
        else:
            dm_low, dm_high = -0.015, 0.0
            dm_desc = f"Netto uitstroom ({permille:+.1f}‰)"
        factors.append(BidFactor(
            label       = "Vraag (migratie)",
            description = dm_desc,
            low_pct     = dm_low,
            high_pct    = dm_high,
            source      = "CBS 37230ned",
            available   = True,
        ))
    else:
        missing.append("Vraag (migratiesaldo)")

    # ── Factor 4: Energy label ────────────────────────────────────────────────
    norm_label = (energy_label or "").strip()
    if norm_label and norm_label != "Onbekend" and norm_label in _LABEL_ADJ:
        el_low, el_high = _LABEL_ADJ[norm_label]
        el_desc  = f"Energielabel {norm_label}"
        el_avail = True
    else:
        el_low, el_high = -0.01, 0.01
        el_desc  = "Energielabel onbekend; kleine onzekerheidsmarge"
        el_avail = False
        missing.append("Energielabel")
    factors.append(BidFactor(
        label       = f"Energielabel ({norm_label or 'onbekend'})",
        description = el_desc,
        low_pct     = el_low,
        high_pct    = el_high,
        source      = "EP-Online",
        available   = el_avail,
    ))

    # ── Factor 5: Condition ───────────────────────────────────────────────────
    if condition and condition in _COND_ADJ:
        cd_low, cd_high = _COND_ADJ[condition]
        cd_desc  = condition
        cd_avail = True
    else:
        cd_low, cd_high = 0.0, 0.0
        cd_desc  = "Staat onbekend"
        cd_avail = False
        missing.append("Staat woning")
    factors.append(BidFactor(
        label       = "Staat",
        description = cd_desc,
        low_pct     = cd_low,
        high_pct    = cd_high,
        source      = "Gebruikersinvoer",
        available   = cd_avail,
    ))

    # ── Factor 6: Days on market ──────────────────────────────────────────────
    if days_for_sale is not None and days_for_sale > 21:
        dom = days_for_sale
        if dom <= 45:
            dom_low, dom_high = -0.01, 0.0
            dom_desc = f"{dom} dagen te koop (licht verouderd)"
        elif dom <= 90:
            dom_low, dom_high = -0.02, -0.01
            dom_desc = f"{dom} dagen te koop (enige tijd te koop)"
        elif dom <= 180:
            dom_low, dom_high = -0.04, -0.02
            dom_desc = f"{dom} dagen te koop (lang te koop)"
        else:
            dom_low, dom_high = -0.08, -0.04
            dom_desc = f"{dom} dagen te koop (zeer lang te koop)"
        factors.append(BidFactor(
            label       = "Doorlooptijd",
            description = dom_desc,
            low_pct     = dom_low,
            high_pct    = dom_high,
            source      = "Gebruikersinvoer",
            available   = True,
        ))

    # ── Renovation costs ──────────────────────────────────────────────────────
    reno_items = renovation_items or []
    reno_low  = sum(RENOVATION_COSTS[i][0] for i in reno_items if i in RENOVATION_COSTS)
    reno_high = sum(RENOVATION_COSTS[i][1] for i in reno_items if i in RENOVATION_COSTS)

    # ── Assembly ──────────────────────────────────────────────────────────────
    sum_low  = sum(f.low_pct  for f in factors)
    sum_high = sum(f.high_pct for f in factors)

    pre_reno_low  = adjusted_base * (1.0 + sum_low)
    pre_reno_high = adjusted_base * (1.0 + sum_high)

    bid_low  = round((pre_reno_low  - reno_high) / 1000) * 1000
    bid_high = round((pre_reno_high - reno_low)  / 1000) * 1000

    return BidRangeResult(
        woz_estimate       = woz_estimate,
        f_lag              = f_lag,
        adjusted_base      = adjusted_base,
        factors            = factors,
        renovation_cost_low  = reno_low,
        renovation_cost_high = reno_high,
        bid_low            = bid_low,
        bid_high           = bid_high,
        missing_inputs     = missing,
    )
