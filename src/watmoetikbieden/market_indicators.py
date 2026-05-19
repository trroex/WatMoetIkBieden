"""
Macro market indicators for WatMoetIkBieden.

Currently provides:
  - Median personal gross income per year (CBS 83931NED; fallback hardcoded)
  - Dutch mortgage interest rates via ECB SDMX API (sourced from DNB)
  - Maximum annuity mortgage capacity for a modal-income earner per year

═══════════════════════════════════════════════════════════════════════════════
MORTGAGE CAPACITY MODEL
═══════════════════════════════════════════════════════════════════════════════

We compute the maximum annuity mortgage (annuïteitenhypotheek) for a person
earning the modal gross income, using the following assumptions:

  woonquote  = 26 %   Share of gross income that may go to mortgage payments.
                       Approximates the NIBUD norm for modal income earners.
                       (NIBUD norms are rate-dependent; 26% is a conservative
                       mid-point.  Adjust WOONQUOTE below to tune.)

  term       = 30 yr  Standard mortgage term in the Netherlands.

  rate       = ECB MIR M.NL.B.A2C.AM.R.A.2250.EUR.N — annualised agreed rate
               (AAR) for new house-purchase lending to households, all
               maturities, Netherlands.  Monthly data aggregated to annual
               means.  Reported by DNB to the ECB; available from 2003.
               Note: AAR excludes commitment fees; the previously used DNB
               "bancaire rente" CSV included fees (NARR), giving values
               ~0.1 pp higher.  The trend is identical; absolute max-mortgage
               estimates differ by roughly €2–5k.

Annuity formula (standard):
  monthly_budget = gross_annual_income × woonquote / 12
  r              = annual_rate_pct / 100 / 12          (monthly rate)
  n              = term_years × 12                     (months)
  max_mortgage   = monthly_budget × [(1+r)^n − 1] / [r × (1+r)^n]

The result is the maximum principal a modal earner can borrow at the average
rate for that year, assuming the payment fills exactly the allowed woonquote.

═══════════════════════════════════════════════════════════════════════════════
DATA SOURCES
═══════════════════════════════════════════════════════════════════════════════

Income:
  CBS OData table 83931NED "Inkomen van personen".
  MediaanInkomen_3 for persoonlijk bruto inkomen, totaal persons.
  Coverage: 2011 – present.  Cached 365 days.
  ⚠ This is the median for ALL persons (incl. part-time), so it sits ~€5k
    below the CPB "modaal loon" benchmark (~€44k in 2024 for full-year FTE).
  For years before 2011 the hardcoded _INCOME_FALLBACK dict is used.

Mortgage rates:
  ECB SDMX REST API — data-api.ecb.europa.eu — dataset MIR.
  No authentication required; open data from ECB / DNB.
  Monthly, Netherlands, new house-purchase loans, all maturities, AAR.
  Coverage: January 2003 – present (typ. 1-month lag).
  Cached in .cache/dnb/mortgage_rates.json, TTL 7 days.
"""

from __future__ import annotations

# ══════════════════════════════════════════════════════════════════════════════
# TUNEABLE PARAMETERS
# ══════════════════════════════════════════════════════════════════════════════

WOONQUOTE: float = 0.26    # fraction of gross income allowed for mortgage payments
TERM_YEARS: int  = 30      # mortgage term in years

# ══════════════════════════════════════════════════════════════════════════════
# INCOME — live CBS fetch with hardcoded fallback for pre-2011 years
# ══════════════════════════════════════════════════════════════════════════════

# Hardcoded fallback for years before CBS 83931NED coverage (pre-2011).
# Based on CPB modaal loon estimates; kept for backward compatibility.
_INCOME_FALLBACK: dict[int, int] = {
    2006: 29_500,
    2007: 30_000,
    2008: 31_500,
    2009: 32_500,
    2010: 32_500,
}


def load_modal_income() -> dict[int, int]:
    """
    Return {year: median_gross_income_eur} merging CBS live data with fallback.

    For 2011 onward: CBS 83931NED MediaanInkomen_3 (median personal gross
    income, all persons, EUR).  For years before 2011: _INCOME_FALLBACK.

    On CBS fetch failure the fallback dict is returned alone.

    ⚠ The CBS median covers all persons including part-time workers and is
    ~€5k lower than the CPB "modaal loon" for full-year FTE employees.
    Adjust WOONQUOTE if you need to calibrate to a different income anchor.
    """
    from watmoetikbieden.sources.cbs_income import fetch_median_income

    cbs_data = fetch_median_income()

    merged: dict[int, int] = dict(_INCOME_FALLBACK)
    for year, income in cbs_data.items():
        merged[year] = int(income)

    return merged


# Convenience alias — populated lazily the first time lending_capacity_series()
# is called; use load_modal_income() directly if you need a fresh dict.
MODAL_INCOME: dict[int, int] = _INCOME_FALLBACK   # initial stub; see load_modal_income()

# ══════════════════════════════════════════════════════════════════════════════
# MORTGAGE INTEREST RATE LOADER  (ECB SDMX, sourced from DNB)
# ══════════════════════════════════════════════════════════════════════════════

def load_dnb_rates() -> dict[int, dict[str, float]]:
    """
    Return yearly average mortgage rates from the ECB SDMX API (DNB-reported).

    Returns
    -------
    {year: {"totaal": float}}

    The "totaal" key preserves backward compatibility with lending_capacity_series().
    Only years with at least one monthly observation are included; partial years
    (e.g. the current year) use the available months.

    Rate definition: ECB MIR M.NL.B.A2C.AM.R.A.2250.EUR.N — annualised agreed
    rate (AAR) for new house-purchase loans, all maturities, Netherlands.
    Slightly lower (~0.1 pp) than the previously used DNB "bancaire rente" NARR
    because AAR excludes commitment fees.
    """
    from watmoetikbieden.sources.dnb_mortgage_rates import fetch_mortgage_rates

    series = fetch_mortgage_rates()
    if not series or not series.points:
        return {}

    accum: dict[int, list[float]] = {}
    for pt in series.points:
        yr = int(pt.period[:4])
        accum.setdefault(yr, []).append(pt.rate_pct)

    return {
        yr: {"totaal": round(sum(vals) / len(vals), 3)}
        for yr, vals in sorted(accum.items())
    }


# ══════════════════════════════════════════════════════════════════════════════
# LENDING CAPACITY CALCULATOR
# ══════════════════════════════════════════════════════════════════════════════

def _annuity_max_mortgage(
    annual_income: float,
    annual_rate_pct: float,
    woonquote: float = WOONQUOTE,
    term_years: int = TERM_YEARS,
) -> float:
    """
    Maximum principal for an annuity mortgage given income and rate.

    Parameters
    ----------
    annual_income    Gross annual income in EUR.
    annual_rate_pct  Annual interest rate as a percentage (e.g. 3.5 for 3.5%).
    woonquote        Fraction of gross income allowed for mortgage payments.
    term_years       Mortgage term in years.
    """
    monthly_budget = annual_income * woonquote / 12
    r = annual_rate_pct / 100.0 / 12.0   # monthly rate
    n = term_years * 12                   # total months

    if r <= 0:
        return monthly_budget * n         # 0% rate edge case

    return monthly_budget * ((1 + r) ** n - 1) / (r * (1 + r) ** n)


def lending_capacity_series() -> list[dict]:
    """
    Compute max annuity mortgage for a modal-income earner per year.

    Returns a list of dicts, one per year where both income and DNB rate
    are available, sorted by year ascending.

    Each dict:
      year            int
      modal_income    int    gross annual EUR (CBS median or fallback)
      rate_totaal     float  DNB yearly average rate (%)
      max_mortgage    int    maximum annuity mortgage principal (EUR)
    """
    rates  = load_dnb_rates()
    income = load_modal_income()

    rows = []
    for year, yr_income in sorted(income.items()):
        if year not in rates:
            continue
        rate = rates[year]["totaal"]
        cap  = _annuity_max_mortgage(yr_income, rate)
        rows.append({
            "year":         year,
            "modal_income": yr_income,
            "rate_totaal":  rate,
            "max_mortgage": round(cap),
        })

    return rows
