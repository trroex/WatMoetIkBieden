"""
Macro market indicators for WatMoetIkBieden.

Currently provides:
  - Modal gross income per year (hardcoded; TODO: replace with CBS OData fetch)
  - DNB average mortgage interest rates (parsed from local CSV)
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

  rate       = DNB "Totaal" yearly average for newly issued residential
               mortgages (column "Totaal" in bancaire_rente CSV).
               This blends all fixed-rate periods proportional to actual
               market take-up, giving a realistic average borrowing cost.

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

Modal income:
  Hardcoded dict below.
  TODO (see TODO.md §2): replace with CBS OData fetch (table TBD).

DNB interest rates:
  data/dnb/(12-05-26)_Bancaire_rente_op_zuiver_nieuw_afgesloten_
           woninghypotheken_huishoudens.csv
  Downloaded manually from dnb.nl dashboard, 2026-05-12.
  Monthly data from December 2014 onwards.
"""

from __future__ import annotations

import csv
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════════
# TUNEABLE PARAMETERS
# ══════════════════════════════════════════════════════════════════════════════

WOONQUOTE: float = 0.26    # fraction of gross income allowed for mortgage payments
TERM_YEARS: int  = 30      # mortgage term in years

# ══════════════════════════════════════════════════════════════════════════════
# MODAL INCOME  (hardcoded – see TODO.md §2 for CBS replacement)
# ══════════════════════════════════════════════════════════════════════════════

# Source: CBS / Nibud modal loon estimates, gross per year in EUR.
# TODO: replace with CBS OData fetch from table TBD.
MODAL_INCOME: dict[int, int] = {
    2006: 29_500,
    2007: 30_000,
    2008: 31_500,
    2009: 32_500,
    2010: 32_500,
    2011: 33_000,
    2012: 33_000,
    2013: 32_500,
    2014: 33_000,
    2015: 33_000,
    2016: 33_500,
    2017: 34_000,
    2018: 34_500,
    2019: 35_000,
    2020: 36_000,
    2021: 37_000,
    2022: 39_000,
    2023: 41_500,
    2024: 44_000,
    2025: 46_500,
}

# ══════════════════════════════════════════════════════════════════════════════
# DNB INTEREST RATE PARSER
# ══════════════════════════════════════════════════════════════════════════════

_DNB_DIR = Path(__file__).parent.parent.parent / "data" / "dnb"

_NL_MONTHS = {
    "januari": 1, "februari": 2, "maart": 3, "april": 4,
    "mei": 5, "juni": 6, "juli": 7, "augustus": 8,
    "september": 9, "oktober": 10, "november": 11, "december": 12,
}


def _find_dnb_csv() -> Path | None:
    """Return the first CSV file found in data/dnb/, or None."""
    if not _DNB_DIR.exists():
        return None
    for p in _DNB_DIR.glob("*.csv"):
        return p
    return None


def load_dnb_rates() -> dict[int, dict[str, float]]:
    """
    Parse the DNB bancaire rente CSV and return yearly averages.

    Returns
    -------
    {year: {"totaal": float, "variabel": float, "1_5": float,
            "5_10": float, "10plus": float}}

    Only years with at least one monthly observation are included.
    Partial years (e.g. 2014 with only December, or the current year)
    use the available months.
    """
    csv_path = _find_dnb_csv()
    if csv_path is None:
        return {}

    # Accumulate monthly values per year
    accum: dict[int, dict[str, list[float]]] = {}
    col_keys = ["totaal", "variabel", "1_5", "5_10", "10plus"]

    with csv_path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        next(reader)  # skip header row
        for row in reader:
            if len(row) < 6:
                continue
            month_str = row[0].strip().strip('"').lower()
            parts = month_str.split()
            if len(parts) != 2:
                continue
            month_name, year_str = parts
            if month_name not in _NL_MONTHS:
                continue
            try:
                year = int(year_str)
            except ValueError:
                continue

            if year not in accum:
                accum[year] = {k: [] for k in col_keys}

            for i, key in enumerate(col_keys):
                try:
                    val = float(row[i + 1])
                    accum[year][key].append(val)
                except (ValueError, IndexError):
                    pass

    # Compute yearly averages
    result: dict[int, dict[str, float]] = {}
    for year, cols in accum.items():
        if not cols["totaal"]:
            continue
        result[year] = {
            k: round(sum(v) / len(v), 3)
            for k, v in cols.items()
            if v
        }

    return result


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
      modal_income    int    gross annual EUR
      rate_totaal     float  DNB yearly average rate (%)
      max_mortgage    int    maximum annuity mortgage principal (EUR)
    """
    rates = load_dnb_rates()

    rows = []
    for year, income in sorted(MODAL_INCOME.items()):
        if year not in rates:
            continue
        rate = rates[year]["totaal"]
        cap = _annuity_max_mortgage(income, rate)
        rows.append({
            "year":         year,
            "modal_income": income,
            "rate_totaal":  rate,
            "max_mortgage": round(cap),
        })

    return rows
