# WatMoetIkBieden
One-stop Dutch housing market bid price estimator – free public data only.

> **Open tasks & planned improvements:** see [`TODO.md`](TODO.md)

## Quick start

```powershell
# 1. Install Python 3.11+  →  https://www.python.org/downloads/
# 2. Create venv & install
python -m venv .venv
.\.venv\Scripts\pip install -e .

# 3. Fetch data for an address
.\.venv\Scripts\fetch-address "Ambachtsweg 72, 5272 RW Sint-Michielsgestel"
```

Or use the convenience script without installing:

```powershell
.\.venv\Scripts\python scripts\fetch_address.py "Ambachtsweg 72, 5272 RW Sint-Michielsgestel"
```

## Data sources

| Source | What we get | Hedonic variable |
|---|---|---|
| PDOK Locatieserver | Validated address, BAG IDs, postcode | postcode fixed-effect, join key |
| BAG WFS verblijfsobject | Floor area, usage type | log(m²), property_type |
| BAG WFS pand | Build year | build_year |
| EP-Online (RVO) | Energy label class | energy_label |
| CBS Statline *(planned)* | Neighbourhood demographics | neighbourhood controls |
| Kadaster *(planned)* | Transaction prices, plot area | log(price), log(plot_m²) |

## Hedonic model

```
log(price) = β₀ + β₁·log(m²) + β₂·build_year + β₃·energy_label
           + β₄·property_type + β₅·log(plot_m²)
           + β₆·postcode_fixed_effect + β₇·transaction_quarter + ε
```

## Project layout

```
src/watmoetikbieden/
├── models.py          Pydantic response models
├── fetcher.py         Async orchestrator (all sources → AddressData)
├── cli.py             Rich pretty-printer + feature table
└── sources/
    ├── bag_locatie.py  PDOK Locatieserver v3.1
    ├── bag_wfs.py      PDOK BAG WFS v2.0
    └── ep_online.py    RVO EP-Online API v4
```
