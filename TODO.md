# WatMoetIkBieden – Open tasks

---

## 1. Regional OLS price relatives for WOZ estimator

**Status:** ✅ implemented (2026-05)  
**Priority:** high – current national relatives cause ~17% underestimation in rural/affluent areas  
**Context:** see conversation history for full design discussion

### Problem

`woz_estimator.py` currently uses national-average price relatives per woningtype
(from CBS 85791NED 2024). These relatives reflect the national mix, which includes
cheap urban tussenwoningen dragging down the tussenwoning relative. In rural or
affluent municipalities (e.g. Sint-Michielsgestel) all types are relatively expensive,
causing systematic underestimation.

Validated example: Ambachtsweg 72, 5272 RW Sint-Michielsgestel  
  Real WOZ 2025:   € 423 000  
  Estimated WOZ:   € 361 579  (−14.7%)

### Solution

Derive **province-level** price relatives from CBS 85984NED (KWB) by running a
weighted OLS regression across all ~3 400 Dutch buurten, grouped by province.
No new data source needed — uses data already fetched for the estimator.

**Regression model (per province):**

```
gem_woz_buurt_i  =  β_tuss  × pct_tussenwoning_i/100
                  + β_hoek  × pct_hoekwoning_i/100
                  + β_twee  × pct_twee_onder_een_kap_i/100
                  + β_vrij  × pct_vrijstaand_i/100
                  + β_meer  × pct_meergezins_i/100
                  + ε_i

weights = woningvoorraad_i   (larger buurten count more)
no intercept (forced through origin)
```

β coefficients are the province-specific average WOZ per woningtype.
Normalise to relatives the same way as `NATIONAL_PRICES_2024` in `woz_estimator.py`.

### Files to create / modify

```
src/watmoetikbieden/sources/kwb_bulk.py      NEW
    - Fetches all buurt rows from CBS 85984NED in one API call
      (measures: M001642, ZW25805, ZW25806, ZW10300, ZW10320, ZW10340, M000297)
    - Fetches gemeentecode → provincie mapping from CBS Gebiedsindelingen 2024
    - Derives provincie per buurt from buurtcode[2:6] → gemeentecode → provincie
    - Pivots to wide DataFrame; caches at .cache/kwb_bulk/buurten_2024.csv
    - TTL: 365 days

src/watmoetikbieden/regional_relatives.py    NEW
    - Loads bulk data (triggers kwb_bulk fetch if cache missing)
    - Runs weighted OLS per province using numpy.linalg.lstsq
    - Only uses regression if province has ≥ 30 buurten with complete data
    - Stores: {provincie: {type_code: relative_factor}}
    - Caches at .cache/kwb_bulk/regional_relatives.json

src/watmoetikbieden/woz_estimator.py         MODIFY
    - Add optional `province: str | None = None` argument to estimate()
    - If province provided and regional relatives available → use them
    - Otherwise → fall back to NATIONAL_PRICES_2024 (current behaviour)
    - Add field to WozEstimate: relatives_source: str
      e.g. "regional (Noord-Brabant)" or "national"

app.py                                        MODIFY
    - Extract a.provincienaam from BAG result
    - Pass as province= to woz_estimate()
    - Show relatives_source in the validation metrics and model detail expander
```

### Dependencies

No new packages. Uses:
- `httpx` (already in deps) — bulk CBS fetch
- `numpy.linalg.lstsq` (already in deps) — weighted OLS
- `csv` stdlib — cache read/write

### CBS API calls

**Bulk buurt data** (one call, ~24 000 rows):
```
GET https://datasets.cbs.nl/odata/v1/CBS/85984NED/Observations
  ?$filter=startswith(WijkenEnBuurten,'BU')
    and Measure in ('M001642','ZW25805','ZW25806','ZW10300','ZW10320','ZW10340','M000297')
  &$select=WijkenEnBuurten,Measure,Value
  &$top=100000
```

**Gemeente → provincie mapping** (CBS Gebiedsindelingen):
```
GET https://opendata.cbs.nl/ODataApi/odata/85215NED/UntypedDataSet
  ?$select=GemeentecodeGM,ProvincienaamPV
  &$top=500
```
*(verify dataset ID before use — Gebiedsindelingen is updated yearly)*

### Validation

After implementing, re-run against the known test case:
- Address: Ambachtsweg 72, 5272 RW Sint-Michielsgestel
- Real WOZ 2025: € 423 000
- Expected improvement: regional estimate should be closer than −14.7%
- Acceptable residual: ± 10% (model uses buurt average, not individual features)

---

## 2. *(next task here)*
