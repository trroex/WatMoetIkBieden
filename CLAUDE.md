# WatMoetIkBieden — Claude Code instructions

## Databronnen table (MANDATORY)

Whenever you add, modify, or remove a data source or CBS dataset in this project,
you **must** also update the `_SOURCES` list in `app.py` (inside `with tab_raw:`).

Each entry requires:
- `Bron` — source organisation (e.g. "CBS", "PDOK / Kadaster", "RVO")
- `Dataset` — human-readable dataset name
- `Tabel-ID` — CBS table ID or "–" if not applicable
- `Beschrijving` — one-sentence description of what the data contains and how it is used
- `Granulariteit` — geographic granularity (e.g. "Adres", "Buurt", "Gemeente", "COROP", "Nationaal")
- `Perioden` — year range or "Actueel" for real-time sources
- `Tijdresolutie` — update cadence (e.g. "Continu", "Maandelijks", "Kwartaal", "Jaarlijks")
- `Laatste update` — most recent known data vintage (e.g. "Q1 2026", "2024-03-31", "Continu")
- `Licentie` — licence identifier (CC0 / NLOD / ODbL / etc.)
- `Licentie omschrijving` — one-line plain-language description of what the licence allows
- `Commercieel` — "✅ Ja", "⚠️ <nuance>", or "❌ Nee"

Flag any source with a non-trivial commercial restriction with ⚠️ and explain briefly
(e.g. the Overpass API endpoint is not suitable for production use even though OSM data itself is ODbL).

## Commercial API rule

Do **not** introduce dependencies on paid third-party APIs without explicit approval.
All data sources must be freely accessible (no API key costs) unless the user explicitly
requests otherwise.

## Coding conventions

- Python 3.11+, typed where practical
- CBS OData: always use `startswith()` filters, never `eq`, for dimension codes
  (CBS pads dimension keys with trailing spaces)
- Cache all external API responses; prefer the existing `.cache/` patterns
- TTL: 7 days for market data, 365 days for reference/mapping data
