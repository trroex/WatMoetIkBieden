"""
BAG address lookup via the PDOK Locatieserver v3.1.

Docs: https://api.pdok.nl/bzk/locatieserver/search/v3_1/ui/
No API key required.

The /free endpoint accepts a free-text query and returns ranked address candidates.
We filter to type:adres to avoid returning streets, municipalities, etc.
"""

from __future__ import annotations

import httpx

from watmoetikbieden.models import BagLocatieDoc, BagLocatieResponse

_BASE = "https://api.pdok.nl/bzk/locatieserver/search/v3_1"


async def lookup_address(
    query: str,
    client: httpx.AsyncClient,
    rows: int = 5,
) -> tuple[BagLocatieResponse, dict]:
    """
    Free-text address lookup.

    Returns (parsed_response, raw_json). We keep raw_json so callers can
    inspect every field without going through the model.
    """
    params = {
        "q": query,
        "fq": "type:adres",   # restrict to address-level hits only
        "rows": rows,
        "fl": "*",            # return all available fields
    }
    resp = await client.get(f"{_BASE}/free", params=params)
    resp.raise_for_status()
    raw = resp.json()

    # The actual result array lives at response.docs
    inner = raw.get("response", {})
    parsed = BagLocatieResponse(
        docs=[BagLocatieDoc(**doc) for doc in inner.get("docs", [])],
        numFound=inner.get("numFound", 0),
    )
    return parsed, raw


async def best_match(
    query: str,
    client: httpx.AsyncClient,
) -> tuple[BagLocatieDoc | None, dict]:
    """Return the top-ranked address candidate, or None if nothing found."""
    parsed, raw = await lookup_address(query, client, rows=1)
    best = parsed.docs[0] if parsed.docs else None
    return best, raw
