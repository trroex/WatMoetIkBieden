"""
Energy label lookup via the RVO EP-Online public API v4.

Docs: https://public.ep-online.nl/swagger/index.html
No API key required for the public read endpoint.

The /PandEnergielabel/Adres endpoint returns all registered energy certificates
for an address. Addresses can have multiple certificates (re-certifications);
we take the most recent one by Pand_opnamedatum.
"""

from __future__ import annotations

import httpx

from watmoetikbieden.models import EpOnlineLabel

_BASE = "https://public.ep-online.nl/api/v4"


async def fetch_energy_label(
    postcode: str,
    huisnummer: str | int,
    client: httpx.AsyncClient,
    huisletter: str | None = None,
    toevoeging: str | None = None,
) -> tuple[EpOnlineLabel | None, list[dict]]:
    """
    Fetch the most recent registered energy label for an address.

    Returns (most_recent_label_or_None, raw_list_of_all_records).
    The raw list is returned so the caller can inspect all historical certs.
    """
    # EP-Online expects postcode without space, e.g. "1015CJ"
    postcode_clean = postcode.replace(" ", "")

    params: dict = {
        "postcode": postcode_clean,
        "huisnummer": str(huisnummer),
    }
    if huisletter:
        params["huisletter"] = huisletter
    if toevoeging:
        params["huisnummertoevoeging"] = toevoeging

    # EP-Online returns 404 when no certificate exists – treat as empty
    resp = await client.get(
        f"{_BASE}/PandEnergielabel/Adres",
        params=params,
        headers={"accept": "application/json"},
    )
    if resp.status_code == 404:
        return None, []

    resp.raise_for_status()
    raw: list[dict] = resp.json()
    if not raw:
        return None, []

    # Sort descending by opnamedatum so index 0 is the newest certificate
    sorted_raw = sorted(
        raw,
        key=lambda r: r.get("Pand_opnamedatum") or "",
        reverse=True,
    )
    label = EpOnlineLabel(**sorted_raw[0])
    return label, sorted_raw
