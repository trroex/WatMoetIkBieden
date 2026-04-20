"""
Main orchestrator: given a Dutch address string, fetch all available
data from BAG Locatieserver, BAG WFS, EP-Online, and WOZ Waardeloket.

Fetch waves:
  Wave 1 – BAG locatieserver (all subsequent calls depend on this)
  Wave 2 – BAG WFS verblijfsobject + EP-Online + WOZ  (parallel)
  Wave 3 – BAG WFS pand  (depends on pandidentificatie from wave 2)
"""

from __future__ import annotations

import asyncio

import httpx

from watmoetikbieden.models import AddressData
from watmoetikbieden.sources import bag_locatie, bag_wfs, ep_online, woz as woz_source

# Generous timeout: WFS and WOZ waardeloket can both be sluggish
_TIMEOUT = httpx.Timeout(30.0)


async def fetch_all(address_query: str) -> AddressData:
    """
    Fetch all public data for a single Dutch address.

    Returns AddressData with parsed models and raw API responses.
    Individual source failures are caught and surfaced as None fields
    rather than raising – the caller sees partial data instead of an error.
    """
    result = AddressData(query=address_query)

    async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
        # ── Wave 1: address lookup ────────────────────────────────────────────
        bag_addr, raw_locatie = await bag_locatie.best_match(address_query, client)
        result.bag_address = bag_addr
        result.raw["bag_locatie"] = raw_locatie

        if bag_addr is None:
            return result

        postcode      = bag_addr.postcode or ""
        huisnummer    = bag_addr.huisnummer or ""
        vbo_id        = bag_addr.adresseerbaarobject_id or ""
        centroide_rd  = bag_addr.centroide_rd or ""
        nummeraand_id = bag_addr.nummeraanduiding_id or ""

        # ── Wave 2: BAG WFS + EP-Online + WOZ (parallel) ─────────────────────
        vbo_task = asyncio.create_task(
            bag_wfs.fetch_verblijfsobject(centroide_rd, vbo_id, client)
            if centroide_rd and vbo_id else _noop_vbo()
        )
        ep_task = asyncio.create_task(
            ep_online.fetch_energy_label(
                postcode, huisnummer, client,
                huisletter=bag_addr.huisletter,
                toevoeging=bag_addr.huisnummertoevoeging,
            ) if postcode and huisnummer else _noop_ep()
        )
        woz_task = asyncio.create_task(
            _fetch_woz(nummeraand_id, client)
            if nummeraand_id else _noop_woz()
        )

        (vbo, raw_vbo), (ep_label, raw_ep), (woz_result, raw_woz) = await asyncio.gather(
            vbo_task, ep_task, woz_task
        )

        result.bag_verblijfsobject = vbo
        result.raw["bag_wfs_verblijfsobject"] = raw_vbo
        result.ep_label = ep_label
        result.raw["ep_online"] = raw_ep
        result.woz = woz_result
        result.raw["woz"] = raw_woz

        # ── Wave 3: pand (depends on pandidentificatie from vbo) ─────────────
        if vbo and vbo.pandidentificatie and centroide_rd:
            pand_id = vbo.pandidentificatie[0]
            pand, raw_pand = await bag_wfs.fetch_pand(centroide_rd, pand_id, client)
            result.bag_pand = pand
            result.raw["bag_wfs_pand"] = raw_pand

    return result


async def _fetch_woz(
    nummeraand_id: str,
    client: httpx.AsyncClient,
):
    return await woz_source.fetch_woz_waardeloket(nummeraand_id, client)


# ---------------------------------------------------------------------------
# Sentinel coroutines – keep gather() uniform when a precondition is missing
# ---------------------------------------------------------------------------

async def _noop_vbo():
    return None, {}

async def _noop_ep():
    return None, []

async def _noop_woz():
    return None, {}
