"""
Main orchestrator: given a Dutch address string, fetch all available
data from BAG Locatieserver, BAG WFS, EP-Online, WOZ Waardeloket, and BRK.

Fetch waves:
  Wave 1 – BAG locatieserver (all subsequent calls depend on this)
  Wave 2 – BAG WFS verblijfsobject + EP-Online + WOZ + BRK (parallel)
  Wave 3 – BAG WFS pand  (depends on pandidentificatie from wave 2)
"""

from __future__ import annotations

import asyncio

import httpx

from watmoetikbieden.models import AddressData
from watmoetikbieden.sources import bag_locatie, bag_wfs, brk as brk_source, ep_online, woz as woz_source
from watmoetikbieden.sources.cbs_demographics import fetch_demographics as cbs_fetch
from watmoetikbieden.sources.leefbaarometer import LeefbarometerLookup

_lbm = LeefbarometerLookup()

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

        # ── Leefbaarometer: local file lookup, no network ─────────────────────
        if bag_addr.buurtcode:
            result.leefbaarometer = _lbm.lookup_by_buurtcode(bag_addr.buurtcode)
        elif bag_addr.centroide_ll:
            # parse "POINT(lon lat)"
            try:
                inner = bag_addr.centroide_ll.strip()[6:-1]
                lon_s, lat_s = inner.split()
                result.leefbaarometer = _lbm.lookup_by_coordinates(float(lat_s), float(lon_s))
            except Exception:
                pass

        postcode        = bag_addr.postcode or ""
        huisnummer      = bag_addr.huisnummer or ""
        vbo_id          = bag_addr.adresseerbaarobject_id or ""
        centroide_rd    = bag_addr.centroide_rd or ""
        nummeraand_id   = bag_addr.nummeraanduiding_id or ""
        perceel_refs    = bag_addr.gekoppeld_perceel  # list[str], may be empty
        buurtcode       = bag_addr.buurtcode or ""
        wijkcode        = bag_addr.wijkcode or ""
        gemeentecode    = f"GM{bag_addr.gemeentecode}" if bag_addr.gemeentecode else ""

        # ── Wave 2: BAG WFS + EP-Online + WOZ + BRK + CBS (parallel) ─────────
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
        brk_task = asyncio.create_task(
            brk_source.fetch_perceel(centroide_rd, perceel_refs, client)
            if centroide_rd and perceel_refs else _noop_brk()
        )
        cbs_task = asyncio.create_task(
            cbs_fetch(buurtcode, wijkcode or None, gemeentecode or None, client)
            if buurtcode else _noop_cbs()
        )

        (vbo, raw_vbo), (ep_label, raw_ep), (woz_result, raw_woz), (brk_result, raw_brk), cbs_result = (
            await asyncio.gather(vbo_task, ep_task, woz_task, brk_task, cbs_task)
        )

        result.bag_verblijfsobject = vbo
        result.raw["bag_wfs_verblijfsobject"] = raw_vbo

        result.ep_label = ep_label
        # Distinguish "queried but no label registered" from "not queried at all"
        if ep_label is None and bool(postcode and huisnummer):
            result.ep_no_label = True
        result.raw["ep_online"] = raw_ep

        result.woz = woz_result
        result.raw["woz"] = raw_woz

        result.brk_perceel = brk_result
        result.raw["brk_perceel"] = raw_brk

        result.cbs_demographics = cbs_result

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

async def _noop_brk():
    return None, {}

async def _noop_cbs():
    return None
