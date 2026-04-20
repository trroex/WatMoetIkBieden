"""
WOZ (Wet Waardering Onroerende Zaken) property valuations.

Primary source: WOZ Waardeloket (www.wozwaardeloket.nl)
  An undocumented internal API used by the Dutch developer community.
  Flow:
    1. POST /session/start  → receive session-id + lb-sticky cookie
    2. GET  /wozwaarde/nummeraanduiding/{nummeraanduiding_id}  → WOZ history

CBS Statline fallback: NOT YET IMPLEMENTED.
  CBS OData $filter is broken server-side on all tested dataset fields
  (same pattern as the PDOK WFS CQL_FILTER bug). The kerncijfers dataset
  interleaves municipalities with wijken/buurten, requiring ~3000+ row
  fetches to reach late-alphabet municipalities. We need a municipality-only
  WOZ dataset with working filtering before this is viable.

Rate limiting: the waardeloket asks for at most 1 request/second.
"""

from __future__ import annotations

import asyncio
import json
import sys
import time

import httpx

from watmoetikbieden.models import WozResult, WozWaarde

_LOKET_BASE = "https://www.wozwaardeloket.nl/wozwaardeloket-api/v1"

_last_loket_call: float = 0.0
_loket_lock = asyncio.Lock()


def _diag(msg: str) -> None:
    """Print a diagnostic line to stderr — always visible, never suppressed."""
    print(f"[WOZ] {msg}", file=sys.stderr, flush=True)


async def _loket_wait() -> None:
    global _last_loket_call
    async with _loket_lock:
        gap = time.monotonic() - _last_loket_call
        if gap < 1.0:
            await asyncio.sleep(1.0 - gap)
        _last_loket_call = time.monotonic()


_LOKET_HOME = "https://www.wozwaardeloket.nl"

# Browser-like headers – the site uses a WAF that rejects bare httpx user-agents
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "nl-NL,nl;q=0.9",
    "Referer": "https://www.wozwaardeloket.nl/",
    "Origin": "https://www.wozwaardeloket.nl",
}


async def _seed_cookies(client: httpx.AsyncClient) -> None:
    """
    GET the main page to seed load-balancer cookies into the client jar.

    The /session/start POST endpoint returns 405 (removed from the API).
    The ADC_CONN / ADC_REQ cookies set by the home page are sufficient
    for the data endpoint to respond with JSON.
    """
    await _loket_wait()
    r = await client.get(_LOKET_HOME, headers=_HEADERS, follow_redirects=True)
    _diag(f"GET home → HTTP {r.status_code}, cookies: {list(r.cookies.keys())}")
    r.raise_for_status()


async def fetch_woz_waardeloket(
    nummeraanduiding_id: str,
    _unused_client: httpx.AsyncClient,
) -> tuple[WozResult | None, list | dict]:
    """
    Fetch all registered WOZ values for a nummeraanduiding ID.
    Returns (WozResult, raw) or (None, {}) on any failure.
    Diagnostic output always goes to stderr.

    Uses its own AsyncClient so WAF cookies don't bleed into the
    shared BAG/EP-Online client jar.
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as c:
        try:
            await _seed_cookies(c)
        except Exception as exc:
            _diag(f"cookie seed exception: {type(exc).__name__}: {exc}")
            return None, {}

        await _loket_wait()
        try:
            resp = await c.get(
                f"{_LOKET_BASE}/wozwaarde/nummeraanduiding/{nummeraanduiding_id}",
                headers=_HEADERS,
            )
        except Exception as exc:
            _diag(f"data fetch exception: {type(exc).__name__}: {exc}")
            return None, {}

        _diag(
            f"data fetch → HTTP {resp.status_code}, "
            f"content-type: {resp.headers.get('content-type', '?')}"
        )

        if resp.status_code in (404, 204):
            _diag("no WOZ data registered for this address")
            return None, {}

        if resp.status_code != 200:
            _diag(f"unexpected status; body: {resp.text[:200]!r}")
            return None, {}

        try:
            raw = resp.json()
        except json.JSONDecodeError:
            _diag(f"response is not JSON; body starts: {resp.text[:200]!r}")
            return None, {}

        records: list[dict] = raw if isinstance(raw, list) else raw.get("wozWaardes", [])
        _diag(f"records in response: {len(records)}")

        if not records:
            return None, raw

        waarden = [
            WozWaarde(
                peildatum=r.get("peildatum", ""),
                vastgesteldeWaarde=r.get("vastgesteldeWaarde"),
            )
            for r in records
            if r.get("peildatum")
        ]

        return WozResult(waarden=waarden, source="wozwaardeloket"), raw
