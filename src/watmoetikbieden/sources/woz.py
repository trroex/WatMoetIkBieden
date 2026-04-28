"""
WOZ (Wet Waardering Onroerende Zaken) property valuations.

Source: Kadaster LVWOZ API (api.kadaster.nl)
  Official public API — no auth, no session, no cookies required.
  Rate limit: 5000 requests/day (logged on every response).

Caching: responses are written to .cache/woz/{nummeraanduiding_id}.json
  and reused for 30 days to stay well within the daily rate limit.
"""

from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

from watmoetikbieden.models import WozResult, WozWaarde

_API_BASE = "https://api.kadaster.nl/lvwoz/wozwaardeloket-api/v1"
_CACHE_DIR = Path(".cache/woz")
_CACHE_TTL_DAYS = 30

_ratelimit_remaining: int | None = None


def _diag(msg: str) -> None:
    print(f"[WOZ] {msg}", file=sys.stderr, flush=True)


def _cache_path(nummeraanduiding_id: str) -> Path:
    return _CACHE_DIR / f"{nummeraanduiding_id}.json"


def _load_cache(nummeraanduiding_id: str) -> dict | None:
    p = _cache_path(nummeraanduiding_id)
    if not p.exists():
        return None
    age_days = (time.time() - p.stat().st_mtime) / 86400
    if age_days > _CACHE_TTL_DAYS:
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def _save_cache(nummeraanduiding_id: str, data: dict) -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _cache_path(nummeraanduiding_id).write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _log_ratelimit(headers: httpx.Headers) -> None:
    global _ratelimit_remaining
    day_limit = headers.get("Kadaster-Ratelimit-Daylimit")
    remaining = headers.get("Kadaster-Ratelimit-Daylimit-Remaining")
    if day_limit or remaining:
        _diag(f"rate-limit: {remaining}/{day_limit} remaining today")
    if remaining is not None:
        try:
            _ratelimit_remaining = int(remaining)
        except ValueError:
            pass


def _parse_response(raw: dict) -> WozResult:
    woz_obj = raw.get("wozObject") or {}
    grondoppervlakte = woz_obj.get("grondoppervlakte")
    wozobjectnummer = woz_obj.get("wozobjectnummer")
    gemeentecode = woz_obj.get("gemeentecode")

    waarden_raw = raw.get("wozWaarden") or []
    waarden = [
        WozWaarde(
            peildatum=r.get("peildatum", ""),
            vastgesteldeWaarde=r.get("vastgesteldeWaarde"),
        )
        for r in waarden_raw
        if r.get("peildatum")
    ]

    return WozResult(
        waarden=waarden,
        source="kadaster_lvwoz",
        grondoppervlakte=grondoppervlakte,
        wozobjectnummer=wozobjectnummer,
        gemeentecode_woz=gemeentecode,
    )


async def fetch_woz_waardeloket(
    nummeraanduiding_id: str,
    _unused_client: httpx.AsyncClient,
) -> tuple[WozResult | None, dict]:
    """
    Fetch all registered WOZ values for a nummeraanduiding ID.
    Returns (WozResult, raw) or (None, {}) on any failure.
    """
    global _ratelimit_remaining

    cached = _load_cache(nummeraanduiding_id)
    if cached is not None:
        _diag("cache hit")
        return _parse_response(cached), cached

    _diag("cache miss → fetching")

    if _ratelimit_remaining is not None and _ratelimit_remaining < 100:
        _diag(f"WARNING: only {_ratelimit_remaining} requests remaining today — skipping WOZ call")
        return None, {}

    url = f"{_API_BASE}/wozwaarde/nummeraanduiding/{nummeraanduiding_id}"
    async with httpx.AsyncClient(timeout=httpx.Timeout(30.0)) as c:
        try:
            resp = await c.get(url)
        except Exception as exc:
            _diag(f"fetch exception: {type(exc).__name__}: {exc}")
            return None, {}

        _log_ratelimit(resp.headers)
        _diag(f"HTTP {resp.status_code}, content-type: {resp.headers.get('content-type', '?')}")

        if resp.status_code in (404, 204):
            _diag("no WOZ data registered for this nummeraanduiding")
            return None, {}

        if resp.status_code != 200:
            _diag(f"unexpected status; body: {resp.text[:200]!r}")
            return None, {}

        try:
            raw = resp.json()
        except json.JSONDecodeError:
            _diag(f"response is not JSON; body starts: {resp.text[:200]!r}")
            return None, {}

    _save_cache(nummeraanduiding_id, raw)
    return _parse_response(raw), raw
