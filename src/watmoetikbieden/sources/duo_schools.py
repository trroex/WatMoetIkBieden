"""
DUO school register + Onderwijsinspectie quality ratings.

Downloads and caches two DUO datasets:
  1. Vestigingen PO  – all primary school locations (BRIN, name, postcode, municipality)
  2. Vestigingen VO  – all secondary school locations
  3. Kwaliteitsoordelen (PO + SO + VO) – inspection ratings per school location

The three are joined on VESTIGINGSCODE (= BRIN + zero-padded vestiging number).

A fuzzy-matching lookup function maps an OSM school name + municipality name to
the most likely DUO school record and returns its rating.

Rating data note
───────────────
The available dataset (peildatum 2018-09-01) reflects the old inspection
framework.  Onderwijsinspectie replaced this with a new framework in August
2019; bulk download of the new ratings is not yet available from DUO.
The 2018 snapshot is displayed with a "(oordeel t/m 2018)" label so users
understand the context.

Cache: .cache/duo/{file}.csv  –  TTL 30 days.
"""

from __future__ import annotations

import csv
import io
import re
import time
import urllib.request
from collections import Counter
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path

# ── URLs ──────────────────────────────────────────────────────────────────────

_PO_VESTIGINGEN_URL = (
    "https://onderwijsdata.duo.nl/dataset/786f12ea-6224-42fd-ab72-de4d7d879535"
    "/resource/dcc9c9a5-6d01-410b-967f-810557588ba4/download/vestigingenbo.csv"
)
_VO_VESTIGINGEN_URL = (
    "https://onderwijsdata.duo.nl/dataset/c8e6ffdd-cc2b-44ee-880f-0ff03f72e868"
    "/resource/5187f8d5-ff9c-4284-8e06-4311f0354956/download/vestigingenvo.csv"
)
_RATINGS_URL = (
    "https://onderwijsdata.duo.nl/dataset/31da72f2-2858-4bc3-848e-dfe4875ba669"
    "/resource/b48d6835-0534-4008-82c8-1754b9080113/download/oordeel_po_so_vo.csv"
)

# ── cache ─────────────────────────────────────────────────────────────────────

_CACHE_DIR = Path(".cache/duo")
_TTL = 30 * 24 * 3600   # 30 days

_RATING_PRIORITY = {
    "Zeer zwak":            0,
    "Zwak":                 1,
    "Onvoldoende":          2,
    "Voldoende":            3,
    "Goed":                 4,
    "Zonder actueel oordeel": -1,
    "Geen oordeel":         -1,
}

# ── data model ─────────────────────────────────────────────────────────────────

@dataclass
class DuoSchoolInfo:
    brin:          str
    vestigingscode: str
    name:          str
    postcode:      str
    gemeente:      str
    sector:        str          # "PO" | "VO"
    denominatie:   str | None = None
    website:       str | None = None
    phone:         str | None = None
    rating:        str | None = None    # e.g. "Voldoende"
    rating_date:   str | None = None    # e.g. "2017-03-21"

    @property
    def rating_emoji(self) -> str:
        return {
            "Goed":        "🟢",
            "Voldoende":   "🟡",
            "Zwak":        "🔴",
            "Onvoldoende": "🔴",
            "Zeer zwak":   "🔴",
        }.get(self.rating or "", "⚪")

    @property
    def rating_label(self) -> str:
        if not self.rating or self.rating in ("Zonder actueel oordeel", "Geen oordeel"):
            return "–"
        yr = self.rating_date[:4] if self.rating_date else "2018"
        return f"{self.rating_emoji} {self.rating} ({yr})"


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _fetch_url(url: str) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": "WatMoetIkBieden/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return r.read()


def _cached_csv(filename: str, url: str) -> str:
    """Return CSV text, downloading and caching if needed."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    p = _CACHE_DIR / filename
    if p.exists() and time.time() - p.stat().st_mtime < _TTL:
        return p.read_text(encoding="utf-8")
    raw = _fetch_url(url)
    text = raw.decode("utf-8", errors="replace")
    p.write_text(text, encoding="utf-8")
    return text


# ── parsers ───────────────────────────────────────────────────────────────────

def _parse_vestigingen(text: str, sector: str) -> dict[str, DuoSchoolInfo]:
    """Return {VESTIGINGSCODE: DuoSchoolInfo} for PO or VO vestigingen CSV."""
    reader = csv.DictReader(io.StringIO(text))
    result: dict[str, DuoSchoolInfo] = {}
    for row in reader:
        vc = row.get("VESTIGINGSCODE", "").strip()
        if not vc:
            continue
        result[vc] = DuoSchoolInfo(
            brin=row.get("INSTELLINGSCODE", "").strip(),
            vestigingscode=vc,
            name=row.get("VESTIGINGSNAAM", "").strip(),
            postcode=row.get("POSTCODE", "").strip(),
            gemeente=row.get("GEMEENTENAAM", "").strip().title(),  # normalize case
            sector=sector,
            denominatie=row.get("DENOMINATIE") or None,
            website=row.get("INTERNETADRES") or None,
            phone=row.get("TELEFOONNUMMER") or None,
        )
    return result


def _parse_ratings(text: str) -> dict[str, tuple[str, str]]:
    """
    Return {VESTIGINGSCODE: (rating, date)} – the most severe / most recent
    rating per school location.

    Multiple rows exist per location (one per Elementtype: HAVO, VWO, etc.).
    Aggregation rule: take the lowest-priority (worst) rating; on ties take
    the most recent VaststellingsdatumEindoordeelKwaliteit.
    """
    reader = csv.DictReader(io.StringIO(text))

    # Accumulate: {vc: [(priority, date, rating), ...]}
    from collections import defaultdict
    accum: dict[str, list[tuple[int, str, str]]] = defaultdict(list)

    for row in reader:
        brin  = (row.get("BRIN") or "").strip()
        vest  = (row.get("Vestiging") or "").strip().zfill(2)
        vc    = brin + vest
        rating = (row.get("EindoordeelKwaliteit") or "").strip()
        date   = (row.get("VaststellingsdatumEindoordeelKwaliteit") or "").strip()
        prio   = _RATING_PRIORITY.get(rating, -2)
        accum[vc].append((prio, date, rating))

    result: dict[str, tuple[str, str]] = {}
    for vc, entries in accum.items():
        # Filter out "no rating" entries if actionable ones exist
        actionable = [(p, d, r) for p, d, r in entries if p >= 0]
        pool = actionable if actionable else entries
        # Sort: worst rating first (lowest prio number), then most recent date
        pool.sort(key=lambda x: (x[0], x[1]))
        worst_prio, _, _ = pool[0]
        worst = [e for e in pool if e[0] == worst_prio]
        # Among equally bad: most recent
        worst.sort(key=lambda x: x[1], reverse=True)
        _, date, rating = worst[0]
        result[vc] = (rating, date)

    return result


# ── combined index ────────────────────────────────────────────────────────────

_index: dict[str, DuoSchoolInfo] | None = None   # {VESTIGINGSCODE: info}


def _load_index() -> dict[str, DuoSchoolInfo]:
    global _index
    if _index is not None:
        return _index

    po_text = _cached_csv("vestigingenbo.csv", _PO_VESTIGINGEN_URL)
    vo_text = _cached_csv("vestigingenvo.csv", _VO_VESTIGINGEN_URL)
    rt_text = _cached_csv("oordeel_po_so_vo.csv", _RATINGS_URL)

    schools: dict[str, DuoSchoolInfo] = {}
    schools.update(_parse_vestigingen(po_text, "PO"))
    schools.update(_parse_vestigingen(vo_text, "VO"))

    ratings = _parse_ratings(rt_text)
    for vc, (rating, date) in ratings.items():
        if vc in schools:
            schools[vc].rating = rating
            schools[vc].rating_date = date

    _index = schools
    return _index


# ── name normalisation + fuzzy matching ───────────────────────────────────────

_STRIP_PREFIXES = (
    "obs ", "cbs ", "rk ", "r.k. ", "o.b.s. ", "c.b.s. ",
    "basisschool ", "school ", "bs ", "ics ",
)


def _normalize(name: str) -> str:
    n = name.lower()
    n = re.sub(r"[''`]", "", n)
    n = re.sub(r"[^a-z0-9\s]", " ", n)
    n = re.sub(r"\s+", " ", n).strip()
    for pfx in _STRIP_PREFIXES:
        if n.startswith(pfx):
            n = n[len(pfx):]
    return n


def _similarity(a: str, b: str) -> float:
    """
    Similarity between two school names.

    DUO often registers schools with long descriptive names, e.g.:
      "Elde College Schijndel voor VWO HAVO VMBO LWOO en PRO"
    while OSM uses the short popular name:
      "Elde College"

    We take the maximum of:
      1. Full SequenceMatcher ratio (handles equal-length names well)
      2. Prefix ratio: compare the OSM name against only the first N words of
         the DUO name (N = word-count of OSM name), but ONLY when the DUO name
         is at least twice as long as the OSM name.  This prevents shared tokens
         like "basisschool" from boosting unrelated same-length names.
    """
    na = _normalize(a)
    nb = _normalize(b)
    full = SequenceMatcher(None, na, nb).ratio()

    na_words = na.split()
    nb_words = nb.split()

    # Prefix boost only when DUO name is substantially longer (>=2×)
    if len(nb_words) >= len(na_words) * 2:
        nb_trunc = " ".join(nb_words[: len(na_words)])
        prefix = SequenceMatcher(None, na, nb_trunc).ratio()
        return max(full, prefix)

    return full


# ── public API ────────────────────────────────────────────────────────────────

def lookup_school(
    osm_name: str,
    gemeente: str,
    min_score: float = 0.60,
) -> DuoSchoolInfo | None:
    """
    Find the best-matching DUO school record for an OSM school name within
    the given gemeente.

    Parameters
    ----------
    osm_name    Name from OpenStreetMap tags.
    gemeente    Municipality name (any case; compared case-insensitively).
    min_score   Minimum SequenceMatcher ratio to accept a match (0–1).

    Returns None when no sufficiently similar record is found.
    """
    idx = _load_index()
    gem_lc = gemeente.lower().strip()

    # Candidates: same gemeente (case-insensitive)
    candidates = [s for s in idx.values() if s.gemeente.lower() == gem_lc]

    if not candidates:
        # Broaden: try nearby – partial postcode match not possible without coords,
        # so fall back to all schools (slower but more forgiving for edge cases).
        candidates = list(idx.values())

    best: DuoSchoolInfo | None = None
    best_score = 0.0
    for s in candidates:
        score = _similarity(osm_name, s.name)
        if score > best_score:
            best_score = score
            best = s

    if best_score >= min_score:
        return best
    return None
