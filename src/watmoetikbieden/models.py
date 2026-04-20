"""
Pydantic models for raw API responses from each public data source.

These models capture only the fields we care about for the hedonic model.
Unknown/extra fields are silently ignored (model_config extra='ignore').
"""

from __future__ import annotations

from typing import Any
from pydantic import BaseModel, Field


class _Base(BaseModel):
    model_config = {"extra": "ignore"}


# ---------------------------------------------------------------------------
# BAG Locatieserver (PDOK) – address lookup
# ---------------------------------------------------------------------------

class BagLocatieDoc(_Base):
    """Single address result from the PDOK locatieserver."""

    id: str
    weergavenaam: str                        # human-readable full address
    straatnaam: str | None = None
    huisnummer: str | int | None = None
    huisletter: str | None = None
    huisnummertoevoeging: str | None = None
    postcode: str | None = None
    woonplaatsnaam: str | None = None
    gemeentenaam: str | None = None
    provincienaam: str | None = None

    # BAG identifiers – critical for joining to building data
    nummeraanduiding_id: str | None = None   # BAG nummeraanduiding (16 digits)
    adresseerbaarobject_id: str | None = None  # verblijfsobject / ligplaats / standplaats ID
    gemeentecode: str | None = None          # e.g. '0363' – used for CBS fallback

    # Geometry
    centroide_ll: str | None = None          # WKT point in WGS84, e.g. "POINT(4.88 52.37)"
    centroide_rd: str | None = None          # WKT point in RD New (EPSG:28992)


class BagLocatieResponse(BaseModel):
    """Top-level wrapper from /free or /lookup endpoint."""
    model_config = {"extra": "ignore"}

    docs: list[BagLocatieDoc] = Field(default_factory=list)
    numFound: int = 0


# ---------------------------------------------------------------------------
# BAG WFS – verblijfsobject (residential unit characteristics)
# ---------------------------------------------------------------------------

class BagVerblijfsobject(BaseModel):
    """Building unit characteristics from the BAG WFS verblijfsobject layer."""
    model_config = {"extra": "ignore"}

    identificatie: str | None = None
    oppervlakte: int | None = None           # floor area in m²
    gebruiksdoel: list[str] = Field(default_factory=list)
    status: str | None = None
    pandidentificatie: list[str] = Field(default_factory=list)  # link to pand(en)
    bouwjaar: int | None = None              # denormalised from pand – present in WFS response


class BagPand(BaseModel):
    """Building-level data from the BAG WFS pand layer."""
    model_config = {"extra": "ignore"}

    identificatie: str | None = None
    bouwjaar: int | None = None              # construction year
    status: str | None = None
    aantal_verblijfsobjecten: int | None = None  # number of units in the building


# ---------------------------------------------------------------------------
# EP-Online (RVO) – energy label
# ---------------------------------------------------------------------------

class EpOnlineLabel(BaseModel):
    """Energy performance certificate record from EP-Online."""
    model_config = {"extra": "ignore"}

    Pand_opnamedatum: str | None = None
    Pand_registratiedatum: str | None = None
    Pand_energieklasse: str | None = None    # e.g. "A", "B", "C+++"
    Pand_energieprestatieindex: float | None = None
    Pand_berekeningstype: str | None = None  # NEN7120 / ISSO75.3 / etc.
    Pand_postcode: str | None = None
    Pand_huisnummer: str | None = None
    Pand_huisletter: str | None = None
    Pand_detailaanduiding: str | None = None
    Pand_gebouwtype: str | None = None
    Pand_gebouwsubtype: str | None = None


# ---------------------------------------------------------------------------
# WOZ (Wet Waardering Onroerende Zaken) – property valuation
# ---------------------------------------------------------------------------

class WozWaarde(BaseModel):
    """Single annual WOZ valuation record."""
    model_config = {"extra": "ignore"}

    peildatum: str                           # reference date, e.g. "2024-01-01"
    vastgesteldeWaarde: int | None = None    # assessed value in euros

    @property
    def year(self) -> int | None:
        try:
            return int(self.peildatum[:4])
        except (ValueError, TypeError):
            return None


class WozResult(BaseModel):
    """All WOZ values for an address, plus source metadata."""

    waarden: list[WozWaarde] = Field(default_factory=list)
    # "wozwaardeloket" when fetched directly; "cbs_gemeente" when CBS fallback was used
    source: str = "wozwaardeloket"
    is_approximation: bool = False           # True when CBS municipality average is used
    gemeente_naam: str | None = None         # populated only for CBS fallback

    @property
    def meest_recent(self) -> WozWaarde | None:
        if not self.waarden:
            return None
        return max(self.waarden, key=lambda w: w.peildatum)

    @property
    def history(self) -> list[dict]:
        return [
            {"jaar": w.year, "waarde": w.vastgesteldeWaarde}
            for w in sorted(self.waarden, key=lambda w: w.peildatum, reverse=True)
        ]


# ---------------------------------------------------------------------------
# Aggregated result for one address
# ---------------------------------------------------------------------------

class AddressData(BaseModel):
    """All fetched data for a single address, ready for feature extraction."""

    query: str                               # original user input
    bag_address: BagLocatieDoc | None = None
    bag_verblijfsobject: BagVerblijfsobject | None = None
    bag_pand: BagPand | None = None
    ep_label: EpOnlineLabel | None = None
    woz: WozResult | None = None

    # Raw responses preserved for exploration / debugging
    raw: dict[str, Any] = Field(default_factory=dict)
