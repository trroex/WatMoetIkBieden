"""
CLI entry point: fetch-address <address>

Pretty-prints raw API responses and extracted model fields side by side
using the rich library, so we can easily identify which fields are
available for the hedonic pricing model.
"""

from __future__ import annotations

import asyncio
import sys

from rich.console import Console
from rich.panel import Panel
from rich.pretty import Pretty
from rich.table import Table
from rich import print as rprint

from watmoetikbieden.fetcher import fetch_all
from watmoetikbieden.models import AddressData

console = Console()


def _section(title: str, obj) -> None:
    console.print(Panel(Pretty(obj, expand_all=True), title=f"[bold cyan]{title}[/]"))


def print_results(data: AddressData) -> None:
    console.rule("[bold green]RAW API RESPONSES[/]")

    for source_name, raw in data.raw.items():
        _section(f"RAW · {source_name}", raw)

    console.rule("[bold green]PARSED MODELS[/]")

    # ── BAG address ──────────────────────────────────────────────────────────
    if data.bag_address:
        _section("BAG Address (locatieserver)", data.bag_address.model_dump(exclude_none=True))
    else:
        console.print("[red]BAG address: no match found[/]")

    # ── BAG verblijfsobject ───────────────────────────────────────────────────
    if data.bag_verblijfsobject:
        _section("BAG Verblijfsobject (unit)", data.bag_verblijfsobject.model_dump(exclude_none=True))
    else:
        console.print("[yellow]BAG verblijfsobject: not found[/]")

    # ── BAG pand ─────────────────────────────────────────────────────────────
    if data.bag_pand:
        _section("BAG Pand (building)", data.bag_pand.model_dump(exclude_none=True))
    else:
        console.print("[yellow]BAG pand: not found[/]")

    # ── Energy label ──────────────────────────────────────────────────────────
    if data.ep_label:
        _section("EP-Online (energy label)", data.ep_label.model_dump(exclude_none=True))
    else:
        console.print("[yellow]EP-Online: no energy label registered[/]")

    # ── WOZ ───────────────────────────────────────────────────────────────────
    if data.woz:
        approx_note = " [bold yellow](⚠ municipality average – approximation)[/]" if data.woz.is_approximation else ""
        source_note = f"[dim]Source: {data.woz.source}[/]{approx_note}"
        console.print(source_note)
        _section("WOZ (waardeloket.nl)", {
            "meest_recent": data.woz.meest_recent.model_dump() if data.woz.meest_recent else None,
            "history": data.woz.history,
            "source": data.woz.source,
            "is_approximation": data.woz.is_approximation,
        })
    else:
        console.print("[yellow]WOZ: no data found (wozwaardeloket + CBS both returned empty)[/]")

    # ── Feature summary for hedonic model ─────────────────────────────────────
    console.rule("[bold green]HEDONIC MODEL FEATURES (extracted)[/]")
    _print_feature_table(data)


def _print_feature_table(data: AddressData) -> None:
    """
    Map the fetched data to the hedonic model variables and show which
    are populated vs. missing for this address.
    """
    table = Table(title="Feature readiness", show_header=True, header_style="bold magenta")
    table.add_column("Model variable", style="cyan")
    table.add_column("Source")
    table.add_column("Value")
    table.add_column("Status")

    def row(var, source, value):
        status = "[green]OK[/]" if value is not None else "[red]MISSING[/]"
        table.add_row(var, source, str(value) if value is not None else "–", status)

    a = data.bag_address
    v = data.bag_verblijfsobject
    p = data.bag_pand
    e = data.ep_label
    w = data.woz
    mr = w.meest_recent if w else None

    row("log(m²)  oppervlakte",       "BAG WFS verblijfsobject", v.oppervlakte if v else None)
    row("build_year  bouwjaar",       "BAG WFS verblijfsobject", v.bouwjaar if v else None)
    row("energy_label  klasse",       "EP-Online",               e.Pand_energieklasse if e else None)
    row("property_type  gebruik",     "BAG WFS verblijfsobject", v.gebruiksdoel[0] if (v and v.gebruiksdoel) else None)
    row("log(plot_m²)  – plot",       "– (not in BAG; need BRK/Kadaster)", None)
    row("postcode  fixed effect",     "BAG Locatieserver",       a.postcode if a else None)
    row("coordinates  (lat/lon)",     "BAG Locatieserver",       a.centroide_ll if a else None)
    row("nummeraanduiding_id",        "BAG Locatieserver",       a.nummeraanduiding_id if a else None)
    row("aantal_verblijfsobjecten",   "BAG WFS pand",            p.aantal_verblijfsobjecten if p else None)

    woz_source_label = f"wozwaardeloket" if (w and not w.is_approximation) else "CBS gemeente (approx)"
    row("woz_value (meest recent)",   woz_source_label,          f"€{mr.vastgesteldeWaarde:,}" if (mr and mr.vastgesteldeWaarde) else None)
    row("woz_peildatum",              woz_source_label,          mr.peildatum if mr else None)
    row("woz_history (#jaren)",       woz_source_label,          len(w.waarden) if w else None)

    console.print(table)


def main() -> None:
    if len(sys.argv) < 2:
        console.print("[bold red]Usage:[/] fetch-address <address>")
        console.print('Example: fetch-address "Keizersgracht 123, 1015 CJ Amsterdam"')
        sys.exit(1)

    address = " ".join(sys.argv[1:])
    console.print(f"\n[bold]Fetching data for:[/] {address}\n")

    try:
        data = asyncio.run(fetch_all(address))
    except Exception as exc:
        console.print(f"[red]Error:[/] {exc}")
        sys.exit(1)

    print_results(data)
