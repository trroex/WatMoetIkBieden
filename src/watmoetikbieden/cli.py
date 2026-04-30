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
from watmoetikbieden.charts import plot_woz_history, plot_lbm_history, plot_gem_woz_longitudinal
from watmoetikbieden.sources.cbs_woz_longitudinal import fetch_gem_woz_series

# CBS measure code → Dutch title, loaded lazily
_cbs_titles: dict[str, str] = {}


def _get_cbs_titles() -> dict[str, str]:
    global _cbs_titles
    if _cbs_titles:
        return _cbs_titles
    try:
        import urllib.request, json
        url = "https://datasets.cbs.nl/odata/v1/CBS/85984NED/MeasureCodes?$top=200"
        with urllib.request.urlopen(url, timeout=10) as r:
            _cbs_titles = {m["Identifier"]: m["Title"] for m in json.loads(r.read())["value"]}
    except Exception:
        pass
    return _cbs_titles

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
    elif data.ep_no_label:
        console.print("[yellow]EP-Online: no certificate registered (NO_LABEL – common for pre-2008 homes)[/]")
    else:
        console.print("[yellow]EP-Online: not queried (address lookup failed or missing postcode)[/]")

    # ── BRK perceel ───────────────────────────────────────────────────────────
    if data.brk_perceel:
        _section("BRK Perceel (plot area)", data.brk_perceel.model_dump(exclude_none=True))
    else:
        console.print("[yellow]BRK perceel: not found[/]")

    # ── Leefbaarometer ───────────────────────────────────────────────────────
    if data.leefbaarometer:
        lbm = data.leefbaarometer
        _section("Leefbaarometer 2024", {
            "buurt": f"{lbm.bu_naam} ({lbm.bu_code})",
            "lbm_2024": lbm.scores_2024.get("lbm"),
            "vs_national": f"{lbm.score_vs_national:+.3f}",
            "trend": lbm.lbm_trend,
            "scores_2024": lbm.scores_2024,
            "lbm_history": lbm.lbm_history,
            "lookup_method": lbm.lookup_method,
        })
    else:
        console.print("[yellow]Leefbaarometer: no data (buurtcode missing or file not loaded)[/]")

    # ── CBS Kerncijfers ───────────────────────────────────────────────────────
    if data.cbs_demographics:
        _print_cbs_table(data.cbs_demographics)
    else:
        console.print("[yellow]CBS demographics: no data[/]")

    # ── WOZ ───────────────────────────────────────────────────────────────────
    if data.woz:
        approx_note = " [bold yellow](⚠ municipality average – approximation)[/]" if data.woz.is_approximation else ""
        source_note = f"[dim]Source: {data.woz.source}[/]{approx_note}"
        console.print(source_note)
        _section("WOZ (Kadaster LVWOZ API)", {
            "meest_recent": data.woz.meest_recent.model_dump() if data.woz.meest_recent else None,
            "history": data.woz.history,
            "source": data.woz.source,
            "is_approximation": data.woz.is_approximation,
        })
    else:
        console.print("[yellow]WOZ: no data found (wozwaardeloket returned empty)[/]")

    # ── Charts ────────────────────────────────────────────────────────────────
    console.rule("[bold green]CHARTS[/]")
    a = data.bag_address
    if data.woz and data.woz.history:
        plot_woz_history(
            data.woz,
            straatnaam=a.straatnaam or "" if a else "",
            huisnummer=str(a.huisnummer) if (a and a.huisnummer) else "",
        )
    if data.leefbaarometer and data.leefbaarometer.lbm_history:
        plot_lbm_history(data.leefbaarometer)

    # ── CBS gem_woz_waarde longitudinal chart ─────────────────────────────────
    if a and a.buurtcode:
        wijkcode    = a.wijkcode or None
        gemeentecode = f"GM{a.gemeentecode}" if a.gemeentecode else None
        buurt_series = fetch_gem_woz_series(a.buurtcode, wijkcode, gemeentecode)
        if buurt_series:
            plot_gem_woz_longitudinal(
                buurt_series,
                woz=data.woz,
                bag_address=a,
                buurtcode=a.buurtcode,
            )

    # ── Feature summary for hedonic model ─────────────────────────────────────
    console.rule("[bold green]HEDONIC MODEL FEATURES (extracted)[/]")
    _print_feature_table(data)


def _print_cbs_table(c) -> None:
    titles = _get_cbs_titles()
    t = Table(
        title=f"CBS Kerncijfers Wijken en Buurten 2024 — {c.code} ({c.data_level})",
        show_header=True,
        header_style="bold magenta",
    )
    t.add_column("Code", style="dim", width=16)
    t.add_column("Variabele", width=46)
    t.add_column("Waarde", justify="right")

    for code, val in c.raw_all.items():
        title = titles.get(code, code)
        if val is None:
            display = "–"
        elif isinstance(val, str):
            display = val.strip()
        elif isinstance(val, float) and val == int(val):
            display = f"{int(val):,}"
        else:
            display = str(val)
        t.add_row(code, title, display)

    console.print(t)


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

    def row(var, source, value, status_override: str | None = None):
        if status_override:
            status = status_override
        else:
            status = "[green]OK[/]" if value is not None else "[red]MISSING[/]"
        table.add_row(var, source, str(value) if value is not None else "–", status)

    a = data.bag_address
    v = data.bag_verblijfsobject
    p = data.bag_pand
    e = data.ep_label
    w = data.woz
    b = data.brk_perceel
    mr = w.meest_recent if w else None

    row("log(m²)  oppervlakte",       "BAG WFS verblijfsobject", v.oppervlakte if v else None)
    row("build_year  bouwjaar",       "BAG WFS verblijfsobject", v.bouwjaar if v else None)

    # EP-Online: show NO_LABEL (not a data gap) when no certificate exists
    if e:
        ep_status = None          # will be set to OK by default logic
        ep_value = e.Pand_energieklasse
    elif data.ep_no_label:
        ep_status = "[yellow]NO_LABEL[/]"
        ep_value = None
    else:
        ep_status = "[red]MISSING[/]"
        ep_value = None
    row("energy_label  klasse",       "EP-Online",               ep_value, status_override=ep_status)

    row("property_type  gebruik",     "BAG WFS verblijfsobject", v.gebruiksdoel[0] if (v and v.gebruiksdoel) else None)

    # Plot area: BRK preferred, WOZ grondoppervlakte as fallback
    brk_m2 = b.kadastraleGrootteWaarde if b else None
    woz_m2 = w.grondoppervlakte if w else None
    if brk_m2 is not None:
        plot_m2, plot_src = brk_m2, "BRK kadastralekaart WFS"
    elif woz_m2 is not None:
        plot_m2, plot_src = woz_m2, "Kadaster LVWOZ API"
    else:
        plot_m2, plot_src = None, "BRK kadastralekaart WFS"
    row("log(plot_m²)  perceel",      plot_src,                  f"{plot_m2} m²" if plot_m2 else None)

    row("postcode  fixed effect",     "BAG Locatieserver",       a.postcode if a else None)
    row("coordinates  (lat/lon)",     "BAG Locatieserver",       a.centroide_ll if a else None)
    row("nummeraanduiding_id",        "BAG Locatieserver",       a.nummeraanduiding_id if a else None)
    row("aantal_verblijfsobjecten",   "BAG WFS pand",            p.aantal_verblijfsobjecten if p else None)

    woz_source_label = "Kadaster LVWOZ API" if (w and not w.is_approximation) else "CBS gemeente (approx)"
    row("woz_value (meest recent)",   woz_source_label,          f"€{mr.vastgesteldeWaarde:,}" if (mr and mr.vastgesteldeWaarde) else None)
    row("woz_peildatum",              woz_source_label,          mr.peildatum if mr else None)
    row("woz_history (#jaren)",       woz_source_label,          len(w.waarden) if w else None)

    lbm = data.leefbaarometer
    lbm_src = "Leefbaarometer 2024"
    row("lbm_score_2024",             lbm_src,                   f"{lbm.scores_2024.get('lbm'):.4f}" if lbm else None)
    row("lbm_vs_national",            lbm_src,                   f"{lbm.score_vs_national:+.4f}" if lbm else None)
    row("lbm_trend  (2016->2024)",    lbm_src,                   lbm.lbm_trend if lbm else None)
    row("lbm_safety  (onv)",          lbm_src,                   f"{lbm.scores_2024.get('onv'):.4f}" if lbm else None)
    row("lbm_amenities  (vrz)",       lbm_src,                   f"{lbm.scores_2024.get('vrz'):.4f}" if lbm else None)

    c = data.cbs_demographics
    cbs_src = f"CBS 85984NED ({c.data_level})" if c else "CBS 85984NED"
    row("cbs_inwoners",               cbs_src,                   c.inwoners if c else None)
    row("cbs_huishoudens_gem",        cbs_src,                   c.gem_huishoudensgrootte if c else None)
    row("cbs_eenpersoons_pct",        cbs_src,                   f"{c.eenpersoonshuishoudens_pct}%" if (c and c.eenpersoonshuishoudens_pct) else None)
    row("cbs_koopwoningen_pct",       cbs_src,                   f"{c.koopwoningen_pct}%" if (c and c.koopwoningen_pct) else None)
    row("cbs_inkomen_gem",            cbs_src,                   f"EUR{c.gem_inkomen_inwoner}k" if (c and c.gem_inkomen_inwoner) else None)
    row("cbs_stedelijkheid",          cbs_src,                   c.stedelijkheid if c else None)
    row("cbs_data_level",             cbs_src,                   c.data_level if c else None)

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
