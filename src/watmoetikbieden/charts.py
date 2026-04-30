"""
Terminal charts via plotext.
"""

from __future__ import annotations

import sys

import plotext as plt


def _show() -> None:
    """plt.show() with UTF-8 stdout — needed on Windows (cp1252 default)."""
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    plt.show()

from watmoetikbieden.models import LeefbarometerResult, WozResult, BagLocatieDoc

_W, _H = 80, 20
_NL_MEAN = 4.166


def plot_woz_history(woz: WozResult, straatnaam: str = "", huisnummer: str = "") -> None:
    history = [r for r in woz.history if r["waarde"] is not None]
    if not history:
        print("[WOZ chart] no data")
        return

    # woz.history is sorted descending; reverse for the chart
    history = list(reversed(history))
    years  = [r["jaar"] for r in history]
    values = [r["waarde"] for r in history]

    plt.clf()
    plt.plotsize(_W, _H)
    plt.title(f"WOZ-waardeontwikkeling {straatnaam} {huisnummer}".strip())
    plt.xlabel("Jaar")
    plt.ylabel("WOZ-waarde")

    # Main line for all but the last point
    plt.plot(years[:-1], values[:-1], marker="dot", color="cyan", label="WOZ-waarde")
    # Most recent value with a different marker
    plt.plot([years[-1]], [values[-1]], marker="star", color="yellow+", label=f"Meest recent ({years[-1]})")

    # Y-axis tick labels in €000k format
    mn, mx = min(values), max(values)
    pad = (mx - mn) * 0.1 or 10000
    plt.ylim(mn - pad, mx + pad)
    plt.yfrequency(5)

    _show()
    print()  # blank line after chart


def plot_lbm_history(lbm: LeefbarometerResult) -> None:
    history = [r for r in lbm.lbm_history if r["lbm"] is not None]
    if not history:
        print("[LBM chart] no data")
        return

    # Already sorted ascending by jaar
    years  = [r["jaar"] for r in history]
    scores = [r["lbm"]  for r in history]

    # Use sequential x positions so irregular years display without gaps
    x_pos  = list(range(len(years)))
    x_labels = [str(y) for y in years]

    plt.clf()
    plt.plotsize(_W, _H)
    plt.title(f"Leefbaarheid {lbm.bu_naam} ({lbm.bu_code})")
    plt.xlabel("Meting jaar")
    plt.ylabel("LBM score")

    # National mean reference line across all x positions
    plt.plot(x_pos, [_NL_MEAN] * len(x_pos),
             marker="dot", color="red", style="dotted", label="NL gemiddeld (4.166)")

    # LBM trend
    plt.plot(x_pos, scores, marker="dot", color="green+", label="Leefbaarheid")

    # Replace numeric x ticks with year labels
    plt.xticks(x_pos, x_labels)

    mn = min(min(scores), _NL_MEAN)
    mx = max(max(scores), _NL_MEAN)
    pad = (mx - mn) * 0.15 or 0.05
    plt.ylim(mn - pad, mx + pad)

    plt.show()
    print()


def plot_gem_woz_longitudinal(
    buurt_series: dict[int, float],
    woz: WozResult | None,
    bag_address: BagLocatieDoc | None = None,
    buurtcode: str = "",
) -> None:
    """
    Dual-line chart: buurt gem_woz_waarde (CBS, per year) vs individual
    property WOZ history (Kadaster LVWOZ), plus a comparison table below.
    """
    if not buurt_series:
        print("[WOZ longitudinal chart] no buurt data")
        return

    buurt_years  = sorted(buurt_series)
    buurt_values = [buurt_series[y] for y in buurt_years]

    # Individual property WOZ (from Kadaster LVWOZ)
    prop: dict[int, int] = {}
    if woz:
        for w in woz.waarden:
            y = w.year
            if y is not None and w.vastgesteldeWaarde is not None:
                prop[y] = w.vastgesteldeWaarde

    # ── Chart ──────────────────────────────────────────────────────────────────
    plt.clf()
    plt.plotsize(90, 22)

    label = buurtcode or "buurt"
    if bag_address and bag_address.buurtnaam:
        label = bag_address.buurtnaam

    plt.title(f"WOZ-waarde ontwikkeling — {label}")
    plt.xlabel("Jaar")
    plt.ylabel("EUR")

    plt.plot(
        buurt_years, buurt_values,
        label="Buurtgemiddelde (CBS)",
        marker="dot", color="cyan",
    )

    if prop:
        prop_years  = sorted(y for y in prop if min(buurt_years) <= y <= max(buurt_years))
        prop_values = [prop[y] for y in prop_years]
        if prop_years:
            addr_label = "Dit object"
            if bag_address:
                stn = bag_address.straatnaam or ""
                nr  = str(bag_address.huisnummer) if bag_address.huisnummer else ""
                if stn:
                    addr_label = f"{stn} {nr}".strip()
            plt.plot(
                prop_years, prop_values,
                label=addr_label,
                marker="dot", color="orange",
            )

    plt.xticks(buurt_years, [str(y) for y in buurt_years])

    _show()
    print()

    # ── Comparison table ───────────────────────────────────────────────────────
    if not prop:
        return

    table_years = sorted(y for y in buurt_series if y in prop or y in buurt_series)

    addr_label = "Dit object"
    if bag_address:
        stn = bag_address.straatnaam or ""
        nr  = str(bag_address.huisnummer) if bag_address.huisnummer else ""
        if stn:
            addr_label = f"{stn} {nr}".strip()

    col = [6, 20, 20, 14]
    sep = "  "

    def fmt(v: float | None) -> str:
        return f"EUR {int(round(v)):,}".replace(",", ".") if v is not None else "-"

    def diff(b: float | None, p: int | None) -> str:
        if b is None or p is None:
            return "-"
        d = p - b
        s = "+" if d >= 0 else ""
        return f"{s}EUR {int(round(d)):,}".replace(",", ".")

    line   = "-" * (sum(col) + len(sep) * 3)
    header = (
        f"{'Jaar':<{col[0]}}{sep}"
        f"{'Buurtgemiddelde':>{col[1]}}{sep}"
        f"{addr_label:>{col[2]}}{sep}"
        f"{'Verschil':>{col[3]}}"
    )
    print(line)
    print(header)
    print(line)
    for y in table_years:
        b = buurt_series.get(y)
        p = prop.get(y)
        print(
            f"{y:<{col[0]}}{sep}"
            f"{fmt(b):>{col[1]}}{sep}"
            f"{fmt(p):>{col[2]}}{sep}"
            f"{diff(b, p):>{col[3]}}"
        )
    print(line)

    # Summary: most recent overlap
    overlap = sorted(set(buurt_series) & set(prop))
    if overlap:
        latest = overlap[-1]
        b, p = buurt_series[latest], prop[latest]
        print(f"\n{latest}: {addr_label} = {p / b:.2f}x buurtgemiddelde "
              f"(EUR {p:,} vs EUR {int(b):,})".replace(",", "."))
    print()
