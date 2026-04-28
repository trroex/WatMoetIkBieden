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

from watmoetikbieden.models import LeefbarometerResult, WozResult

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
