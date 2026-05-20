"""
WatMoetIkBieden – Streamlit web interface.

Run with:
    .venv/Scripts/streamlit run app.py
"""

from __future__ import annotations

import asyncio

import pandas as pd
import streamlit as st

# ── page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="WatMoetIkBieden",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── constants ─────────────────────────────────────────────────────────────────
HOUSE_TYPES = [
    "Tussenwoning",
    "Hoekwoning",
    "Twee-onder-één-kap",
    "Vrijstaande woning",
    "Appartement / flat",
    "Bovenwoning",
    "Benedenwoning",
    "Penthouse",
    "Woonboerderij",
    "Anders / onbekend",
]

ENERGY_LABELS = ["A++++", "A+++", "A++", "A+", "A", "B", "C", "D", "E", "F", "G", "Onbekend"]

HOUSE_CONDITIONS = [
    "Instapklaar – niks aan te doen",
    "Goed onderhouden – kleine opknapbeurten",
    "Bewoonbaar maar verouderd – modernisering gewenst",
    "Beperkt bewoonbaar – grote renovatie nodig",
    "Onbewoonbaar – volledig renovatieproject",
]

RENOVATION_NEEDS = [
    "Badkamer vernieuwen",
    "Keuken vernieuwen",
    "Dak vernieuwen / isoleren",
    "Vloerisolatie",
    "Gevelisolatie",
    "Kozijnen / ramen vervangen",
    "Elektra vernieuwen",
    "Loodgieterswerk vernieuwen",
    "Vloeren vervangen",
    "Schilderwerk buiten",
    "Schilderwerk binnen",
    "Asbest sanering",
    "Fundering herstel",
    "CV-installatie vervangen",
    "Warmtepomp installeren",
    "Zonnepanelen",
]


# ── helpers ───────────────────────────────────────────────────────────────────

def _fmt_eur(value: int | float | None, unit: str = "") -> str:
    if value is None:
        return "–"
    return f"€ {int(value):,}{unit}".replace(",", ".")


def _pct(value: float | None) -> str:
    return f"{value:.0f}%" if value is not None else "–"


# ── fetch ─────────────────────────────────────────────────────────────────────

def run_fetch(address: str):
    from watmoetikbieden.fetcher import fetch_all
    return asyncio.run(fetch_all(address))


# ── result display ────────────────────────────────────────────────────────────

def show_results(data, user_input: dict) -> None:
    from watmoetikbieden.models import AddressData
    from watmoetikbieden.woz_estimator import estimate as woz_estimate
    assert isinstance(data, AddressData)

    a = data.bag_address
    v = data.bag_verblijfsobject
    p = data.bag_pand
    e = data.ep_label
    w = data.woz
    b = data.brk_perceel
    lbm = data.leefbaarometer
    cbs = data.cbs_demographics
    mr = w.meest_recent if w else None

    # Perceel size: prefer BRK cadastral area, fall back to WOZ grondoppervlakte
    perceel_m2: int | None = None
    if b and b.kadastraleGrootteWaarde:
        perceel_m2 = b.kadastraleGrootteWaarde
    elif w and w.grondoppervlakte:
        perceel_m2 = w.grondoppervlakte

    # Compute WOZ estimate (needs CBS data + user house type + perceel size + province)
    province = a.provincienaam if a else None
    woz_est = woz_estimate(
        cbs,
        user_input.get("house_type", "Anders / onbekend"),
        perceel_m2,
        province=province,
    ) if cbs else None

    st.divider()

    if not a:
        st.error("Adres niet gevonden in de BAG. Controleer de invoer.")
        return

    # ── address header ────────────────────────────────────────────────────────
    real_woz = mr.vastgesteldeWaarde if mr else None

    # ── data fetching block ───────────────────────────────────────────────────
    # WOZ history
    woz_pts: list[tuple[int, int]] = []
    if w and w.history:
        woz_pts = sorted((h["jaar"], h["waarde"]) for h in w.history if h["waarde"])

    # Buurt average WOZ series
    buurt_series: dict | None = None
    gemeente_woz_series: dict | None = None
    if a and a.buurtcode:
        with st.spinner("CBS gem. WOZ buurt & gemeente ophalen…"):
            from watmoetikbieden.sources.cbs_woz_longitudinal import fetch_gem_woz_series
            wijkcode     = a.wijkcode or None
            gemeentecode = f"GM{a.gemeentecode}" if a.gemeentecode else None
            buurt_series = fetch_gem_woz_series(a.buurtcode, wijkcode, gemeentecode)
            if gemeentecode:
                gemeente_woz_series = fetch_gem_woz_series(gemeentecode)

    # Lending capacity series
    from watmoetikbieden.market_indicators import lending_capacity_series, WOONQUOTE, TERM_YEARS
    lc_rows = lending_capacity_series()

    # DNB mortgage rates
    mortgage_rates: object = None
    try:
        from watmoetikbieden.sources.dnb_mortgage_rates import fetch_mortgage_rates
        mortgage_rates = fetch_mortgage_rates()
    except Exception:
        mortgage_rates = None

    _has_rate = mortgage_rates is not None and bool(
        getattr(mortgage_rates, "points", None)
    )

    # COROP market data
    pbk = None
    bouw = None
    permits = None
    migration = None
    if a and a.gemeentecode:
        with st.spinner("Prijsindex COROP ophalen (CBS)…"):
            try:
                import plotly.graph_objects as go
                from watmoetikbieden.sources.pbk_corop import fetch_pbk_corop
                pbk = fetch_pbk_corop(a.gemeentecode)
            except Exception as exc:
                pbk = None
                st.warning(f"Marktdynamiek niet beschikbaar: {exc}")

        if pbk and pbk.latest:
            pbk24 = fetch_pbk_corop(a.gemeentecode, n_quarters=32)
            try:
                from watmoetikbieden.sources.building_stats import fetch_building_stats
                bouw = fetch_building_stats(a.gemeentecode, pbk24)
            except Exception:
                bouw = None

            try:
                from watmoetikbieden.sources.building_permits import fetch_building_permits
                permits = fetch_building_permits(a.gemeentecode)
            except Exception:
                permits = None

            try:
                from watmoetikbieden.sources.migration import fetch_migration
                migration = fetch_migration(a.gemeentecode)
            except Exception:
                migration = None

    _gm_label    = (a.gemeentenaam or a.gemeentecode or (bouw.gemeente_code if bouw else "")) if a else ""
    _corop_label = (bouw.corop_name.replace(" (CR)", "") if bouw else (pbk.corop_name.replace(" (CR)", "") if pbk else ""))

    # Schools (fetch eagerly — Streamlit renders all tabs on first load)
    schools: list = []
    coords = None
    try:
        from watmoetikbieden.sources.nearby_schools import (
            fetch_nearby_schools, enrich_with_duo_ratings,
            parse_bag_centroide, DEFAULT_RADIUS_M,
        )
        coords = parse_bag_centroide(a.centroide_ll) if a else None
        if coords:
            lat, lon = coords
            with st.spinner("Scholen ophalen via OpenStreetMap…"):
                schools = fetch_nearby_schools(lat, lon, DEFAULT_RADIUS_M)
            if schools:
                gemeente_naam = a.gemeentenaam or ""
                with st.spinner("DUO schooloordelen koppelen…"):
                    schools = enrich_with_duo_ratings(schools, gemeente_naam)
    except Exception:
        schools = []

    # National monthly transactions (CBS 85773NED monthly rows)
    nat_tx = None
    try:
        from watmoetikbieden.sources.cbs_national_transactions import fetch_national_transactions
        nat_tx = fetch_national_transactions()
    except Exception:
        nat_tx = None

    # Bid range
    from watmoetikbieden.bid_range import estimate_bid_range, RENOVATION_COSTS
    bid_result = None
    if woz_est and woz_est.estimated_value:
        try:
            bid_result = estimate_bid_range(
                woz_estimate        = woz_est.estimated_value,
                corop_price_yoy_pct = pbk.latest.yoy_pct if (pbk and pbk.latest) else None,
                national_price_yoy_pct = nat_tx.trailing_avg("price_yoy_pct") if nat_tx else None,
                heat_label          = pbk.heat_label if pbk else None,
                corop_sales_yoy_pct = pbk.latest.sales_yoy_pct if (pbk and pbk.latest) else None,
                national_tx_yoy_pct = nat_tx.trailing_avg("tx_yoy_pct") if nat_tx else None,
                supply_pressure     = bouw.latest.supply_pressure if (bouw and bouw.latest) else None,
                permits_yoy_pct     = float(permits.yoy_trailing_12m()) if (permits and permits.yoy_trailing_12m() is not None) else None,
                migration_net_12m   = migration.trailing_12m_net() if migration else None,
                inwoners            = cbs.inwoners if cbs else None,
                energy_label        = e.Pand_energieklasse if e else None,
                condition           = user_input.get("condition"),
                renovation_items    = user_input.get("renovation", []),
                days_for_sale       = user_input.get("days_for_sale") if user_input.get("te_koop") else None,
            )
        except Exception:
            bid_result = None

    # ── hero block ────────────────────────────────────────────────────────────
    st.subheader(f"📍 {a.weergavenaam}")

    col_bid, col_prop, col_mkt = st.columns([2, 1.5, 1.5])

    with col_bid:
        if bid_result:
            st.markdown(
                f"### € {bid_result.bid_low:,} – € {bid_result.bid_high:,}".replace(",", ".")
            )
            st.caption(
                f"Geschatte marktwaarde · midpunt € {bid_result.bid_midpoint:,}".replace(",", ".")
                + f" · betrouwbaarheid: {bid_result.confidence_icon} {bid_result.confidence}"
            )
        else:
            st.markdown("### Marktwaarde")
            st.caption("Onvoldoende data voor schatting")

    with col_prop:
        st.metric("WOZ (Kadaster)", _fmt_eur(real_woz), help=f"Peildatum: {mr.peildatum}" if mr else None)
        st.metric(
            "Oppervlak / bouwjaar",
            f"{v.oppervlakte} m² · {v.bouwjaar}" if (v and v.oppervlakte and v.bouwjaar) else "–",
        )

    with col_mkt:
        if pbk and pbk.latest:
            st.metric(
                "Markt",
                pbk.heat_label,
                delta=f"{pbk.latest.yoy_pct:+.1f}% YoY" if pbk.latest.yoy_pct is not None else None,
            )
        if lbm:
            st.metric(
                "Leefbaarheid buurt",
                f"{lbm.scores_2024.get('lbm', 0):.2f}",
                delta=f"{lbm.score_vs_national:+.3f} t.o.v. NL",
            )

    # ── tabs ──────────────────────────────────────────────────────────────────
    tab_woning, tab_buurt, tab_markt, tab_prijs, tab_data = st.tabs([
        "🏠 Woning", "🏘️ Buurt & omgeving", "📈 Markt", "💡 Prijsopbouw", "🔍 Databronnen"
    ])

    # ── tab 1: woning ─────────────────────────────────────────────────────────
    with tab_woning:
        st.markdown("##### Kerncijfers")
        c1, c2, c3, c4, c5, c6 = st.columns(6)

        c1.metric(
            "WOZ-waarde (Kadaster)",
            _fmt_eur(real_woz),
            help=f"Peildatum: {mr.peildatum}" if mr else "Niet beschikbaar",
        )

        if woz_est and woz_est.estimated_value:
            delta_str = None
            delta_col = None
            if real_woz:
                d = woz_est.validation_delta_pct(real_woz)
                if d is not None:
                    delta_str = f"{d:+.1f}% t.o.v. Kadaster"
                    delta_col = "normal" if abs(d) < 10 else "off"
            confidence_icon = {"high": "🟢", "medium": "🟡", "low": "🔴"}.get(woz_est.confidence, "⚪")
            c2.metric(
                f"Geschatte WOZ ({user_input.get('house_type','?')})",
                _fmt_eur(woz_est.estimated_value),
                delta=delta_str,
                delta_color=delta_col or "normal",
                help=(
                    f"Methode: {woz_est.method} {confidence_icon}\n"
                    f"Prijsrelatieven: {woz_est.relatives_source}\n"
                    f"Type relatief: {woz_est.type_relative:.3f}\n"
                    f"Buurtdekking: {woz_est.coverage_pct:.0f}%\n"
                    f"Buurt gem. WOZ: {_fmt_eur(woz_est.source_gem_woz_eur)}"
                ),
            )
        else:
            c2.metric("Geschatte WOZ", "–", help="CBS buurtdata of woningtype ontbreekt")

        c3.metric(
            "Vloeroppervlak",
            f"{v.oppervlakte} m²" if (v and v.oppervlakte) else "–",
        )
        c4.metric(
            "Perceeloppervlak",
            f"{perceel_m2} m²" if perceel_m2 else "–",
            help="Bron: BRK kadastrale grootte" if (b and b.kadastraleGrootteWaarde) else "Bron: WOZ grondoppervlakte",
        )
        c5.metric(
            "Bouwjaar",
            str(v.bouwjaar) if (v and v.bouwjaar) else (str(p.bouwjaar) if (p and p.bouwjaar) else "–"),
        )
        c6.metric(
            "Energielabel (EP-Online)",
            e.Pand_energieklasse if e else ("Geen cert." if data.ep_no_label else "–"),
        )

        # User inputs recap
        if user_input.get("te_koop"):
            st.markdown("##### Verkoopinformatie (gebruikersinvoer)")
            uc1, uc2, uc3 = st.columns(3)
            uc1.metric("Vermeld energielabel", user_input.get("listed_label", "–"))
            uc2.metric("Dagen te koop", user_input.get("days_for_sale", "–"))
            uc3.metric("Staat", user_input.get("condition", "–"))
            if user_input.get("renovation"):
                st.markdown("**Renovatiebehoeften:** " + " · ".join(user_input["renovation"]))

        # WOZ history chart
        import plotly.graph_objects as go

        if w and w.is_approximation:
            st.warning("⚠️ Gemeente-gemiddelde gebruikt als benadering (geen individuele waarde beschikbaar)")

        if woz_pts or buurt_series or gemeente_woz_series or lc_rows or _has_rate:
            st.markdown("##### WOZ-waarde & maximale hypotheek bij modaal inkomen")

            from plotly.subplots import make_subplots

            if _has_rate:
                fig_woz = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    row_heights=[0.65, 0.35],
                    vertical_spacing=0.06,
                )
            else:
                fig_woz = make_subplots(rows=1, cols=1)

            if woz_pts:
                years_woz, vals_woz = zip(*woz_pts)
                fig_woz.add_trace(go.Scatter(
                    x=years_woz, y=vals_woz,
                    mode="lines+markers",
                    name="WOZ dit adres",
                    line=dict(color="#1f77b4", width=2),
                    marker=dict(size=5),
                    hovertemplate="%{x}: € %{y:,.0f}<extra>WOZ dit adres</extra>",
                ), row=1, col=1)

            if buurt_series:
                buurt_pts = sorted(buurt_series.items())
                years_b, vals_b = zip(*buurt_pts)
                fig_woz.add_trace(go.Scatter(
                    x=years_b, y=vals_b,
                    mode="lines+markers",
                    name="Gem. WOZ buurt",
                    line=dict(color="#ff7f0e", width=2, dash="dot"),
                    marker=dict(size=4),
                    hovertemplate="%{x}: € %{y:,.0f}<extra>Gem. WOZ buurt</extra>",
                ), row=1, col=1)

            if gemeente_woz_series:
                gm_pts = sorted(gemeente_woz_series.items())
                years_gm, vals_gm = zip(*gm_pts)
                fig_woz.add_trace(go.Scatter(
                    x=years_gm, y=vals_gm,
                    mode="lines+markers",
                    name="Gem. WOZ gemeente",
                    line=dict(color="#9467bd", width=1.5, dash="dash"),
                    marker=dict(size=4),
                    hovertemplate="%{x}: € %{y:,.0f}<extra>Gem. WOZ gemeente</extra>",
                ), row=1, col=1)

            if lc_rows:
                df_lc = pd.DataFrame(lc_rows)
                fig_woz.add_trace(go.Scatter(
                    x=df_lc["year"], y=df_lc["max_mortgage"],
                    mode="lines+markers",
                    name="Max. hypotheek (modaal)",
                    line=dict(color="#2ca02c", width=2, dash="dash"),
                    marker=dict(size=4),
                    hovertemplate="%{x}: € %{y:,.0f}<extra>Max. hypotheek modaal</extra>",
                ), row=1, col=1)

            if _has_rate:
                def _period_to_decimal(p: str) -> float:
                    yr, mo = int(p[:4]), int(p[5:7])
                    return yr + (mo - 1) / 12

                _rate_x      = [_period_to_decimal(pt.period) for pt in mortgage_rates.points]
                _rate_labels = [pt.period for pt in mortgage_rates.points]
                _rate_vals   = [pt.rate_pct for pt in mortgage_rates.points]
                fig_woz.add_trace(go.Scatter(
                    x=_rate_x, y=_rate_vals,
                    customdata=_rate_labels,
                    mode="lines",
                    name="Hypotheekrente NL",
                    line=dict(color="#d62728", width=1.5),
                    hovertemplate="%{customdata}: %{y:.2f}%<extra>Hypotheekrente</extra>",
                    showlegend=True,
                ), row=2, col=1)

            _woz_x_start: int | str | None = None
            if buurt_series:
                _woz_x_start = min(buurt_series.keys())
            elif woz_pts:
                _woz_x_start = min(y for y, _ in woz_pts)
            elif gemeente_woz_series:
                _woz_x_start = min(gemeente_woz_series.keys())

            fig_woz.update_yaxes(
                title_text="Waarde (€)", tickformat=",.0f", autorange=True,
                row=1, col=1,
            )
            if _has_rate:
                fig_woz.update_yaxes(
                    title_text="Rente (%)", ticksuffix="%", rangemode="tozero",
                    row=2, col=1,
                )
                fig_woz.update_xaxes(title_text="", row=1, col=1)
                _x_end = (
                    _period_to_decimal(mortgage_rates.latest.period) + 1 / 12
                    if mortgage_rates and mortgage_rates.latest else None
                )
                _xrange = (
                    [_woz_x_start - 0.5, _x_end]
                    if (_woz_x_start is not None and _x_end is not None) else None
                )
                fig_woz.update_xaxes(
                    title_text="",
                    tickformat="d",
                    range=_xrange,
                    row=2, col=1,
                )
            else:
                fig_woz.update_xaxes(title_text="Jaar", tickformat="d", row=1, col=1)

            fig_woz.update_layout(
                height=480 if _has_rate else 360,
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                hovermode="x unified",
            )
            st.plotly_chart(fig_woz, use_container_width=True, key="woz_history")

            captions_woz = []
            if lc_rows:
                captions_woz.append(
                    f"Max. hypotheek: annuïteit {TERM_YEARS} jr, woonquote {WOONQUOTE*100:.0f}% bruto, "
                    "ECB/DNB-rente jaargemiddelde (AAR). Inkomen: CBS 83931NED mediaan bruto (≥2011); hardcoded vóór 2011."
                )
            if _has_rate:
                captions_woz.append(
                    f"Hypotheekrente: maandelijks AAR nieuwe woninghypotheken NL "
                    f"(ECB MIR · DNB, 2003–heden). Meest recent: {mortgage_rates.latest.period} "
                    f"→ {mortgage_rates.latest.rate_pct:.2f}%"
                )
            for cap in captions_woz:
                st.caption(cap)

        # Model features readiness table (collapsed)
        with st.expander("📋 Beschikbare data"):
            rows_feat = []

            def feat(var, source, value, status=""):
                ok = value not in (None, "–", "")
                rows_feat.append({
                    "Feature": var,
                    "Bron": source,
                    "Waarde": str(value) if ok else "–",
                    "Status": "✅" if ok else "❌",
                })

            feat("log(m²) oppervlak", "BAG WFS", f"{v.oppervlakte} m²" if (v and v.oppervlakte) else None)
            feat("bouwjaar", "BAG WFS", v.bouwjaar if v else (p.bouwjaar if p else None))
            feat("energielabel", "EP-Online", e.Pand_energieklasse if e else None)
            feat("gebruiksdoel", "BAG WFS", v.gebruiksdoel[0] if (v and v.gebruiksdoel) else None)
            feat("log(perceel m²)", "BRK / WOZ", b.kadastraleGrootteWaarde if b else (w.grondoppervlakte if w else None))
            feat("postcode", "BAG Locatieserver", a.postcode)
            feat("WOZ-waarde", "Kadaster LVWOZ", mr.vastgesteldeWaarde if mr else None)
            feat("lbm_score_2024", "Leefbaarometer", lbm.scores_2024.get("lbm") if lbm else None)
            feat("cbs_inwoners", "CBS 85984NED", cbs.inwoners if cbs else None)
            feat("cbs_woningtype buurt", "CBS 85984NED", f"{cbs.pct_tussenwoning}% tussenwoning" if (cbs and cbs.pct_tussenwoning is not None) else None)

            df_feat = pd.DataFrame(rows_feat)
            st.dataframe(df_feat, use_container_width=True, hide_index=True)

    # ── tab 2: buurt & omgeving ───────────────────────────────────────────────
    with tab_buurt:
        col_lbm, col_cbs = st.columns(2)

        with col_lbm:
            st.markdown("##### Leefbaarometer 2024")
            if lbm:
                s = lbm.scores_2024
                lc1, lc2 = st.columns(2)
                lc1.metric("Buurt", f"{lbm.bu_naam}")
                lc2.metric("LBM-score 2024", f"{s.get('lbm', 0):.3f}", delta=f"{lbm.score_vs_national:+.3f} t.o.v. nationaal")

                score_rows = [
                    ("Totaal (lbm)", s.get("lbm")),
                    ("Fysiek (fys)", s.get("fys")),
                    ("Onveiligheid (onv)", s.get("onv")),
                    ("Sociaal (soc)", s.get("soc")),
                    ("Voorzieningen (vrz)", s.get("vrz")),
                    ("Woningen (won)", s.get("won")),
                ]
                df_lbm = pd.DataFrame(score_rows, columns=["Dimensie", "Score"])
                st.dataframe(df_lbm, use_container_width=True, hide_index=True)

                if lbm.lbm_history:
                    import plotly.graph_objects as go
                    from watmoetikbieden.fetcher import get_lbm_lookup

                    _DIM_LABELS = {
                        "lbm": "Totaal leefbaarheid",
                        "fys": "Fysieke omgeving",
                        "onv": "Onveiligheid",
                        "soc": "Sociaal",
                        "vrz": "Voorzieningen",
                        "won": "Woningen",
                    }
                    national_means = lbm.national_means_2024

                    _gm_history: list[dict] = []
                    if a and a.gemeentecode:
                        try:
                            _gm_history = get_lbm_lookup().gemeente_history(a.gemeentecode)
                        except Exception:
                            _gm_history = []
                    _gm_hist_map: dict[str, dict[int, float]] = {}
                    for row in _gm_history:
                        for dim in _DIM_LABELS:
                            if row.get(dim) is not None:
                                _gm_hist_map.setdefault(dim, {})[int(row["jaar"])] = row[dim]

                    _gm_label_lbm = a.gemeentenaam or a.gemeentecode if a else "Gemeente"

                    st.markdown("**Historiek per dimensie**")
                    dims = list(_DIM_LABELS.keys())
                    for row_start in range(0, len(dims), 2):
                        c_left, c_right = st.columns(2)
                        for col_idx, dim in enumerate(dims[row_start : row_start + 2]):
                            col_widget = c_left if col_idx == 0 else c_right
                            with col_widget:
                                pts = [
                                    (int(r["jaar"]), r[dim])
                                    for r in lbm.lbm_history
                                    if r.get(dim) is not None
                                ]
                                if not pts:
                                    st.caption(f"_{_DIM_LABELS[dim]}: geen data_")
                                    continue
                                pts.sort()
                                years  = [p[0] for p in pts]
                                values = [p[1] for p in pts]

                                fig = go.Figure()
                                fig.add_trace(go.Scatter(
                                    x=years, y=values,
                                    mode="lines+markers",
                                    name="Buurt",
                                    line=dict(color="#1f77b4", width=2),
                                    marker=dict(size=5),
                                    hovertemplate="%{x}: %{y:.3f}<extra>Buurt</extra>",
                                ))

                                gm_dim = _gm_hist_map.get(dim, {})
                                if gm_dim:
                                    gm_yrs = sorted(gm_dim.keys())
                                    gm_vals = [gm_dim[y] for y in gm_yrs]
                                    fig.add_trace(go.Scatter(
                                        x=gm_yrs, y=gm_vals,
                                        mode="lines",
                                        name=f"Gem. {_gm_label_lbm}",
                                        line=dict(color="#ff7f0e", width=1.5, dash="dot"),
                                        hovertemplate="%{x}: %{y:.3f}<extra>Gemeente gem.</extra>",
                                    ))

                                nat = national_means.get(dim)
                                if nat is not None:
                                    fig.add_hline(
                                        y=nat,
                                        line_dash="dash",
                                        line_color="#aaaaaa",
                                        annotation_text=f"NL {nat:.3f}",
                                        annotation_position="bottom right",
                                        annotation_font_size=10,
                                    )

                                fig.update_layout(
                                    title=dict(
                                        text=_DIM_LABELS[dim],
                                        font=dict(size=13),
                                    ),
                                    height=220,
                                    margin=dict(l=10, r=10, t=35, b=10),
                                    showlegend=bool(gm_dim),
                                    legend=dict(
                                        orientation="h", yanchor="bottom",
                                        y=1.02, xanchor="left", x=0,
                                        font=dict(size=10),
                                    ),
                                    xaxis=dict(tickformat="d"),
                                    yaxis=dict(autorange=True),
                                )
                                st.plotly_chart(fig, use_container_width=True, key=f"lbm_{dim}")
            else:
                st.info("Geen Leefbaarometer-data beschikbaar.")

        with col_cbs:
            st.markdown("##### CBS buurtkenmerken 2024")
            if cbs:
                st.caption(f"Dataniveau: {cbs.data_level} ({cbs.code})")
                cbs_rows = [
                    ("Inwoners", f"{cbs.inwoners:,}" if cbs.inwoners else "–"),
                    ("Bevolkingsdichtheid", f"{cbs.bevolkingsdichtheid:,.0f} / km²" if cbs.bevolkingsdichtheid else "–"),
                    ("Huishoudens", f"{cbs.huishoudens_totaal:,}" if cbs.huishoudens_totaal else "–"),
                    ("Eenpersoons %", _pct(cbs.eenpersoonshuishoudens_pct)),
                    ("Gem. huishoudensgrootte", str(cbs.gem_huishoudensgrootte) if cbs.gem_huishoudensgrootte else "–"),
                    ("Koopwoningen %", _pct(cbs.koopwoningen_pct)),
                    ("Huurwoningen %", _pct(cbs.huurwoningen_pct)),
                    ("Gem. inkomen (€1k)", str(cbs.gem_inkomen_inwoner) if cbs.gem_inkomen_inwoner else "–"),
                    ("Stedelijkheid (1–5)", str(cbs.stedelijkheid) if cbs.stedelijkheid else "–"),
                    ("Gem. WOZ buurt (€1k)", str(cbs.gem_woz_waarde) if cbs.gem_woz_waarde else "–"),
                ]
                df_cbs = pd.DataFrame(cbs_rows, columns=["Kenmerk", "Waarde"])
                st.dataframe(df_cbs, use_container_width=True, hide_index=True)

                wt_data = {
                    "Tussenwoning": cbs.pct_tussenwoning,
                    "Hoekwoning": cbs.pct_hoekwoning,
                    "Twee-onder-één-kap": cbs.pct_twee_onder_een_kap,
                    "Vrijstaand": cbs.pct_vrijstaand,
                    "Meergezins": cbs.pct_meergezinswoning,
                }
                wt_data = {k: v for k, v in wt_data.items() if v is not None}
                if wt_data:
                    st.markdown("**Woningtypeverdeling buurt**")
                    df_wt = pd.DataFrame(
                        wt_data.items(), columns=["Type", "Percentage"]
                    )
                    st.bar_chart(df_wt.set_index("Type"), y="Percentage", use_container_width=True)
            else:
                st.info("Geen CBS-data beschikbaar.")

        # Schools section (full width below columns)
        st.markdown("---")
        st.markdown(f"##### Scholen & kinderopvang binnen {DEFAULT_RADIUS_M // 1000} km")
        st.caption(f"Bron: OpenStreetMap / Overpass API · DUO oordelen t/m 2018 · straal {DEFAULT_RADIUS_M:,} m · looptijd op basis van 5 km/u")

        if not coords:
            st.info("Geen coördinaten beschikbaar voor dit adres — kan omgeving niet ophalen.")
        elif schools:
            CATEGORY_ORDER = [
                "Universiteit / Hogeschool",
                "MBO / Vakschool",
                "Middelbare school",
                "Basisschool",
                "Kindcentrum / IKC",
                "Kinderopvang / peuterspeelzaal",
                "Speciaal onderwijs",
                "School (onbekend type)",
                "Onderwijsinstelling",
            ]
            DUO_CATEGORIES = {"Basisschool", "Kindcentrum / IKC", "Middelbare school", "School (onbekend type)"}

            from collections import defaultdict
            by_cat: dict[str, list] = defaultdict(list)
            for s in schools:
                by_cat[s.category].append(s)

            present_ordered = [c for c in CATEGORY_ORDER if c in by_cat]
            present_ordered += [c for c in by_cat if c not in CATEGORY_ORDER]

            COL_CFG = {
                "Naam":             st.column_config.TextColumn("Naam",             width=200),
                "Afstand":          st.column_config.TextColumn("Afstand",          width=80),
                "Looptijd":         st.column_config.TextColumn("Looptijd",         width=80),
                "Oordeel (t/m 2018)": st.column_config.TextColumn("Oordeel (t/m 2018)", width=160),
                "Denominatie":      st.column_config.TextColumn("Denominatie",      width=160),
                "Beheerder":        st.column_config.TextColumn("Beheerder",        width=160),
                "Website":          st.column_config.TextColumn("Website",          width=160),
            }
            ALL_COLS = list(COL_CFG.keys())

            for cat in present_ordered:
                items = by_cat[cat]
                st.markdown(f"**{cat}** ({len(items)})")
                show_duo = cat in DUO_CATEGORIES
                rows_sch = []
                for s in items:
                    rows_sch.append({
                        "Naam":               s.name,
                        "Afstand":            f"{s.distance_m} m",
                        "Looptijd":           f"~{s.walk_min} min",
                        "Oordeel (t/m 2018)": (s.rating_label or "–") if show_duo else "–",
                        "Denominatie":        (s.denominatie or "–") if show_duo else "–",
                        "Beheerder":          s.operator or "–",
                        "Website":            s.website or "–",
                    })
                st.dataframe(
                    pd.DataFrame(rows_sch, columns=ALL_COLS),
                    column_config=COL_CFG,
                    use_container_width=True,
                    hide_index=True,
                )
        else:
            st.info(f"Geen scholen gevonden binnen {DEFAULT_RADIUS_M:,} m.")

    # ── tab 3: markt ──────────────────────────────────────────────────────────
    with tab_markt:
        import plotly.graph_objects as go

        st.markdown("##### Marktdynamiek")
        if pbk and pbk.latest:
            latest  = pbk.latest
            nat     = pbk.national_latest
            delta   = pbk.heat_delta_pct

            mc1, mc2, mc3, mc4, mc5, mc6, mc7 = st.columns(7)
            mc1.metric(
                "Prijsdruk regio",
                pbk.heat_label,
                help=f"COROP-regio: {pbk.corop_name}",
            )
            mc2.metric(
                f"Prijsstijging {latest.period_label}",
                f"{latest.yoy_pct:+.1f}%" if latest.yoy_pct is not None else "–",
                delta=f"{delta:+.1f}pp t.o.v. NL" if delta is not None else None,
                delta_color="normal" if (delta is not None and delta >= 0) else "inverse",
                help="Jaar-op-jaar prijsverandering in deze COROP-regio (CBS PBK 85819NED)",
            )
            mc3.metric(
                f"Transacties {latest.period_label}",
                f"{latest.sales_count:,}".replace(",", ".") if latest.sales_count else "–",
                delta=f"{latest.sales_yoy_pct:+.1f}% YoY" if latest.sales_yoy_pct is not None else None,
                delta_color="normal",
                help="Aantal verkochte woningen in de COROP-regio (Kadaster via CBS)",
            )
            mc4.metric(
                f"Gem. verkoopprijs {latest.period_label}",
                _fmt_eur(latest.avg_price),
                help="Gemiddelde verkoopprijs bestaande koopwoningen in de COROP-regio",
            )
            if bouw and bouw.latest:
                bl = bouw.latest
                mc5.metric(
                    f"Nieuwbouw {bl.year}",
                    f"{bl.nieuwbouw:,}".replace(",", ".") if bl.nieuwbouw else "–",
                    delta=f"{bl.supply_pressure:.0f}% van transacties" if bl.supply_pressure else None,
                    delta_color="off",
                    help=f"Nieuw opgeleverde woningen in gemeente {_gm_label} · Aanboddruk t.o.v. COROP-transacties: {bouw.supply_label}",
                )
            else:
                mc5.metric("Nieuwbouw", "–")

            if permits and permits.latest:
                _t12 = permits.trailing_12m()
                _t12_delta = permits.yoy_trailing_12m()
                mc6.metric(
                    f"Vergunningen {permits.latest.period_label}",
                    f"{_t12:,}".replace(",", ".") if _t12 else "–",
                    delta=f"{_t12_delta:+,}".replace(",", ".") + " YoY" if _t12_delta else None,
                    delta_color="normal",
                    help=f"Verleende bouwvergunningen (12m cumulatief) in {_gm_label} · Voorlopende indicator: ~12–18m voor opleveringen",
                )
            else:
                mc6.metric("Vergunningen", "–")

            if migration and migration.latest:
                _m12     = migration.trailing_12m_net()
                _m12_yoy = migration.yoy_trailing_12m()
                _m_sign  = "normal" if (_m12 is not None and _m12 >= 0) else "inverse"
                mc7.metric(
                    f"Netto instroom {migration.latest.period_label}",
                    f"{_m12:+,}".replace(",", ".") if _m12 is not None else "–",
                    delta=f"{_m12_yoy:+,}".replace(",", ".") + " YoY" if _m12_yoy is not None else None,
                    delta_color="normal",
                    help=f"Netto binnenlandse verhuizingen naar {_gm_label} (12m cumulatief) · Vraagzijde-indicator: positief = instroom overtreft vertrek",
                )
            else:
                mc7.metric("Netto instroom", "–")

            # COROP YoY chart
            _nat_tx_map  = {p.period: p.sales_count for p in pbk.national_data}
            periods      = [p.period_label for p in pbk.corop_data]
            yoy_reg      = [p.yoy_pct for p in pbk.corop_data]
            yoy_nat      = [p.yoy_pct for p in pbk.national_data]
            _tx_share_abs = [
                round(p.sales_count / _nat_tx_map[p.period] * 100, 3)
                if (p.sales_count and _nat_tx_map.get(p.period))
                else None
                for p in pbk.corop_data
            ]
            tx_share = [None] * 4 + [
                round(_tx_share_abs[i] - _tx_share_abs[i - 4], 3)
                if (_tx_share_abs[i] is not None and _tx_share_abs[i - 4] is not None)
                else None
                for i in range(4, len(_tx_share_abs))
            ]
            _corop_short = pbk.corop_name.replace(" (CR)", "")

            fig_pbk = go.Figure()

            fig_pbk.add_trace(go.Bar(
                x=periods, y=tx_share,
                name="Δ COROP-aandeel (pp YoY)",
                marker_color=[
                    "rgba(44,160,44,0.30)" if (v is not None and v >= 0) else "rgba(214,39,40,0.25)"
                    for v in tx_share
                ],
                yaxis="y2",
                hovertemplate="%{x}: %{y:+.3f}pp<extra>Δ COROP-aandeel</extra>",
            ))

            fig_pbk.add_trace(go.Scatter(
                x=periods, y=yoy_reg,
                mode="lines+markers",
                name=f"Prijsgroei {_corop_short}",
                line=dict(color="#1f77b4", width=2),
                marker=dict(size=5),
                hovertemplate="%{x}: %{y:+.1f}%<extra>Prijsgroei COROP</extra>",
            ))
            fig_pbk.add_trace(go.Scatter(
                x=periods, y=yoy_nat,
                mode="lines+markers",
                name="Prijsgroei NL",
                line=dict(color="#888888", width=1.5, dash="dot"),
                marker=dict(size=4),
                hovertemplate="%{x}: %{y:+.1f}%<extra>Prijsgroei NL</extra>",
            ))
            fig_pbk.add_hline(y=0, line_color="#dddddd", line_width=1)
            fig_pbk.update_layout(
                yaxis=dict(title="Prijsverandering YoY (%)", ticksuffix="%"),
                yaxis2=dict(
                    title="Δ aandeel (pp)",
                    overlaying="y", side="right",
                    ticksuffix="pp", showgrid=False,
                ),
                xaxis=dict(title=""),
                height=300,
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                hovermode="x unified",
            )
            st.plotly_chart(fig_pbk, use_container_width=True, key="pbk_yoy")

            # Nieuwbouw & aanbod expander
            if bouw and bouw.years:
                with st.expander(f"Nieuwbouw & woningaanbod – {_gm_label} (gemeente-niveau)"):
                    years_b  = [y.year for y in bouw.years]
                    nb_vals  = [y.nieuwbouw or 0 for y in bouw.years]
                    sl_vals  = [-(y.sloop or 0) for y in bouw.years]
                    sp_vals  = [y.supply_pressure for y in bouw.years]

                    fig_bouw = go.Figure()
                    fig_bouw.add_trace(go.Bar(
                        x=years_b, y=nb_vals,
                        name="Nieuwbouw",
                        marker_color="#2ca02c",
                        hovertemplate="%{x}: %{y:,} woningen<extra>Nieuwbouw</extra>",
                    ))
                    fig_bouw.add_trace(go.Bar(
                        x=years_b, y=sl_vals,
                        name="Sloop",
                        marker_color="#d62728",
                        hovertemplate="%{x}: %{customdata:,} woningen<extra>Sloop</extra>",
                        customdata=[y.sloop or 0 for y in bouw.years],
                    ))
                    fig_bouw.add_trace(go.Scatter(
                        x=years_b, y=sp_vals,
                        name="Aanboddruk (% v. transacties)",
                        mode="lines+markers",
                        line=dict(color="#ff7f0e", width=2),
                        marker=dict(size=6),
                        yaxis="y2",
                        hovertemplate="%{x}: %{y:.1f}%<extra>Aanboddruk</extra>",
                    ))
                    fig_bouw.update_layout(
                        barmode="relative",
                        yaxis=dict(title="Woningen"),
                        yaxis2=dict(
                            title="Aanboddruk (%)",
                            overlaying="y", side="right",
                            ticksuffix="%", showgrid=False,
                        ),
                        xaxis=dict(title="", tickformat="d"),
                        height=280,
                        margin=dict(l=10, r=10, t=40, b=10),
                        title=dict(
                            text=f"<b>{_gm_label}</b>  <span style='font-size:12px;color:#888'>gemeente-niveau · aanboddruk t.o.v. {_corop_label} (COROP)</span>",
                            font=dict(size=13),
                            x=0, xanchor="left",
                        ),
                        legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="left", x=0),
                        hovermode="x unified",
                    )
                    st.plotly_chart(fig_bouw, use_container_width=True, key="bouw_supply")
                    st.caption(
                        f"Nieuwbouw/sloop: gemeente-niveau ({_gm_label}). "
                        f"Aanboddruk = gemeente-nieuwbouw / COROP-transacties ({_corop_label}) × 100. "
                        f"Aanbodlabel: **{bouw.supply_label}**. "
                        "Bron: CBS 86054NED · 85819NED · NLOD"
                    )

                    if permits and permits.quarters:
                        st.markdown("**Bouwvergunningen per kwartaal** (gemeente-niveau · voorlopende indicator)")
                        _pq_periods  = [q.period_label for q in permits.quarters]
                        _pq_vals     = [q.permits      for q in permits.quarters]

                        _pq_t12 = []
                        for i in range(len(permits.quarters)):
                            window = [q.permits for q in permits.quarters[max(0, i-3):i+1]]
                            known  = [v for v in window if v is not None]
                            _pq_t12.append(sum(known) if len(known) == 4 else None)

                        fig_perm = go.Figure()
                        fig_perm.add_trace(go.Bar(
                            x=_pq_periods, y=_pq_vals,
                            name="Vergunningen (kwartaal)",
                            marker_color="rgba(148,103,189,0.55)",
                            hovertemplate="%{x}: %{y:,}<extra>Vergunningen</extra>",
                        ))
                        fig_perm.add_trace(go.Scatter(
                            x=_pq_periods, y=_pq_t12,
                            name="12m voortschrijdend totaal",
                            mode="lines",
                            line=dict(color="#7b2fa8", width=2, dash="dot"),
                            yaxis="y2",
                            hovertemplate="%{x}: %{y:,}<extra>12m totaal</extra>",
                        ))
                        fig_perm.update_layout(
                            yaxis =dict(title="Vergunningen (kwartaal)"),
                            yaxis2=dict(
                                title="12m totaal",
                                overlaying="y", side="right",
                                showgrid=False,
                            ),
                            xaxis =dict(title=""),
                            height=240,
                            margin=dict(l=10, r=10, t=10, b=10),
                            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                            hovermode="x unified",
                        )
                        st.plotly_chart(fig_perm, use_container_width=True, key="bouw_permits")
                        st.caption(
                            f"Bouwvergunningen voor woningen in {_gm_label}. "
                            "Vergunningen lopen ~12–18 maanden voor op opleveringen. "
                            "Bron: CBS 83671NED · NLOD"
                        )

                    if bouw.type_years:
                        st.markdown("**Nieuwbouw naar woningtype** (COROP-niveau)")
                        _type_colors = {
                            "Tussenwoning": "#1f77b4",
                            "Hoekwoning":   "#aec7e8",
                            "2-onder-1-kap":"#ff7f0e",
                            "Vrijstaand":   "#2ca02c",
                            "Meergezins":   "#9467bd",
                        }
                        _type_fields = {
                            "Tussenwoning": [ty.tussenwoning for ty in bouw.type_years],
                            "Hoekwoning":   [ty.hoekwoning   for ty in bouw.type_years],
                            "2-onder-1-kap":[ty.twee_kap     for ty in bouw.type_years],
                            "Vrijstaand":   [ty.vrijstaand   for ty in bouw.type_years],
                            "Meergezins":   [ty.meergezins   for ty in bouw.type_years],
                        }
                        years_t = [ty.year for ty in bouw.type_years]

                        fig_type = go.Figure()
                        for type_name, vals in _type_fields.items():
                            fig_type.add_trace(go.Bar(
                                x=years_t, y=vals,
                                name=type_name,
                                marker_color=_type_colors[type_name],
                                hovertemplate=f"%{{x}}: %{{y:,}}<extra>{type_name}</extra>",
                            ))
                        fig_type.update_layout(
                            barmode="stack",
                            title=dict(
                                text=f"<b>{_corop_label}</b>  <span style='font-size:12px;color:#888'>COROP-niveau</span>",
                                font=dict(size=13), x=0, xanchor="left",
                            ),
                            yaxis=dict(title="Woningen"),
                            xaxis=dict(title="", tickformat="d"),
                            height=280,
                            margin=dict(l=10, r=10, t=40, b=10),
                            legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="left", x=0),
                            hovermode="x unified",
                        )
                        st.plotly_chart(fig_type, use_container_width=True, key="bouw_type")
                        st.caption("Bron: CBS 86084NED · NLOD")

            # Migration expander
            if migration and migration.months:
                with st.expander("🚚 Bevolking & instroom", expanded=False):
                    st.markdown(
                        f"**Netto binnenlandse verhuizingen** (gemeente-niveau · maandelijkse CBS-data)"
                    )

                    _mg_periods  = [m.period_label for m in migration.months]
                    _mg_vest     = [m.vestiging for m in migration.months]
                    _mg_vert     = [m.vertrek   for m in migration.months]
                    _mg_net      = [m.net        for m in migration.months]

                    _mg_roll12: list[int | None] = []
                    for i in range(len(migration.months)):
                        window = [m.net for m in migration.months[max(0, i-11):i+1]]
                        known  = [v for v in window if v is not None]
                        _mg_roll12.append(sum(known) if len(known) == 12 else None)

                    fig_mig = go.Figure()
                    fig_mig.add_trace(go.Bar(
                        x=_mg_periods, y=_mg_vest,
                        name="Vestiging (in)",
                        marker_color="rgba(44,160,44,0.55)",
                        hovertemplate="%{x}: %{y:,}<extra>Vestiging</extra>",
                    ))
                    _mg_vert_neg = [-v if v is not None else None for v in _mg_vert]
                    fig_mig.add_trace(go.Bar(
                        x=_mg_periods, y=_mg_vert_neg,
                        customdata=_mg_vert,
                        name="Vertrek (uit)",
                        marker_color="rgba(214,39,40,0.55)",
                        hovertemplate="%{x}: %{customdata:,}<extra>Vertrek</extra>",
                    ))
                    fig_mig.add_trace(go.Scatter(
                        x=_mg_periods, y=_mg_roll12,
                        name="12m netto voortschrijdend",
                        mode="lines",
                        line=dict(color="#1f77b4", width=2, dash="dot"),
                        yaxis="y2",
                        hovertemplate="%{x}: %{y:+,}<extra>12m netto</extra>",
                    ))
                    fig_mig.update_layout(
                        barmode="overlay",
                        yaxis =dict(title="Personen / maand"),
                        yaxis2=dict(
                            title="12m netto instroom",
                            overlaying="y", side="right",
                            showgrid=False,
                            zeroline=True, zerolinecolor="#aaa",
                        ),
                        xaxis =dict(title=""),
                        height=280,
                        margin=dict(l=10, r=10, t=10, b=10),
                        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                        hovermode="x unified",
                    )
                    st.plotly_chart(fig_mig, use_container_width=True, key="migration")
                    st.caption(
                        f"Binnenlandse vestiging & vertrek in {_gm_label}. "
                        "Positief 12m netto = meer instroom dan uitstroom → verhoogt druk op woningmarkt. "
                        "Bron: CBS 37230ned · NLOD"
                    )

            st.caption("Bron: CBS PBK 85819NED · 85773NED · 86054NED · Kadaster · NLOD")
        else:
            st.info("Geen COROP-data beschikbaar voor dit adres.")

        st.divider()

        # Max hypotheek + interest rate chart
        if lc_rows or _has_rate:
            st.markdown("##### Maximale hypotheek bij modaal inkomen & rente-ontwikkeling")

            from plotly.subplots import make_subplots

            if _has_rate:
                fig_hyp = make_subplots(
                    rows=2, cols=1,
                    shared_xaxes=True,
                    row_heights=[0.65, 0.35],
                    vertical_spacing=0.06,
                )
            else:
                fig_hyp = make_subplots(rows=1, cols=1)

            if lc_rows:
                df_lc_markt = pd.DataFrame(lc_rows)
                fig_hyp.add_trace(go.Scatter(
                    x=df_lc_markt["year"], y=df_lc_markt["max_mortgage"],
                    mode="lines+markers",
                    name="Max. hypotheek (modaal)",
                    line=dict(color="#2ca02c", width=2, dash="dash"),
                    marker=dict(size=4),
                    hovertemplate="%{x}: € %{y:,.0f}<extra>Max. hypotheek modaal</extra>",
                ), row=1, col=1)

            if _has_rate:
                def _period_to_decimal_markt(p: str) -> float:
                    yr, mo = int(p[:4]), int(p[5:7])
                    return yr + (mo - 1) / 12

                _rate_x_m      = [_period_to_decimal_markt(pt.period) for pt in mortgage_rates.points]
                _rate_labels_m = [pt.period for pt in mortgage_rates.points]
                _rate_vals_m   = [pt.rate_pct for pt in mortgage_rates.points]
                fig_hyp.add_trace(go.Scatter(
                    x=_rate_x_m, y=_rate_vals_m,
                    customdata=_rate_labels_m,
                    mode="lines",
                    name="Hypotheekrente NL",
                    line=dict(color="#d62728", width=1.5),
                    hovertemplate="%{customdata}: %{y:.2f}%<extra>Hypotheekrente</extra>",
                    showlegend=True,
                ), row=2, col=1)

            fig_hyp.update_yaxes(
                title_text="Max. hypotheek (€)", tickformat=",.0f", autorange=True,
                row=1, col=1,
            )
            if _has_rate:
                fig_hyp.update_yaxes(
                    title_text="Rente (%)", ticksuffix="%", rangemode="tozero",
                    row=2, col=1,
                )
                fig_hyp.update_xaxes(title_text="", row=1, col=1)
                _x_end_m = (
                    _period_to_decimal_markt(mortgage_rates.latest.period) + 1 / 12
                    if mortgage_rates and mortgage_rates.latest else None
                )
                fig_hyp.update_xaxes(
                    title_text="",
                    tickformat="d",
                    range=[2003, _x_end_m] if _x_end_m else None,
                    row=2, col=1,
                )
            else:
                fig_hyp.update_xaxes(title_text="Jaar", tickformat="d", row=1, col=1)

            fig_hyp.update_layout(
                height=480 if _has_rate else 300,
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                hovermode="x unified",
            )
            st.plotly_chart(fig_hyp, use_container_width=True, key="hypotheek_rate")

            if lc_rows:
                st.caption(
                    f"Max. hypotheek: annuïteit {TERM_YEARS} jr, woonquote {WOONQUOTE*100:.0f}% bruto, "
                    "ECB/DNB-rente jaargemiddelde (AAR). Inkomen: CBS 83931NED mediaan bruto (≥2011); hardcoded vóór 2011."
                )
            if _has_rate:
                st.caption(
                    f"Hypotheekrente: maandelijks AAR nieuwe woninghypotheken NL "
                    f"(ECB MIR · DNB, 2003–heden). Meest recent: {mortgage_rates.latest.period} "
                    f"→ {mortgage_rates.latest.rate_pct:.2f}%"
                )

        # Lending capacity expander
        if lc_rows:
            with st.expander("Leencapaciteit – onderliggende data"):
                df_lc_exp = pd.DataFrame(lc_rows)
                mc1, mc2 = st.columns(2)
                with mc1:
                    st.markdown("**Rente (jaargemiddelde ECB/DNB AAR, %)**")
                    fig_r = go.Figure(go.Scatter(
                        x=df_lc_exp["year"], y=df_lc_exp["rate_totaal"],
                        mode="lines+markers",
                        line=dict(color="#d62728", width=2),
                        marker=dict(size=4),
                        hovertemplate="%{x}: %{y:.2f}%<extra></extra>",
                    ))
                    fig_r.update_layout(
                        height=200, margin=dict(l=10, r=10, t=10, b=10),
                        yaxis=dict(autorange=True),
                        xaxis=dict(tickformat="d"),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_r, use_container_width=True, key="lc_rate")
                with mc2:
                    st.markdown("**Modaal inkomen (€ bruto/jaar)**")
                    fig_inc = go.Figure(go.Scatter(
                        x=df_lc_exp["year"], y=df_lc_exp["modal_income"],
                        mode="lines+markers",
                        line=dict(color="#2ca02c", width=2),
                        marker=dict(size=4),
                        hovertemplate="%{x}: € %{y:,.0f}<extra></extra>",
                    ))
                    fig_inc.update_layout(
                        height=200, margin=dict(l=10, r=10, t=10, b=10),
                        yaxis=dict(autorange=True, tickformat=",.0f"),
                        xaxis=dict(tickformat="d"),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_inc, use_container_width=True, key="lc_income")
                st.dataframe(
                    df_lc_exp.rename(columns={
                        "year": "Jaar", "modal_income": "Modaal inkomen (€)",
                        "rate_totaal": "Rente % (DNB)", "max_mortgage": "Max. hypotheek (€)",
                    })[["Jaar", "Modaal inkomen (€)", "Rente % (DNB)", "Max. hypotheek (€)"]],
                    use_container_width=True,
                    hide_index=True,
                )

    # ── tab 4: prijsopbouw ────────────────────────────────────────────────────
    with tab_prijs:
        if bid_result:
            st.markdown("##### Opbouw marktwaardeschatting")

            waterfall_rows = []
            waterfall_rows.append({
                "Stap": "WOZ-schatting (model)",
                "Toelichting": "Buurtgemiddelde gecorrigeerd voor woningtype en perceel",
                "Laag": _fmt_eur(bid_result.woz_estimate),
                "Hoog": _fmt_eur(bid_result.woz_estimate),
                "Bron": "CBS 85984NED · Kadaster",
            })
            waterfall_rows.append({
                "Stap": f"Tijdcorrectie (×{bid_result.f_lag:.3f})",
                "Toelichting": "Prijsindex bijgesteld naar huidige markt",
                "Laag": _fmt_eur(bid_result.adjusted_base),
                "Hoog": _fmt_eur(bid_result.adjusted_base),
                "Bron": "CBS 85773NED / 85819NED",
            })
            for f in bid_result.factors:
                low_eur  = round(bid_result.adjusted_base * f.low_pct)
                high_eur = round(bid_result.adjusted_base * f.high_pct)
                waterfall_rows.append({
                    "Stap": f.label,
                    "Toelichting": f.description,
                    "Laag": f"{low_eur:+,}".replace(",", "."),
                    "Hoog": f"{high_eur:+,}".replace(",", "."),
                    "Bron": f.source,
                })
            if bid_result.renovation_cost_high > 0:
                waterfall_rows.append({
                    "Stap": "Renovatiekosten",
                    "Toelichting": f"{len(user_input.get('renovation', []))} posten",
                    "Laag": f"-{bid_result.renovation_cost_high:,}".replace(",", "."),
                    "Hoog": f"-{bid_result.renovation_cost_low:,}".replace(",", "."),
                    "Bron": "Gebruikersinvoer",
                })
            waterfall_rows.append({
                "Stap": "**Geschatte marktwaarde**",
                "Toelichting": "",
                "Laag": f"**{_fmt_eur(bid_result.bid_low)}**",
                "Hoog": f"**{_fmt_eur(bid_result.bid_high)}**",
                "Bron": "",
            })

            st.dataframe(pd.DataFrame(waterfall_rows), use_container_width=True, hide_index=True)

            if bid_result.missing_inputs:
                st.warning(
                    "Ontbrekende data beïnvloedt de betrouwbaarheid: "
                    + ", ".join(bid_result.missing_inputs)
                )
        else:
            st.info("Onvoldoende data voor biedrange-berekening (WOZ-schatting ontbreekt).")

        st.divider()

        # Model validation
        if woz_est and woz_est.estimated_value:
            import plotly.graph_objects as go
            st.markdown("##### 🔬 Modelvalidatie: Geschatte WOZ vs. Kadaster")

            vc1, vc2, vc3, vc4, vc5 = st.columns(5)
            vc1.metric("Geschatte WOZ", _fmt_eur(woz_est.estimated_value))
            vc2.metric(
                "w.v. stap 1 (type)",
                _fmt_eur(woz_est.step1_value),
                help="Buurtgemiddelde gecorrigeerd voor woningtype, vóór perceelcorrectie",
            )
            vc3.metric(
                "Perceelfactor",
                f"{woz_est.perceel_factor:.3f}" if woz_est.perceel_applied else "–",
                help=f"{woz_est.perceel_m2} m² vs. ref {woz_est.perceel_reference_m2} m², γ={woz_est.perceel_gamma}"
                     if woz_est.perceel_applied else "Geen perceeldata of niet van toepassing",
            )
            vc4.metric("Kadaster WOZ", _fmt_eur(real_woz))

            if real_woz:
                delta = woz_est.validation_delta_pct(real_woz)
                vc5.metric(
                    "Afwijking",
                    f"{delta:+.1f}%" if delta is not None else "–",
                    delta=f"{delta:+.1f}%" if delta is not None else None,
                    delta_color="normal" if (delta is not None and abs(delta) < 10) else "off",
                    help="(Geschat − Kadaster) / Kadaster × 100",
                )

            with st.expander("Modeldetails"):
                from watmoetikbieden.woz_estimator import (
                    NATIONAL_PRICES_2024, RELATIVES, PERCEEL_PARAMS,
                    MIN_PERCEEL_FACTOR, MAX_PERCEEL_FACTOR,
                    _get_regional_relatives,
                )

                st.markdown("#### Stap 1 – Type-aanpassing op buurtgemiddelde")
                st.caption(f"Prijsrelatieven: **{woz_est.relatives_source}**")
                st.markdown(f"""
```
W_type = gem_woz_buurt × r_type / Σ_t(pct_t × r_t)
       = {_fmt_eur(woz_est.source_gem_woz_eur)} × {woz_est.type_relative:.4f} / {woz_est.composition_weight or '?'}
       = {_fmt_eur(woz_est.step1_value)}
```
- **Buurt gem. WOZ:** {_fmt_eur(woz_est.source_gem_woz_eur)} (bron: CBS 85984NED, in €1000 opgeslagen)
- **r_type ({user_input.get('house_type','?')}):** {woz_est.type_relative:.4f} (relatief t.o.v. gemiddelde)
- **Σ(pct·r) buurtsamenstelling:** {woz_est.composition_weight or '–'} (gewogen relatief op basis van buurtmix)
- **Buurtdekking:** {woz_est.coverage_pct:.0f}% van woningtypes bekend uit CBS
""")

                is_regional = woz_est.relatives_source.startswith("regional")
                prov_rels = _get_regional_relatives(province) if (is_regional and province) else None

                type_rows_def = [
                    ("Tussenwoning",       "ZW25805"),
                    ("Hoekwoning",         "ZW25806"),
                    ("Twee-onder-één-kap", "ZW10300"),
                    ("Vrijstaande woning", "ZW10320"),
                    ("Appartement",        "ZW25810"),
                ]

                if prov_rels:
                    st.markdown(f"**Provinciale prijsrelatieven via OLS ({woz_est.relatives_source})**")
                    rel_rows = [
                        (
                            lbl,
                            f"€ {prov_rels[code] * 1000:,.0f}".replace(",", ".") if code in prov_rels else "–",
                            f"{prov_rels[code]:.4f}" if code in prov_rels else "–",
                            "← dit adres" if code == woz_est.house_type_code else "",
                        )
                        for lbl, code in type_rows_def
                    ]
                    st.dataframe(
                        pd.DataFrame(rel_rows, columns=["Type", "Geschatte gem. WOZ (€)", "β (€1k)", ""]),
                        use_container_width=True, hide_index=True,
                    )
                    st.caption("β-waarden uit gewogen OLS over alle buurten in deze provincie (CBS 85984NED).")
                else:
                    st.markdown("**Nationale prijsrelatieven (CBS 85791NED, 2024)**")
                    rel_rows = [
                        (
                            lbl,
                            f"€ {NATIONAL_PRICES_2024[code]:,}".replace(",", ".") if code in NATIONAL_PRICES_2024 else "–",
                            f"{RELATIVES[code]:.4f}" if code in RELATIVES else "–",
                            "← dit adres" if code == woz_est.house_type_code else "",
                        )
                        for lbl, code in type_rows_def + [("Nationaal gemiddelde", "T001100")]
                    ]
                    st.dataframe(
                        pd.DataFrame(rel_rows, columns=["Type", "Gem. verkoopprijs 2024", "Relatief", ""]),
                        use_container_width=True, hide_index=True,
                    )

                st.markdown("#### Stap 2 – Perceelgrootte-aanpassing")
                if woz_est.perceel_applied:
                    st.markdown(f"""
```
perceel_factor = (actual_m2 / reference_m2) ^ γ
               = ({woz_est.perceel_m2} / {woz_est.perceel_reference_m2}) ^ {woz_est.perceel_gamma}
               = {woz_est.perceel_factor:.4f}   (geclamped op [{MIN_PERCEEL_FACTOR}, {MAX_PERCEEL_FACTOR}])

W_final = W_type × perceel_factor
        = {_fmt_eur(woz_est.step1_value)} × {woz_est.perceel_factor:.4f}
        = {_fmt_eur(woz_est.estimated_value)}
```
""")
                else:
                    st.info(
                        f"Perceelaanpassing overgeslagen — {woz_est.method.split('overgeslagen: ')[-1] if 'overgeslagen' in woz_est.method else 'niet van toepassing'}. "
                        f"Eindwaarde = stap 1 waarde = {_fmt_eur(woz_est.step1_value)}"
                    )

                st.markdown("**Perceelparameters per woningtype**")
                perc_rows = [
                    (
                        lbl,
                        f"{PERCEEL_PARAMS[code]['reference_m2']} m²" if PERCEEL_PARAMS[code]["reference_m2"] else "–",
                        str(PERCEEL_PARAMS[code]["gamma"]),
                        "← dit adres" if code == woz_est.house_type_code else "",
                    )
                    for lbl, code in [
                        ("Tussenwoning", "ZW25805"),
                        ("Hoekwoning", "ZW25806"),
                        ("Twee-onder-één-kap", "ZW10300"),
                        ("Vrijstaande woning", "ZW10320"),
                        ("Appartement", "ZW25810"),
                    ]
                ]
                st.dataframe(
                    pd.DataFrame(perc_rows, columns=["Type", "Referentie perceel", "γ (elasticiteit)", ""]),
                    use_container_width=True, hide_index=True,
                )
                st.caption(
                    "γ = prijselasticiteit t.o.v. perceelgrootte (log-log). "
                    "Pas aan in `woz_estimator.py` → PERCEEL_PARAMS."
                )

        # Lending capacity detail expander
        if lc_rows:
            with st.expander("Leencapaciteit – onderliggende data"):
                import plotly.graph_objects as go
                df_lc_exp2 = pd.DataFrame(lc_rows)
                mc1, mc2 = st.columns(2)
                with mc1:
                    st.markdown("**Rente (jaargemiddelde ECB/DNB AAR, %)**")
                    fig_r2 = go.Figure(go.Scatter(
                        x=df_lc_exp2["year"], y=df_lc_exp2["rate_totaal"],
                        mode="lines+markers",
                        line=dict(color="#d62728", width=2),
                        marker=dict(size=4),
                        hovertemplate="%{x}: %{y:.2f}%<extra></extra>",
                    ))
                    fig_r2.update_layout(
                        height=200, margin=dict(l=10, r=10, t=10, b=10),
                        yaxis=dict(autorange=True),
                        xaxis=dict(tickformat="d"),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_r2, use_container_width=True, key="prijs_rate")
                with mc2:
                    st.markdown("**Modaal inkomen (€ bruto/jaar)**")
                    fig_inc2 = go.Figure(go.Scatter(
                        x=df_lc_exp2["year"], y=df_lc_exp2["modal_income"],
                        mode="lines+markers",
                        line=dict(color="#2ca02c", width=2),
                        marker=dict(size=4),
                        hovertemplate="%{x}: € %{y:,.0f}<extra></extra>",
                    ))
                    fig_inc2.update_layout(
                        height=200, margin=dict(l=10, r=10, t=10, b=10),
                        yaxis=dict(autorange=True, tickformat=",.0f"),
                        xaxis=dict(tickformat="d"),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_inc2, use_container_width=True, key="prijs_income")
                st.dataframe(
                    df_lc_exp2.rename(columns={
                        "year": "Jaar", "modal_income": "Modaal inkomen (€)",
                        "rate_totaal": "Rente % (DNB)", "max_mortgage": "Max. hypotheek (€)",
                    })[["Jaar", "Modaal inkomen (€)", "Rente % (DNB)", "Max. hypotheek (€)"]],
                    use_container_width=True,
                    hide_index=True,
                )

    # ── tab 5: databronnen ────────────────────────────────────────────────────
    with tab_data:
        st.markdown("##### Databronnen")

        # !! CLAUDE: when adding a new data source anywhere in the app, add a row
        # !! here too. Required fields: see CLAUDE.md § "Databronnen table".
        _SOURCES = [
            {
                "Bron":           "PDOK / Kadaster",
                "Dataset":        "BAG Locatieserver v3.1",
                "Tabel-ID":       "–",
                "Beschrijving":   "Vrij-tekst adresopzoek; levert coördinaten, buurtcode, wijkcode, gemeentecode en perceel-ID.",
                "Granulariteit":  "Adres",
                "Perioden":       "Actueel",
                "Tijdresolutie":  "Continu",
                "Laatste update": "Continu",
                "Licentie":       "CC0",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "PDOK / Kadaster",
                "Dataset":        "BAG WFS v2.0",
                "Tabel-ID":       "–",
                "Beschrijving":   "Bouwkenmerken per verblijfsobject: oppervlakte, bouwjaar, gebruiksdoel, status.",
                "Granulariteit":  "Adres / pand",
                "Perioden":       "Actueel",
                "Tijdresolutie":  "Continu",
                "Laatste update": "Continu",
                "Licentie":       "CC0",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "PDOK / Kadaster",
                "Dataset":        "BRK Kadastralekaart WFS v5.0",
                "Tabel-ID":       "–",
                "Beschrijving":   "Kadastraal perceeloppervlak per perceel.",
                "Granulariteit":  "Perceel",
                "Perioden":       "Actueel",
                "Tijdresolutie":  "Continu",
                "Laatste update": "Continu",
                "Licentie":       "CC0",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "Kadaster",
                "Dataset":        "Landelijke Voorziening WOZ (LVWOZ)",
                "Tabel-ID":       "–",
                "Beschrijving":   "Officiële WOZ-waarden per adres per peildatum (1 jan). Limiet 5 000 req/dag.",
                "Granulariteit":  "Adres",
                "Perioden":       "Meerdere peildata",
                "Tijdresolutie":  "Jaarlijks",
                "Laatste update": "Peildatum 2025-01-01",
                "Licentie":       "CC0",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "RVO",
                "Dataset":        "EP-Online v4",
                "Tabel-ID":       "–",
                "Beschrijving":   "Energielabels (energieklasse, opnamedatum) per adres; meest recente certificaat.",
                "Granulariteit":  "Adres",
                "Perioden":       "Actueel",
                "Tijdresolutie":  "Continu",
                "Laatste update": "Continu",
                "Licentie":       "CC0",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "CBS",
                "Dataset":        "Kerncijfers Wijken en Buurten 2025",
                "Tabel-ID":       "86165NED",
                "Beschrijving":   "Demografische kenmerken, woningtypen, inkomen en gem. WOZ per buurt/wijk/gemeente. Ook gebruikt voor WOZ-trendgrafiek (2013–2025) via eerdere jaaredities.",
                "Granulariteit":  "Buurt / wijk / gemeente",
                "Perioden":       "2013–2025 (per jaarlijkse tabel)",
                "Tijdresolutie":  "Jaarlijks (nieuwe tabel-ID per jaar)",
                "Laatste update": "2026-03-31",
                "Licentie":       "NLOD",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "CBS",
                "Dataset":        "Prijsindex Bestaande Koopwoningen – COROP",
                "Tabel-ID":       "85819NED",
                "Beschrijving":   "Kwartaal prijsindex (2020=100), transactievolume en gemiddelde verkoopprijs per COROP-regio (40 regio's). Inclusief 95%-betrouwbaarheidsmarges.",
                "Granulariteit":  "COROP (40 regio's)",
                "Perioden":       "Q1 1995 – heden",
                "Tijdresolutie":  "Kwartaal (~6 weken vertraging)",
                "Laatste update": "Q1 2026",
                "Licentie":       "NLOD",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "CBS / Kadaster",
                "Dataset":        "Bestaande koopwoningen; prijsindex en transacties",
                "Tabel-ID":       "85773NED",
                "Beschrijving":   "Maandelijkse nationale transactieaantallen en prijsindex bestaande koopwoningen. Gebruikt als recency-correctie op COROP-kwartaaldata in het biedmodel.",
                "Granulariteit":  "Nationaal",
                "Perioden":       "1995 – heden",
                "Tijdresolutie":  "Maandelijks",
                "Laatste update": "apr 2026",
                "Licentie":       "NLOD",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "CBS",
                "Dataset":        "Gebiedsindelingen – gemeente → COROP",
                "Tabel-ID":       "84721NED",
                "Beschrijving":   "Koppeling van gemeentecodes (GM) aan COROP-codes (CR). Gebruikt voor geografische aggregatie.",
                "Granulariteit":  "Gemeente",
                "Perioden":       "2024",
                "Tijdresolutie":  "Jaarlijks",
                "Laatste update": "2024",
                "Licentie":       "NLOD",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "CBS",
                "Dataset":        "Voorraad woningen; toevoegingen en onttrekkingen",
                "Tabel-ID":       "86054NED",
                "Beschrijving":   "Jaarlijkse nieuwbouw, sloop en netto mutaties in de woningvoorraad per gemeente en COROP.",
                "Granulariteit":  "Gemeente / COROP / Provincie",
                "Perioden":       "2020–2024",
                "Tijdresolutie":  "Jaarlijks",
                "Laatste update": "2024 (voorlopig)",
                "Licentie":       "NLOD",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "CBS",
                "Dataset":        "Nieuwbouw en transformaties; kenmerken woning",
                "Tabel-ID":       "86084NED",
                "Beschrijving":   "Jaarlijkse nieuwbouw per woningtype (tussenwoning, hoekwoning, 2-kap, vrijstaand, meergezins) per COROP.",
                "Granulariteit":  "COROP",
                "Perioden":       "2018–2024",
                "Tijdresolutie":  "Jaarlijks",
                "Laatste update": "2024 (voorlopig)",
                "Licentie":       "NLOD",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "DUO",
                "Dataset":        "Vestigingen PO + VO & Kwaliteitsoordelen",
                "Tabel-ID":       "–",
                "Beschrijving":   "Schoollocaties (BRIN, naam, postcode, gemeente) voor basisonderwijs en voortgezet onderwijs, aangevuld met inspectiebeoordelingen.",
                "Granulariteit":  "School / vestiging",
                "Perioden":       "Locaties: actueel · Oordelen: t/m 2018",
                "Tijdresolutie":  "Jaarlijks (locaties); onregelmatig (oordelen)",
                "Laatste update": "Oordelen: 2018-09-01",
                "Licentie":       "CC0",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "OpenStreetMap",
                "Dataset":        "Overpass API – onderwijsvoorzieningen",
                "Tabel-ID":       "–",
                "Beschrijving":   "Nabije scholen, kinderopvang, MBO en universiteiten binnen opgegeven straal via OSM-tags.",
                "Granulariteit":  "Punt / vlak",
                "Perioden":       "Actueel",
                "Tijdresolutie":  "Continu (community)",
                "Laatste update": "Continu",
                "Licentie":       "ODbL",
                "Commercieel":    "⚠️ Data: ja · Endpoint: niet voor productie",
            },
            {
                "Bron":           "CBS",
                "Dataset":        "Bouwvergunningen woonruimten; gemeente",
                "Tabel-ID":       "83671NED",
                "Beschrijving":   "Kwartaal aantal verleende bouwvergunningen voor woningen per gemeente (koop + huur, alle opdrachtgevers). Voorlopende indicator: ~12–18 maanden voor opleveringen.",
                "Granulariteit":  "Gemeente",
                "Perioden":       "Q1 2012 – heden",
                "Tijdresolutie":  "Kwartaal",
                "Laatste update": "Q4 2025 (voorlopig)",
                "Licentie":       "NLOD",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "CBS",
                "Dataset":        "Bevolkingsontwikkeling per gemeente per maand",
                "Tabel-ID":       "37230ned",
                "Beschrijving":   "Maandelijkse vestiging en vertrek van personen per gemeente, uitgesplitst naar binnenlandse verhuizingen en internationale migratie. Netto binnenlandse instroom = vraagzijde-indicator voor woningmarkt.",
                "Granulariteit":  "Gemeente",
                "Perioden":       "2002 – heden",
                "Tijdresolutie":  "Maandelijks",
                "Laatste update": "mei 2025 (voorlopig)",
                "Licentie":       "NLOD",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "CBS",
                "Dataset":        "Inkomen van personen",
                "Tabel-ID":       "83931NED",
                "Beschrijving":   "Mediaan en gemiddeld persoonlijk bruto inkomen per jaar (alle personen, alle inkomensklassen). Gebruikt als inkomensinput voor de maximale hypotheekberekening (≥2011). Vóór 2011: hardcoded CPB-schattingen.",
                "Granulariteit":  "Nationaal",
                "Perioden":       "2011 – heden",
                "Tijdresolutie":  "Jaarlijks",
                "Laatste update": "2024",
                "Licentie":       "NLOD",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "ECB / DNB",
                "Dataset":        "MFI Interest Rate Statistics — woninghypotheken NL",
                "Tabel-ID":       "MIR",
                "Beschrijving":   "Maandelijkse gemiddelde hypotheekrente voor nieuwe woninghypotheken in Nederland (annualised agreed rate, alle looptijden). Macro-overlay: lage rente correleert met prijsacceleratie.",
                "Granulariteit":  "Nationaal",
                "Perioden":       "2003 – heden",
                "Tijdresolutie":  "Maandelijks",
                "Laatste update": "mrt 2026",
                "Licentie":       "ECB open data",
                "Commercieel":    "✅ Ja",
            },
            {
                "Bron":           "Min. BZK / Leefbaarometer",
                "Dataset":        "Leefbaarometer-scores buurten",
                "Tabel-ID":       "–",
                "Beschrijving":   "Leefbaarheidsscore en -klasse per buurt over de tijd.",
                "Granulariteit":  "Buurt",
                "Perioden":       "2002–2024",
                "Tijdresolutie":  "~2-jaarlijks",
                "Laatste update": "2024",
                "Licentie":       "CC0",
                "Commercieel":    "✅ Ja",
            },
        ]

        df_sources = pd.DataFrame(_SOURCES)
        st.dataframe(
            df_sources,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Bron":           st.column_config.TextColumn("Bron",           width=140),
                "Dataset":        st.column_config.TextColumn("Dataset",        width=220),
                "Tabel-ID":       st.column_config.TextColumn("Tabel-ID",       width=90),
                "Beschrijving":   st.column_config.TextColumn("Beschrijving",   width=340),
                "Granulariteit":  st.column_config.TextColumn("Granulariteit",  width=160),
                "Perioden":       st.column_config.TextColumn("Perioden",       width=140),
                "Tijdresolutie":  st.column_config.TextColumn("Tijdresolutie",  width=170),
                "Laatste update": st.column_config.TextColumn("Laatste update", width=130),
                "Licentie":       st.column_config.TextColumn("Licentie",       width=70),
                "Commercieel":    st.column_config.TextColumn("Commercieel",    width=200),
            },
        )
        st.caption(
            "CC0 = publiek domein. NLOD = Nationale Open Data Licentie (hergebruik incl. commercieel, met bronvermelding). "
            "ODbL = Open Database License (idem, share-alike voor afgeleiden). "
            "⚠️ = zie TODO voor openstaande actie."
        )

        st.markdown("---")

        # BAG / EP-Online / BRK raw JSON (from current Bouwtechnisch tab)
        with st.expander("BAG verblijfsobject & pand"):
            c1_raw, c2_raw = st.columns(2)
            with c1_raw:
                st.markdown("**BAG verblijfsobject**")
                if v:
                    st.json(v.model_dump(exclude_none=True))
                else:
                    st.info("Geen verblijfsobject-data.")
                st.markdown("**BAG pand**")
                if p:
                    st.json(p.model_dump(exclude_none=True))
                else:
                    st.info("Geen pand-data.")
            with c2_raw:
                st.markdown("**Energielabel (EP-Online)**")
                if e:
                    st.json(e.model_dump(exclude_none=True))
                elif data.ep_no_label:
                    st.info("Geen energiecertificaat geregistreerd (gebruikelijk voor woningen van vóór 2008).")
                else:
                    st.info("Niet beschikbaar.")
                st.markdown("**BRK perceel**")
                if b:
                    st.json(b.model_dump(exclude_none=True))
                else:
                    st.info("Geen perceeldata beschikbaar.")

        st.markdown("##### Ruwe API-responses")
        st.caption("Volledige API-responses voor alle bronnen.")
        for source, raw in data.raw.items():
            with st.expander(source):
                st.json(raw)


# ── main layout ───────────────────────────────────────────────────────────────

st.title("🏠 WatMoetIkBieden")
st.caption("Gratis publieke data · BAG · WOZ · EP-Online · Leefbaarometer · CBS")

st.markdown("### Adresgegevens")

address = st.text_input(
    "Adres",
    placeholder="bijv. Keizersgracht 123, 1015 CJ Amsterdam",
    label_visibility="collapsed",
)

col_type, col_koop = st.columns([2, 1])
with col_type:
    house_type = st.selectbox("Type woning", HOUSE_TYPES)
with col_koop:
    te_koop = st.checkbox("Te koop", value=False)

# For-sale details — only shown when checkbox is ticked
listed_label = None
days_for_sale = None
condition = None
renovation = []

if te_koop:
    st.markdown("#### Verkoopinformatie")
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        listed_label = st.selectbox("Vermeld energielabel", ENERGY_LABELS)
    with fc2:
        days_for_sale = st.number_input("Dagen te koop", min_value=0, max_value=3650, step=1, value=0)
    with fc3:
        condition = st.selectbox("Staat van de woning", HOUSE_CONDITIONS)

    renovation = st.multiselect(
        "Renovatiebehoeften",
        options=RENOVATION_NEEDS,
        placeholder="Selecteer één of meer behoeften…",
    )

st.markdown("")
analyze = st.button("Analyseer woning 🔍", type="primary", use_container_width=True, disabled=not address.strip())

# ── run & display ─────────────────────────────────────────────────────────────

if analyze and address.strip():
    user_input = {
        "house_type": house_type,
        "te_koop": te_koop,
        "listed_label": listed_label,
        "days_for_sale": days_for_sale,
        "condition": condition,
        "renovation": renovation,
    }

    with st.spinner("Gegevens ophalen… (eerste keer kan Leefbaarometer-data gedownload worden)"):
        try:
            result = run_fetch(address.strip())
            st.session_state["result"] = result
            st.session_state["user_input"] = user_input
            st.session_state["address"] = address.strip()
        except Exception as exc:
            st.error(f"Fout bij ophalen: {exc}")
            st.session_state.pop("result", None)

if "result" in st.session_state:
    show_results(st.session_state["result"], st.session_state.get("user_input", {}))
