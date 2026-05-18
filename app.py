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

    # ── address header ────────────────────────────────────────────────────────
    if a:
        st.subheader(f"📍 {a.weergavenaam}")
        cols = st.columns(4)
        cols[0].metric("Postcode", a.postcode or "–")
        cols[1].metric("Gemeente", a.gemeentenaam or "–")
        cols[2].metric("Buurt", a.buurtnaam or "–")
        cols[3].metric(
            "Woningtype (gebruiker)",
            user_input.get("house_type", "–"),
        )
    else:
        st.error("Adres niet gevonden in de BAG. Controleer de invoer.")
        return

    tab_samen, tab_woz, tab_buurt, tab_omgeving, tab_bouw, tab_raw = st.tabs(
        ["📊 Samenvatting", "💰 WOZ-waarde", "🏘️ Buurt & leefbaarheid", "🏫 Omgeving", "🏗️ Bouwtechnisch", "🔍 Ruwe data"]
    )

    # ── tab 1: samenvatting ───────────────────────────────────────────────────
    with tab_samen:
        st.markdown("##### Kerncijfers")
        c1, c2, c3, c4, c5, c6 = st.columns(6)

        # Real WOZ (Kadaster) — validation target
        real_woz = mr.vastgesteldeWaarde if mr else None
        c1.metric(
            "WOZ-waarde (Kadaster)",
            _fmt_eur(real_woz),
            help=f"Peildatum: {mr.peildatum}" if mr else "Niet beschikbaar",
        )

        # Estimated WOZ (our model)
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

        # Feature readiness table
        st.markdown("##### Model features")
        rows = []

        def feat(var, source, value, status=""):
            ok = value not in (None, "–", "")
            rows.append({
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

        df_feat = pd.DataFrame(rows)
        st.dataframe(df_feat, use_container_width=True, hide_index=True)

        # ── marktdynamiek – PBK COROP heat indicator ──────────────────────────
        st.markdown("##### Marktdynamiek")
        if a and a.gemeentecode:
            with st.spinner("Prijsindex COROP ophalen (CBS)…"):
                try:
                    from watmoetikbieden.sources.pbk_corop import fetch_pbk_corop
                    import plotly.graph_objects as go
                    pbk = fetch_pbk_corop(a.gemeentecode)
                except Exception as exc:
                    pbk = None
                    st.warning(f"Marktdynamiek niet beschikbaar: {exc}")

            if pbk and pbk.latest:
                latest  = pbk.latest
                nat     = pbk.national_latest
                delta   = pbk.heat_delta_pct

                # Fetch building stats — use 32q so that even if CBS has released
                # Q1/Q2 of the current year, the window still includes all 4 quarters
                # of 2020 (needed to compute annual transaction totals for supply pressure).
                pbk24 = fetch_pbk_corop(a.gemeentecode, n_quarters=32)
                try:
                    from watmoetikbieden.sources.building_stats import fetch_building_stats
                    bouw = fetch_building_stats(a.gemeentecode, pbk24)
                except Exception:
                    bouw = None

                _gm_label    = a.gemeentenaam or a.gemeentecode or (bouw.gemeente_code if bouw else "")
                _corop_label = bouw.corop_name.replace(" (CR)", "") if bouw else pbk.corop_name.replace(" (CR)", "")

                mc1, mc2, mc3, mc4, mc5 = st.columns(5)
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

                # ── price trend chart: COROP YoY% vs national + COROP share ───
                # Align corop and national by period so the share ratio is correct
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
                # Quarter-over-quarter change in market share (pp) — velocity signal
                tx_share = [None] + [
                    round(_tx_share_abs[i] - _tx_share_abs[i - 1], 3)
                    if (_tx_share_abs[i] is not None and _tx_share_abs[i - 1] is not None)
                    else None
                    for i in range(1, len(_tx_share_abs))
                ]
                _corop_short = pbk.corop_name.replace(" (CR)", "")

                fig_pbk = go.Figure()

                # COROP share velocity bars on secondary y-axis (behind price lines)
                fig_pbk.add_trace(go.Bar(
                    x=periods, y=tx_share,
                    name="Δ COROP-aandeel (pp QoQ)",
                    marker_color=[
                        "rgba(44,160,44,0.30)" if (v is not None and v >= 0) else "rgba(214,39,40,0.25)"
                        for v in tx_share
                    ],
                    yaxis="y2",
                    hovertemplate="%{x}: %{y:+.3f}pp<extra>Δ COROP-aandeel</extra>",
                ))

                # Price YoY% lines on primary y-axis
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
                st.plotly_chart(fig_pbk, use_container_width=True)

                # ── building activity expander ────────────────────────────────
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
                        # Supply pressure on secondary y-axis
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
                        st.plotly_chart(fig_bouw, use_container_width=True)
                        st.caption(
                            f"Nieuwbouw/sloop: gemeente-niveau ({_gm_label}). "
                            f"Aanboddruk = gemeente-nieuwbouw / COROP-transacties ({_corop_label}) × 100. "
                            f"Aanbodlabel: **{bouw.supply_label}**. "
                            "Bron: CBS 86054NED · 85819NED · NLOD"
                        )

                        # ── woningtype breakdown (COROP, 86084NED) ───────────
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
                            st.plotly_chart(fig_type, use_container_width=True)
                            st.caption("Bron: CBS 86084NED · NLOD")

                st.caption("Bron: CBS PBK 85819NED · 85773NED · 86054NED · Kadaster · NLOD")
            else:
                st.info("Geen COROP-data beschikbaar voor dit adres.")
        else:
            st.info("Gemeentecode ontbreekt — marktdynamiek niet beschikbaar.")

    # ── tab 2: woz ───────────────────────────────────────────────────────────
    with tab_woz:
        import plotly.graph_objects as go
        from watmoetikbieden.market_indicators import lending_capacity_series, WOONQUOTE, TERM_YEARS

        # ── fetch all series first so we can combine into one chart ───────────
        # House-specific WOZ history
        woz_pts: list[tuple[int, int]] = []
        if w and w.history:
            woz_pts = sorted((h["jaar"], h["waarde"]) for h in w.history if h["waarde"])
            if w.is_approximation:
                st.warning("⚠️ Gemeente-gemiddelde gebruikt als benadering (geen individuele waarde beschikbaar)")

        # Buurt average WOZ series
        buurt_series: dict | None = None
        if a and a.buurtcode:
            with st.spinner("CBS gem. WOZ buurt ophalen…"):
                from watmoetikbieden.sources.cbs_woz_longitudinal import fetch_gem_woz_series
                wijkcode = a.wijkcode or None
                gemeentecode = f"GM{a.gemeentecode}" if a.gemeentecode else None
                buurt_series = fetch_gem_woz_series(a.buurtcode, wijkcode, gemeentecode)

        # Lending capacity series
        lc_rows = lending_capacity_series()

        if woz_pts or buurt_series or lc_rows:
            st.markdown("##### WOZ-waarde & maximale hypotheek bij modaal inkomen")

            fig_woz = go.Figure()

            if woz_pts:
                years_woz, vals_woz = zip(*woz_pts)
                fig_woz.add_trace(go.Scatter(
                    x=years_woz, y=vals_woz,
                    mode="lines+markers",
                    name="WOZ dit adres",
                    line=dict(color="#1f77b4", width=2),
                    marker=dict(size=5),
                    hovertemplate="%{x}: € %{y:,.0f}<extra>WOZ dit adres</extra>",
                ))

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
                ))

            if lc_rows:
                df_lc = pd.DataFrame(lc_rows)
                fig_woz.add_trace(go.Scatter(
                    x=df_lc["year"], y=df_lc["max_mortgage"],
                    mode="lines+markers",
                    name="Max. hypotheek (modaal)",
                    line=dict(color="#2ca02c", width=2, dash="dash"),
                    marker=dict(size=4),
                    hovertemplate="%{x}: € %{y:,.0f}<extra>Max. hypotheek modaal</extra>",
                ))

            fig_woz.update_layout(
                yaxis=dict(title="Waarde (€)", tickformat=",.0f", autorange=True),
                xaxis=dict(title="Jaar", tickformat="d"),
                height=380,
                margin=dict(l=10, r=10, t=10, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
                hovermode="x unified",
            )
            st.plotly_chart(fig_woz, use_container_width=True)

            if lc_rows:
                st.caption(
                    f"Max. hypotheek: annuïteit {TERM_YEARS} jr, woonquote {WOONQUOTE*100:.0f}% bruto, "
                    "DNB-rente jaargemiddelde. Modaal inkomen: CBS/hardcoded."
                )
        elif w and not w.history:
            st.info("Geen WOZ-data beschikbaar voor dit adres.")

        # ── lending capacity detail expander ──────────────────────────────────
        if lc_rows:
            with st.expander("Leencapaciteit – onderliggende data"):
                df_lc_exp = pd.DataFrame(lc_rows)
                mc1, mc2 = st.columns(2)
                with mc1:
                    st.markdown("**Rente (jaargemiddelde DNB, %)**")
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
                    st.plotly_chart(fig_r, use_container_width=True)
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
                    st.plotly_chart(fig_inc, use_container_width=True)
                st.dataframe(
                    df_lc_exp.rename(columns={
                        "year": "Jaar", "modal_income": "Modaal inkomen (€)",
                        "rate_totaal": "Rente % (DNB)", "max_mortgage": "Max. hypotheek (€)",
                    })[["Jaar", "Modaal inkomen (€)", "Rente % (DNB)", "Max. hypotheek (€)"]],
                    use_container_width=True,
                    hide_index=True,
                )

        # ── model validation ──────────────────────────────────────────────────
        if woz_est and woz_est.estimated_value:
            st.divider()
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

            # Model details
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

                # Relatives table: regional β values when available, else national
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

    # ── tab 3: buurt & leefbaarheid ───────────────────────────────────────────
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

                    _DIM_LABELS = {
                        "lbm": "Totaal leefbaarheid",
                        "fys": "Fysieke omgeving",
                        "onv": "Onveiligheid",
                        "soc": "Sociaal",
                        "vrz": "Voorzieningen",
                        "won": "Woningen",
                    }
                    national_means = lbm.national_means_2024

                    st.markdown("**Historiek per dimensie**")
                    dims = list(_DIM_LABELS.keys())
                    # Render 2 charts per row
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
                                    showlegend=False,
                                    xaxis=dict(tickformat="d"),
                                    yaxis=dict(autorange=True),
                                )
                                st.plotly_chart(fig, use_container_width=True)
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

                # Woningtype verdeling
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

    # ── tab 4: omgeving – scholen & kinderopvang ─────────────────────────────
    with tab_omgeving:
        from watmoetikbieden.sources.nearby_schools import (
            fetch_nearby_schools, enrich_with_duo_ratings,
            parse_bag_centroide, DEFAULT_RADIUS_M,
        )

        coords = parse_bag_centroide(a.centroide_ll) if a else None

        if not coords:
            st.info("Geen coördinaten beschikbaar voor dit adres — kan omgeving niet ophalen.")
        else:
            lat, lon = coords
            radius_m = DEFAULT_RADIUS_M
            gemeente_naam = a.gemeentenaam or ""

            st.markdown(f"##### Scholen & kinderopvang binnen {radius_m // 1000} km")
            st.caption(f"Bron: OpenStreetMap / Overpass API · DUO oordelen t/m 2018 · straal {radius_m:,} m · looptijd op basis van 5 km/u")

            # Fetch from Overpass (7-day file cache — fast after first call)
            schools: list = []
            try:
                with st.spinner("Scholen ophalen via OpenStreetMap…"):
                    schools = fetch_nearby_schools(lat, lon, radius_m)
            except Exception as exc:
                st.error(f"Kon scholen niet ophalen: {exc}")

            if schools:
                # Enrich with DUO ratings (module-level cache — fast after first load)
                try:
                    with st.spinner("DUO schooloordelen koppelen…"):
                        schools = enrich_with_duo_ratings(schools, gemeente_naam)
                except Exception as exc:
                    st.warning(f"DUO-oordelen niet beschikbaar: {exc}")

                # Group by category, descending from higher to lower education
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
                # Categories for which DUO PO/VO enrichment is attempted
                DUO_CATEGORIES = {"Basisschool", "Kindcentrum / IKC", "Middelbare school", "School (onbekend type)"}

                from collections import defaultdict
                by_cat: dict[str, list] = defaultdict(list)
                for s in schools:
                    by_cat[s.category].append(s)

                # Render present categories in order, then any unlisted ones
                present_ordered = [c for c in CATEGORY_ORDER if c in by_cat]
                present_ordered += [c for c in by_cat if c not in CATEGORY_ORDER]

                # Always use the same fixed column set so all tables share identical widths
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
                    rows = []
                    for s in items:
                        rows.append({
                            "Naam":               s.name,
                            "Afstand":            f"{s.distance_m} m",
                            "Looptijd":           f"~{s.walk_min} min",
                            "Oordeel (t/m 2018)": (s.rating_label or "–") if show_duo else "–",
                            "Denominatie":        (s.denominatie or "–") if show_duo else "–",
                            "Beheerder":          s.operator or "–",
                            "Website":            s.website or "–",
                        })
                    st.dataframe(
                        pd.DataFrame(rows, columns=ALL_COLS),
                        column_config=COL_CFG,
                        use_container_width=True,
                        hide_index=True,
                    )
            else:
                st.info(f"Geen scholen gevonden binnen {radius_m:,} m.")

    # ── tab 5: bouwtechnisch ──────────────────────────────────────────────────
    with tab_bouw:
        c1, c2 = st.columns(2)
        with c1:
            st.markdown("##### BAG verblijfsobject")
            if v:
                st.json(v.model_dump(exclude_none=True))
            else:
                st.info("Geen verblijfsobject-data.")

            st.markdown("##### BAG pand")
            if p:
                st.json(p.model_dump(exclude_none=True))
            else:
                st.info("Geen pand-data.")

        with c2:
            st.markdown("##### Energielabel (EP-Online)")
            if e:
                st.json(e.model_dump(exclude_none=True))
            elif data.ep_no_label:
                st.info("Geen energiecertificaat geregistreerd (gebruikelijk voor woningen van vóór 2008).")
            else:
                st.info("Niet beschikbaar.")

            st.markdown("##### BRK perceel")
            if b:
                st.json(b.model_dump(exclude_none=True))
            else:
                st.info("Geen perceeldata beschikbaar.")

    # ── tab 5: ruwe data ─────────────────────────────────────────────────────
    with tab_raw:
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
