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

    tab_samen, tab_woz, tab_buurt, tab_bouw, tab_raw = st.tabs(
        ["📊 Samenvatting", "💰 WOZ-waarde", "🏘️ Buurt & leefbaarheid", "🏗️ Bouwtechnisch", "🔍 Ruwe data"]
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

    # ── tab 4: bouwtechnisch ──────────────────────────────────────────────────
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
