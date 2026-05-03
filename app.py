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
        c1, c2, c3, c4, c5 = st.columns(5)

        c1.metric(
            "WOZ-waarde (meest recent)",
            _fmt_eur(mr.vastgesteldeWaarde) if mr else "–",
            help=f"Peildatum: {mr.peildatum}" if mr else None,
        )
        c2.metric(
            "Vloeroppervlak",
            f"{v.oppervlakte} m²" if (v and v.oppervlakte) else "–",
        )
        c3.metric(
            "Bouwjaar",
            str(v.bouwjaar) if (v and v.bouwjaar) else (str(p.bouwjaar) if (p and p.bouwjaar) else "–"),
        )
        c4.metric(
            "Energielabel (EP-Online)",
            e.Pand_energieklasse if e else ("Geen certificaat" if data.ep_no_label else "–"),
        )
        c5.metric(
            "Perceeloppervlak",
            f"{b.kadastraleGrootteWaarde} m²" if (b and b.kadastraleGrootteWaarde) else
            (f"{w.grondoppervlakte} m²" if (w and w.grondoppervlakte) else "–"),
            help="BRK kadaster" if (b and b.kadastraleGrootteWaarde) else ("WOZ-object" if (w and w.grondoppervlakte) else None),
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
        if w and w.history:
            st.markdown("##### WOZ-waarde historiek")
            if w.is_approximation:
                st.warning("⚠️ Gemeente-gemiddelde gebruikt als benadering (geen individuele waarde beschikbaar)")

            df_woz = pd.DataFrame(
                [(h["jaar"], h["waarde"]) for h in w.history if h["waarde"]],
                columns=["Jaar", "WOZ-waarde (€)"],
            ).sort_values("Jaar")
            st.line_chart(df_woz.set_index("Jaar"), y="WOZ-waarde (€)", use_container_width=True)

            # cbs gem woz history
            if a and a.buurtcode:
                with st.spinner("CBS gem. WOZ buurt ophalen…"):
                    from watmoetikbieden.sources.cbs_woz_longitudinal import fetch_gem_woz_series
                    wijkcode = a.wijkcode or None
                    gemeentecode = f"GM{a.gemeentecode}" if a.gemeentecode else None
                    buurt_series = fetch_gem_woz_series(a.buurtcode, wijkcode, gemeentecode)
                if buurt_series:
                    df_buurt_woz = pd.DataFrame(
                        sorted(buurt_series.items()), columns=["Jaar", "Gem. WOZ buurt (€)"]
                    )
                    st.markdown("##### Gem. WOZ-waarde buurt (CBS)")
                    st.line_chart(df_buurt_woz.set_index("Jaar"), y="Gem. WOZ buurt (€)", use_container_width=True)
        else:
            st.info("Geen WOZ-data beschikbaar voor dit adres.")

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
                    st.markdown("**Historiek**")
                    df_lhist = pd.DataFrame(
                        [(r["jaar"], r["lbm"]) for r in lbm.lbm_history if r["lbm"] is not None],
                        columns=["Jaar", "LBM-score"],
                    ).sort_values("Jaar")
                    st.line_chart(df_lhist.set_index("Jaar"), y="LBM-score", use_container_width=True)
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
