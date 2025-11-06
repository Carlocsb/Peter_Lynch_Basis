import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
import uuid
from src.lynch_criteria import CATEGORIES
from src.funktionen import (
    get_es_connection, load_data_from_es, load_industries, score_row,
    ensure_portfolio_index, build_portfolio_doc, save_portfolio,
    list_portfolios, load_portfolio, delete_portfolio,
    render_source_selector,   # 👈 NEU: Datenquellen-Umschalter
)

# -------- Session-Seed für stabile Widget-Keys --------
if "widget_seed" not in st.session_state:
    st.session_state["widget_seed"] = str(uuid.uuid4())

# Pfad-Setup (Pages → src importierbar machen)
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# === 1️⃣ Setup UI ===
st.set_page_config(page_title="Portfolio-Erstellung", layout="wide")
st.sidebar.image("assets/Logo-TH-Köln1.png")
st.title("📁 Portfolio-Zusammenstellung nach Peter Lynch")

SAVE_PATH = "portfolio_speichern.csv"

# === 2️⃣ Verbindung & Datenquellen-Umschalter ===
es = get_es_connection()
source_mode = render_source_selector("📡 Datenquelle")          # 👈 Sidebar-Umschalter
ensure_portfolio_index(es)

# Daten laden – jeweils mit source_mode
df_industries = load_industries(es, source_mode=source_mode)
df_stocks = load_data_from_es(es, source_mode=source_mode)

# 👇👇👇 HYDRATION: geladene Werte EINMALIG vor Widget-Erzeugung in den Session State schreiben
if "hydrate_payload" in st.session_state:
    payload = st.session_state.pop("hydrate_payload")  # einmalig verwenden
    st.session_state["current_portfolio_id"] = payload.get("portfolio_id")
    st.session_state["portfolio_name"] = payload.get("portfolio_name", "")
    # Auswahl je Kategorie (für multiselect-Defaults)
    for cat, tickers in payload.get("auswahl", {}).items():
        st.session_state[f"ms_{cat}"] = list(dict.fromkeys(tickers))  # dedupe, Reihenfolge beibehalten
    # Beträge je Symbol (für number_input-Defaults)
    for cat, symvals in payload.get("betraege", {}).items():
        for sym, amt in symvals.items():
            st.session_state[f"amt_{cat}_{sym}"] = float(amt)

# === 3️⃣ Branchenfilter ===
st.sidebar.header("🔍 Filter")
industries = ["Alle"] + sorted(df_industries["industry"].dropna().unique().tolist()) if not df_industries.empty else ["Alle"]
selected_industry = st.sidebar.selectbox("Branche", industries)

# === 4️⃣ Strategien nach Marktlage ===
strategien = {
    "Markt fällt": {"Slow Grower": 30, "Stalwarts": 25, "Fast Grower": 10, "Cyclicals": 10, "Turn Around": 10, "Assets Player": 15},
    "Seitwärtsmarkt": {"Slow Grower": 20, "Stalwarts": 25, "Fast Grower": 20, "Cyclicals": 15, "Turn Around": 10, "Assets Player": 10},
    "Markt boomt": {"Slow Grower": 10, "Stalwarts": 15, "Fast Grower": 35, "Cyclicals": 20, "Turn Around": 10, "Assets Player": 10}
}
ALIAS = {
    "Slow Grower": "Slow Growers",
    "Stalwarts": "Stalwarts",
    "Fast Grower": "Fast Growers",
    "Cyclicals": "Cyclicals",
    "Turn Around": "Turnarounds",
    "Assets Player": "Asset Plays",
}

marktlage = st.selectbox("📉 Wähle die aktuelle Marktlage:", list(strategien.keys()))
verteilung = strategien[marktlage]

col1, col2 = st.columns([1, 1])
with col1:
    st.subheader("📋 Empfohlene Verteilung:")
    for k, v in verteilung.items():
        st.write(f"- **{v}%** {k}")
with col2:
    st.subheader("📊 Visuelle Darstellung")
    fig, ax = plt.subplots(figsize=(3.5, 3.5))
    ax.pie(verteilung.values(), labels=verteilung.keys(), autopct='%1.1f%%', startangle=90)
    ax.axis("equal")
    st.pyplot(fig)

# === 6️⃣ Top-Aktien je Kategorie (aus gewählter Quelle) ===
top10_by_category = {}
for cat_label in verteilung.keys():
    rules = CATEGORIES[ALIAS.get(cat_label, cat_label)]
    df_temp = df_stocks.copy()
    if selected_industry != "Alle" and "industry" in df_temp.columns:
        df_temp = df_temp[df_temp["industry"] == selected_industry]
    if df_temp.empty:
        top10_by_category[cat_label] = []
        continue
    df_temp["Score"] = df_temp.apply(lambda r: score_row(r, rules), axis=1)
    top10_by_category[cat_label] = (
        df_temp.sort_values("Score", ascending=False)
               .head(10)["symbol"].dropna().drop_duplicates().tolist()
    )

# === 7️⃣ Auswahl (links) & Zusammenfassung (rechts) ===
col_left, col_right = st.columns([1.6, 1])

with col_left:
    st.subheader("🔎 Aktienauswahl je Kategorie")
    ausgewaehlte_aktien = {}

    for k in verteilung:
        # Basis-Optionen (Top 10)
        options_base = top10_by_category.get(k, [])
        # bisherige Auswahl (aus Session/Hydration)
        current_selection = list(dict.fromkeys(st.session_state.get(f"ms_{k}", [])))
        # nur gültige Symbole zulassen
        if not df_stocks.empty and "symbol" in df_stocks.columns:
            valid = set(df_stocks["symbol"].dropna().unique())
            current_selection = [t for t in current_selection if t in valid]
        # Defaults in options aufnehmen (Union)
        options = list(dict.fromkeys(options_base + [t for t in current_selection if t not in options_base]))

        # Optional: Kategorie-Namen für Keys säubern
        safe_k = k.replace(" ", "_")

        selected = st.multiselect(
            f"{k} – Wähle Aktien aus:",
            options=options,
            default=current_selection,
            key=f"ms_{safe_k}_{st.session_state['widget_seed']}",
        )
        selected = list(dict.fromkeys(selected))
        ausgewaehlte_aktien[k] = selected
        if len(selected) != len(set(selected)):
            st.info(f"🔁 Doppelte Ticker in **{k}** wurden entfernt.")

    # CSS für Expander
    st.markdown("""
    <style>
      div[data-testid="stExpander"] div[role="button"] p {
        font-size: 1.4rem !important;
        font-weight: 700 !important;
        color: #ffffff !important;
        margin: 0 !important;
      }
    </style>
    """, unsafe_allow_html=True)

    # 💵 Beträge sammeln (bleibt links!)
    betraege = {}
    with st.expander("💵 Betrag pro ausgewählter Aktie – klicken zum Ein-/Ausklappen", expanded=False):
        suchtext = st.text_input("🔎 Ticker-Suche (Filter innerhalb der Auswahl)", "", key="betrag_suche_global").lower().strip()
        for k, tickers in ausgewaehlte_aktien.items():
            filtered = [t for t in tickers if suchtext in t.lower()] if suchtext else tickers
            filtered = list(dict.fromkeys(filtered))
            if not filtered:
                continue
            st.markdown(f"### {k}")
            cols = st.columns(3)
            betraege[k] = {}
            for i, t in enumerate(filtered):
                with cols[i % 3]:
                    betraege[k][t] = st.number_input(
                        f"{t} – Betrag",
                        min_value=0,
                        step=100,
                        value=int(st.session_state.get(f"amt_{k}_{t}", 0)),
                        key=f"amt_{k}_{t}_{st.session_state['widget_seed']}",
                    )

with col_right:
    st.subheader("📈 Aktuelle Portfolio-Zusammensetzung (nach Betrag)")
    # Summen je Kategorie & Gesamt
    sum_cat = {k: sum(betraege.get(k, {}).values()) for k in verteilung}
    total_amt = sum(sum_cat.values())

    if total_amt > 0:
        aktuelle = {k: round((sum_cat[k] / total_amt) * 100, 1) for k in verteilung}
        df_vergleich = pd.DataFrame([
            {
                "Kategorie": k,
                "Empfohlen (%)": verteilung[k],
                "Investiert (USD)": round(sum_cat[k], 2),
                "Aktuell (%)": round(aktuelle.get(k, 0), 1),
                "Differenz (%)": round(aktuelle.get(k, 0) - verteilung[k], 1),
            }
            for k in verteilung
        ])
        st.dataframe(
            df_vergleich.style.format({
                "Investiert (USD)": "{:,.2f}",
                "Empfohlen (%)": "{:.1f}",
                "Aktuell (%)": "{:.1f}",
                "Differenz (%)": "{:+.1f}",
            }),
            use_container_width=True
        )
        st.metric("Gesamt investiert", f"{total_amt:,.2f} USD")
    else:
        st.info("Noch keine Beträge erfasst.")

# === 8️⃣ Portfolios verwalten (Elasticsearch CRUD) ===
st.markdown("---")
st.subheader("🗂 Portfolios verwalten")

# Kommentar-Feld (bleibt)
begruendung = st.text_area("📝 Bemerkung: Auf was muss ich achten?")

# Name + vorhandene Portfolios
col_a, col_b = st.columns([2, 2])
with col_a:
    portfolio_name = st.text_input("📛 Portfolio-Name", value=st.session_state.get("portfolio_name", ""))
with col_b:
    vorhandene = list_portfolios(es)
    labels = ["—"] + [
        f'{p["name"]} — {p.get("market_condition","")} — {p.get("totals",{}).get("total_amount",0):,.2f} USD'
        for p in vorhandene
    ]
    selected_label = st.selectbox("Vorhandenes Portfolio laden:", labels, index=0)

# Session-State für aktives Portfolio
if "current_portfolio_id" not in st.session_state:
    st.session_state["current_portfolio_id"] = None

# Buttons
c1, c2, c3, c4 = st.columns(4)
with c1: load_clicked = st.button("📥 Laden")
with c2: save_new_clicked = st.button("💾 Neu speichern")
with c3: update_clicked = st.button("🔁 Aktualisieren")
with c4: delete_clicked = st.button("🗑️ Löschen")

# Aktionen
if load_clicked and selected_label != "—":
    idx = labels.index(selected_label) - 1
    p = vorhandene[idx]
    data = load_portfolio(es, p["id"])
    if data:
        # 🚫 Widgets existieren schon → Hydrations-Payload + rerun
        gespeicherte_auswahl = {}
        gespeicherte_betraege = {}
        for it in data.get("items", []):
            gespeicherte_auswahl.setdefault(it["category"], []).append(it["symbol"])
            gespeicherte_betraege.setdefault(it["category"], {})[it["symbol"]] = float(it["amount"])

        st.session_state["hydrate_payload"] = {
            "portfolio_id": p["id"],
            "portfolio_name": data.get("name", ""),
            "auswahl": gespeicherte_auswahl,
            "betraege": gespeicherte_betraege,
        }
        st.success(f'Portfolio "{data.get("name")}" geladen.')
        st.rerun()
    else:
        st.error("Portfolio nicht gefunden.")

if save_new_clicked:
    if not portfolio_name.strip():
        st.warning("Bitte Portfolio-Name angeben.")
    else:
        doc = build_portfolio_doc(
            name=portfolio_name.strip(),
            marktlage=marktlage,
            selected_industry=selected_industry,
            comment=begruendung,
            ausgewaehlte_aktien=ausgewaehlte_aktien,
            betraege=betraege,
            verteilung=verteilung
        )
        new_id = save_portfolio(es, doc)
        st.session_state["hydrate_payload"] = {
            "portfolio_id": new_id,
            "portfolio_name": doc.get("name", ""),
            "auswahl": ausgewaehlte_aktien,
            "betraege": betraege,
        }
        st.session_state["widget_seed"] = str(uuid.uuid4())
        st.success(f'Portfolio "{doc.get("name","")}" gespeichert.')
        st.rerun()

if update_clicked:
    pid = st.session_state.get("current_portfolio_id")
    if not pid:
        st.warning("Kein Portfolio geladen. Bitte zuerst laden oder neu speichern.")
    elif not portfolio_name.strip():
        st.warning("Bitte Portfolio-Name angeben.")
    else:
        doc = build_portfolio_doc(
            name=portfolio_name.strip(),
            marktlage=marktlage,
            selected_industry=selected_industry,
            comment=begruendung,
            ausgewaehlte_aktien=ausgewaehlte_aktien,
            betraege=betraege,
            verteilung=verteilung
        )
        save_portfolio(es, doc, portfolio_id=pid)
        st.success("Portfolio aktualisiert.")
        st.rerun()

if delete_clicked:
    pid = st.session_state.get("current_portfolio_id")
    if not pid:
        st.warning("Kein Portfolio geladen.")
    else:
        if delete_portfolio(es, pid):
            st.success("Portfolio gelöscht.")
            st.session_state["hydrate_payload"] = {
                "portfolio_id": None,
                "portfolio_name": "",
                "auswahl": {},
                "betraege": {},
            }
            st.rerun()
        else:
            st.error("Löschen fehlgeschlagen.")
