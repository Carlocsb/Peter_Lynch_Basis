import streamlit as st
import pandas as pd
import sys
import os

# Pfad-Setup
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from src.funktionen import (
    get_es_connection,
    render_source_selector,
    suche_aktie_in_es,
    lade_historische_kennzahlen,
    zeige_kennzahlverlauf,
    berechne_peter_lynch_kategorie,
    berechne_kennzahlen_tabelle,
    beschreibe_kennzahlen
)

# === 1️⃣ Setup ===
st.set_page_config(page_title="Aktiensuche", layout="wide")
st.sidebar.image("assets/Logo-TH-Köln1.png", caption="")
st.title("🔍 Aktiensuche und Kennzahlenanzeige")
st.markdown("Bitte gib das **Ticker-Symbol** einer Aktie ein (z. B. AAPL, MSFT, NVDA):")

# === 2️⃣ Elasticsearch Verbindung & Datenquellen-Umschalter ===
es = get_es_connection()
source_mode = render_source_selector()   # Sidebar-Umschalter für Datenquelle

# === 3️⃣ Eingabefeld ===
raw = st.text_input("", placeholder="z. B. AAPL oder TSLA")
suchbegriff = (raw or "").strip().upper()

# Styling für Eingabefeld
st.markdown("""
<style>
    input {
        color: white;
        background-color: #1e1e1e;
    }
</style>
""", unsafe_allow_html=True)

# === 4️⃣ Hauptanzeige ===
if suchbegriff:
    daten = suche_aktie_in_es(es, suchbegriff, source_mode)  # Modus mitgeben

    if daten:
        st.subheader(f"📊 Kennzahlen für: {daten.get('symbol','N/A')}")
        st.caption(
            f"Datenquelle (Dokument): {daten.get('source','—')} • "
            f"Modus: {source_mode} • "
            f"Datum: {daten.get('date','N/A')}"
        )

        # === Hauptkennzahlen ===
        col1, col2, col3 = st.columns(3)
        col1.metric("🏭 Branche", daten.get("industry", "—"))
        col2.metric("💼 Sektor", daten.get("sector", "—"))
        col3.metric(
            "💰 Marktkapitalisierung",
            f"{daten.get('marketCap', 0)/1e9:.2f} Mrd USD" if daten.get("marketCap") else "—"
        )

        col4, col5, col6 = st.columns(3)
        col4.metric("📈 KGV (PE Ratio)", round(daten.get("peRatio", 0), 2) if daten.get("peRatio") else "—")
        col5.metric("🏦 Buchwert/Aktie", round(daten.get("bookValuePerShare", 0), 2) if daten.get("bookValuePerShare") else "—")
        col6.metric("📉 Preis/Buchwert", round(daten.get("priceToBook", 0), 2) if daten.get("priceToBook") else "—")

        col7, col8, col9 = st.columns(3)
        col7.metric("💸 Dividendenrendite", f"{daten.get('dividendYield')*100:.2f} %" if daten.get("dividendYield") else "—")
        col8.metric("📊 Gewinn/Aktie (EPS)", round(daten.get("eps", 0), 2) if daten.get("eps") else "—")
        col9.metric("⚖️ Verschuldungsgrad (Debt/Equity)", round(daten.get("debtToEquity", 0), 2) if daten.get("debtToEquity") else "—")
        
        # === 🧭 Peter-Lynch-Kategorisierung ===
        st.markdown("---")
        st.markdown("### 🧭 Peter-Lynch-Kategorisierung")

        kategorie_text, trefferquote = berechne_peter_lynch_kategorie(daten)

        st.success(
            f"🏷️ Diese Aktie gehört wahrscheinlich zur Kategorie **{kategorie_text}**, "
            f"weil sie {trefferquote}% der Kriterien erfüllt. "
        )


        # === 📈 Verlauf ausgewählter Kennzahlen ===
        st.markdown("---")
        st.markdown("### 📈 Verlauf ausgewählter Kennzahlen")

        colA, colB, colC = st.columns(3)
        if colA.button("KGV-Verlauf anzeigen"):
            df = lade_historische_kennzahlen(es, daten["symbol"], "peRatio", source_mode)
            fig = zeige_kennzahlverlauf(df, daten["symbol"], "KGV (PE Ratio)")
            if fig: st.plotly_chart(fig, use_container_width=True)

        if colB.button("EPS-Verlauf anzeigen"):
            df = lade_historische_kennzahlen(es, daten["symbol"], "eps", source_mode)
            fig = zeige_kennzahlverlauf(df, daten["symbol"], "Gewinn je Aktie (EPS)")
            if fig: st.plotly_chart(fig, use_container_width=True)

        if colC.button("Preis/Buchwert-Verlauf anzeigen"):
            df = lade_historische_kennzahlen(es, daten["symbol"], "priceToBook", source_mode)
            fig = zeige_kennzahlverlauf(df, daten["symbol"], "Preis/Buchwert")
            if fig: st.plotly_chart(fig, use_container_width=True)

        colD, colE, colF = st.columns(3)
        if colD.button("Dividendenrendite-Verlauf"):
            df = lade_historische_kennzahlen(es, daten["symbol"], "dividendYield", source_mode)
            fig = zeige_kennzahlverlauf(df, daten["symbol"], "Dividendenrendite", "%")
            if fig: st.plotly_chart(fig, use_container_width=True)

        if colE.button("Verschuldungsgrad-Verlauf"):
            df = lade_historische_kennzahlen(es, daten["symbol"], "debtToEquity", source_mode)
            fig = zeige_kennzahlverlauf(df, daten["symbol"], "Debt/Equity-Ratio")
            if fig: st.plotly_chart(fig, use_container_width=True)

        if colF.button("Free Cash Flow-Verlauf"):
            df = lade_historische_kennzahlen(es, daten["symbol"], "freeCashFlow", source_mode)
            fig = zeige_kennzahlverlauf(df, daten["symbol"], "Free Cash Flow", "USD")
            if fig: st.plotly_chart(fig, use_container_width=True)

        # === 🧩 Weitere Kennzahlen ===
        st.markdown("---")
        st.markdown("### 🧩 Weitere Kennzahlen")
        df_tbl = berechne_kennzahlen_tabelle(daten)
        st.dataframe(df_tbl, use_container_width=True)

        # === 📘 Beschreibung wichtiger Kennzahlen ===
        st.markdown("---")
        st.markdown("### 📘 Beschreibung wichtiger Kennzahlen")
        for key, text in beschreibe_kennzahlen().items():
            if daten.get(key) is not None:
                st.markdown(f"**{key}** – {text}")

else:
    st.info("🔎 Bitte gib oben ein Ticker-Symbol ein (z. B. AAPL, MSFT, TSLA).")

