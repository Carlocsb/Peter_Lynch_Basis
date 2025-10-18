import streamlit as st
import pandas as pd
import os
from elasticsearch import Elasticsearch
import plotly.express as px

# === 1️⃣ Setup ===
st.set_page_config(page_title="Aktiensuche", layout="wide")
st.sidebar.image("assets/Logo-TH-Köln1.png", caption="")
st.title("🔍 Aktiensuche und Kennzahlenanzeige")
st.markdown("Bitte gib das **Ticker-Symbol** einer Aktie ein (z. B. AAPL, MSFT, NVDA):")

# === 2️⃣ Elasticsearch Verbindung ===
ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
INDEX = "stocks"

@st.cache_resource
def get_es_connection():
    es = Elasticsearch(ES_URL, request_timeout=30)
    if not es.ping():
        st.error("❌ Verbindung zu Elasticsearch fehlgeschlagen!")
    return es

es = get_es_connection()
def suche_aktie_in_es(symbol: str):
    """Sucht Aktie in Elasticsearch nach Symbol (robust, unabhängig vom Mapping)"""
    query = {
        "size": 1,
        "query": {
            "bool": {
                "should": [
                    {"term": {"symbol.keyword": symbol}},  # funktioniert falls keyword vorhanden
                    {"term": {"symbol": symbol}},          # fallback falls kein .keyword existiert
                    {"match": {"symbol": symbol}}          # zusätzlicher fuzzy fallback
                ],
                "minimum_should_match": 1
            }
        },
        "sort": [{"date": {"order": "desc"}}]
    }

    resp = es.search(index=INDEX, body=query)
    hits = resp.get("hits", {}).get("hits", [])
    return hits[0]["_source"] if hits else None




# === 3️⃣ Eingabefeld ===
suchbegriff = st.text_input("", placeholder="z. B. AAPL oder TSLA").upper().strip()

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
    daten = suche_aktie_in_es(suchbegriff)

    if daten:
        st.subheader(f"📊 Kennzahlen für: {daten.get('symbol', 'N/A')}")
        st.caption(f"Quelle: Elasticsearch • Datum: {daten.get('date', 'N/A')}")

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

        

    # === Funktionen für historische Diagramme ===
    def lade_historische_kennzahlen(symbol, kennzahl):
        """Lädt historische Daten für eine bestimmte Kennzahl"""
        es = get_es_connection()
        query = {
            "size": 1000,
            "query": {"term": {"symbol": symbol}},
            "sort": [{"date": {"order": "asc"}}]
        }
        resp = es.search(index=INDEX, body=query)
        hits = resp.get("hits", {}).get("hits", [])
        daten = [
            {"Datum": h["_source"]["date"], "Wert": h["_source"].get(kennzahl)}
            for h in hits if kennzahl in h["_source"]
        ]
        return pd.DataFrame(daten)

    def zeige_kennzahlverlauf(symbol, kennzahl, titel, einheit=""):
        """Zeigt interaktives Zeitdiagramm für eine Kennzahl"""
        df = lade_historische_kennzahlen(symbol, kennzahl)
        if df.empty:
            st.info(f"Keine historischen Daten für **{titel}** verfügbar.")
            return
        fig = px.line(df, x="Datum", y="Wert", title=f"{titel}-Verlauf für {symbol}", markers=True)
        fig.update_layout(template="plotly_dark", hovermode="x unified")
        if einheit:
            fig.update_yaxes(title_text=einheit)
        st.plotly_chart(fig, use_container_width=True)

    # === Buttons für Zeitreihen ===
    st.markdown("---")
    st.markdown("### 📈 Verlauf ausgewählter Kennzahlen")

    colA, colB, colC = st.columns(3)
    if colA.button("KGV-Verlauf anzeigen"):
        zeige_kennzahlverlauf(daten["symbol"], "peRatio", "KGV (PE Ratio)")

    if colB.button("EPS-Verlauf anzeigen"):
        zeige_kennzahlverlauf(daten["symbol"], "eps", "Gewinn je Aktie (EPS)")

    if colC.button("Preis/Buchwert-Verlauf anzeigen"):
        zeige_kennzahlverlauf(daten["symbol"], "priceToBook", "Preis/Buchwert")

    colD, colE, colF = st.columns(3)
    if colD.button("Dividendenrendite-Verlauf"):
        zeige_kennzahlverlauf(daten["symbol"], "dividendYield", "Dividendenrendite", einheit="%")

    if colE.button("Verschuldungsgrad-Verlauf"):
        zeige_kennzahlverlauf(daten["symbol"], "debtToEquity", "Debt/Equity-Ratio")

    if colF.button("Free Cash Flow-Verlauf"):
        zeige_kennzahlverlauf(daten["symbol"], "freeCashFlow", "Free Cash Flow", einheit="USD")


        # === Weitere Kennzahlen als Tabelle ===
       
    st.markdown("---")
    st.markdown("### 🧩 Weitere Kennzahlen")

    details = {
        "Free Cashflow": daten.get("freeCashFlow"),
        "Umsatzwachstum": daten.get("revenueGrowth"),
        "Profit Margin": daten.get("profitMargin"),
        "Gesamtschulden": daten.get("totalDebt"),
        "Quick Ratio": daten.get("quickRatio"),
        "Current Ratio": daten.get("currentRatio"),
        "Cash/Aktie": daten.get("cashPerShare"),
        "Beta": daten.get("beta"),
    }

    # → DataFrame erstellen
    df = pd.DataFrame(details.items(), columns=["Kennzahl", "Wert"])

    # → sichere Rundung nur, wenn der Wert eine Zahl ist
    df["Wert"] = df["Wert"].apply(lambda x: round(x, 4) if isinstance(x, (int, float)) else "—")

    # → Tabelle anzeigen
    st.dataframe(df, use_container_width=True)

    # === Kennzahlenbeschreibung ===
    st.markdown("---")
    st.markdown("### 📘 Beschreibung wichtiger Kennzahlen")

    beschreibungen = {
        "peRatio": "Das Kurs-Gewinn-Verhältnis (KGV) zeigt, wie viel Anleger für 1 USD Gewinn zahlen.",
        "priceToBook": "Das Kurs-Buchwert-Verhältnis (P/B) vergleicht den Aktienkurs mit dem Buchwert.",
        "dividendYield": "Die Dividendenrendite zeigt, wie viel Prozent Dividende pro Jahr gezahlt wird.",
        "eps": "Earnings per Share (EPS) misst den Gewinn je Aktie.",
        "bookValuePerShare": "Der Buchwert pro Aktie zeigt den Eigenkapitalwert pro Anteil.",
        "debtToEquity": "Das Verhältnis von Schulden zu Eigenkapital; niedrigere Werte bedeuten geringeres Risiko.",
    }

    for key, text in beschreibungen.items():
        if daten.get(key) is not None:
            st.markdown(f"**{key}** – {text}")

   
            
else:
    st.info("🔎 Bitte gib oben ein Ticker-Symbol ein (z. B. AAPL, MSFT, TSLA).")
