# pages/Portfolio.py
import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import os
from elasticsearch import Elasticsearch

# === 1️⃣ Grundkonfiguration ===
st.set_page_config(page_title="Portfolio-Erstellung", layout="wide")
st.sidebar.image("assets/Logo-TH-Köln1.png", caption="")
st.title("📁 Portfolio-Zusammenstellung nach Peter Lynch")

SAVE_PATH = "portfolio_speichern.csv"
ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
INDEX = "stocks"

# === 2️⃣ Elasticsearch-Verbindung ===
@st.cache_resource
def get_es_connection():
    es = Elasticsearch(ES_URL, request_timeout=30)
    if not es.ping():
        st.error("❌ Verbindung zu Elasticsearch fehlgeschlagen!")
    return es

# === 3️⃣ Branchen-Daten laden ===
@st.cache_data
def load_industries():
    es = get_es_connection()
    query = {"size": 1000, "_source": ["symbol", "industry"]}
    resp = es.search(index=INDEX, body=query)
    hits = [h["_source"] for h in resp["hits"]["hits"]]
    df = pd.DataFrame(hits)
    return df if not df.empty else pd.DataFrame(columns=["symbol", "industry"])

df_industries = load_industries()

# === 4️⃣ Branchen-Filter in Sidebar ===
st.sidebar.header("🔍 Filter")
if not df_industries.empty:
    industries = ["Alle"] + sorted(df_industries["industry"].dropna().unique().tolist())
    selected_industry = st.sidebar.selectbox("Branche", industries)
else:
    selected_industry = "Alle"

# === 5️⃣ Strategien nach Marktlage ===
strategien = {
    "Markt fällt": {"Slow Grower": 30, "Stalwarts": 25, "Fast Grower": 10, "Cyclicals": 10, "Turn Around": 10, "Assets Player": 15},
    "Seitwärtsmarkt": {"Slow Grower": 20, "Stalwarts": 25, "Fast Grower": 20, "Cyclicals": 15, "Turn Around": 10, "Assets Player": 10},
    "Markt boomt": {"Slow Grower": 10, "Stalwarts": 15, "Fast Grower": 35, "Cyclicals": 20, "Turn Around": 10, "Assets Player": 10}
}

# === 6️⃣ Auswahl der Marktlage ===
marktlage = st.selectbox("📉 Wähle die aktuelle Marktlage:", list(strategien.keys()))
verteilung = strategien[marktlage]
# === 7️⃣ Zwei-Spalten-Layout: Text + Diagramm (ausbalanciert) ===
col1, col2 = st.columns([1, 1])  # beide Spalten gleich breit

with col1:
    st.subheader("📋 Empfohlene Verteilung:")
    for kategorie, anteil in verteilung.items():
        st.write(f"- **{anteil}%** {kategorie}")

with col2:
    st.subheader("📊 Visuelle Darstellung")
    fig, ax = plt.subplots(figsize=(3.5, 3.5))  # kleineres, kompaktes Diagramm
    wedges, texts, autotexts = ax.pie(
        verteilung.values(),
        labels=verteilung.keys(),
        autopct='%1.1f%%',
        startangle=90,
        textprops={'fontsize': 10}
    )
    ax.axis("equal")
    st.pyplot(fig)


# === 8️⃣ Daten laden & Scoring ===
@st.cache_data
def load_stock_data():
    es = get_es_connection()
    query = {"size": 2000, "query": {"match_all": {}}}
    resp = es.search(index=INDEX, body=query)
    hits = [hit["_source"] for hit in resp["hits"]["hits"]]
    df = pd.DataFrame(hits)
    if "marketCap" in df.columns:
        df["marketCap"] = df["marketCap"] / 1e9
    return df

df_stocks = load_stock_data()

# === 9️⃣ Kriterien ===
CRITERIA = {
    "Slow Grower": [
        ("eps", "< 5 EPS", lambda x: x < 5),
        ("dividendYield", "3–9 % Dividende", lambda x: 0.03 <= x <= 0.09),
        ("peRatio", "< 15 KGV", lambda x: x < 15),
        ("priceToBook", "< 2.5 P/B", lambda x: x < 2.5),
        ("marketCap", "> 10 Mrd USD", lambda x: x > 10),
    ],
    "Stalwarts": [
        ("eps", "5–10 EPS", lambda x: 5 <= x <= 10),
        ("dividendYield", "≥ 2 % Dividende", lambda x: x >= 0.02),
        ("peRatio", "< 25 KGV", lambda x: x < 25),
        ("marketCap", "> 50 Mrd USD", lambda x: x > 50),
        ("bookValuePerShare", "> 10 USD Buchwert", lambda x: x > 10),
    ],
    "Fast Grower": [
        ("eps", "> 10 EPS", lambda x: x > 10),
        ("pegRatio", "< 1 PEG", lambda x: x < 1),
        ("peRatio", "< 25 KGV", lambda x: x < 25),
        ("priceToBook", "< 4 P/B", lambda x: x < 4),
        ("revenueGrowth", "> 0.1 Umsatzwachstum", lambda x: x > 0.1),
    ],
    "Cyclicals": [
        ("eps", "EPS > 0", lambda x: x > 0),
        ("peRatio", "KGV < 25", lambda x: x < 25),
        ("marketCap", "> 10 Mrd USD", lambda x: x > 10),
        ("freeCashFlow", "positiver FCF", lambda x: x > 0),
    ],
    "Turn Around": [
        ("eps", "EPS > 0", lambda x: x > 0),
        ("bookValuePerShare", "> 10 USD Buchwert", lambda x: x > 10),
        ("peRatio", "< 20 KGV", lambda x: x < 20),
        ("totalDebt", "< 20 Mrd USD Schulden", lambda x: x < 20000),
        ("cashPerShare", "> 5 USD Cash/Aktie", lambda x: x > 5),
    ],
    "Assets Player": [
        ("bookValuePerShare", "≥ 20 $ Buchwert", lambda x: x >= 20),
        ("priceToBook", "< 1.5 P/B", lambda x: x < 1.5),
        ("peRatio", "< 20 KGV", lambda x: x < 20),
        ("marketCap", "< 10 Mrd USD", lambda x: x < 10),
        ("cashPerShare", "> 5 USD Cash/Aktie", lambda x: x > 5),
    ],
}

def score_row(row, criteria):
    score = 0
    for field, _, rule in criteria:
        val = row.get(field)
        if isinstance(val, (int, float)) and not pd.isna(val):
            try:
                if rule(val):
                    score += 1
            except Exception:
                pass
    return score

# === 🔟 Top 10 berechnen (optional Branchenfilter) ===
top10_by_category = {}

for cat, rules in CRITERIA.items():
    df_temp = df_stocks.copy()
    if selected_industry != "Alle" and "industry" in df_temp.columns:
        df_temp = df_temp[df_temp["industry"] == selected_industry]
    if df_temp.empty:
        top10_by_category[cat] = []
        continue
    df_temp["Score"] = df_temp.apply(lambda r: score_row(r, rules), axis=1)
    df_temp = df_temp.sort_values("Score", ascending=False).head(10)
    top10_by_category[cat] = df_temp["symbol"].dropna().tolist()

# === 11️⃣ Auswahl der Aktien ===
# === 11️⃣ Auswahl der Aktien + Verteilung nebeneinander ===
st.subheader("🔎 Aktienauswahl je Kategorie")

# Container mit zwei Spalten nebeneinander
col_links, col_rechts = st.columns([1.2, 1])

with col_links:
    ausgewaehlte_aktien = {}
    for kategorie in verteilung:
        aktien = st.multiselect(
            f"{kategorie} – Wähle Aktien aus:",
            options=top10_by_category.get(kategorie, []),
            key=kategorie
        )
        ausgewaehlte_aktien[kategorie] = aktien

with col_rechts:
    st.subheader("📈 Aktuelle Portfolio-Zusammensetzung")

    gesamt_anzahl = sum(len(aktien) for aktien in ausgewaehlte_aktien.values())

    if gesamt_anzahl > 0:
        aktuelle_verteilung = {
            kategorie: round((len(aktien) / gesamt_anzahl) * 100, 1)
            for kategorie, aktien in ausgewaehlte_aktien.items()
        }

        df_vergleich = pd.DataFrame([
            {
                "Kategorie": kategorie,
                "Empfohlen (%)": verteilung[kategorie],
                "Aktuell (%)": aktuelle_verteilung.get(kategorie, 0),
                "Differenz (%)": aktuelle_verteilung.get(kategorie, 0) - verteilung[kategorie]
            }
            for kategorie in verteilung.keys()
        ])

        # === Farbdefinition für Differenzen ===
        def color_diff(val):
            if abs(val) <= 5:
                color = '#4CAF50'  # grün
            elif abs(val) <= 10:
                color = '#FFC107'  # gelb
            else:
                color = '#F44336'  # rot
            return f'color: {color}; font-weight: bold'

        st.dataframe(
            df_vergleich.style
            .format({
                "Empfohlen (%)": "{:.1f}",
                "Aktuell (%)": "{:.1f}",
                "Differenz (%)": "{:+.1f}"
            })
            .applymap(color_diff, subset=["Differenz (%)"])
        )
    else:
        st.info("Noch keine Aktien ausgewählt – wähle Aktien, um deine aktuelle Verteilung zu sehen.")

# === 12️⃣ Begründung ===
st.subheader("📝 Bemerkung")
begruendung = st.text_area("Auf was muss ich achten?")

# === 13️⃣ Speichern ===
if st.button("💾 Portfolio speichern"):
    daten = []
    for kategorie, aktien in ausgewaehlte_aktien.items():
        for aktie in aktien:
            daten.append({
                "Marktlage": marktlage,
                "Kategorie": kategorie,
                "Aktie": aktie,
                "Branche": selected_industry,
                "Begründung": begruendung
            })
    df = pd.DataFrame(daten)
    df.to_csv(SAVE_PATH, index=False)
    st.success("✅ Portfolio erfolgreich gespeichert!")

# === 14️⃣ Gespeicherte Daten anzeigen ===
st.markdown("---")
st.subheader("📂 Letztes gespeichertes Portfolio anzeigen")

if os.path.exists(SAVE_PATH):
    df_geladen = pd.read_csv(SAVE_PATH)
    st.dataframe(df_geladen)
else:
    st.info("Noch kein Portfolio gespeichert.")
