# pages/Portfolio.py
import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st

# === 🔧 Pfad zur src-Ebene hinzufügen ===
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# === Import aus funktionen.py ===
from src.funktionen import (
    get_es_connection,
    load_data_from_es,
    load_industries,
    score_row
)

# === 1️⃣ Grundkonfiguration ===
st.set_page_config(page_title="Portfolio-Erstellung", layout="wide")
st.sidebar.image("assets/Logo-TH-Köln1.png")
st.title("📁 Portfolio-Zusammenstellung nach Peter Lynch")

SAVE_PATH = "portfolio_speichern.csv"

# === 2️⃣ Verbindung & Daten ===
es = get_es_connection()
df_industries = load_industries(es)
df_stocks = load_data_from_es(es)

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

# === 5️⃣ Bewertungskriterien (nur einmal definiert) ===
CRITERIA = {
    "Slow Grower": [("eps", lambda x: x < 5), ("dividendYield", lambda x: 0.03 <= x <= 0.09),
                    ("peRatio", lambda x: x < 15), ("priceToBook", lambda x: x < 2.5), ("marketCap", lambda x: x > 10)],
    "Stalwarts": [("eps", lambda x: 5 <= x <= 10), ("dividendYield", lambda x: x >= 0.02),
                  ("peRatio", lambda x: x < 25), ("marketCap", lambda x: x > 50), ("bookValuePerShare", lambda x: x > 10)],
    "Fast Grower": [("eps", lambda x: x > 10), ("pegRatio", lambda x: x < 1),
                    ("peRatio", lambda x: x < 25), ("priceToBook", lambda x: x < 4), ("revenueGrowth", lambda x: x > 0.1)],
    "Cyclicals": [("eps", lambda x: x > 0), ("peRatio", lambda x: x < 25),
                  ("marketCap", lambda x: x > 10), ("freeCashFlow", lambda x: x > 0)],
    "Turn Around": [("eps", lambda x: x > 0), ("bookValuePerShare", lambda x: x > 10),
                    ("peRatio", lambda x: x < 20), ("totalDebt", lambda x: x < 20000), ("cashPerShare", lambda x: x > 5)],
    "Assets Player": [("bookValuePerShare", lambda x: x >= 20), ("priceToBook", lambda x: x < 1.5),
                      ("peRatio", lambda x: x < 20), ("marketCap", lambda x: x < 10), ("cashPerShare", lambda x: x > 5)]
}

# === 6️⃣ Top-Aktien je Kategorie ===
top10_by_category = {}
for cat, rules in CRITERIA.items():
    df_temp = df_stocks.copy()
    if selected_industry != "Alle" and "industry" in df_temp.columns:
        df_temp = df_temp[df_temp["industry"] == selected_industry]
    if df_temp.empty:
        top10_by_category[cat] = []
        continue
    df_temp["Score"] = df_temp.apply(lambda r: score_row(r, rules), axis=1)
    top10_by_category[cat] = df_temp.sort_values("Score", ascending=False).head(10)["symbol"].dropna().tolist()

# === 7️⃣ Aktienauswahl & Portfolio ===
st.subheader("🔎 Aktienauswahl je Kategorie")
col_links, col_rechts = st.columns([1.2, 1])

with col_links:
    ausgewaehlte_aktien = {
        k: st.multiselect(f"{k} – Wähle Aktien aus:", options=top10_by_category.get(k, []), key=k)
        for k in verteilung
    }

with col_rechts:
    st.subheader("📈 Aktuelle Portfolio-Zusammensetzung")
    gesamt = sum(len(a) for a in ausgewaehlte_aktien.values())
    if gesamt > 0:
        aktuelle = {k: round((len(a)/gesamt)*100, 1) for k, a in ausgewaehlte_aktien.items()}
        df_vergleich = pd.DataFrame([
            {"Kategorie": k, "Empfohlen (%)": verteilung[k], "Aktuell (%)": aktuelle.get(k, 0),
             "Differenz (%)": aktuelle.get(k, 0) - verteilung[k]}
            for k in verteilung
        ])
        st.dataframe(df_vergleich.style.format("{:.1f}"))
    else:
        st.info("Noch keine Aktien ausgewählt.")

# === 8️⃣ Speichern & Laden ===
st.subheader("📝 Bemerkung")
begruendung = st.text_area("Auf was muss ich achten?")

if st.button("💾 Portfolio speichern"):
    daten = [{"Marktlage": marktlage, "Kategorie": k, "Aktie": a,
              "Branche": selected_industry, "Begründung": begruendung}
             for k, aktien in ausgewaehlte_aktien.items() for a in aktien]
    pd.DataFrame(daten).to_csv(SAVE_PATH, index=False)
    st.success("✅ Portfolio erfolgreich gespeichert!")

st.markdown("---")
st.subheader("📂 Letztes gespeichertes Portfolio anzeigen")
if os.path.exists(SAVE_PATH):
    st.dataframe(pd.read_csv(SAVE_PATH))
else:
    st.info("Noch kein Portfolio gespeichert.")
