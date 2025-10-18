# pages/Top_10_Kategorien.py
import streamlit as st
import pandas as pd
import os
from elasticsearch import Elasticsearch

# === 1️⃣ Setup ===
st.set_page_config(page_title="Top 10 Aktien je Peter-Lynch-Kategorie", layout="wide")
st.sidebar.image("assets/Logo-TH-Köln1.png", caption="")
st.title("📊 Top 10 Aktien je Peter-Lynch-Kategorie")
st.markdown("*(Daten live aus Elasticsearch – bewertet nach Lynch-Kriterien)*")

# === 2️⃣ Elasticsearch Setup ===
ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
INDEX = "stocks"

def get_es_connection():
    es = Elasticsearch(ES_URL, request_timeout=30)
    if not es.ping():
        st.error("❌ Verbindung zu Elasticsearch fehlgeschlagen!")
    return es

def load_data_from_es(limit: int = 2000):
    es = get_es_connection()
    query = {"size": limit, "query": {"match_all": {}}}
    resp = es.search(index=INDEX, body=query)
    hits = [hit["_source"] for hit in resp["hits"]["hits"]]
    df = pd.DataFrame(hits)
    if "marketCap" in df.columns:
        df["marketCap"] = df["marketCap"] / 1e9  # Milliarden USD
    return df

# === 3️⃣ Lynch-Kriterien ===
CRITERIA = {
    "Slow Growers": [
        ("eps", "< 5 EPS (langsames Gewinnwachstum)", lambda x: x < 5, False),
        ("dividendYield", "3–9 % Dividende", lambda x: 0.03 <= x <= 0.09, False),
        ("peRatio", "< 15 KGV", lambda x: x < 15, False),
        ("priceToBook", "< 2.5 P/B", lambda x: x < 2.5, True),
        ("marketCap", "> 10 Mrd USD", lambda x: x > 10, False),
    ],
    "Stalwarts": [
        ("eps", "5–10 EPS", lambda x: 5 <= x <= 10, False),
        ("dividendYield", "≥ 2 % Dividende", lambda x: x >= 0.02, False),
        ("peRatio", "< 25 KGV", lambda x: x < 25, False),
        ("marketCap", "> 50 Mrd USD", lambda x: x > 50, False),
        ("bookValuePerShare", "> 10 USD Buchwert", lambda x: x > 10, True),
    ],
    "Fast Growers": [
        ("eps", "> 10 EPS", lambda x: x > 10, False),
        ("pegRatio", "< 1 PEG", lambda x: x < 1, False),
        ("peRatio", "< 25 KGV", lambda x: x < 25, False),
        ("priceToBook", "< 4 P/B", lambda x: x < 4, True),
        ("revenueGrowth", "> 0.1 Umsatzwachstum", lambda x: x > 0.1, False),
    ],
    "Cyclicals": [
        ("eps", "EPS > 0 (Erholung)", lambda x: x > 0, False),
        ("peRatio", "KGV < 25", lambda x: x < 25, False),
        ("marketCap", "> 10 Mrd USD", lambda x: x > 10, False),
        ("freeCashFlow", "positiver FCF", lambda x: x > 0, False),
    ],
    "Turnarounds": [
        ("eps", "EPS > 0 (Erholung)", lambda x: x > 0, False),
        ("bookValuePerShare", "> 10 USD Buchwert", lambda x: x > 10, False),
        ("peRatio", "< 20 KGV", lambda x: x < 20, False),
        ("totalDebt", "< 20 Mrd USD Schulden", lambda x: x < 20000, False),
        ("cashPerShare", "> 5 USD Cash/Aktie", lambda x: x > 5, True),
    ],
    "Asset Plays": [
        ("bookValuePerShare", "≥ 20 $ Buchwert", lambda x: x >= 20, False),
        ("priceToBook", "< 1.5 P/B", lambda x: x < 1.5, False),
        ("peRatio", "< 20 KGV", lambda x: x < 20, False),
        ("marketCap", "< 10 Mrd USD", lambda x: x < 10, True),
        ("cashPerShare", "> 5 USD Cash/Aktie", lambda x: x > 5, True),
    ],
}

# === 4️⃣ Kategorie-Auswahl ===
kategorie = st.selectbox("Kategorie wählen:", list(CRITERIA.keys()))
criteria = CRITERIA[kategorie]

# === 5️⃣ Daten laden ===
df = load_data_from_es()

if df.empty:
    st.warning("⚠️ Keine Daten in Elasticsearch gefunden. Bitte ingest_yf ausführen.")
    st.stop()

# === 6️⃣ Filtersektion ===
st.sidebar.header("🔍 Filter")

# Branche (falls vorhanden)
if "industry" in df.columns:
    industries = ["Alle"] + sorted(df["industry"].dropna().unique().tolist())
    selected_industry = st.sidebar.selectbox("Branche", industries)
    if selected_industry != "Alle":
        df = df[df["industry"] == selected_industry]

# MarketCap Range
min_cap, max_cap = float(df["marketCap"].min()), float(df["marketCap"].max())
cap_range = st.sidebar.slider(
    "Marktkapitalisierung (in Mrd USD)",
    min_value=round(min_cap, 1),
    max_value=round(max_cap, 1),
    value=(round(min_cap, 1), round(max_cap, 1)),
)
df = df[(df["marketCap"] >= cap_range[0]) & (df["marketCap"] <= cap_range[1])]

# Datumsauswahl (neueste oder spezifisches)
if "date" in df.columns:
    available_dates = sorted(df["date"].unique(), reverse=True)
    selected_date = st.sidebar.selectbox("Datum wählen", ["Neuestes"] + available_dates)
    if selected_date != "Neuestes":
        df = df[df["date"] == selected_date]
    else:
        latest_date = max(df["date"])
        df = df[df["date"] == latest_date]

# === 7️⃣ Bewertung ===
def score_row(row, criteria):
    results = []
    for field, label, rule, optional in criteria:
        val = row.get(field, None)
        if val is None:
            ok = False
        else:
            try:
                ok = rule(val)
            except Exception:
                ok = False
        results.append({
            "Kennzahl": label,
            "Istwert": val,
            "Erfüllt": ok,
            "Optional": optional,
        })
    score = sum(1 for r in results if r["Erfüllt"])
    max_score = len(results)
    return score, max_score, results

scores = df.apply(lambda r: score_row(r, criteria), axis=1)
df["Score"] = [s[0] for s in scores]
df["MaxScore"] = [s[1] for s in scores]
df["Score %"] = (df["Score"] / df["MaxScore"] * 100).round(1)
df["Details"] = [s[2] for s in scores]

df = df.sort_values("Score %", ascending=False).reset_index(drop=True)

# === 8️⃣ Anzeige ===
st.markdown(f"### 📈 Ranking – {kategorie}")
st.dataframe(df[["symbol", "marketCap", "Score", "MaxScore", "Score %"]].head(10), use_container_width=True)

aktie = st.selectbox("Wähle eine Aktie für Details:", df["symbol"].head(10))
row = df[df["symbol"] == aktie].iloc[0]

st.markdown("---")
st.subheader(f"🔍 Detailansicht: {aktie}")

for item in row["Details"]:
    icon = "✅" if item["Erfüllt"] else "❌"
    optional_tag = " *(optional)*" if item["Optional"] else ""
    val_disp = (
        f"{item['Istwert']*100:.2f} %" if isinstance(item["Istwert"], (int, float)) and abs(item["Istwert"]) < 1 and "Ratio" not in item["Kennzahl"]
        else item["Istwert"]
    )
    st.write(f"{icon} **{item['Kennzahl']}**{optional_tag} → **Ist:** {val_disp}")

st.metric("Gesamtscore", f"{row['Score']} / {row['MaxScore']}", f"{row['Score %']} %")
st.caption("Datenquelle: Elasticsearch (via yfinance) – bewertet nach Peter Lynch Kriterien.")
