# pages/Top_10_Kategorien.py (ganz oben)

import os, sys, pandas as pd, streamlit as st
import importlib

# Pfad zur src-Ebene
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

# Modul laden & sicher neu laden
from src import lynch_criteria
importlib.reload(lynch_criteria)
CATEGORIES = lynch_criteria.CATEGORIES  # <-- NUR HIER holen

from src.funktionen import get_es_connection, load_data_from_es, score_row


# === 1️⃣ Setup ===
st.set_page_config(page_title="Top 10 Aktien je Peter-Lynch-Kategorie", layout="wide")
st.sidebar.image("assets/Logo-TH-Köln1.png", caption="")
st.title("📊 Top 10 Aktien je Peter-Lynch-Kategorie")
st.markdown("*(Daten live aus Elasticsearch – bewertet nach Lynch-Kriterien)*")

# === 2️⃣ Verbindung & Daten ===
es = get_es_connection()
df = load_data_from_es(es)

if df.empty:
    st.warning("⚠️ Keine Daten in Elasticsearch gefunden. Bitte ingest_yf ausführen.")
    st.stop()

def make_criteria_with_labels():
    """
    Nimmt die Regeln aus CATEGORIES (2er- oder 4er-Tupel) und ergänzt nur ein hübsches Label.
    Anzahl/Logik der Regeln bleiben 1:1 bestehen, damit z.B. Slow Growers = 6 Regeln hat.
    """
    # optionale Feld-Aliasse: was in CATEGORIES steht -> wie es im DataFrame heißt
    FIELD_ALIAS = {
        "trailingPE": "peRatio",   # wir lesen peRatio aus ES
    }

    # deutsche Labels pro Feld (fallback = Feldname)
    LABEL_MAP = {
        "earningsGrowth":   "Gewinnwachstum",
        "epsGrowth":        "EPS-Wachstum",
        "eps":              "EPS",
        "dividendYield":    "Dividendenrendite",
        "payoutRatio":      "Payout Ratio",
        "revenueGrowth":    "Umsatzwachstum",
        "peRatio":          "KGV",
        "priceToBook":      "P/B",
        "marketCap":        "Marktkapitalisierung",
        "freeCashFlow":     "Free Cash Flow",
        "freeCashFlowPerShare": "FCF/Aktie",
        "debtToAssets":     "Debt/Assets",
        "cashPerShare":     "Cash/Aktie",
        "bookValuePerShare":"Buchwert/Aktie",
        "totalDebt":        "Gesamtschulden",
        "sector":           "Sektor",
    }

    labeled = {}
    for cat, rules in CATEGORIES.items():
        augmented = []
        for item in rules:
            # CATEGORIES kann (feld, regel) ODER (feld, label, regel, optional) enthalten
            if len(item) == 2:
                field, rule = item
                optional = False
                label_text = LABEL_MAP.get(field, field)
            else:
                field, label_text_in, rule, optional = item
                label_text = label_text_in or LABEL_MAP.get(field, field)

            # Alias anwenden, damit wir sicher die Spalte im df treffen
            used_field = FIELD_ALIAS.get(field, field)

            augmented.append((used_field, label_text, rule, optional))
        labeled[cat] = augmented
    return labeled




# === 4️⃣ Kategorie-Auswahl ===
CRITERIA = make_criteria_with_labels()
kategorie = st.selectbox("Kategorie wählen:", list(CRITERIA.keys()))
criteria = CRITERIA[kategorie]



# === 5️⃣ Filter: Branche / MarketCap / Datum ===
st.sidebar.header("🔍 Filter")

if "industry" in df.columns:
    industries = ["Alle"] + sorted(df["industry"].dropna().unique().tolist())
    selected_industry = st.sidebar.selectbox("Branche", industries)
    if selected_industry != "Alle":
        df = df[df["industry"] == selected_industry]

if "marketCap" in df.columns:
    # In Milliarden USD umrechnen
    df["marketCap_Mrd"] = df["marketCap"] / 1e9
    min_cap, max_cap = df["marketCap_Mrd"].min(), df["marketCap_Mrd"].max()

    cap_range = st.sidebar.slider(
        "Marktkapitalisierung (in Mrd USD)",
        min_value=round(min_cap, 1),
        max_value=round(max_cap, 1),
        value=(round(min_cap, 1), round(max_cap, 1)),
        step=10.0
    )

    # Nach Auswahl filtern (in Milliarden!)
    df = df[(df["marketCap_Mrd"] >= cap_range[0]) & (df["marketCap_Mrd"] <= cap_range[1])]



# === 6️⃣ Bewertung (zentral via score_row) ===
def evaluate_stock(row, criteria):
    """Erweitertes Scoring mit Detailausgabe"""
    results = []
    score = 0
    for field, label, rule, optional in criteria:
        val = row.get(field, None)
        ok = False
        if isinstance(val, (int, float)):
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
        if ok:
            score += 1
    return score, len(criteria), results

scores = df.apply(lambda r: evaluate_stock(r, criteria), axis=1)
df["Score"] = [s[0] for s in scores]
df["MaxScore"] = [s[1] for s in scores]
df["Score %"] = (df["Score"] / df["MaxScore"] * 100).round(1)
df["Details"] = [s[2] for s in scores]

df = df.sort_values(
    by=["Score %", "marketCap"],
    ascending=[False, False]
).reset_index(drop=True)

# MarketCap in Milliarden USD umrechnen und formatieren
df["MarketCap (Mrd USD)"] = (df["marketCap"] / 1e9).round(1)

# === 7️⃣ Anzeige ===
st.markdown(f"### 📈 Ranking – {kategorie}")
st.dataframe(
    df[["symbol", "MarketCap (Mrd USD)", "Score", "MaxScore", "Score %"]].head(10)
      .style.format({
          "MarketCap (Mrd USD)": "{:,.1f}",
          "Score %": "{:.0f} %",
      }),
    use_container_width=True
)





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
st.caption("Datenquelle: Elasticsearch (via yfinance) – bewertet nach Peter-Lynch-Kriterien.")
