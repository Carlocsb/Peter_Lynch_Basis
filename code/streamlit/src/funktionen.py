import os
import pandas as pd
from elasticsearch import Elasticsearch
import plotly.express as px

# ==========================================================
# 1️⃣ ELASTICSEARCH-VERBINDUNG UND DATENABRUF
# ==========================================================

ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
INDEX = "stocks"

def get_es_connection():
    """Erstellt eine Verbindung zu Elasticsearch."""
    es = Elasticsearch(ES_URL, request_timeout=30)
    if not es.ping():
        print("❌ Verbindung zu Elasticsearch fehlgeschlagen.")
    return es


def suche_aktie_in_es(es, symbol: str):
    """Sucht eine Aktie in Elasticsearch anhand des Symbols (z. B. 'AAPL')."""
    query = {
        "size": 1,
        "query": {
            "bool": {
                "should": [
                    {"term": {"symbol.keyword": symbol}},
                    {"term": {"symbol": symbol}},
                    {"match": {"symbol": symbol}}
                ],
                "minimum_should_match": 1
            }
        },
        "sort": [{"date": {"order": "desc"}}]
    }
    resp = es.search(index=INDEX, body=query)
    hits = resp.get("hits", {}).get("hits", [])
    return hits[0]["_source"] if hits else None


def lade_historische_kennzahlen(es, symbol: str, kennzahl: str):
    """Lädt Zeitreihendaten einer bestimmten Kennzahl für eine Aktie."""
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


# ==========================================================
# 2️⃣ VISUALISIERUNG
# ==========================================================

def zeige_kennzahlverlauf(df: pd.DataFrame, symbol: str, titel: str, einheit: str = ""):
    """Erzeugt Plotly-Diagramm für eine Kennzahl."""
    if df.empty:
        return None
    fig = px.line(df, x="Datum", y="Wert", title=f"{titel}-Verlauf für {symbol}", markers=True)
    fig.update_layout(template="plotly_dark", hovermode="x unified")
    if einheit:
        fig.update_yaxes(title_text=einheit)
    return fig


# ==========================================================
# 3️⃣ PETER-LYNCH-KATEGORISIERUNG
# ==========================================================

def berechne_peter_lynch_kategorie(daten: dict):
    """Berechnet, zu welcher Peter-Lynch-Kategorie eine Aktie am besten passt."""

    CRITERIA = {
        "Slow Growers": [
            ("eps", lambda x: x < 5),
            ("dividendYield", lambda x: 0.03 <= x <= 0.09),
            ("peRatio", lambda x: x < 15),
            ("priceToBook", lambda x: x < 2.5),
            ("marketCap", lambda x: x > 10),
        ],
        "Stalwarts": [
            ("eps", lambda x: 5 <= x <= 10),
            ("dividendYield", lambda x: x >= 0.02),
            ("peRatio", lambda x: x < 25),
            ("marketCap", lambda x: x > 50),
            ("bookValuePerShare", lambda x: x > 10),
        ],
        "Fast Growers": [
            ("eps", lambda x: x > 10),
            ("peRatio", lambda x: x < 25),
            ("priceToBook", lambda x: x < 4),
            ("revenueGrowth", lambda x: x > 0.1),
        ],
        "Cyclicals": [
            ("eps", lambda x: x > 0),
            ("peRatio", lambda x: x < 25),
            ("marketCap", lambda x: x > 10),
            ("freeCashFlow", lambda x: x > 0),
        ],
        "Turnarounds": [
            ("eps", lambda x: x > 0),
            ("bookValuePerShare", lambda x: x > 10),
            ("peRatio", lambda x: x < 20),
            ("totalDebt", lambda x: x < 20000),
            ("cashPerShare", lambda x: x > 5),
        ],
        "Asset Plays": [
            ("bookValuePerShare", lambda x: x >= 20),
            ("priceToBook", lambda x: x < 1.5),
            ("peRatio", lambda x: x < 20),
            ("marketCap", lambda x: x < 10),
            ("cashPerShare", lambda x: x > 5),
        ],
    }

    # Bewertung durchführen
    results = {}
    for cat, rules in CRITERIA.items():
        score = sum(
            1 for field, cond in rules
            if isinstance(daten.get(field), (int, float)) and cond(daten[field])
        )
        results[cat] = score / len(rules) if rules else 0

    beste_kategorie = max(results, key=results.get)
    trefferquote = round(results[beste_kategorie] * 100, 1)

    # Dynamischer Satz
    if trefferquote < 70:
        vergleich = "In den anderen Kategorien liegt die Übereinstimmung noch darunter."
    elif trefferquote < 90:
        vergleich = "Auch andere Kategorien zeigen eine gewisse Übereinstimmung, jedoch etwas geringer."
    else:
        vergleich = "Diese Kategorie passt am besten und deutlich stärker als alle anderen."

    return beste_kategorie, trefferquote, vergleich, results


def erklaere_kategorie(kategorie: str) -> str:
    """Gibt eine Beschreibung der Peter-Lynch-Kategorie zurück."""
    texte = {
        "Slow Growers": "💤 Langsam wachsende Unternehmen mit stabilen Dividenden. Geeignet für defensive Anleger.",
        "Stalwarts": "💪 Etablierte Unternehmen mit solidem Wachstum. Geringeres Risiko, aber noch Potenzial.",
        "Fast Growers": "🚀 Schnelles Gewinnwachstum. Hohe Renditechancen, aber auch höheres Risiko.",
        "Cyclicals": "🔄 Zyklische Unternehmen – stark abhängig von Wirtschaftsphasen.",
        "Turnarounds": "🔁 Unternehmen in Erholungsphase – riskant, aber mit großem Potenzial.",
        "Asset Plays": "💎 Unternehmen mit versteckten Vermögenswerten, die vom Markt unterbewertet sind.",
    }
    return texte.get(kategorie, "")


# ==========================================================
# 4️⃣ HILFSFUNKTIONEN / UTILITIES
# ==========================================================

def berechne_kennzahlen_tabelle(daten: dict) -> pd.DataFrame:
    """Erstellt DataFrame mit zusätzlichen Kennzahlen."""
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
    df = pd.DataFrame(details.items(), columns=["Kennzahl", "Wert"])
    df["Wert"] = df["Wert"].apply(lambda x: round(x, 4) if isinstance(x, (int, float)) else "—")
    return df


def beschreibe_kennzahlen() -> dict:
    """Gibt Beschreibungen der wichtigsten Kennzahlen zurück."""
    return {
        "peRatio": "Das Kurs-Gewinn-Verhältnis (KGV) zeigt, wie viel Anleger für 1 USD Gewinn zahlen.",
        "priceToBook": "Das Kurs-Buchwert-Verhältnis (P/B) vergleicht den Aktienkurs mit dem Buchwert.",
        "dividendYield": "Die Dividendenrendite zeigt, wie viel Prozent Dividende pro Jahr gezahlt wird.",
        "eps": "Earnings per Share (EPS) misst den Gewinn je Aktie.",
        "bookValuePerShare": "Der Buchwert pro Aktie zeigt den Eigenkapitalwert pro Anteil.",
        "debtToEquity": "Das Verhältnis von Schulden zu Eigenkapital; niedrigere Werte bedeuten geringeres Risiko.",
    }
# ==========================================================
# 5️⃣ DATENLADEN & SCORING – FÜR PORTFOLIO UND TOP-10-SEITE
# ==========================================================

def load_data_from_es(es=None, limit: int = 2000, index: str = INDEX) -> pd.DataFrame:
    """
    Lädt Aktienbasisdaten aus Elasticsearch.
    - Gibt einen DataFrame mit den wichtigsten Feldern zurück.
    - Rechnet 'marketCap' automatisch in Milliarden USD um.
    """
    if es is None:
        es = get_es_connection()

    query = {"size": limit, "query": {"match_all": {}}}
    resp = es.search(index=index, body=query)
    hits = [h["_source"] for h in resp["hits"]["hits"]]
    df = pd.DataFrame(hits)

    if "marketCap" in df.columns:
        df["marketCap"] = df["marketCap"] / 1e9  # in Milliarden USD
    return df


def load_industries(es=None, index: str = INDEX) -> pd.DataFrame:
    """
    Lädt verfügbare Branchen aus Elasticsearch (Symbol & Industry).
    Wird z. B. im Portfolio-Filter verwendet.
    """
    if es is None:
        es = get_es_connection()

    query = {"size": 1000, "_source": ["symbol", "industry"]}
    resp = es.search(index=index, body=query)
    hits = [h["_source"] for h in resp["hits"]["hits"]]
    return pd.DataFrame(hits) if hits else pd.DataFrame(columns=["symbol", "industry"])


def score_row(row, criteria):
    """
    Bewertet eine Aktie nach übergebenen Kriterien.
    Unterstützt zwei Formate:
    - [(feld, regel)] → z. B. Portfolio
    - [(feld, beschreibung, regel, optional)] → z. B. Top 10 Kategorien
    Gibt den Score (Anzahl erfüllter Bedingungen) zurück.
    """
    score = 0
    for item in criteria:
        try:
            # Einfaches Kriterium (2er-Tupel)
            if len(item) == 2:
                field, rule = item
            # Erweitertes Kriterium (4er-Tupel)
            elif len(item) >= 3:
                field, _, rule = item[:3]
            else:
                continue

            val = row.get(field)
            if isinstance(val, (int, float)) and not pd.isna(val):
                if rule(val):
                    score += 1
        except Exception:
            continue
    return score
