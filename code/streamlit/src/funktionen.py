import os
import pandas as pd
from elasticsearch import Elasticsearch,NotFoundError
import streamlit as st
from typing import Optional

import plotly.express as px
from typing import Dict, List, Tuple, Callable
import math
from .lynch_criteria import CATEGORIES
from datetime import datetime, timezone



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


def suche_aktie_in_es(es, symbol: str, source_mode: Optional[str] = None):
    must = [{
        "bool": {
            "should": [
                {"term": {"symbol.keyword": symbol}},
                {"term": {"symbol": symbol}},
                {"match": {"symbol": symbol}}
            ],
            "minimum_should_match": 1
        }
    }]
    q_src = _es_query_for_mode(source_mode)
    if q_src:
        must.append(q_src)

    query = {
        "size": 1000,
        "query": {"bool": {"must": must}},
        "sort": [{"date": {"order": "desc"}}]
    }
    resp = es.search(index=INDEX, body=query)
    hits = [h["_source"] for h in resp.get("hits", {}).get("hits", [])]
    if not hits:
        return None

    df = pd.DataFrame(hits)
    df = _filter_dedupe_by_mode(df, source_mode)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values("date")

    return df.iloc[-1].to_dict()



def lade_historische_kennzahlen(es, symbol: str, kennzahl: str, source_mode: Optional[str] = None):
    must = [{"term": {"symbol": symbol}}]
    q_src = _es_query_for_mode(source_mode)
    if q_src:
        must.append(q_src)

    query = {
        "size": 10000,
        "query": {"bool": {"must": must}},
        "sort": [{"date": {"order": "asc"}}]
    }
    resp = es.search(index=INDEX, body=query)
    hits = [h["_source"] for h in resp.get("hits", {}).get("hits", [])]
    df = pd.DataFrame(hits)
    if df.empty:
        return df

    df = _filter_dedupe_by_mode(df, source_mode)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.sort_values("date")
    out = df[["date", kennzahl]].dropna().rename(columns={"date": "Datum", kennzahl: "Wert"})
    return out


# -------- Quelle & Dedupe zentral steuern --------
SOURCE_MODES = [
    "Nur yfinance",
    "Nur Alpha Vantage",
    "Beides – yfinance bevorzugen",
    "Beides – jüngster Import gewinnt",
]

def render_source_selector() -> str:
    """Sidebar-Umschalter (global via Session State)."""
    if "src_mode" not in st.session_state:
        st.session_state["src_mode"] = "Beides – yfinance bevorzugen"
    return st.sidebar.radio("📡 Datenquelle", SOURCE_MODES, key="src_mode")

def _es_query_for_mode(mode: Optional[str]):
    """Nur für Single-Source-Modi schon im ES-Query filtern."""
    if mode == "Nur yfinance":
        return {"term": {"source": "yfinance"}}
    if mode == "Nur Alpha Vantage":
        return {"term": {"source": "alphavantage"}}
    return None

def _filter_dedupe_by_mode(df: pd.DataFrame, mode: Optional[str]) -> pd.DataFrame:
    if df.empty or not mode or "source" not in df.columns:
        return df
    out = df.copy()
    if "ingested_at" in out.columns:
        out["ingested_at"] = pd.to_datetime(out["ingested_at"], utc=True, errors="coerce")

    if mode == "Nur yfinance":
        return out[out["source"] == "yfinance"]
    if mode == "Nur Alpha Vantage":
        return out[out["source"] == "alphavantage"]

    # Kombi-Modi: pro (symbol, date) auf 1 Zeile reduzieren
    if mode == "Beides – yfinance bevorzugen":
        pref = {"yfinance": 0, "alphavantage": 1}
        out["__src_rank"] = out["source"].map(pref).fillna(9)
        out = (out.sort_values(["symbol", "date", "__src_rank", "ingested_at"])
                  .drop_duplicates(subset=["symbol", "date"], keep="first")
                  .drop(columns=["__src_rank"]))
        return out

    if mode == "Beides – jüngster Import gewinnt":
        if "ingested_at" in out.columns:
            return (out.sort_values(["symbol", "date", "ingested_at"])
                      .drop_duplicates(subset=["symbol", "date"], keep="last"))
        return out.drop_duplicates(subset=["symbol", "date"], keep="last")

    return out

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
    """
    Berechnet die passendste Lynch-Kategorie.
    Nutzt 'score_row', sodass sowohl 2er- als auch 4er-Regeln funktionieren.
    """
    results = {}
    for cat, rules in CATEGORIES.items():
        score = score_row(daten, rules)
        max_rules = len(rules) if rules else 1
        results[cat] = score / max_rules

    beste_kategorie = max(results, key=results.get)
    trefferquote = round(results[beste_kategorie] * 100, 1)

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
def load_data_from_es(es=None, limit: int = 2000, index: str = INDEX, source_mode: Optional[str] = None) -> pd.DataFrame:
    if es is None:
        es = get_es_connection()

    base_query = {"match_all": {}}
    q_src = _es_query_for_mode(source_mode)
    if q_src:
        base_query = {"bool": {"must": [q_src]}}

    query = {"size": limit, "sort": [{"date": {"order": "desc"}}], "query": base_query}
    resp = es.search(index=index, body=query)
    hits = [h["_source"] for h in resp.get("hits", {}).get("hits", [])]
    df = pd.DataFrame(hits)
    if df.empty:
        return df

    # neueste pro Symbol
    if "symbol" in df.columns and "date" in df.columns:
        df = df.sort_values(["symbol", "date"], ascending=[True, False]).drop_duplicates("symbol", keep="first")

    # MarketCap etc. (deins bleibt)
    if "marketCap" in df.columns:
        df["marketCapBn"] = df["marketCap"] / 1e9
    if "earningsGrowth" in df.columns and "epsGrowth" not in df.columns:
        df["epsGrowth"] = df["earningsGrowth"]
    if "peRatio" not in df.columns and "trailingPE" in df.columns:
        df["peRatio"] = df["trailingPE"]
    if "freeCashFlowPerShare" not in df.columns and {"freeCashflow", "sharesOutstanding"} <= set(df.columns):
        with pd.option_context("mode.use_inf_as_na", True):
            df["freeCashFlowPerShare"] = df["freeCashflow"] / df["sharesOutstanding"]
    if "fcfMargin" not in df.columns and {"freeCashflow", "revenue"} <= set(df.columns):
        with pd.option_context("mode.use_inf_as_na", True):
            df["fcfMargin"] = df["freeCashflow"] / df["revenue"]
    if "cashToDebt" not in df.columns and {"totalCash", "totalDebt"} <= set(df.columns):
        with pd.option_context("mode.use_inf_as_na", True):
            df["cashToDebt"] = df["totalCash"] / df["totalDebt"]
    if "equityRatio" not in df.columns and {"totalStockholderEquity", "totalAssets"} <= set(df.columns):
        with pd.option_context("mode.use_inf_as_na", True):
            df["equityRatio"] = df["totalStockholderEquity"] / df["totalAssets"]

    # >>> zentrale Quelle/Dedupe anwenden
    df = _filter_dedupe_by_mode(df, source_mode)
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

# ==========================================================
# 5️⃣ Portfolio funktionen 
# ==========================================================

PORTFOLIO_INDEX = "portfolios"

def ensure_portfolio_index(es):
    if es.indices.exists(index=PORTFOLIO_INDEX):
        return
    es.indices.create(
        index=PORTFOLIO_INDEX,
        body={
            "mappings": {
                "properties": {
                    "name": {"type": "keyword"},
                    "market_condition": {"type": "keyword"},
                    "industry_filter": {"type": "keyword"},
                    "comment": {"type": "text"},
                    "created_at": {"type": "date"},
                    "updated_at": {"type": "date"},
                    "allocation_target": {"type": "object"},
                    "items": {
                        "type": "nested",
                        "properties": {
                            "category": {"type": "keyword"},
                            "symbol": {"type": "keyword"},
                            "amount": {"type": "double"},
                            "industry": {"type": "keyword"},
                        },
                    },
                    "totals": {
                        "type": "object",
                        "properties": {
                            "by_category": {"type": "object"},
                            "total_amount": {"type": "double"},
                        },
                    },
                }
            }
        },
    )

def build_portfolio_doc(name, marktlage, selected_industry, comment, ausgewaehlte_aktien, betraege, verteilung):
    sum_cat = {k: float(sum(betraege.get(k, {}).values())) for k in verteilung}
    total_amt = float(sum(sum_cat.values()))
    items = []
    for k, tickers in ausgewaehlte_aktien.items():
        for t in tickers:
            amt = float(betraege.get(k, {}).get(t, 0.0))
            if amt > 0:
                items.append({"category": k, "symbol": t, "amount": amt, "industry": selected_industry})
    now = datetime.now(timezone.utc).isoformat()
    return {
        "name": name,
        "market_condition": marktlage,
        "industry_filter": selected_industry,
        "comment": comment,
        "created_at": now,
        "updated_at": now,
        "allocation_target": verteilung,
        "items": items,
        "totals": {"by_category": sum_cat, "total_amount": total_amt},
    }

def save_portfolio(es, doc, portfolio_id=None):
    doc["updated_at"] = datetime.now(timezone.utc).isoformat()
    if portfolio_id:
        es.update(index=PORTFOLIO_INDEX, id=portfolio_id, body={"doc": doc, "doc_as_upsert": True}, refresh=True)
        return portfolio_id
    res = es.index(index=PORTFOLIO_INDEX, body=doc, refresh=True)
    return res["_id"]

def list_portfolios(es, limit=200):
    resp = es.search(
        index=PORTFOLIO_INDEX,
        body={"size": limit, "sort": [{"updated_at": {"order": "desc"}}],
              "_source": ["name", "market_condition", "updated_at", "totals.total_amount"]},
    )
    return [{"id": h["_id"], **h["_source"]} for h in resp.get("hits", {}).get("hits", [])]

def load_portfolio(es, portfolio_id):
    try:
        return es.get(index=PORTFOLIO_INDEX, id=portfolio_id)["_source"]
    except NotFoundError:
        return None

def delete_portfolio(es, portfolio_id):
    try:
        es.delete(index=PORTFOLIO_INDEX, id=portfolio_id, refresh=True)
        return True
    except NotFoundError:
        return False
