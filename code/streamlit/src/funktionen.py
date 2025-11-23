import os
import pandas as pd
from elasticsearch import Elasticsearch, NotFoundError
import streamlit as st
from typing import Optional, Dict, Any
import plotly.express as px
from datetime import datetime, timezone

from .lynch_criteria import CATEGORIES

# ==========================================================
# 1️⃣ ELASTICSEARCH-VERBINDUNG UND DATENABRUF
# ==========================================================

ES_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
INDEX  = os.getenv("ELASTICSEARCH_INDEX", "stocks")

# --------- NEU: Historik-Helfer für Feldlisten ---------
def _ensure_list(x):
    return x if isinstance(x, (list, tuple)) else [x]

def _first_nonempty(*dfs):
    for d in dfs:
        if isinstance(d, pd.DataFrame) and not d.empty:
            return d
    return pd.DataFrame()

def _merge_asof_two(left: pd.DataFrame, right: pd.DataFrame):
    """Erwartet Spalten ['Datum','Wert']; führt asof-Join aus und gibt gemeinsamen Frame zurück."""
    if left.empty or right.empty:
        return pd.DataFrame()
    l = left.sort_values("Datum")
    r = right.sort_values("Datum")
    m = pd.merge_asof(l, r, on="Datum", direction="nearest")
    m = m.dropna()
    return m

def get_es_connection():
    """Erstellt eine Verbindung zu Elasticsearch."""
    es = Elasticsearch(ES_URL, request_timeout=30)
    if not es.ping():
        print("❌ Verbindung zu Elasticsearch fehlgeschlagen.")
    return es


# -------- Quelle & Dedupe zentral steuern --------
SOURCE_MODES = [
    "Nur yfinance",
    "Nur Alpha Vantage",
    "Nur FMP",
    "Beides – yfinance bevorzugen",
    "Beides – jüngster Import gewinnt",
]


def render_source_selector(label: str = "📡 Datenquelle") -> str:
    """Sidebar-Umschalter (global via Session State)."""
    if "src_mode" not in st.session_state:
        st.session_state["src_mode"] = "Nur FMP"  # Default jetzt FMP
    return st.sidebar.radio(label, SOURCE_MODES, key="src_mode")


def _term(field: str, value: str) -> Dict[str, Any]:
    """Hilfsfunktion: term-Query mit .keyword-Fallback."""
    return {
        "bool": {
            "should": [
                {"term": {f"{field}.keyword": value}},
                {"term": {field: value}},
            ],
            "minimum_should_match": 1,
        }
    }


def _es_query_for_mode(mode: Optional[str]) -> Optional[Dict[str, Any]]:
    """Nur für Single-Source-Modi schon im ES-Query filtern."""
    if mode == "Nur yfinance":
        return _term("source", "yfinance")
    if mode == "Nur Alpha Vantage":
        return _term("source", "alphavantage")
    if mode == "Nur FMP":
        return _term("source", "fmp")
    return None


def _filter_dedupe_by_mode(df: pd.DataFrame, mode: Optional[str]) -> pd.DataFrame:
    if df.empty or "source" not in df.columns:
        return df

    out = df.copy()
    if "ingested_at" in out.columns:
        out["ingested_at"] = pd.to_datetime(out["ingested_at"], utc=True, errors="coerce")

    if mode == "Nur yfinance":
        return out[out["source"] == "yfinance"]
    if mode == "Nur Alpha Vantage":
        return out[out["source"] == "alphavantage"]
    if mode == "Nur FMP":
        return out[out["source"] == "fmp"]

    # Kombi-Modi: pro (symbol, date) auf 1 Zeile reduzieren
    if mode == "Beides – yfinance bevorzugen":
        # Reihenfolge: yfinance → fmp → alphavantage → sonst
        pref = {"yfinance": 0, "fmp": 1, "alphavantage": 2}
        out["__src_rank"] = out["source"].map(pref).fillna(9)
        out = (
            out.sort_values(["symbol", "date", "__src_rank", "ingested_at"])
               .drop_duplicates(subset=["symbol", "date"], keep="first")
               .drop(columns=["__src_rank"])
        )
        return out

    if mode == "Beides – jüngster Import gewinnt":
        if "ingested_at" in out.columns:
            return (
                out.sort_values(["symbol", "date", "ingested_at"])
                   .drop_duplicates(subset=["symbol", "date"], keep="last")
            )
        return out.drop_duplicates(subset=["symbol", "date"], keep="last")

    return out


# ==========================================================
# 1b️⃣ Enrichment/Abgeleitete Kennzahlen
# ==========================================================

def _safe_div(a, b):
    try:
        if a is None or b in (None, 0):
            return None
        return float(a) / float(b)
    except Exception:
        return None


def _first_float(x):
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _get_any(d: dict, *keys):
    """Erster nicht-None-Wert aus d für eine Liste möglicher Keys."""
    for k in keys:
        if k in d and d.get(k) is not None:
            return d.get(k)
    return None


def lade_historische_kennzahlen(es, symbol: str, kennzahl, source_mode: Optional[str] = None):
    """
    Lädt Historie für eine Kennzahl oder eine Liste möglicher Feldnamen (Aliasse).
    Rückgabe-DF: Spalten ['Datum','Wert'] (aufsteigend sortiert).
    """
    symbol = (symbol or "").strip().upper()
    fields = _ensure_list(kennzahl)

    must = [_term("symbol", symbol)]
    q_src = _es_query_for_mode(source_mode)
    if q_src:
        must.append(q_src)

    # wir holen alle Kandidatenfelder in einem Rutsch und picken später das erste, das Daten hat
    _source = list(dict.fromkeys(["symbol", "date", "source", "ingested_at", *fields]))

    query = {
        "size": 10000,
        "query": {"bool": {"must": must}},
        "sort": [{"date": {"order": "asc"}}, {"ingested_at": {"order": "asc"}}],
        "_source": _source,
    }
    resp = es.search(index=INDEX, body=query)
    hits = [h["_source"] for h in resp.get("hits", {}).get("hits", [])]
    raw = pd.DataFrame(hits)
    if raw.empty:
        return pd.DataFrame(columns=["Datum", "Wert"])

    raw["date"] = pd.to_datetime(raw["date"], errors="coerce")
    raw = _filter_dedupe_by_mode(raw, source_mode)
    raw = raw.sort_values("date")

    # erstes Feld mit echten Werten wählen
    for f in fields:
        if f in raw.columns and raw[f].notna().any():
            out = raw[["date", f]].dropna()
            if not out.empty:
                out = out.rename(columns={"date": "Datum", f: "Wert"})
                return out
    return pd.DataFrame(columns=["Datum", "Wert"])


def _compute_yoy_from_history(es, symbol: str, fields, source_mode: Optional[str] = None, periods_back: int = 4):
    """
    YoY (aktuelles Quartal vs. Vorjahresquartal) für ein Feld ODER eine Feldliste.
    Bei Feldliste wird das erste mit Daten verwendet.
    """
    df_hist = lade_historische_kennzahlen(es, symbol, fields, source_mode)
    if df_hist.empty or len(df_hist) <= periods_back:
        return None
    latest = _first_float(df_hist["Wert"].iloc[-1])
    prev = _first_float(df_hist["Wert"].iloc[-1 - periods_back])
    if latest is None or prev in (None, 0):
        return None
    return (latest - prev) / abs(prev)


def enrich_document_fields(doc: dict, es=None, source_mode: Optional[str] = None, fill_growth_from_history: bool = True) -> dict:
    """
    Füllt NUR fehlende Felder (None) auf.
    - mapped FMP-Aliasse -> Standardfelder
    - berechnet sinnvolle Ableitungen (pro Aktie, Margen, Quoten)
    - optional: YoY-Wachstum aus ES-Historie
    """
    d = dict(doc)  # copy

    # --- 0) Aliasse (FMP -> Standardnamen), NUR wenn Standardfeld fehlt ---
    # Multiples / Ratios
    if d.get("peRatio") is None:
        d["peRatio"] = _get_any(d, "priceEarningsRatio", "trailingPE", "peRatio")
    if d.get("priceToBook") is None:
        d["priceToBook"] = _get_any(d, "priceToBookRatio")
    if d.get("pegRatio") is None:
        d["pegRatio"] = _get_any(d, "priceEarningsToGrowthRatio")

    # Dividenden
    if d.get("dividendYield") is None:
        d["dividendYield"] = _get_any(d, "dividendYield")
    if d.get("payoutRatio") is None:
        d["payoutRatio"] = _get_any(d, "payoutRatio")

    # Profil/Meta
    if d.get("beta") is None:
        d["beta"] = _get_any(d, "beta")
    if d.get("marketCap") is None:
        d["marketCap"] = _get_any(d, "marketCap", "mktCap")
    if d.get("industry") is None:
        d["industry"] = _get_any(d, "industry")
    if d.get("sector") is None:
        d["sector"] = _get_any(d, "sector")

    # Income Statement
    if d.get("revenue") is None:
        d["revenue"] = _get_any(d, "revenue")
    if d.get("netIncome") is None:
        d["netIncome"] = _get_any(d, "netIncome")
    if d.get("eps") is None:
        d["eps"] = _get_any(d, "eps", "epsdiluted", "epsDiluted")

    # Balance Sheet
    if d.get("totalAssets") is None:
        d["totalAssets"] = _get_any(d, "totalAssets")
    if d.get("totalStockholderEquity") is None:
        d["totalStockholderEquity"] = _get_any(
            d, "totalStockholderEquity", "totalStockholdersEquity", "shareholdersEquity"
        )
    if d.get("totalDebt") is None:
        d["totalDebt"] = _get_any(d, "totalDebt")
    if d.get("totalCash") is None:
        d["totalCash"] = _get_any(d, "cashAndShortTermInvestments", "cashAndCashEquivalents")
    if d.get("sharesOutstanding") is None:
        d["sharesOutstanding"] = _get_any(
            d, "sharesOutstanding", "weightedAverageShsOut", "weightedAverageShsOutDil"
        )
    if d.get("totalCurrentAssets") is None:
        d["totalCurrentAssets"] = _get_any(d, "totalCurrentAssets")
    if d.get("totalCurrentLiabilities") is None:
        d["totalCurrentLiabilities"] = _get_any(d, "totalCurrentLiabilities")
    if d.get("inventory") is None:
        d["inventory"] = _get_any(d, "inventory")

    # Ratios/KeyMetrics pro Aktie
    if d.get("cashPerShare") is None:
        d["cashPerShare"] = _get_any(d, "cashPerShare")
    if d.get("bookValuePerShare") is None:
        d["bookValuePerShare"] = _get_any(
            d, "bookValuePerShare", "shareholdersEquityPerShare", "tangibleBookValuePerShare"
        )
    if d.get("freeCashFlowPerShare") is None:
        d["freeCashFlowPerShare"] = _get_any(d, "freeCashFlowPerShare")

    # Cash Flow Statement → OCF / CapEx / FCF
    if d.get("operatingCashflow") is None and d.get("operatingCashFlow") is not None:
        d["operatingCashflow"] = d.get("operatingCashFlow")
    if d.get("capitalExpenditures") is None and d.get("capitalExpenditure") is not None:
        d["capitalExpenditures"] = d.get("capitalExpenditure")
    if d.get("freeCashFlow") is None:
        d["freeCashFlow"] = _get_any(d, "freeCashFlow", "freeCashflow")

    # --- 1) Direkte ABLEITUNGEN (nur wenn Ziel noch fehlt) ---
    if d.get("profitMargin") is None and d.get("netIncome") is not None and d.get("revenue"):
        d["profitMargin"] = _safe_div(d["netIncome"], d["revenue"])

    if d.get("currentRatio") is None and d.get("totalCurrentAssets") is not None and d.get("totalCurrentLiabilities"):
        d["currentRatio"] = _safe_div(d["totalCurrentAssets"], d["totalCurrentLiabilities"])

    if d.get("quickRatio") is None and all(k in d for k in ("totalCurrentAssets", "inventory", "totalCurrentLiabilities")):
        ca = _first_float(d.get("totalCurrentAssets"))
        inv = _first_float(d.get("inventory"))
        cl  = _first_float(d.get("totalCurrentLiabilities"))
        if ca is not None and inv is not None and cl not in (None, 0):
            d["quickRatio"] = (ca - inv) / cl

    if d.get("cashToDebt") is None and d.get("totalCash") is not None and d.get("totalDebt"):
        d["cashToDebt"] = _safe_div(d["totalCash"], d["totalDebt"])

    if d.get("equityRatio") is None and d.get("totalStockholderEquity") is not None and d.get("totalAssets"):
        d["equityRatio"] = _safe_div(d["totalStockholderEquity"], d["totalAssets"])

    if d.get("debtToAssets") is None and d.get("totalDebt") is not None and d.get("totalAssets"):
        d["debtToAssets"] = _safe_div(d["totalDebt"], d["totalAssets"])

    if d.get("debtToEquity") is None and d.get("totalDebt") is not None and d.get("totalStockholderEquity"):
        d["debtToEquity"] = _safe_div(d["totalDebt"], d["totalStockholderEquity"])

    if d.get("bookValuePerShare") is None and d.get("totalStockholderEquity") is not None and d.get("sharesOutstanding"):
        d["bookValuePerShare"] = _safe_div(d["totalStockholderEquity"], d["sharesOutstanding"])

    if d.get("cashPerShare") is None and d.get("totalCash") is not None and d.get("sharesOutstanding"):
        d["cashPerShare"] = _safe_div(d["totalCash"], d["sharesOutstanding"])

    if d.get("freeCashFlowPerShare") is None and d.get("freeCashFlow") is not None and d.get("sharesOutstanding"):
        d["freeCashFlowPerShare"] = _safe_div(d["freeCashFlow"], d["sharesOutstanding"])

    if d.get("fcfMargin") is None and d.get("freeCashFlow") is not None and d.get("revenue"):
        d["fcfMargin"] = _safe_div(d["freeCashFlow"], d["revenue"])

    # Price/Book aus MarketCap/Equity (Fallback)
    if d.get("priceToBook") is None and d.get("marketCap") is not None and d.get("totalStockholderEquity"):
        d["priceToBook"] = _safe_div(d["marketCap"], d["totalStockholderEquity"])

    # EPS (Fallback) aus NetIncome / Shares
    if d.get("eps") is None and d.get("netIncome") is not None and d.get("sharesOutstanding"):
        d["eps"] = _safe_div(d["netIncome"], d["sharesOutstanding"])

    # PEG (PE / earningsGrowth in Prozent- oder Dezimalform)
    if d.get("pegRatio") is None and d.get("peRatio") is not None and d.get("earningsGrowth") not in (None, 0):
        try:
            eg = float(d.get("earningsGrowth"))
            denom = eg * 100.0 if abs(eg) < 1 else eg
            if denom:
                d["pegRatio"] = float(d.get("peRatio")) / denom
        except Exception:
            pass

    # --- 2) YoY-Wachstum aus Historie (nur wenn fehlt & gewünscht) ---
    symbol = d.get("symbol")
    if fill_growth_from_history and es is not None and symbol:
        if d.get("revenueGrowth") is None:
            d["revenueGrowth"] = _compute_yoy_from_history(es, symbol, "revenue", source_mode)
        if d.get("epsGrowth") is None:
            d["epsGrowth"] = _compute_yoy_from_history(es, symbol, "eps", source_mode)
        if d.get("earningsGrowth") is None:
            d["earningsGrowth"] = d.get("epsGrowth")

    return d


# ==========================================================
# 1c️⃣ Suche (mit Enrichment)
# ==========================================================

def suche_aktie_in_es(es, symbol: str, source_mode: Optional[str] = None):
    symbol = (symbol or "").strip().upper()

    must = [
        {
            "bool": {
                "should": [
                    {"term": {"symbol.keyword": symbol}},
                    {"term": {"symbol": symbol}},
                    {"match": {"symbol": symbol}},
                ],
                "minimum_should_match": 1,
            }
        }
    ]
    q_src = _es_query_for_mode(source_mode)
    if q_src:
        must.append(q_src)

    query = {
        "size": 1000,
        "query": {"bool": {"must": must}},
        "sort": [{"date": {"order": "desc"}}, {"ingested_at": {"order": "desc"}}],
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

    doc = df.iloc[-1].to_dict()
    doc = enrich_document_fields(doc, es=es, source_mode=source_mode, fill_growth_from_history=True)
    return doc


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

def score_row(row_or_dict, criteria):
    """
    Bewertet eine Aktie nach übergebenen Kriterien.
    Unterstützt zwei Formate:
    - [(feld, regel)]
    - [(feld, beschreibung, regel, optional)]
    """
    getv = (row_or_dict.get if isinstance(row_or_dict, dict) else row_or_dict.__getitem__)
    score = 0
    for item in criteria:
        try:
            if len(item) == 2:
                field, rule = item
            elif len(item) >= 3:
                field, _, rule = item[:3]
            else:
                continue
            val = getv(field)
            if isinstance(val, (int, float)) and not pd.isna(val) and rule(val):
                score += 1
        except Exception:
            continue
    return score


def berechne_peter_lynch_kategorie(daten: dict, schwelle_gleichheit: float = 0.02):
    results = {}
    for cat, rules in CATEGORIES.items():
        score = score_row(daten, rules)
        max_rules = len(rules) if rules else 1
        results[cat] = score / max_rules

    # bester Score
    best_score = max(results.values())
    trefferquote = round(best_score * 100, 1)

    # Kategorien, die fast gleich gut passen
    top_kategorien = [
        cat for cat, s in results.items()
        if best_score - s <= schwelle_gleichheit
    ]

    # Text für Anzeige: entweder eine Kategorie oder mehrere
    if len(top_kategorien) == 1:
        kategorien_text = top_kategorien[0]
    elif len(top_kategorien) == 2:
        kategorien_text = f"{top_kategorien[0]} oder {top_kategorien[1]}"
    else:
        kategorien_text = ", ".join(top_kategorien[:-1]) + f" oder {top_kategorien[-1]}"
    return kategorien_text, trefferquote

def erklaere_kategorie(kategorie: str) -> str:
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
    details = {
        "Free Cashflow": daten.get("freeCashflow") if "freeCashflow" in daten else daten.get("freeCashFlow"),
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

    base_query: Dict[str, Any] = {"match_all": {}}
    q_src = _es_query_for_mode(source_mode)
    if q_src:
        base_query = {"bool": {"must": [q_src]}}

    query = {
        "size": limit,
        "sort": [{"date": {"order": "desc"}}, {"ingested_at": {"order": "desc"}}],
        "query": base_query,
    }
    resp = es.search(index=index, body=query)
    hits = [h["_source"] for h in resp.get("hits", {}).get("hits", [])]
    df = pd.DataFrame(hits)
    if df.empty:
        return df

    # neueste pro Symbol
    if "symbol" in df.columns and "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = df.sort_values(["symbol", "date"], ascending=[True, False]).drop_duplicates("symbol", keep="first")

    # Harmonisierung/abgeleitete Felder (einfach)
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
    if "debtToAssets" not in df.columns and {"totalDebt", "totalAssets"} <= set(df.columns):
        with pd.option_context("mode.use_inf_as_na", True):
            df["debtToAssets"] = df["totalDebt"] / df["totalAssets"]


    # Quelle/Dedupe
    df = _filter_dedupe_by_mode(df, source_mode)

    # Dokument-weise Enrichment (ohne teure YoY-Abfragen)
    if not df.empty:
        df = pd.DataFrame([
            enrich_document_fields(r.to_dict(), es=es, source_mode=source_mode, fill_growth_from_history=False)
            for _, r in df.iterrows()
        ])

    return df


def load_industries(es=None, index: str = INDEX, source_mode: Optional[str] = None) -> pd.DataFrame:
    """
    Lädt Symbol & Industry ohne 'collapse', damit es auch funktioniert,
    wenn 'symbol.keyword' nicht existiert. Dedupe machen wir in Python.
    """
    if es is None:
        es = get_es_connection()

    must = []
    q_src = _es_query_for_mode(source_mode)
    if q_src:
        must.append(q_src)

    query = {
        "size": 10000,
        "_source": ["symbol", "industry"],
        "query": {"bool": {"must": must}} if must else {"match_all": {}},
        "sort": [{"date": {"order": "desc"}}]
    }

    resp = es.search(index=index, body=query)
    hits = [h["_source"] for h in resp.get("hits", {}).get("hits", [])]
    df = pd.DataFrame(hits) if hits else pd.DataFrame(columns=["symbol", "industry"])

    # Dedupe in Python: pro Symbol der jüngste Eintrag
    if not df.empty and "symbol" in df.columns:
        df = (
            df.dropna(subset=["symbol"])
              .drop_duplicates(subset=["symbol"], keep="first")
        )

    return df[["symbol", "industry"]] if not df.empty else df


# ==========================================================
# 6️⃣ Portfolio-Funktionen
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
        body={
            "size": limit,
            "sort": [{"updated_at": {"order": "desc"}}],
            "_source": ["name", "market_condition", "updated_at", "totals.total_amount"],
        },
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
    

def force_fill_metrics(d: dict, es, source_mode: Optional[str] = None, flags: Optional[dict] = None) -> dict:
   
    if flags is None:
        flags = {
            "revenueGrowth_from_history": True,
            "epsGrowth_from_history": True,
            "fcf_buildable": True,
            "fcf_per_share_buildable": True,
            "debtToAssets_buildable": True,
            "cashToDebt_buildable": True,
            "cashPerShare_buildable": True,
            "bookValuePerShare_buildable": True,
            "sgaTrend_buildable": True,
        }

    out = enrich_document_fields(d, es=es, source_mode=source_mode, fill_growth_from_history=False)

    # 1) YoY über Alias-Listen
    if flags.get("revenueGrowth_from_history") and out.get("revenueGrowth") is None and es is not None:
        out["revenueGrowth"] = _compute_yoy_from_history(
            es, out.get("symbol"),
            fields=["revenue", "totalRevenue", "Revenue", "revenueTTM", "totalRevenueTTM"],
            source_mode=source_mode
        )
    if flags.get("epsGrowth_from_history") and out.get("epsGrowth") is None and es is not None:
        out["epsGrowth"] = _compute_yoy_from_history(
            es, out.get("symbol"),
            fields=["eps", "trailingEps", "reportedEPS", "epsDiluted", "epsdiluted"],
            source_mode=source_mode
        )
    if out.get("earningsGrowth") is None and out.get("epsGrowth") is not None:
        out["earningsGrowth"] = out["epsGrowth"]

    # 2) FCF (OCF - |CapEx|)
    if flags.get("fcf_buildable") and out.get("freeCashFlow") is None:
        ocf   = _get_any(out, "freeCashFlow", "operatingCashflow", "operatingCashFlow",
                         "netCashProvidedByOperatingActivities")
        capex = _get_any(out, "capitalExpenditures", "capitalExpenditure")
        if ocf is not None and capex is not None:
            try:
                out["freeCashFlow"] = float(ocf) - abs(float(capex))
            except Exception:
                pass

    # 3) FCF je Aktie
    if flags.get("fcf_per_share_buildable") and out.get("freeCashFlowPerShare") is None:
        if out.get("freeCashFlow") is not None and out.get("sharesOutstanding"):
            out["freeCashFlowPerShare"] = _safe_div(out["freeCashFlow"], out["sharesOutstanding"])
        # === Backfill totalDebt (falls yfinance z. B. leer ist) ===
    if out.get("totalDebt") is None and es is not None:
        td = lade_historische_kennzahlen(
            es, out.get("symbol"),
            ["totalDebt", "shortLongTermDebtTotal", "shortLongTermDebt"],
            source_mode
        )
        if not td.empty:
            out["totalDebt"] = float(td["Wert"].iloc[-1])

    # === Backfill totalAssets ===
    if out.get("totalAssets") is None and es is not None:
        ta = lade_historische_kennzahlen(
            es, out.get("symbol"),
            ["totalAssets", "TotalAssets"],
            source_mode
        )
        if not ta.empty:
            out["totalAssets"] = float(ta["Wert"].iloc[-1])

    # 4) Quoten/Ableitungen
    if flags.get("debtToAssets_buildable") and out.get("debtToAssets") is None:
        if out.get("totalDebt") is not None and out.get("totalAssets") is not None:
            out["debtToAssets"] = _safe_div(out["totalDebt"], out["totalAssets"])

    if flags.get("cashToDebt_buildable") and out.get("cashToDebt") is None:
        if out.get("totalCash") is not None and out.get("totalDebt") is not None:
            out["cashToDebt"] = _safe_div(out["totalCash"], out["totalDebt"])

    if flags.get("cashPerShare_buildable") and out.get("cashPerShare") is None:
        if out.get("totalCash") is not None and out.get("sharesOutstanding"):
            out["cashPerShare"] = _safe_div(out["totalCash"], out["sharesOutstanding"])

    if flags.get("bookValuePerShare_buildable") and out.get("bookValuePerShare") is None:
        if out.get("totalStockholderEquity") is not None and out.get("sharesOutstanding"):
            out["bookValuePerShare"] = _safe_div(out["totalStockholderEquity"], out["sharesOutstanding"])

    # 5) SG&A-Trend (SG&A/Revenue rückläufig?)
    if flags.get("sgaTrend_buildable") and out.get("sgaTrend") is None and es is not None:
        sym = out.get("symbol")
        if sym:
            df_rev = lade_historische_kennzahlen(es, sym, ["revenue","totalRevenue","Revenue","revenueTTM"], source_mode)
            df_sga = lade_historische_kennzahlen(es, sym, ["sgaExpense","sellingGeneralAndAdministrative","sga"], source_mode)
            m = _merge_asof_two(df_sga, df_rev)
            if not m.empty and len(m) >= 5:
                # m: Spalten 'Wert_x' (SGA), 'Wert_y' (Revenue)
                ratio = m["Wert_x"] / m["Wert_y"]
                try:
                    idx = pd.RangeIndex(len(ratio))
                    slope = pd.Series(ratio.values).cov(idx) / pd.Series(idx).var()
                    out["sgaTrend"] = (slope is not None) and (slope < 0)
                except Exception:
                    pass

    return out

