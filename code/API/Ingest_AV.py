# code/API/ingest_av.py
import os, json, time, random
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List
import requests
from elasticsearch import helpers
from utils import es_client, es_healthcheck, ensure_index  # <- vorhanden in API/utils.py

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR  = BASE_DIR / "data"
CACHE_FILE = DATA_DIR / "sp500_symbols.json"   # falls vorhanden
ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "stocks")

API_KEY = os.getenv("ALPHAVANTAGE_API_KEY")
assert API_KEY, "ALPHAVANTAGE_API_KEY fehlt (.env)!"

AV_BASE = "https://www.alphavantage.co/query"

es = es_client()

def load_symbols() -> List[str]:
    if CACHE_FILE.exists():
        return json.loads(CACHE_FILE.read_text())
    # kleine Fallback-Liste
    return ["AAPL","MSFT","AMZN","GOOGL","META","NVDA","TSLA","JPM","PG","PFE","BAC","XOM","CVX","INTC","T","UNH","DIS"]

def av_get(params: Dict) -> Dict:
    params = {**params, "apikey": API_KEY}
    r = requests.get(AV_BASE, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    # AV gibt bei Limits auch 200 zurück, aber mit "Note" / "Information"
    if "Note" in data or "Information" in data:
        raise RuntimeError(data.get("Note") or data.get("Information"))
    return data

def fetch_overview(symbol: str) -> Dict:
    return av_get({"function": "OVERVIEW", "symbol": symbol})

def fetch_income(symbol: str) -> Dict:
    return av_get({"function": "INCOME_STATEMENT", "symbol": symbol})

def fetch_balance(symbol: str) -> Dict:
    return av_get({"function": "BALANCE_SHEET", "symbol": symbol})

def fetch_cashflow(symbol: str) -> Dict:
    return av_get({"function": "CASH_FLOW", "symbol": symbol})

def fetch_earnings(symbol: str) -> Dict:
    # reportedEPS je Quartal/Jahr (für EPS-/Earnings-Growth)
    return av_get({"function": "EARNINGS", "symbol": symbol})

def latest_quarter_value(arr: List[Dict], key: str):
    """Nimmt das jüngste Quartal; AV liefert bereits neu -> alt."""
    if not arr:
        return None
    v = arr[0].get(key)
    try:
        return float(v)
    except Exception:
        return None

def yoy_growth(latest: float, prev_year: float):
    try:
        if prev_year in (None, 0):
            return None
        return (latest - prev_year) / abs(prev_year)
    except Exception:
        return None

def build_metrics(symbol: str) -> Dict:
    """
    Mapped AV-Felder auf deine ES-Feldnamen.
    Nutzt:
    - OVERVIEW: PE, PB, DividendYield, PayoutRatio, MarketCap, SharesOutstanding, Sector/Industry
    - Quartalsstatements: Debt/Assets, Cash/Share, FCF, FCF/Share, Current Ratio
    - EARNINGS: EPS (Quartal) für YoY EPS/Earnings Growth
    - INCOME: Revenue (Quartal) für YoY Revenue Growth + SG&A-Quote (Trend)
    """
    ov  = fetch_overview(symbol)
    inc = fetch_income(symbol)
    bal = fetch_balance(symbol)
    cfs = fetch_cashflow(symbol)
    ern = fetch_earnings(symbol)

    # --- overview (aktuell)
    def to_float(x):
        try: return float(x)
        except: return None

    marketCap     = to_float(ov.get("MarketCapitalization"))
    peRatio       = to_float(ov.get("PERatio"))
    priceToBook   = to_float(ov.get("PriceToBookRatio"))
    payoutRatio   = to_float(ov.get("PayoutRatio"))
    dividendYield = to_float(ov.get("DividendYield"))
    sector        = ov.get("Sector")
    industry      = ov.get("Industry")
    shares_out    = to_float(ov.get("SharesOutstanding"))

    # --- Quartals-Arrays
    q_inc = inc.get("quarterlyReports", []) or []
    q_bal = bal.get("quarterlyReports", []) or []
    q_cfs = cfs.get("quarterlyReports", []) or []

    # Helper für jüngsten Wert
    def qv(arr, key):
        if not arr: return None
        v = arr[0].get(key)
        try: return float(v)
        except: return None

    # Balance Sheet
    total_assets = qv(q_bal, "totalAssets")
    total_debt   = qv(q_bal, "totalLiabilities") or qv(q_bal, "shortLongTermDebtTotal")
    total_cash   = qv(q_bal, "cashAndCashEquivalentsAtCarryingValue")
    total_equity = qv(q_bal, "totalShareholderEquity")
    cur_assets   = qv(q_bal, "totalCurrentAssets")
    cur_liab     = qv(q_bal, "totalCurrentLiabilities")

    # Current Ratio
    currentRatio = None
    if isinstance(cur_assets, float) and isinstance(cur_liab, float) and cur_liab:
        currentRatio = cur_assets / cur_liab

    # Cashflow → FCF (OCF - CapEx)
    ocf   = qv(q_cfs, "operatingCashflow")
    capex = qv(q_cfs, "capitalExpenditures")
    freeCashflow = None
    if isinstance(ocf, float) and isinstance(capex, float):
        freeCashflow = ocf - abs(capex)
    freeCashFlowPerShare = None
    if isinstance(freeCashflow, float) and isinstance(shares_out, float) and shares_out:
        freeCashFlowPerShare = freeCashflow / shares_out

    # Debt/Assets, Equity/Assets, Cash/Debt
    debtToAssets = None
    if isinstance(total_debt, float) and isinstance(total_assets, float) and total_assets:
        debtToAssets = total_debt / total_assets
    equityRatio = None
    if isinstance(total_equity, float) and isinstance(total_assets, float) and total_assets:
        equityRatio = total_equity / total_assets
    cashToDebt = None
    if isinstance(total_cash, float) and isinstance(total_debt, float) and total_debt:
        cashToDebt = total_cash / total_debt

    # YoY Revenue Growth (Vorjahresquartal ~ Index 4)
    revenueGrowth = None
    try:
        if len(q_inc) >= 5:
            rev_latest = float(q_inc[0]["totalRevenue"])
            rev_prev_y = float(q_inc[4]["totalRevenue"])
            revenueGrowth = yoy_growth(rev_latest, rev_prev_y)
    except Exception:
        pass

    # EPS/Earnings Growth (YoY) via EARNINGS
    earningsGrowth = None
    epsGrowth = None
    try:
        q_ern = ern.get("quarterlyEarnings", []) or []
        if len(q_ern) >= 5:
            eps_latest = float(q_ern[0]["reportedEPS"])
            eps_prev_y = float(q_ern[4]["reportedEPS"])
            epsGrowth = yoy_growth(eps_latest, eps_prev_y)
            earningsGrowth = epsGrowth  # Alias
    except Exception:
        pass

    # SG&A-Trend (Quote ggü. Vorquartal gefallen?)
    sgaTrend = None
    try:
        if len(q_inc) >= 2:
            def safe(x):
                try: return float(x)
                except: return None
            rev0 = safe(q_inc[0].get("totalRevenue"))
            rev1 = safe(q_inc[1].get("totalRevenue"))
            sga0 = safe(q_inc[0].get("sellingGeneralAdministrative")) or safe(q_inc[0].get("sellingGeneralAdministrativeExpenses"))
            sga1 = safe(q_inc[1].get("sellingGeneralAdministrative")) or safe(q_inc[1].get("sellingGeneralAdministrativeExpenses"))
            if all(isinstance(v, float) and v for v in [rev0, rev1, sga0, sga1]):
                q0 = sga0 / rev0
                q1 = sga1 / rev1
                sgaTrend = (q0 < q1)  # True, wenn Quote gefallen ist
    except Exception:
        pass

    # Cash per Share
    cashPerShare = None
    if isinstance(total_cash, float) and isinstance(shares_out, float) and shares_out:
        cashPerShare = total_cash / shares_out

    metrics = {
        "marketCap": marketCap,
        "peRatio": peRatio,
        "priceToBook": priceToBook,
        "payoutRatio": payoutRatio,
        "dividendYield": dividendYield,
        "sector": sector,
        "industry": industry,

        "freeCashflow": freeCashflow,
        "freeCashFlowPerShare": freeCashFlowPerShare,
        "cashPerShare": cashPerShare,

        "debtToAssets": debtToAssets,
        "equityRatio": equityRatio,
        "cashToDebt": cashToDebt,
        "currentRatio": currentRatio,

        "revenueGrowth": revenueGrowth,
        "earningsGrowth": earningsGrowth,
        "epsGrowth": epsGrowth,
        "sgaTrend": sgaTrend,
    }

    # Nur nicht-None-Felder zurückgeben
    return {k: v for k, v in metrics.items() if v is not None}

def build_doc(symbol: str, metrics: Dict) -> Dict:
    today = str(datetime.now(UTC).date())
    return {
        "_index": ES_INDEX,
        "_id": f"{symbol}|{today}|av",
        "_source": {
            "symbol": symbol,
            "date": today,
            "source": "alphavantage",
            "ingested_at": datetime.now(UTC).isoformat(),
            **metrics
        },
    }

def run():
    print(es_healthcheck(es))
    ensure_index(es, ES_INDEX)

    symbols = load_symbols()
    random.shuffle(symbols)

    buffer, written = [], 0
    for i, sym in enumerate(symbols, 1):
        try:
            metrics = build_metrics(sym)
            if not metrics:
                continue
            buffer.append(build_doc(sym, metrics))

            if len(buffer) >= 25:
                helpers.bulk(es, buffer)
                written += len(buffer)
                buffer.clear()
                print(f"[{i}/{len(symbols)}] {written} Dokumente gespeichert...")
        except Exception as e:
            print(f"[FEHLER] {sym}: {e}")

        # Free-Tier Rate Limit: max 5 Requests / Minute
        time.sleep(15)

    if buffer:
        helpers.bulk(es, buffer)
        written += len(buffer)

    print(f"✅ Fertig. Gesamt gespeichert: {written} Dokumente.")

if __name__ == "__main__":
    run()
