# code/API/ingest_fmp_sp.py
import os, json, random
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List, Any
from elasticsearch import helpers
from utils import es_client, es_healthcheck, ensure_index  # vorhanden in code/API/utils.py

# === Pfade & Config ===
BASE_DIR    = Path(__file__).resolve().parent          # .../code/API
PROJECTROOT = BASE_DIR.parents[1]                      # .../ (Projektwurzel)
FMP_DIR = PROJECTROOT / "data" / "sp_data" / "total_sp_data"  # Ordner mit den FMP-Dateien
ES_INDEX    = os.getenv("ELASTICSEARCH_INDEX", "stocks")

es = es_client()

# === Helfer ===
def _f(x):
    try:
        if x is None: return None
        if isinstance(x, (int, float)): return float(x)
        return float(str(x).replace(",", ""))
    except Exception:
        return None

def _read_json(path: Path):
    txt = path.read_text(encoding="utf-8", errors="ignore").strip()
    if not txt: return None
    try:
        return json.loads(txt)
    except Exception:
        rows = []
        for ln in txt.splitlines():
            ln = ln.strip()
            if not ln: continue
            try: rows.append(json.loads(ln))
            except: pass
        return rows or None

def _latest_row(seq):
    if isinstance(seq, list) and seq:
        return seq[0] if isinstance(seq[0], dict) else {}
    return seq if isinstance(seq, dict) else {}

def _discover_symbols(folder: Path) -> List[str]:
    syms = set()
    for p in folder.glob("*_*.json"):
        name = p.name
        if "_" in name:
            syms.add(name.split("_", 1)[0])
    return sorted(syms)

# === Kern: baue YF-kompatible Felder aus lokalen FMP-Dateien ===
def build_metrics_fmp(symbol: str, base_dir: Path) -> Dict[str, Any]:
    files = {
        "Profile":           base_dir / f"{symbol}_Profile.json",
        "IncomeStatement":   base_dir / f"{symbol}_IncomeStatement.json",
        "BalanceSheet":      base_dir / f"{symbol}_BalanceSheet.json",
        "CashflowStatement": base_dir / f"{symbol}_CashflowStatement.json",
        "KeyMetrics":        base_dir / f"{symbol}_KeyMetrics.json",
        "Ratios":            base_dir / f"{symbol}_Ratios.json",
    }
    data = {k: (_read_json(p) if p.exists() else None) for k, p in files.items()}

    prof = data.get("Profile")
    if isinstance(prof, list) and prof: prof = prof[0]
    if not isinstance(prof, dict): prof = {}

    inc  = _latest_row(data.get("IncomeStatement")   or {})
    bal  = _latest_row(data.get("BalanceSheet")      or {})
    cfs  = _latest_row(data.get("CashflowStatement") or {})
    kms  = _latest_row(data.get("KeyMetrics")        or {})
    rat  = _latest_row(data.get("Ratios")            or {})

    # Basisfelder (wie ingest_yf)
    marketCap   = _f(prof.get("mktCap") or prof.get("marketCap"))
    peRatio     = _f(kms.get("peRatio") or rat.get("priceEarningsRatio"))
    priceToBook = _f(kms.get("priceToBookRatio") or rat.get("priceToBookRatio"))
    dividendYield = _f(rat.get("dividendYield") or kms.get("dividendYield"))
    payoutRatio   = _f(rat.get("payoutRatio"))
    pegRatio      = _f(kms.get("pegRatio"))
    beta          = _f(prof.get("beta"))
    sector        = prof.get("sector")
    industry      = prof.get("industry")

    shares_out = _f(prof.get("sharesOutstanding") or kms.get("sharesOutstanding"))

    # Income
    revenue    = _f(inc.get("revenue") or inc.get("totalRevenue"))
    net_income = _f(inc.get("netIncome"))
    profitMargin = (net_income / revenue) if isinstance(net_income, float) and isinstance(revenue, float) and revenue else None

    # YoY Revenue Growth (0 vs 4)
    revenueGrowth = None
    inc_list = data.get("IncomeStatement") if isinstance(data.get("IncomeStatement"), list) else []
    if len(inc_list) >= 5:
        try:
            r0 = _f(inc_list[0].get("revenue") or inc_list[0].get("totalRevenue"))
            r4 = _f(inc_list[4].get("revenue") or inc_list[4].get("totalRevenue"))
            if isinstance(r0, float) and isinstance(r4, float) and r4:
                revenueGrowth = (r0 - r4) / abs(r4)
        except: pass

    # Balance
    totalAssets  = _f(bal.get("totalAssets"))
    totalEquity  = _f(bal.get("totalStockholdersEquity") or bal.get("totalStockholderEquity"))
    totalCash    = _f(bal.get("cashAndCashEquivalents"))
    cur_assets   = _f(bal.get("totalCurrentAssets"))
    cur_liab     = _f(bal.get("totalCurrentLiabilities"))
    inventory    = _f(bal.get("inventory"))

    long_debt  = _f(bal.get("longTermDebt"))
    short_debt = _f(bal.get("shortTermDebt"))
    sld_total  = _f(bal.get("shortLongTermDebtTotal"))
    if isinstance(long_debt, float) or isinstance(short_debt, float):
        totalDebt = (long_debt or 0.0) + (short_debt or 0.0)
    else:
        totalDebt = sld_total

    currentRatio = (cur_assets / cur_liab) if isinstance(cur_assets, float) and isinstance(cur_liab, float) and cur_liab else None
    quickRatio   = ((cur_assets - inventory) / cur_liab) if all(isinstance(x, float) for x in [cur_assets, inventory, cur_liab]) and cur_liab else None
    debtToEquity = (totalDebt / totalEquity) if isinstance(totalDebt, float) and isinstance(totalEquity, float) and totalEquity else None

    # Cashflow -> Free Cash Flow (OCF - CapEx)
    ocf   = _f(cfs.get("operatingCashFlow") or cfs.get("operatingCashflow"))
    capex = _f(cfs.get("capitalExpenditure") or cfs.get("capitalExpenditures"))
    freeCashflow = (ocf - abs(capex)) if isinstance(ocf, float) and isinstance(capex, float) else None
    cashPerShare = (totalCash / shares_out) if isinstance(totalCash, float) and isinstance(shares_out, float) and shares_out else None
    freeCashFlowPerShare = (freeCashflow / shares_out) if isinstance(freeCashflow, float) and isinstance(shares_out, float) and shares_out else None

    # Abgeleitet
    cashToDebt  = (totalCash / totalDebt) if isinstance(totalCash, float) and isinstance(totalDebt, float) and totalDebt else None
    equityRatio = (totalEquity / totalAssets) if isinstance(totalEquity, float) and isinstance(totalAssets, float) and totalAssets else None
    fcfMargin   = (freeCashflow / revenue) if isinstance(freeCashflow, float) and isinstance(revenue, float) and revenue else None

    # Buchwert/Aktie & EPS
    bookValuePerShare = (totalEquity / shares_out) if isinstance(totalEquity, float) and isinstance(shares_out, float) and shares_out else _f(kms.get("bookValuePerShare"))
    eps = _f(kms.get("eps") or inc.get("eps"))

    return {k: v for k, v in {
        "marketCap": marketCap,
        "peRatio": peRatio,
        "priceToBook": priceToBook,
        "dividendYield": dividendYield,
        "payoutRatio": payoutRatio,
        "sector": sector,
        "industry": industry,
        "beta": beta,
        "pegRatio": pegRatio,

        "revenue": revenue,
        "profitMargin": profitMargin,
        "revenueGrowth": revenueGrowth,

        "totalAssets": totalAssets,
        "totalDebt": totalDebt,
        "totalCash": totalCash,
        "totalStockholderEquity": totalEquity,
        "sharesOutstanding": shares_out,

        "currentRatio": currentRatio,
        "quickRatio": quickRatio,
        "debtToEquity": debtToEquity,

        "freeCashflow": freeCashflow,
        "cashPerShare": cashPerShare,
        "freeCashFlowPerShare": freeCashFlowPerShare,

        "cashToDebt": cashToDebt,
        "equityRatio": equityRatio,
        "fcfMargin": fcfMargin,

        "bookValuePerShare": bookValuePerShare,
        "eps": eps,
    }.items() if v is not None}

def build_doc(symbol: str, metrics: Dict[str, Any]) -> Dict[str, Any]:
    today = str(datetime.now(UTC).date())
    return {
        "_index": ES_INDEX,
        "_id": f"{symbol}|{today}|fmp",  # getrennt von yfinance/av
        "_source": {
            "symbol": symbol,
            "date": today,
            "source": "fmp",
            "ingested_at": datetime.now(UTC).isoformat(),
            **metrics
        },
    }

def run(batch_flush: int = 500):
    print(es_healthcheck(es))
    ensure_index(es, ES_INDEX)

    if not FMP_DIR.exists():
        raise FileNotFoundError(f"FMP-Datenordner nicht gefunden: {FMP_DIR}")

    symbols = _discover_symbols(FMP_DIR)
    random.shuffle(symbols)

    buffer, written = [], 0
    for i, sym in enumerate(symbols, 1):
        try:
            metrics = build_metrics_fmp(sym, FMP_DIR)
            if not metrics:
                continue
            buffer.append(build_doc(sym, metrics))

            if len(buffer) >= batch_flush:
                helpers.bulk(es, buffer)
                written += len(buffer)
                buffer.clear()
                print(f"[{i}/{len(symbols)}] {written} Dokumente gespeichert...")
        except Exception as e:
            print(f"[FEHLER] {sym}: {e}")

    if buffer:
        helpers.bulk(es, buffer)
        written += len(buffer)

    print(f"✅ FMP-Ingest fertig. Gesamt gespeichert: {written} Dokumente in '{ES_INDEX}'.")

if __name__ == "__main__":
    run()
