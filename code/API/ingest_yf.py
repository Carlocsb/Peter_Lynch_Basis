# code/API/ingest_yf.py
import os
import json
import time
import random
from datetime import datetime, UTC
from pathlib import Path
from typing import Dict, List
import yfinance as yf
from elasticsearch import helpers
from dotenv import load_dotenv

from utils import es_client, es_healthcheck, ensure_index

# === 1️⃣ Setup ===
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env", override=False)
load_dotenv(BASE_DIR / ".env.local", override=False)

ES_INDEX = os.getenv("ELASTICSEARCH_INDEX", "stocks")
es = es_client()

DATA_DIR = BASE_DIR / "data"
CACHE_FILE = DATA_DIR / "sp500_symbols.json"

# === 2️⃣ Symbol-Quelle ===
def load_symbols() -> List[str]:
    if CACHE_FILE.exists():
        with open(CACHE_FILE, "r") as f:
            symbols = json.load(f)
            print(f"✅ {len(symbols)} Symbole aus Cache geladen.")
            return symbols
    else:
        symbols = [
            "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA",
            "TSLA", "JPM", "JNJ", "PG", "V", "MA", "HD", "DIS",
            "PFE", "NFLX", "KO", "PEP", "XOM", "CSCO", "BAC",
            "CVX", "NKE", "ORCL", "ABBV", "INTC", "ADBE", "T", "WMT", "UNH"
        ]
        print(f"⚠️ Keine Cache-Datei gefunden – nutze {len(symbols)} Test-Symbole.")
        return symbols

# === 3️⃣ Metriken ===
METRIC_FIELDS = [
    "trailingPE", "priceToBook", "trailingEps", "dividendYield", "marketCap",
    "bookValue", "freeCashflow", "totalDebt", "totalRevenue",
    "revenueGrowth", "profitMargins", "pegRatio", "sector", "industry", "beta",
    "trailingAnnualDividendRate", "debtToEquity", "quickRatio", "currentRatio",
    "payoutRatio", "totalCashPerShare"
]

FIELD_MAP = {
    "trailingPE": "peRatio",
    "priceToBook": "priceToBook",
    "trailingEps": "eps",
    "dividendYield": "dividendYield",
    "marketCap": "marketCap",
    "bookValue": "bookValuePerShare",
    "freeCashflow": "freeCashFlow",
    "totalDebt": "totalDebt",
    "totalRevenue": "revenue",
    "revenueGrowth": "revenueGrowth",
    "profitMargins": "profitMargin",
    "pegRatio": "pegRatio",
    "sector": "sector",
    "industry": "industry",
    "beta": "beta",
    "trailingAnnualDividendRate": "dividendRate",
    "debtToEquity": "debtToEquity",
    "quickRatio": "quickRatio",
    "currentRatio": "currentRatio",
    "payoutRatio": "payoutRatio",
    "totalCashPerShare": "cashPerShare",
}

# === 4️⃣ Daten laden ===
def get_metrics(symbol: str) -> Dict:
    ticker = yf.Ticker(symbol)
    info = ticker.info
    metrics = {}

    for yf_field, our_field in FIELD_MAP.items():
        val = info.get(yf_field)
        if isinstance(val, (int, float)):
            metrics[our_field] = float(val)
        elif isinstance(val, str):
            metrics[our_field] = val

    return metrics

# === 5️⃣ Dokument aufbauen ===
def build_doc(symbol: str, metrics: Dict) -> Dict:
    today = str(datetime.now(UTC).date())
    return {
        "_index": ES_INDEX,
        "_id": f"{symbol}|{today}",
        "_source": {
            "symbol": symbol,
            "date": today,
            "source": "yfinance",
            "ingested_at": datetime.now(UTC).isoformat(),
            **metrics
        },
    }

# === 6️⃣ Pipeline ===
def run(batch_sleep: float = 2.5):
    print(es_healthcheck(es))
    ensure_index(es, ES_INDEX)

    symbols = load_symbols()
    random.shuffle(symbols)

    docs_buffer = []
    written = 0

    for i, symbol in enumerate(symbols, 1):
        try:
            metrics = get_metrics(symbol)
            if not metrics:
                continue

            doc = build_doc(symbol, metrics)
            docs_buffer.append(doc)

            if len(docs_buffer) >= 25:
                helpers.bulk(es, docs_buffer)
                written += len(docs_buffer)
                docs_buffer.clear()
                print(f"[{i}/{len(symbols)}] {written} Dokumente gespeichert...")

        except Exception as e:
            print(f"[FEHLER] {symbol}: {e}")

        time.sleep(batch_sleep + random.uniform(0.4, 0.8))

    if docs_buffer:
        helpers.bulk(es, docs_buffer)
        written += len(docs_buffer)

    print(f"✅ Fertig. Gesamt gespeichert: {written} Dokumente.")

# === 7️⃣ Einstiegspunkt ===
if __name__ == "__main__":
    run()
