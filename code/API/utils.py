import os
import random
import time
from datetime import datetime
from elasticsearch import Elasticsearch
from elastic_transport import ConnectionError as ESConnectionError
from requests.adapters import HTTPAdapter, Retry
import requests


# === 1️⃣ Elasticsearch-Client ===

def es_client() -> Elasticsearch:
    """Erstellt Elasticsearch-Client basierend auf Umgebungsvariablen."""
    es_url = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
    es = Elasticsearch(
        es_url,
        request_timeout=25,
        retry_on_timeout=True,
        max_retries=5,
    )
    return es


def es_healthcheck(es: Elasticsearch) -> str:
    """Überprüft Verbindung zu Elasticsearch."""
    try:
        if not es.ping():
            return "⚠️ Elasticsearch antwortet nicht."
        info = es.info()
        name = info.get("name", "unbekannt")
        ver = info.get("version", {}).get("number", "?")
        return f"✅ Verbunden mit Elasticsearch '{name}' (v{ver})"
    except ESConnectionError as e:
        return f"❌ Keine Verbindung zu Elasticsearch: {e}"
    except Exception as e:
        return f"❌ Fehler beim Healthcheck: {e}"


def ensure_index(es: Elasticsearch, index_name: str):
    """Erstellt Index mit Mapping, falls er nicht existiert."""
    try:
        if es.indices.exists(index=index_name):
            print(f"ℹ️ Index '{index_name}' existiert bereits.")
            return
    except Exception as e:
        print(f"Fehler beim Prüfen des Index: {e}")

    body = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "properties": {
                "symbol": {"type": "keyword"},
                "date": {"type": "date"},
                "source": {"type": "keyword"},
                "ingested_at": {"type": "date"},
                "peRatio": {"type": "double"},
                "bookValuePerShare": {"type": "double"},
                "dividendYield": {"type": "double"},
                "priceToBook": {"type": "double"},
                "eps": {"type": "double"},
                "marketCap": {"type": "double"},
            }
        },
    }

    try:
        es.indices.create(index=index_name, body=body)
        print(f"✅ Index '{index_name}' wurde neu erstellt.")
    except Exception as e:
        print(f"Fehler beim Erstellen des Index: {e}")


# === 2️⃣ HTTP Session (mit Retry & Anti-Bot Headern) ===

def requests_session() -> requests.Session:
    """Erstellt HTTP-Session mit Retry-Logik."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# === 3️⃣ Random User-Agent für Anti-Bot Verhalten ===

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/18.17763",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110 Safari/537.36",
]

def random_user_agent() -> str:
    """Gibt zufälligen User-Agent zurück."""
    return random.choice(USER_AGENTS)


# === 4️⃣ Kleine Helper ===

def sleep_with_jitter(base: float = 1.2, var: float = 1.0):
    """Schläft mit leichtem Zufallsintervall (Anti-Bot)."""
    delay = base + random.uniform(0, var)
    time.sleep(delay)
    return delay


def log(msg: str):
    """Konsolen-Log mit Zeitstempel."""
    now = datetime.now().strftime("%H:%M:%S")
    print(f"[{now}] {msg}")
