# Automatisierte Aktienanalyse nach Peter Lynch (Praxisprojekt)

---

## Inhaltsverzeichnis
- [1. Einleitung](#1-einleitung)
- [2. Bezug zum WI-Projekt](#2-bezug-zum-wi-projekt)
- [3. Theoretische Grundlage – Kurzüberblick](#3-theoretische-grundlage--kurzüberblick)
- [4. Systemarchitektur](#4-systemarchitektur)
- [5. Datenquellen und API-Abruf](#5-datenquellen-und-api-abruf)
- [6. Datenmodell und Elasticsearch-Mapping](#6-datenmodell-und-elasticsearch-mapping)
- [7. Implementierung (Code-Auszüge)](#7-implementierung-code-auszüge)
- [8. Streamlit-Dashboard](#8-streamlit-dashboard)
- [9. Setup & Deployment](#9-setup--deployment)
- [10. Geplante Erweiterungen](#10-geplante-erweiterungen)
- [11. Fazit](#11-fazit)
- [12. Quellen](#12-quellen)

---

## 1. Einleitung

### 1.1 Motivation  
Öffentlich verfügbare Finanzdaten werden häufig nur manuell ausgewertet oder in unstrukturierten Formaten bereitgestellt.  
Dieses Praxisprojekt verfolgt das Ziel, fundamentale Unternehmensdaten automatisiert zu verarbeiten, zentral zu speichern und anhand eines strukturierten Bewertungsmodells – basierend auf der Investmentstrategie von Peter Lynch – visuell analysierbar zu machen.

### 1.2 Projektziel  
Ziel ist der Aufbau einer durchgängigen Datenpipeline, die:

1. Kennzahlen zu allen S&P-500-Aktien automatisiert über APIs abruft  
2. Sie in Elasticsearch strukturiert speichert  
3. Sie anhand der 6 Lynch-Kategorien klassifiziert  
4. Über ein Streamlit-Dashboard filter-, vergleich- und visualisierbar macht  
5. Grundlage für tägliche Updates, Alerts, Backtesting und ML-Erweiterungen bildet  

### 1.3 Abgrenzung  
- Keine Echtzeit-Kursdaten, nur Fundamentaldaten  
- Keine Handels- oder Orderfunktion (kein Trading-Bot)  
- Fokus: **Technische Analyseumgebung**, nicht Finanzberatung  

---

## 2. Bezug zum WI-Projekt

Dieses Praxisprojekt baut auf dem vorherigen WI-Projekt auf, in dem die **theoretische Grundlage** zu Peter Lynch detailliert dokumentiert wurde (inkl. Kennzahlen, Zielwerte, Quellen und API-Endpunkte).

| WI-Projekt (Theorie) | Praxisprojekt (Technik) |
|----------------------|--------------------------|
| Lynch-Kategorien erklärt | Kategorien werden automatisch berechnet |
| Tabellen mit Kennzahlen | API-Daten werden geladen & gespeichert |
| Zielwerte definiert | Bewertung erfolgt algorithmisch |
| Dokumentation in Textform | Analyse & UI über Dashboard |

➡️ Vollständige Theorie befindet sich in der WI-Projektdokumentation.  
➡️ Diese README enthält **nur eine kompakte Zusammenfassung**.

---

## 3. Theoretische Grundlage – Kurzüberblick

| Kategorie | Typische Unternehmen | Kerneigenschaft |
|-----------|---------------------|-----------------|
| Slow Growers | Versorger, alte Industrien | Niedriges Wachstum, stabile Dividenden |
| Stalwarts | Markenriesen (z. B. Coca-Cola) | 5–10 % Gewinnwachstum |
| Fast Growers | Wachstumsunternehmen | > 20 % Gewinnwachstum |
| Cyclicals | Auto, Stahl, Airlines | Schwanken mit Konjunktur |
| Turnarounds | Sanierungskandidaten | Rückkehr zur Profitabilität |
| Asset Plays | Versteckte Werte | Buchwert > Marktwert |

Nur die technische Umsetzung erfolgt hier.  
Die vollständige Theorie → siehe WI-Projekt.

---

## 4. Systemarchitektur

### 4.1 Architekturübersicht

| Komponente | Aufgabe |
|------------|---------|
| **Python** | Datenabruf, Transformation, ETL-Pipeline |
| **Elasticsearch** | Speicherung & indexbasiertes Querying |
| **Docker Compose** | Infrastruktur-Orchestrierung |
| **Streamlit** | Web-Frontend für Analyse |
| **APIs** | FMP, yfinance, Alpha Vantage |

### 4.2 Architekturdiagramm

            ┌────────────────────────────┐
            │  Datenquellen (APIs)       │
            │  FMP / yfinance / AV       │
            └────────────┬───────────────┘
                         │ JSON-Response
                         ▼
                ┌──────────────┐
                │  Python ETL  │
                │ (load_data)  │
                └──────┬───────┘
                       │ transformed data
                       ▼
            ┌────────────────────────┐
            │   Elasticsearch Index  │
            │   (stocks, metrics)    │
            └─────────┬──────────────┘
                      │ query
                      ▼
            ┌────────────────────────┐
            │   Streamlit Dashboard  │
            │   Charts, Filter, UI   │
            └────────────────────────┘

---

## 5. Datenquellen und API-Abruf

### 5.1 APIs im Einsatz

| API | Zweck | Beispiel-Endpunkt |
|-----|-------|-------------------|
| FMP | Fundamentaldaten | `/key-metrics-ttm` |
| yfinance | Bilanz- & Marktdaten | `ticker.info` |
| Alpha Vantage | EPS-Historie | `EARNINGS` |

### 5.2 Beispiel-Response (FMP)

    ```json
    {
    "symbol": "AAPL",
    "peRatioTTM": 28.31,
    "revenuePerShareTTM": 24.52,
    "dividendYieldTTM": 0.0059
    }
    {
    "symbol": "AAPL",
    "category": "Stalwart",
    "metrics": {
        "pe": 28.31,
        "epsGrowth": 0.12,
        "dividendYield": 0.0059
    },
    "meta": {
        "last_update": "2025-01-03",
        "source": "FMP"
    }
    }
### 6.2 Felddefinitionen

| Feld | Typ | Bedeutung |
|-------|------|-----------|
| `symbol` | keyword | Tickersymbol |
| `category` | keyword | Lynch-Kategorie |
| `metrics.pe` | float | KGV (Price/Earnings) |
| `metrics.epsGrowth` | float | Gewinnwachstum p.a. |
| `metrics.dividendYield` | float | Dividendenrendite |
| `meta.last_update` | date | Zeitpunkt des API-Abrufs |

7 Implementierung (Code-Auszüge)


    ```json
    7.1 Datenabruf
    import requests

    def get_key_metrics(symbol, api_key):
        url = f"https://financialmodelingprep.com/api/v3/key-metrics-ttm/{symbol}?apikey={api_key}"
        return requests.get(url).json()[0]
    from elasticsearch import Elasticsearch

    es = Elasticsearch("http://localhost:9200")

    def store_stock_doc(doc):
        es.index(index="stocks", id=doc["symbol"], document=doc)

    from elasticsearch import Elasticsearch

    es = Elasticsearch("http://localhost:9200")

    def store_stock_doc(doc):
        es.index(index="stocks", id=doc["symbol"], document=doc)

    7.3 Einfache Lynch-Klassifikation
    def classify(eps_growth):
        if eps_growth > 0.2:
            return "Fast Grower"
        elif 0.05 < eps_growth <= 0.1:
            return "Stalwart"
        return "Slow Grower"

## 8. Streamlit-Dashboard

### 8.1 Funktionen
- **Filter & Suche:** nach Lynch-Kategorie, Ticker, Zeitraum
- **Kennzahlen-Vergleich:** interaktive Charts (z. B. KGV, EPS-Growth, FCF)
- **Detailansicht pro Aktie:** Rohdaten, Kennzahlen-Panel, Kategorie-Begründung
- **Datenaktualisierung:** Live-Abruf über APIs möglich
- **Export:** CSV/Excel (geplant)

### 8.2 Screens (Platzhalter)
> Ersetze die folgenden Platzhalter durch deine echten Bilder (z. B. `docs/img/...`).

![Dashboard – Übersicht](docs/img/dashboard_overview.png "Dashboard – Übersicht")
![Vergleichsansicht – Kennzahlen](docs/img/dashboard_compare.png "Vergleichsansicht – Kennzahlen")

---

## 9. Setup & Deployment

### 9.1 Voraussetzungen
- Docker & Docker Compose  
- Python **3.10+**  
- API-Keys in einer `.env`-Datei

    Beispiel `.env`:
    ```env
    FMP_API_KEY=dein_fmp_key
    ALPHA_VANTAGE_KEY=dein_alpha_vantage_key
    ELASTICSEARCH_URL=http://localhost:9200

    9.2 Startreihenfolge
    Container starten
    docker compose up -d
    Daten laden (S&P 500)
    python load_sp500.py
    Dashboard starten
    streamlit run dashboard.py
    Tipp: Prüfe http://localhost:8501 (Streamlit) und http://localhost:9200 (Elasticsearch).

---

## 10. Geplante Erweiterungen

| Feature                         | Status   |
|---------------------------------|----------|
| Automatisches Daily-Update      | geplant  |
| Alert-System (E-Mail/Telegram)  | geplant  |
| Backtesting-Modul               | geplant  |
| Machine-Learning-EPS-Forecasts  | offen    |

---

## 11. Fazit

Dieses Projekt zeigt, wie ein klassisches Investmentmodell durch moderne ETL-Architektur, eine Such-/Analyse-Engine und ein UI-Framework in eine skalierbare, reproduzierbare und nachvollziehbare Datenanwendung transformiert werden kann.

Die Lösung dient als robuste Grundlage für weiterführende Funktionen wie:

- Backtesting historischer Strategien  
- Benachrichtigungssysteme (Alerts)  
- ML-gestützte Prognosen (EPS-, KGV-Modelle)  
- Automatische Rebalancing- oder Watchlist-Logik  

---

## 12. Quellen

| Quelle | Inhalt |
|--------|--------|
| Lynch, Peter – *One Up on Wall Street* (1989) | Originalquelle der 6 Kategorien |
| Investopedia | Begriffe wie KGV, PEG, FCF, Equity Ratio |
| Financial Modeling Prep API | Fundamentaldaten (`/key-metrics-ttm`) |
| yfinance | Bilanz- & Marktdaten-Wrapper |
| Alpha Vantage API | EPS- und Earnings-Daten |

---


